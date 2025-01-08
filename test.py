import os
import time
import json
import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
import mysql.connector
from mysql.connector import Error as MySQLError
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables
load_dotenv()

@dataclass
class PropertyListing:
    """Data class to store property information"""
    listing_id: str
    title: str
    property_type: str
    listing_type: str
    price: float
    currency: str
    rooms: int
    area: float
    floor: int
    total_floors: int
    location: str
    district: str
    metro_station: str
    description: str
    contact_type: str
    contact_phone: str
    address: str
    coordinates: tuple
    photos: List[str]
    listed_date: datetime
    source_url: str
    
    def validate(self) -> bool:
        """Validate required fields are present and in correct format"""
        try:
            required_fields = ['listing_id', 'title', 'price']
            return all(getattr(self, field) is not None for field in required_fields)
        except AttributeError:
            return False

class DatabaseManager:
    """Handle database connections and operations"""
    
    def __init__(self, config: Dict[str, str], max_retries: int = 3):
        self.config = config
        self.max_retries = max_retries
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_connection(self) -> mysql.connector.MySQLConnection:
        """Get database connection with retry logic"""
        for attempt in range(self.max_retries):
            try:
                return mysql.connector.connect(**self.config)
            except MySQLError as e:
                if attempt == self.max_retries - 1:
                    raise
                self.logger.warning(f"Database connection attempt {attempt + 1} failed: {str(e)}")
                time.sleep(2 ** attempt)  # Exponential backoff

class BaseScraper(ABC):
    """Base scraper class with common functionality"""
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME')
        }
        self.session = self._setup_session()
        self.setup_logging()
        self.db_manager = DatabaseManager(self.db_config)
        self.last_request_time = 0
        self.min_request_interval = float(os.getenv('REQUEST_DELAY', '1'))

    def _setup_session(self) -> requests.Session:
        """Setup requests session with retries and headers"""
        session = requests.Session()
        
        retries = Retry(
            total=int(os.getenv('MAX_RETRIES', '5')),
            backoff_factor=0.1,
            status_forcelist=[500, 502, 503, 504]
        )
        
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        session.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        
        return session

    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        logging.basicConfig(
            filename=log_dir / 'scraper.log',
            level=getattr(logging, os.getenv('LOGGING_LEVEL', 'INFO')),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def _make_request(self, url: str, params: Dict = None) -> requests.Response:
        """Make a rate-limited request with improved error handling"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last_request)
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            self.last_request_time = time.time()
            return response
        except requests.RequestException as e:
            self.logger.error(f"Request failed for URL {url}: {str(e)}")
            raise

    @abstractmethod
    def get_listings_page(self, page: int) -> List[Dict[str, Any]]:
        """Get all listings from a page"""
        pass

    @abstractmethod
    def get_listing_details(self, url: str) -> PropertyListing:
        """Get details of a specific listing"""
        pass

    def save_to_db(self, listing: PropertyListing):
        """Save listing to database with improved error handling"""
        if not listing.validate():
            self.logger.error(f"Invalid listing data for ID {listing.listing_id}")
            return

        conn = None
        cursor = None
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            query = """
                INSERT INTO properties (
                    listing_id, title, property_type, listing_type, 
                    price, currency, rooms, area, floor, total_floors,
                    location, district, metro_station, description,
                    contact_type, contact_phone, address, latitude,
                    longitude, photos, listing_date, source_url
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s
                )
                ON DUPLICATE KEY UPDATE
                price = VALUES(price),
                updated_at = CURRENT_TIMESTAMP
            """
            
            photos_json = json.dumps(listing.photos)
            
            cursor.execute(query, (
                listing.listing_id,
                listing.title,
                listing.property_type,
                listing.listing_type,
                listing.price,
                listing.currency,
                listing.rooms,
                listing.area,
                listing.floor,
                listing.total_floors,
                listing.location,
                listing.district,
                listing.metro_station,
                listing.description,
                listing.contact_type,
                listing.contact_phone,
                listing.address,
                listing.coordinates[0],
                listing.coordinates[1],
                photos_json,
                listing.listed_date,
                listing.source_url
            ))
            
            conn.commit()
            self.logger.info(f"Saved listing {listing.listing_id} to database")
            
        except MySQLError as e:
            self.logger.error(f"Database error for listing {listing.listing_id}: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if conn and conn.is_connected():
                conn.close()

class ArendaScraper(BaseScraper):
    """Scraper for Arenda.az"""
    
    BASE_URL = "https://arenda.az"
    LISTINGS_URL = "https://arenda.az/filtirli-axtaris/"

    def __init__(self):
        super().__init__()

    def get_listings_page(self, page: int) -> List[Dict[str, Any]]:
        """Get all listings from a page with improved error handling"""
        params = {
            'home_search': '1',
            'lang': '1',
            'site': '1',
            'home_s': '1',
            'page': page
        }
        
        try:
            response = self._make_request(self.LISTINGS_URL, params=params)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            listings = []
            for item in soup.select('li.new_elan_box'):
                try:
                    # Safely extract elements with proper error handling
                    id_elem = item.get('id', '')
                    url_elem = item.select_one('a')
                    title_elem = item.select_one('.elan_property_title')
                    price_elem = item.select_one('.elan_price')
                    
                    if not all([id_elem, url_elem, title_elem, price_elem]):
                        self.logger.warning(f"Missing required elements in listing item")
                        continue

                    listing = {
                        'id': id_elem.replace('elan_', ''),
                        'url': url_elem['href'],
                        'title': title_elem.text.strip(),
                        'price': self._extract_price(price_elem.text),
                        'rooms': self._extract_rooms(item),
                        'area': self._extract_area(item),
                        'floor_info': self._extract_floor_info(item)
                    }
                    
                    # Only add listing if it has minimum required data
                    if listing['id'] and listing['url'] and listing['title']:
                        listings.append(listing)
                    else:
                        self.logger.warning(f"Skipping listing due to missing required data")
                        
                except Exception as e:
                    self.logger.error(f"Error processing listing item: {str(e)}")
                    continue
            
            return listings
            
        except Exception as e:
            self.logger.error(f"Error getting listings page {page}: {str(e)}")
            raise

    def get_listing_details(self, url: str) -> PropertyListing:
        """Get details of a specific listing with improved error handling"""
        try:
            response = self._make_request(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract coordinates with validation
            lat = soup.find('input', {'id': 'lat'})
            lon = soup.find('input', {'id': 'lon'})
            
            lat_val = lat['value'] if lat else None
            lon_val = lon['value'] if lon else None
            
            try:
                coordinates = (float(lat_val), float(lon_val)) if lat_val and lon_val else (None, None)
            except (ValueError, TypeError):
                coordinates = (None, None)
                self.logger.warning(f"Invalid coordinates for listing URL {url}")
            
            listing = PropertyListing(
                listing_id=self._extract_listing_id(soup),
                title=self._extract_title(soup),
                property_type=self._extract_property_type(soup),
                listing_type=self._extract_listing_type(soup),
                price=self._extract_detailed_price(soup),
                currency=self._extract_currency(soup),
                rooms=self._extract_detailed_rooms(soup),
                area=self._extract_detailed_area(soup),
                floor=self._extract_detailed_floor(soup)[0],
                total_floors=self._extract_detailed_floor(soup)[1],
                location=self._extract_location(soup),
                district=self._extract_district(soup),
                metro_station=self._extract_metro(soup),
                description=self._extract_description(soup),
                contact_type=self._extract_contact_type(soup),
                contact_phone=self._extract_phone(soup),
                address=self._extract_address(soup),
                coordinates=coordinates,
                photos=self._extract_photos(soup),
                listed_date=self._extract_date(soup),
                source_url=url
            )
            
            if not listing.validate():
                self.logger.warning(f"Listing validation failed for URL {url}")
                
            return listing
            
        except Exception as e:
            self.logger.error(f"Error getting listing details for {url}: {str(e)}")
            raise

    def _extract_price(self, price_text: str) -> Optional[float]:
        """Extract price from text with improved cleaning"""
        if not price_text:
            return None
            
        try:
            price_clean = ''.join(c for c in price_text if c.isdigit())
            return float(price_clean) if price_clean else None
        except Exception as e:
            self.logger.error(f"Error extracting price: {str(e)}")
            return None

    def _extract_rooms(self, item) -> Optional[int]:
        """Extract number of rooms with improved handling"""
        if not item:
            return None
            
        try:
            # Look specifically for room information
            rooms_cell = item.select_one('.n_elan_box_otaq')  # Adjust selector based on HTML structure
            if not rooms_cell or not rooms_cell.text:
                return None
                
            # Extract only digits from the text
            room_text = rooms_cell.text.strip()
            if 'otaq' not in room_text.lower():  # Verify this is actually room information
                return None
                
            number = ''.join(c for c in room_text if c.isdigit())
            return int(number) if number and not number.startswith('0') else None
            
        except Exception as e:
            self.logger.error(f"Error extracting rooms: {str(e)} from element: {rooms_cell.text if rooms_cell else 'None'}")
            return None

    def _extract_area(self, item) -> Optional[float]:
        """Extract area in square meters with improved handling"""
        if not item:
            return None
            
        try:
            cells = item.select('td')
            if len(cells) < 2:
                return None
                
            area_text = cells[1].text.strip()
            # Remove square meter symbol and other non-numeric characters except decimal point
            area_clean = ''.join(c for c in area_text if c.isdigit() or c == '.')
            
            if not area_clean:
                return None
                
            area_value = float(area_clean)
            # Convert from sotka (hundred square meters) if specified
            return area_value * 100 if 'sot' in area_text.lower() else area_value
            
        except Exception as e:
            self.logger.error(f"Error extracting area: {str(e)} from text: {area_text}")
            return None

    def _extract_floor_info(self, item) -> Tuple[Optional[int], Optional[int]]:
        """Extract floor and total floors information with improved handling"""
        if not item:
            return (None, None)
            
        try:
            cells = item.select('td')
            if len(cells) < 3 or not cells[2].text:
                return (None, None)
                
            floor_text = cells[2].text.strip()
            if '/' not in floor_text:
                return (None, None)
            
            floor_parts = floor_text.split('/', 1)
            current = ''.join(c for c in floor_parts[0] if c.isdigit())
            total = ''.join(c for c in floor_parts[1] if c.isdigit())
            
            if current and total:
                return (int(current), int(total))
            
        except Exception as e:
            self.logger.error(f"Error extracting floor info: {str(e)}")
        return (None, None)

    def _extract_detailed_area(self, soup) -> Optional[float]:
        """Extract area from detail page with improved handling"""
        if not soup:
            return None
            
        try:
            area_element = soup.select_one('.elan_property_list li:nth-child(2) a')
            if not area_element or not area_element.text:
                return None
                
            area_text = area_element.text.strip()
            # Remove square meter symbol and other non-numeric characters except decimal point
            area_clean = ''.join(c for c in area_text if c.isdigit() or c == '.')
            
            if not area_clean:
                return None
                
            area_value = float(area_clean)
            # Convert from sotka if specified
            return area_value * 100 if 'sot' in area_text.lower() else area_value
            
        except Exception as e:
            self.logger.error(f"Error extracting detailed area: {str(e)} from text: {area_text}")
            return None

    def _extract_detailed_floor(self, soup) -> Tuple[Optional[int], Optional[int]]:
        """Extract floor information from detail page with improved handling"""
        if not soup:
            return (None, None)
            
        try:
            floor_element = soup.select_one('.elan_property_list li:nth-child(3) a')
            if not floor_element or not floor_element.text:
                return (None, None)
                
            floor_text = floor_element.text.strip()
            if '/' not in floor_text:
                return (None, None)
                
            floor_parts = floor_text.split('/')
            if len(floor_parts) != 2:
                return (None, None)
                
            current = ''.join(c for c in floor_parts[0] if c.isdigit())
            total = ''.join(c for c in floor_parts[1] if c.isdigit())
            
            if current and total:
                return (int(current), int(total))
                
        except Exception as e:
            self.logger.error(f"Error extracting detailed floor: {str(e)}")
        return (None, None)

    def _extract_listing_id(self, soup) -> Optional[str]:
        """Extract listing ID from the page with improved handling"""
        if not soup:
            return None
            
        try:
            code_element = soup.select_one('.elan_date_box_rside')
            if not code_element:
                return None
                
            code_text = code_element.find(string=lambda text: text and 'Elanın kodu:' in text)
            if not code_text:
                return None
                
            listing_id = code_text.strip().split(': ')[1]
            return listing_id if listing_id else None
            
        except Exception as e:
            self.logger.error(f"Error extracting listing ID: {str(e)}")
        return None

    def _extract_title(self, soup) -> Optional[str]:
        """Extract title from the page with improved handling"""
        if not soup:
            return None
            
        try:
            title_element = soup.select_one('.elan_main_title')
            if title_element and title_element.text:
                title = title_element.text.strip()
                return title if title else None
        except Exception as e:
            self.logger.error(f"Error extracting title: {str(e)}")
        return None

    def _extract_property_type(self, soup) -> Optional[str]:
        """Extract property type from title with improved handling"""
        if not soup:
            return None
            
        try:
            title = self._extract_title(soup)
            if title:
                words = title.split()
                if words:
                    return words[0]
        except Exception as e:
            self.logger.error(f"Error extracting property type: {str(e)}")
        return None

    def _extract_listing_type(self, soup) -> Optional[str]:
        """Extract listing type with improved handling"""
        if not soup:
            return None
            
        try:
            type_element = soup.select_one('.elan_property_title1')
            if not type_element or not type_element.text:
                return None
                
            type_text = type_element.text.lower()
            if 'günlük' in type_text:
                return 'daily'
            elif 'aylıq' in type_text:
                return 'monthly'
            elif 'satılır' in type_text:
                return 'sale'
        except Exception as e:
            self.logger.error(f"Error extracting listing type: {str(e)}")
        return None

    def _extract_detailed_price(self, soup) -> Optional[float]:
        """Extract price from detail page with improved handling"""
        if not soup:
            return None
            
        try:
            price_element = soup.select_one('.elan_new_price_box')
            if not price_element or not price_element.text:
                return None
                
            price_text = price_element.text.strip()
            price_clean = ''.join(c for c in price_text if c.isdigit())
            return float(price_clean) if price_clean else None
        except Exception as e:
            self.logger.error(f"Error extracting detailed price: {str(e)}")
        return None

    def _extract_currency(self, soup) -> str:
        """Extract currency"""
        return 'AZN'  # Default currency for arenda.az

    def _extract_detailed_rooms(self, soup) -> Optional[int]:
        """Extract number of rooms from detail page with improved handling"""
        if not soup:
            return None
            
        try:
            rooms_element = soup.select_one('.elan_property_list li:first-child a')
            if not rooms_element or not rooms_element.text:
                return None
                
            rooms_text = rooms_element.text.strip().split()[0]
            return int(rooms_text) if rooms_text.isdigit() else None
        except Exception as e:
            self.logger.error(f"Error extracting detailed rooms: {str(e)}")
        return None

    def _extract_location(self, soup) -> Optional[str]:
        """Extract location information with improved handling"""
        if not soup:
            return None
            
        try:
            location_element = soup.select_one('.elan_unvan_txt')
            if location_element and location_element.text:
                location = location_element.text.strip()
                return location if location else None
        except Exception as e:
            self.logger.error(f"Error extracting location: {str(e)}")
        return None

    def _extract_district(self, soup) -> Optional[str]:
        """Extract district information with improved handling"""
        if not soup:
            return None
            
        try:
            district_element = soup.select_one('.elan_property_title.elan_unvan')
            if district_element and district_element.text:
                text = district_element.text.strip()
                parts = text.split(',')
                if len(parts) > 1:
                    district = parts[-1].strip()
                    return district if district else None
        except Exception as e:
            self.logger.error(f"Error extracting district: {str(e)}")
        return None

    def _extract_metro(self, soup) -> Optional[str]:
        """Extract nearest metro station with improved handling"""
        if not soup:
            return None
            
        try:
            location_text = soup.select_one('.elan_property_title.elan_unvan')
            if location_text and location_text.text and 'm.' in location_text.text:
                parts = location_text.text.split(',')
                for part in parts:
                    if 'm.' in part:
                        metro = part.strip()
                        return metro if metro else None
        except Exception as e:
            self.logger.error(f"Error extracting metro station: {str(e)}")
        return None

    def _extract_description(self, soup) -> Optional[str]:
        """Extract property description with improved handling"""
        if not soup:
            return None
            
        try:
            desc_element = soup.select_one('.elan_info_txt')
            if desc_element and desc_element.text:
                description = desc_element.text.strip()
                return description if description else None
        except Exception as e:
            self.logger.error(f"Error extracting description: {str(e)}")
        return None

    def _extract_contact_type(self, soup) -> Optional[str]:
        """Extract contact type with improved handling"""
        if not soup:
            return None
            
        try:
            contact_element = soup.select_one('.new_elan_user_info p')
            if contact_element and contact_element.text:
                contact_type = contact_element.text.strip()
                return contact_type if contact_type else None
        except Exception as e:
            self.logger.error(f"Error extracting contact type: {str(e)}")
        return None

    def _extract_phone(self, soup) -> Optional[str]:
        """Extract contact phone number with improved handling"""
        if not soup:
            return None
            
        try:
            phone_element = soup.select_one('.elan_in_tel')
            if phone_element and phone_element.text:
                phone = phone_element.text.replace('(', '').replace(')', '').replace('-', '').strip()
                return phone if phone else None
        except Exception as e:
            self.logger.error(f"Error extracting phone: {str(e)}")
        return None

    def _extract_address(self, soup) -> Optional[str]:
        """Extract full address with improved handling"""
        if not soup:
            return None
            
        try:
            address_element = soup.select_one('.elan_unvan_txt')
            if address_element and address_element.text:
                address = address_element.text.strip()
                return address if address else None
        except Exception as e:
            self.logger.error(f"Error extracting address: {str(e)}")
        return None

    def _extract_photos(self, soup) -> List[str]:
        """Extract photo URLs with improved handling"""
        if not soup:
            return []
            
        photos = []
        try:
            photo_elements = soup.select('.full.elan_img_box img')
            for img in photo_elements:
                photo_url = None
                if img.get('data-src'):
                    photo_url = img['data-src']
                elif img.get('src') and not img['src'].endswith('load.gif'):
                    photo_url = img['src']
                    
                if photo_url and photo_url.strip():
                    photos.append(photo_url.strip())
        except Exception as e:
            self.logger.error(f"Error extracting photos: {str(e)}")
        return photos

    def _extract_date(self, soup) -> Optional[datetime]:
        """Extract listing date with improved handling"""
        if not soup:
            return None
            
        try:
            date_element = soup.select_one('.elan_date_box_rside')
            if not date_element:
                return None
                
            date_text = date_element.find(string=lambda text: text and 'Elanın tarixi:' in text)
            if not date_text:
                return None
                
            date_str = date_text.strip().split(': ')[1]
            try:
                return datetime.strptime(date_str, '%d.%m.%Y')
            except ValueError:
                self.logger.error(f"Invalid date format: {date_str}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error extracting date: {str(e)}")
        return None

def main():
    """Main function with improved error handling and logging"""
    scraper = None
    try:
        scraper = ArendaScraper()
        
        for page in range(1, 4):
            try:
                scraper.logger.info(f"Starting to process page {page}")
                listings = scraper.get_listings_page(page)
                scraper.logger.info(f"Found {len(listings)} listings on page {page}")
                
                for listing in listings:
                    try:
                        scraper.logger.info(f"Processing listing {listing['id']}")
                        details = scraper.get_listing_details(listing['url'])
                        
                        if details.validate():
                            scraper.save_to_db(details)
                            scraper.logger.info(f"Successfully processed listing {listing['id']}")
                        else:
                            scraper.logger.warning(f"Skipping invalid listing {listing['id']}")
                            
                        time.sleep(float(os.getenv('REQUEST_DELAY', '1')))
                        
                    except Exception as e:
                        scraper.logger.error(f"Error processing listing {listing['id']}: {str(e)}")
                        continue
                        
            except Exception as e:
                scraper.logger.error(f"Error processing page {page}: {str(e)}")
                continue

    except Exception as e:
        if scraper:
            scraper.logger.error(f"Fatal error in main: {str(e)}")
        else:
            logging.error(f"Fatal error in main before scraper initialization: {str(e)}")
        raise

    finally:
        if scraper:
            scraper.logger.info("Scraping process completed")

if __name__ == "__main__":
    main()        
    
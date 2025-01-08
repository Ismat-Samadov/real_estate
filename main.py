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
        """Make a rate-limited request"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last_request)
        
        response = self.session.get(url, params=params, timeout=30)
        self.last_request_time = time.time()
        
        return response

    @abstractmethod
    def get_listings_page(self, page: int) -> List[Dict[str, Any]]:
        """Get all listings from a page"""
        pass

    @abstractmethod
    def get_listing_details(self, url: str) -> PropertyListing:
        """Get details of a specific listing"""
        pass

    def save_to_db(self, listing: PropertyListing):
        """Save listing to database"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            query = """
                INSERT INTO properties (
                    listing_id, title, property_type, listing_type, 
                    price, currency, rooms, area, floor, total_floors,
                    location, district, metro_station, description,
                    contact_type, contact_phone, address, latitude,
                    longitude, photos, listed_date, source_url
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
            
        except Exception as e:
            self.logger.error(f"Error saving to database: {str(e)}")
            raise
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

class ArendaScraper(BaseScraper):
    """Scraper for Arenda.az"""
    
    BASE_URL = "https://arenda.az"
    LISTINGS_URL = "https://arenda.az/filtirli-axtaris/"

    def __init__(self):
        super().__init__()

    def get_listings_page(self, page: int) -> List[Dict[str, Any]]:
        """Get all listings from a page"""
        params = {
            'home_search': '1',
            'lang': '1',
            'site': '1',
            'home_s': '1',
            'page': page
        }
        
        try:
            response = self._make_request(self.LISTINGS_URL, params=params)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            listings = []
            for item in soup.select('li.new_elan_box'):
                try:
                    listing = {
                        'id': item.get('id', '').replace('elan_', ''),
                        'url': item.select_one('a')['href'],
                        'title': item.select_one('.elan_property_title').text.strip(),
                        'price': self._extract_price(item.select_one('.elan_price').text),
                        'rooms': self._extract_rooms(item),
                        'area': self._extract_area(item),
                        'floor_info': self._extract_floor_info(item)
                    }
                    listings.append(listing)
                except Exception as e:
                    self.logger.error(f"Error processing listing item: {str(e)}")
                    continue
            
            return listings
            
        except Exception as e:
            self.logger.error(f"Error getting listings page {page}: {str(e)}")
            raise

    def get_listing_details(self, url: str) -> PropertyListing:
        """Get details of a specific listing"""
        try:
            response = self._make_request(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            lat = soup.find('input', {'id': 'lat'})['value'] if soup.find('input', {'id': 'lat'}) else None
            lon = soup.find('input', {'id': 'lon'})['value'] if soup.find('input', {'id': 'lon'}) else None
            coordinates = (float(lat), float(lon)) if lat and lon else (None, None)
            
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
            
            return listing
            
        except Exception as e:
            self.logger.error(f"Error getting listing details for {url}: {str(e)}")
            raise

    def _extract_price(self, price_text: str) -> Optional[float]:
        """Extract price from text"""
        try:
            return float(price_text.replace('AZN', '').replace(' ', '').strip())
        except Exception as e:
            self.logger.error(f"Error extracting price: {str(e)}")
            return None

    def _extract_rooms(self, item) -> Optional[int]:
        """Extract number of rooms"""
        try:
            rooms_text = item.select_one('td').text
            return int(rooms_text.split()[0])
        except Exception as e:
            self.logger.error(f"Error extracting rooms: {str(e)}")
            return None

    def _extract_area(self, item) -> Optional[float]:
        """Extract area in square meters"""
        try:
            area_text = item.select('td')[1].text
            return float(area_text.replace('m²', '').strip())
        except Exception as e:
            self.logger.error(f"Error extracting area: {str(e)}")
            return None

    def _extract_floor_info(self, item) -> Tuple[Optional[int], Optional[int]]:
        """Extract floor and total floors information"""
        try:
            floor_text = item.select('td')[2].text
            floor_parts = floor_text.split('/')
            if len(floor_parts) == 2:
                current_floor = int(floor_parts[0].strip())
                total_floors = int(floor_parts[1].split()[0].strip())
                return (current_floor, total_floors)
        except Exception as e:
            self.logger.error(f"Error extracting floor info: {str(e)}")
        return (None, None)

    def _extract_listing_id(self, soup) -> Optional[str]:
        """Extract listing ID from the page"""
        try:
            code_element = soup.select_one('.elan_date_box_rside')
            if code_element:
                code_text = code_element.find(string=lambda text: 'Elanın kodu:' in text if text else False)
                if code_text:
                    return code_text.strip().split(': ')[1]
        except Exception as e:
            self.logger.error(f"Error extracting listing ID: {str(e)}")
        return None

    def _extract_title(self, soup) -> Optional[str]:
        """Extract title from the page"""
        try:
            title_element = soup.select_one('.elan_main_title')
            return title_element.text.strip() if title_element else None
        except Exception as e:
            self.logger.error(f"Error extracting title: {str(e)}")
        return None

    def _extract_property_type(self, soup) -> Optional[str]:
        """Extract property type from title"""
        try:
            title = self._extract_title(soup)
            if title:
                return title.split()[0]
        except Exception as e:
            self.logger.error(f"Error extracting property type: {str(e)}")
        return None

    def _extract_listing_type(self, soup) -> Optional[str]:
        """Extract listing type (daily/monthly/sale)"""
        try:
            type_text = soup.select_one('.elan_property_title1').text.lower()
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
        """Extract price from the detail page"""
        try:
            price_element = soup.select_one('.elan_new_price_box')
            if price_element:
                price_text = price_element.text.strip()
                return float(price_text.replace('M', '').strip())
        except Exception as e:
            self.logger.error(f"Error extracting detailed price: {str(e)}")
        return None

    def _extract_currency(self, soup) -> str:
        """Extract currency"""
        return 'AZN'  # Default currency for arenda.az

    def _extract_detailed_rooms(self, soup) -> Optional[int]:
        """Extract number of rooms from detail page"""
        try:
            rooms_element = soup.select_one('.elan_property_list li:first-child a')
            if rooms_element:
                rooms_text = rooms_element.text.strip()
                return int(rooms_text.split()[0])
        except Exception as e:
            self.logger.error(f"Error extracting detailed rooms: {str(e)}")
        return None

    def _extract_detailed_area(self, soup) -> Optional[float]:
        """Extract area from detail page"""
        try:
            area_element = soup.select_one('.elan_property_list li:nth-child(2) a')
            if
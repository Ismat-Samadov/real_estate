import asyncio
import aiohttp
import random
import os
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Optional, Tuple
import datetime
import re
import json
import time

class IpotekaScraper:
    """Scraper for ipoteka.az"""
    
    BASE_URL = "https://ipoteka.az"
    SEARCH_URL = "https://ipoteka.az/search"
    
    MAX_TITLE_LENGTH = 200
    
    @staticmethod
    def safe_truncate(text: Optional[str], max_length: int) -> Optional[str]:
        """Truncate text to fit max_length safely"""
        if text is None:
            return None
        text = text.strip()
        if len(text) > max_length:
            return text[:max_length]
        return text

    def __init__(self):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None

    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                              '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,'
                          'image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,az;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0'
            }
            
            self.session = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(ssl=False)
            )

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None


    async def get_page_content(self, url: str, params: Optional[Dict] = None) -> str:
        """Fetch page content with retry logic and anti-bot measures"""
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))
        # Convert to float in case you want fractional delays
        DELAY = float(os.getenv('REQUEST_DELAY', '1'))
        
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(DELAY + random.random() * 2)
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 403:
                        self.logger.warning(f"Access forbidden (403) on attempt {attempt + 1}")
                        await asyncio.sleep(DELAY * (attempt + 2))
                    else:
                        self.logger.warning(f"Failed to fetch {url}, status: {response.status}")
                        
            except Exception as e:
                self.logger.error(f"Error fetching {url}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                
            await asyncio.sleep(DELAY * (attempt + 1))
        
        raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} attempts")

    def extract_number(self, text: str) -> Optional[float]:
        """Extract numeric value from text"""
        if not text:
            return None
        try:
            return float(re.sub(r'[^\d.]', '', text))
        except:
            return None

    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('.col-xs-6.col-md-3'):
            try:
                # Get listing anchor element
                anchor = listing.select_one('a.item')
                if not anchor:
                    continue
                
                # Extract URL and ID
                listing_url = anchor.get('href')
                if not listing_url:
                    continue
                
                listing_id_match = re.search(r'/(\d+)-', listing_url)
                if not listing_id_match:
                    continue
                
                listing_id = listing_id_match.group(1)
                listing_url = self.BASE_URL + listing_url
                
                # Initialize basic data
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'ipoteka.az',
                    'created_at': datetime.datetime.now(),
                    'updated_at': datetime.datetime.now()
                }
                
                # Extract price and check for document
                price_elem = anchor.select_one('span.img span.price')
                if price_elem:
                    price_text = price_elem.text.strip()
                    try:
                        price = float(re.sub(r'[^\d.]', '', price_text))
                        listing_data.update({
                            'price': price,
                            'currency': 'AZN'
                        })
                    except (ValueError, TypeError):
                        pass
                
                # Check if document exists
                doc_elem = anchor.select_one('span.reg[data-title="Sənəd var"]')
                listing_data['has_document'] = bool(doc_elem)
                
                # Extract area
                area_elem = anchor.select_one('span.desc:-soup-contains("Sahəsi:")')
                if area_elem:
                    area_match = re.search(r'Sahəsi:\s*([\d.]+)', area_elem.text)
                    if area_match:
                        try:
                            area = float(area_match.group(1))
                            listing_data['area'] = area
                        except (ValueError, TypeError):
                            pass
                
                # Extract rooms
                rooms_elem = anchor.select_one('span:-soup-contains("Otaq sayı:")')
                if rooms_elem:
                    rooms_match = re.search(r'Otaq sayı:\s*(\d+)', rooms_elem.text)
                    if rooms_match:
                        try:
                            rooms = int(rooms_match.group(1))
                            listing_data['rooms'] = rooms
                        except (ValueError, TypeError):
                            pass
                
                # Extract location and date
                date_elem = anchor.select_one('span[style*="float: right"]')
                if date_elem:
                    date_text = date_elem.text.strip()
                    # Extract city
                    city_match = re.match(r'([^,]+),\s*(.+)', date_text)
                    if city_match:
                        location = city_match.group(1).strip()
                        if location:
                            listing_data['location'] = location
                        
                        # Handle date
                        date_part = city_match.group(2).strip()
                        if 'Bu gün' in date_part:
                            listing_data['listing_date'] = datetime.datetime.now().date()
                        else:
                            try:
                                listing_data['listing_date'] = datetime.datetime.strptime(
                                    date_part, '%d.%m.%Y'
                                ).date()
                            except ValueError:
                                pass
                
                # Extract title/address
                title_elem = anchor.select_one('span.title')
                if title_elem:
                    raw_title = title_elem.text.strip()
                    if raw_title:
                        # Truncate the raw title to avoid DB error
                        truncated_title = self.safe_truncate(raw_title, self.MAX_TITLE_LENGTH)
                        listing_data['title'] = truncated_title
                        listing_data['address'] = truncated_title
                        
                        # Try to extract district
                        district_match = re.search(r'(\w+)\s*r\.', raw_title)
                        if district_match:
                            listing_data['district'] = district_match.group(1).title()
                            
                        # -- Extract any mention of metro station --
                        station_pattern = re.compile(
                            r'([A-Za-zƏIıİÖöĞğŞşÇçÜü]+(?:\s+[A-Za-zƏIıİÖöĞğŞşÇçÜü]+)*)'
                            r'\s*(?:m/s\.?|m\.|metrosu\.?|metrosunun|metro)',
                            re.IGNORECASE
                        )
                        station_match = station_pattern.search(raw_title)
                        if station_match:
                            listing_data['metro_station'] = station_match.group(1).strip()
                        
                        # Fallback approach for "(\w+) m."
                        if 'metro_station' not in listing_data:
                            metro_match = re.search(r'(\w+)\s*m\.', raw_title)
                            if metro_match:
                                listing_data['metro_station'] = metro_match.group(1).title()
                
                # All listings on ipoteka.az are for sale
                listing_data['listing_type'] = 'sale'
                
                # Extract property type from (possibly truncated) listing_data['title']
                if 'title' in listing_data:
                    title_lower = listing_data['title'].lower()
                    if 'yeni tikili' in title_lower:
                        listing_data['property_type'] = 'new'
                    elif 'köhnə tikili' in title_lower:
                        listing_data['property_type'] = 'old'
                    elif any(x in title_lower for x in ['həyət evi', 'villa']):
                        listing_data['property_type'] = 'house'
                    else:
                        listing_data['property_type'] = 'apartment'
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
                
        return listings

    def extract_metro_station(self, text: str) -> Optional[str]:
        """
        Extract metro station name from text using a predefined list of Baku metro stations.
        
        Args:
            text: Text to search for metro station names
            
        Returns:
            Metro station name if found, None otherwise
        """
        if not text:
            return None
            
        # Comprehensive list of Baku metro stations (both Azerbaijani and common names)
        metro_stations = [
            "20 Yanvar", "20 yanvar",
            "28 May", "28 may",
            "8 Noyabr", "8 noyabr",
            "Azadlıq prospekti", "azadlıq prospekti",
            "Avtovağzal", "avtovağzal",
            "Bakmil", "bakmil",
            "Cəfər Cabbarlı", "cəfər cabbarlı",
            "Dərnəgül", "dərnəgül",
            "Elmlər Akademiyası", "elmlər akademiyası",
            "Əhmədli", "əhmədli",
            "Gənclik", "gənclik",
            "Həzi Aslanov", "həzi aslanov",
            "Xalqlar dostluğu", "xalqlar dostluğu",
            "İçərişəhər", "içərişəhər",
            "İnşaatçılar", "inşaatçılar",
            "Koroğlu", "koroğlu",
            "Qara Qarayev", "qara qarayev",
            "Memar Əcəmi", "memar əcəmi",
            "Nəsimi", "nəsimi",
            "Nərimanov", "nərimanov",
            "Neftçilər", "neftçilər",
            "Nizami", "nizami",
            "Sahil", "sahil",
            "Xətai", "xətai",
            "Xocəsən", "xocəsən",
            "Ulduz", "ulduz"
        ]
        
        # Convert text to lowercase for case-insensitive matching
        text_lower = text.lower()
        
        # First try exact matches
        for station in metro_stations:
            station_lower = station.lower()
            
            # Look for the station name with various indicators
            patterns = [
                rf'\b{re.escape(station_lower)}\b',  # Exact match
                rf'\b{re.escape(station_lower)}\s+metro\b',  # Station metro
                rf'\b{re.escape(station_lower)}\s+m\.\b',  # Station m.
                rf'metro\s+{re.escape(station_lower)}\b',  # metro Station
                rf'm\.\s+{re.escape(station_lower)}\b',  # m. Station
                rf'{re.escape(station_lower)}\s+metrosu\b'  # Station metrosu
            ]
            
            for pattern in patterns:
                if re.search(pattern, text_lower):
                    # Return the properly capitalized version
                    if station.lower() == station:  # If we matched a lowercase version
                        # Find the corresponding capitalized version
                        idx = metro_stations.index(station)
                        if idx % 2 == 1:  # If it's an odd index, get the previous item
                            return metro_stations[idx - 1]
                        return station
                    return station
        
        # If no exact match found, try more general patterns
        general_patterns = [
            r'\b([A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+(?:\s+[A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+){0,2})\s+metro\s+stansiy',
            r'\b([A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+(?:\s+[A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+){0,2})\s+metrosu',
            r'\b([A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+(?:\s+[A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+){0,2})\s+m\.'
        ]
        
        for pattern in general_patterns:
            match = re.search(pattern, text_lower)
            if match:
                candidate = match.group(1).strip()
                # Check if the candidate is similar to any known station
                for station in metro_stations:
                    station_lower = station.lower()
                    # Check for partial matches or station names without diacritics
                    if (station_lower in candidate or 
                        candidate in station_lower or
                        self._similarity_score(station_lower, candidate) > 0.7):
                        return station
        
        return None

    def _similarity_score(self, s1: str, s2: str) -> float:
        """
        Calculate a simple similarity score between two strings.
        Used to match metro stations with slight spelling variations.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Similarity score between 0 and 1
        """
        # Remove diacritics/accents for better matching
        def remove_diacritics(text):
            return text.replace('ə', 'e').replace('ı', 'i').replace('ö', 'o').replace('ü', 'u').replace('ğ', 'g').replace('ş', 's').replace('ç', 'c')
        
        s1_clean = remove_diacritics(s1)
        s2_clean = remove_diacritics(s2)
        
        # For very short strings, require exact match
        if len(s1) <= 3 or len(s2) <= 3:
            return 1.0 if s1_clean == s2_clean else 0.0
        
        # Count matching characters
        matches = sum(c1 == c2 for c1, c2 in zip(s1_clean, s2_clean))
        
        # Calculate Jaccard similarity for longer strings
        if len(s1_clean) > 0 and len(s2_clean) > 0:
            return matches / max(len(s1_clean), len(s2_clean))
        return 0.0

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse the detailed listing page and extract all available information"""
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'ipoteka.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract title and parse components
            title_elem = soup.select_one('.desc_block h2.title')
            if title_elem:
                raw_detail_title = title_elem.text.strip()
                # Truncate to avoid DB errors
                truncated_detail_title = self.safe_truncate(raw_detail_title, self.MAX_TITLE_LENGTH)
                data['title'] = truncated_detail_title
                
                # Extract clean address from the title - get only the location parts at the end
                title_parts = [part.strip() for part in raw_detail_title.split(',')]
                
                # Extract the address (last 2-3 parts that contain location info)
                address_parts = []
                for part in reversed(title_parts):
                    part_lower = part.lower().strip()
                    
                    # If it contains district or location indicators, include it
                    if ('r.' in part_lower or 
                        any(loc in part_lower for loc in ['rayon', 'qəs.', 'küç.', 'mkr', 'prospekt']) or
                        len(address_parts) < 2):  # Always include at least the last two parts
                        address_parts.insert(0, part.strip())
                    
                    # Stop once we've collected enough location parts or hit property description
                    if len(address_parts) >= 3 and 'otaq' in part_lower:
                        break
                
                # Set the cleaned address
                if address_parts:
                    data['address'] = ', '.join(address_parts)
                
                # Parse components from raw_detail_title
                for part in title_parts:
                    part_lower = part.lower().strip()
                    
                    # Extract rooms
                    if 'otaq' in part_lower:
                        rooms_match = re.search(r'(\d+)\s*otaq', part_lower)
                        if rooms_match:
                            data['rooms'] = int(rooms_match.group(1))
                    
                    # Extract property type
                    if 'yeni tikili' in part_lower:
                        data['property_type'] = 'new'
                    elif 'köhnə tikili' in part_lower:
                        data['property_type'] = 'old'
                    elif any(x in part_lower for x in ['ev', 'villa', 'həyət']):
                        data['property_type'] = 'house'
                    
                    # Extract district
                    if 'r.' in part_lower:
                        district_match = re.search(r'(\w+)\s*r\.', part)
                        if district_match:
                            data['district'] = district_match.group(1).title()
                    
                    # Extract area
                    area_match = re.search(r'([\d.]+)\s*m²', part)
                    if area_match:
                        try:
                            data['area'] = float(area_match.group(1))
                        except (ValueError, TypeError):
                            pass
            
            # Extract description
            desc_elem = soup.select_one('.desc_block .text p')
            if desc_elem:
                description_text = desc_elem.text.strip()
                data['description'] = description_text
                
                # Extract metro station from description
                metro_station = self.extract_metro_station(description_text)
                if metro_station:
                    data['metro_station'] = metro_station
            
            # Extract price
            price_elem = soup.select_one('.desc_block .price')
            if price_elem:
                price_text = price_elem.text.strip()
                try:
                    price = float(re.sub(r'[^\d.]', '', price_text))
                    data['price'] = price
                    data['currency'] = 'AZN'
                except (ValueError, TypeError):
                    pass
            
            # Extract location info from map
            map_elem = soup.select_one('#map')
            if map_elem:
                try:
                    data['latitude'] = float(map_elem.get('data-lat'))
                    data['longitude'] = float(map_elem.get('data-lng'))
                except (ValueError, TypeError, AttributeError):
                    pass
            
            # Extract contact info
            contact_elem = soup.select_one('.contact .user')
            if contact_elem:
                data['contact_name'] = contact_elem.text.strip()
                # Determine contact type
                if 'agent' in contact_elem.text.lower() or 'vasitəçi' in contact_elem.text.lower():
                    data['contact_type'] = 'agent'
                else:
                    data['contact_type'] = 'owner'
            
            # Extract phone numbers
            phone_elems = soup.select('ul.links .active')
            if phone_elems:
                phones = []
                for phone in phone_elems:
                    phone_number = phone.get('number') or phone.text.strip()
                    if phone_number:
                        phones.append(re.sub(r'\s+', '', phone_number))
                if phones:
                    data['contact_phone'] = phones[0]  # Store primary phone number
            
            # Extract stats (views, dates)
            stats_elem = soup.select_one('.stats')
            if stats_elem:
                for row in stats_elem.select('.rw'):
                    label = row.select_one('div:first-child')
                    value = row.select_one('div:last-child')
                    if not (label and value):
                        continue
                    
                    label_text = label.text.strip().lower()
                    value_text = value.text.strip()
                    
                    if 'yeniləndi' in label_text:
                        try:
                            data['listing_date'] = datetime.datetime.strptime(
                                value_text, '%d.%m.%Y'
                            ).date()
                        except ValueError:
                            pass
                    elif 'baxış sayı' in label_text:
                        try:
                            data['views_count'] = int(value_text)
                        except ValueError:
                            pass
            
            # Initialize amenities list
            amenities = []
            
            # Extract section titles as categories
            section_titles = soup.select('.params_block h3.title')
            for title in section_titles:
                title_text = title.text.strip()
                if title_text and title_text not in amenities:
                    amenities.append(title_text)
            
            # Extract property details and amenities
            params_block = soup.select_one('.params_block')
            if params_block:
                for row in params_block.select('.rw'):
                    label = row.select_one('div:first-child')
                    value = row.select_one('div:last-child')
                    if not (label and value):
                        continue
                    
                    label_text = label.text.strip().lower()
                    value_text = value.text.strip()
                    
                    # Add each property detail to amenities
                    amenities.append(f"{label.text.strip()}: {value_text}")
                    
                    if 'sahə' in label_text:
                        area_match = re.search(r'([\d.]+)', value_text)
                        if area_match:
                            try:
                                data['area'] = float(area_match.group(1))
                            except (ValueError, TypeError):
                                pass
                    elif 'mərtəbə' in label_text:
                        # Corrected pattern for floor/total_floors
                        floor_match = re.search(r'(\d+)/(\d+)', value_text)
                        if floor_match:
                            try:
                                # In ipoteka.az, the format is "total_floors/floor"
                                data['total_floors'] = int(floor_match.group(1))
                                data['floor'] = int(floor_match.group(2))
                            except (ValueError, TypeError):
                                pass
                    elif 'otaq sayı' in label_text:
                        rooms_match = re.search(r'(\d+)', value_text)
                        if rooms_match:
                            try:
                                data['rooms'] = int(rooms_match.group(1))
                            except (ValueError, TypeError):
                                pass
                    elif 'təmir' in label_text:
                        data['has_repair'] = any(
                            x in value_text.lower() for x in ['əla', 'təmirli', 'yaxşı']
                        )
                    elif 'sənədin tipi' in label_text:
                        data['has_document'] = (
                            'çıxarış' in value_text.lower() or 'kupça' in value_text.lower()
                        )
            
            # Extract additional features/utilities from the page
            utility_keywords = ['Qaz', 'Su', 'İşıq', 'Kombi', 'Lift', 'Parkinq', 'Eyvan']
            
            # Check description for utilities
            if data.get('description'):
                desc_lower = data['description'].lower()
                for keyword in utility_keywords:
                    keyword_lower = keyword.lower()
                    if keyword_lower in desc_lower and keyword not in amenities:
                        amenities.append(keyword)
            
            # Check if any additional features are explicitly shown in the page
            for keyword in utility_keywords:
                # Look for spans/divs that might indicate features
                feature_elem = soup.select_one(f'span:-soup-contains("{keyword}"), div:-soup-contains("{keyword}")')
                if feature_elem and keyword not in amenities:
                    amenities.append(keyword)
            
            # Store amenities in the data dictionary
            if amenities:
                data['amenities'] = json.dumps(amenities)
            
            # Extract photos
            photos = []
            photo_links = soup.select('a[data-fancybox="gallery_ads_view"]')
            for link in photo_links:
                href = link.get('href')
                if href and not href.endswith('load.gif'):
                    if not href.startswith('http'):
                        href = f"{self.BASE_URL}{href}"
                    photos.append(href)
            
            if photos:
                data['photos'] = json.dumps(photos)
            
            # If no address found so far, use the most relevant parts of the title
            if 'address' not in data and data.get('title'):
                title_parts = [part.strip() for part in data['title'].split(',')]
                relevant_parts = []
                for part in reversed(title_parts):
                    if len(relevant_parts) < 2:  # Take the last two parts
                        relevant_parts.insert(0, part)
                data['address'] = ', '.join(relevant_parts)
            
            # Default to 'sale'
            data['listing_type'] = 'sale'
            
            # Default property type if not set
            if 'property_type' not in data:
                data['property_type'] = 'apartment'
            
            # Extract floor/total floor from description if not already found
            if ('floor' not in data or 'total_floors' not in data) and data.get('description'):
                desc_text = data['description']
                floor_matches = re.search(r'(\d+)/(\d+)[^\d]*mərtəbə', desc_text)
                if floor_matches:
                    try:
                        # In ipoteka.az descriptions as well, the format is "total_floors/floor"
                        total_floors = int(floor_matches.group(1))
                        floor = int(floor_matches.group(2))
                        if 'total_floors' not in data:
                            data['total_floors'] = total_floors
                        if 'floor' not in data:
                            data['floor'] = floor
                    except (ValueError, TypeError):
                        pass
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise
    
    async def run(self, pages: int = 2):
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Starting Ipoteka.az scraper")
            await self.init_session()
            all_results = []
            
            for page in range(1, pages + 1):
                try:
                    # Prepare search parameters
                    params = {
                        'ad_type': '0',
                        'search_type': '0',
                        'page': str(page)
                    }
                    
                    # Fetch and parse listings page
                    html = await self.get_page_content(self.SEARCH_URL, params)
                    listings = await self.parse_listing_page(html)
                    
                    self.logger.info(f"Found {len(listings)} listings on page {page}")
                    
                    # Fetch and parse each listing detail
                    for listing in listings:
                        try:
                            detail_html = await self.get_page_content(listing['source_url'])
                            detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                            # Merge base listing data with the detailed data
                            all_results.append({**listing, **detail_data})
                        except Exception as e:
                            self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                            continue
                            
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    continue
                    
                # Add delay between pages
                await asyncio.sleep(random.uniform(1, 2))
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        finally:
            await self.close_session()
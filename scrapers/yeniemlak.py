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

class YeniEmlakScraper:
    """Scraper for yeniemlak.az"""
    
    BASE_URL = "https://yeniemlak.az"
    SEARCH_URL = "https://yeniemlak.az/elan/axtar"
    
    # Mapping listing types to database enum values
    LISTING_TYPE_MAP = {
        "1": "sale",      # elan_nov=1
        "2": "monthly",   # elan_nov=2 
        "3": "daily"      # elan_nov=3
    }
    
    def __init__(self):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None

    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br'
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
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
        DELAY = int(os.getenv('REQUEST_DELAY', 1))
        
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

    def extract_coordinates(self, html: str) -> Tuple[Optional[float], Optional[float]]:
        """Extract latitude and longitude from page HTML"""
        lat = None
        lon = None
        
        lat_match = re.search(r'id="lat"[^>]*value="([^"]+)"', html)
        lon_match = re.search(r'id="lon"[^>]*value="([^"]+)"', html)
        
        if lat_match and lon_match:
            try:
                lat = float(lat_match.group(1))
                lon = float(lon_match.group(1))
            except ValueError:
                pass
                
        return lat, lon

    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('table.list'):
            try:
                # Extract listing ID and URL
                detail_link = listing.select_one('a.detail')
                if not detail_link:
                    continue
                    
                listing_url = self.BASE_URL + detail_link['href']
                listing_id = re.search(r'-(\d+)$', listing_url).group(1)
                
                # Extract price
                price_elem = listing.select_one('price')
                price = self.extract_number(price_elem.text) if price_elem else None
                
                # Extract basic metadata
                title = listing.select_one('.text emlak')
                title = title.text.strip() if title else None
                
                # Basic listing data
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'yeniemlak.az',
                    'title': title,
                    'price': price,
                    'currency': 'AZN',
                    'created_at': datetime.datetime.now()
                }
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
                
        return listings

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse the detailed listing page with improved selectors"""
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            # Initialize data dictionary
            data = {
                'listing_id': listing_id,
                'source_website': 'yeniemlak.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract title (using more specific selector)
            title_elem = soup.select_one('div.title tip')
            if title_elem:
                data['title'] = title_elem.text.strip()
            
            # Extract description
            description = soup.select_one('div.text')
            if description:
                data['description'] = description.text.strip()
            
            # Extract price with improved selector
            price_elem = soup.select_one('price')
            if price_elem:
                try:
                    price = float(re.sub(r'[^\d.]', '', price_elem.text))
                    data['price'] = price
                    data['currency'] = 'AZN'
                except (ValueError, TypeError):
                    self.logger.warning(f"Failed to extract price for listing {listing_id}")
            
            # Extract listing date
            date_elem = soup.select_one('div.title titem:contains("Tarix:") b')
            if date_elem:
                try:
                    date_str = date_elem.text.strip()
                    data['listing_date'] = datetime.datetime.strptime(date_str, '%d.%m.%Y').date()
                except (ValueError, AttributeError):
                    self.logger.warning(f"Failed to parse listing date: {date_elem.text if date_elem else 'None'}")
            
            # Extract views count
            views_elem = soup.select_one('div.title titem:contains("Baxış") b')
            if views_elem:
                try:
                    views_count = int(re.sub(r'[^\d]', '', views_elem.text))
                    data['views_count'] = views_count
                except (ValueError, TypeError):
                    pass
            
            # Extract property type
            property_type_elem = soup.select_one('emlak')
            if property_type_elem:
                property_type_text = property_type_elem.text.strip().lower()
                if 'bina evi' in property_type_text:
                    data['property_type'] = 'apartment'
                elif 'həyət evi' in property_type_text:
                    data['property_type'] = 'house'
                elif 'villa' in property_type_text:
                    data['property_type'] = 'villa'
                elif 'ofis' in property_type_text:
                    data['property_type'] = 'office'
                
                # Check for "Yeni tikili" or "Köhnə tikili" in the sibling text
                if property_type_elem.next_sibling and 'yeni tikili' in property_type_elem.next_sibling.lower():
                    data['property_type'] = 'new'
                elif property_type_elem.next_sibling and 'köhnə tikili' in property_type_elem.next_sibling.lower():
                    data['property_type'] = 'old'
            
            # Extract room count, area, floor info with more precise selectors
            for param_div in soup.select('div.params'):
                text = param_div.text.strip()
                
                # Extract room count
                if 'otaq' in text.lower():
                    b_elem = param_div.select_one('b')
                    if b_elem:
                        try:
                            rooms = int(b_elem.text.strip())
                            data['rooms'] = rooms
                        except (ValueError, TypeError):
                            pass
                
                # Extract area
                elif 'm2' in text.lower() or 'm²' in text.lower():
                    b_elem = param_div.select_one('b')
                    if b_elem:
                        try:
                            area = float(b_elem.text.strip())
                            data['area'] = area
                        except (ValueError, TypeError):
                            pass
                
                # Extract floor information
                elif 'mərtəbə' in text.lower():
                    b_elems = param_div.select('b')
                    if len(b_elems) >= 2:
                        try:
                            data['floor'] = int(b_elems[0].text.strip())
                            data['total_floors'] = int(b_elems[1].text.strip())
                        except (ValueError, TypeError, IndexError):
                            pass
                
                # Extract district
                elif 'rayonu' in text.lower():
                    b_elem = param_div.select_one('b')
                    if b_elem:
                        data['district'] = b_elem.text.strip()
                
                # Extract metro station
                elif 'metro' in text.lower():
                    b_elem = param_div.select_one('b')
                    if b_elem:
                        data['metro_station'] = b_elem.text.strip()
                
                # Extract location/settlement
                elif 'qəs.' in text.lower():
                    b_elem = param_div.select_one('b')
                    if b_elem:
                        data['location'] = b_elem.text.strip()
            
            # Extract full address
            address_text_elem = soup.select_one('div.params + div.text')
            if address_text_elem:
                data['address'] = address_text_elem.text.strip()
            
            # Extract contact info and type
            contact_name = soup.select_one('div.ad')
            contact_type = soup.select_one('div.elvrn')
            
            if contact_name:
                data['contact_name'] = contact_name.text.strip()
            
            if contact_type:
                contact_type_text = contact_type.text.lower().strip()
                if 'vasitəçi' in contact_type_text or 'rieltor' in contact_type_text:
                    data['contact_type'] = 'agent'
                else:
                    data['contact_type'] = 'owner'
            
            # Extract phone number
            phone_img = soup.select_one('div.tel img')
            if phone_img:
                src = phone_img.get('src')
                if src:
                    # Extract phone number from image src (e.g., "/tel-show/0555553908")
                    phone_match = re.search(r'/tel-show/(\d+)', src)
                    if phone_match:
                        data['contact_phone'] = phone_match.group(1)
            
            # Extract amenities
            amenities = []
            for check in soup.select('div.check'):
                if check and check.text.strip():
                    amenities.append(check.text.strip())
            
            if amenities:
                data['amenities'] = json.dumps(amenities)
                data['has_repair'] = 'Təmirli' in amenities
            
            # Extract photos
            photos = []
            for img_link in soup.select('a.fancybox-thumb[href]'):
                href = img_link.get('href')
                if href and not href.endswith(('load.gif', 'placeholder.png')):
                    # Ensure absolute URL
                    if not href.startswith('http'):
                        href = f"https:{href}" if href.startswith('//') else f"{self.BASE_URL}{href}"
                    photos.append(href)
            
            if photos:
                data['photos'] = json.dumps(photos)
            
            # Extract coordinates (if available)
            map_iframe = soup.find('iframe', src=lambda x: x and 'google.com/maps' in x)
            if map_iframe:
                src = map_iframe.get('src', '')
                coords_match = re.search(r'q=(-?\d+\.\d+),(-?\d+\.\d+)', src)
                if coords_match:
                    try:
                        data['latitude'] = float(coords_match.group(1))
                        data['longitude'] = float(coords_match.group(2))
                    except (ValueError, TypeError):
                        pass
            
            # If no coordinates found via iframe, try extracting from any other map element
            if 'latitude' not in data:
                map_div = soup.find('div', id='map')
                if map_div:
                    lat_attr = map_div.get('data-lat')
                    lng_attr = map_div.get('data-lng')
                    if lat_attr and lng_attr:
                        try:
                            data['latitude'] = float(lat_attr)
                            data['longitude'] = float(lng_attr)
                        except (ValueError, TypeError):
                            pass
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}", exc_info=True)
            raise

    async def scrape_listing_type(self, listing_type: str, pages: int = 2) -> List[Dict]:
        """Scrape specified number of pages for a given listing type"""
        results = []
        
        for page in range(1, pages + 1):
            try:
                # Prepare search parameters
                params = {
                    'elan_nov': listing_type,
                    'emlak': '0',
                    'page': str(page)
                }
                
                # Fetch and parse listings page
                html = await self.get_page_content(self.SEARCH_URL, params)
                listings = await self.parse_listing_page(html)
                
                # Fetch and parse each listing detail
                for listing in listings:
                    try:
                        detail_html = await self.get_page_content(listing['source_url'])
                        detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                        
                        # Map listing type to database enum
                        detail_data['listing_type'] = self.LISTING_TYPE_MAP[listing_type]
                        
                        results.append({**listing, **detail_data})
                        
                    except Exception as e:
                        self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"Error processing page {page}: {str(e)}")
                continue
                
        return results

    async def run(self, pages: int = 2):
        """Run the scraper for all listing types"""
        try:
            self.logger.info("Starting YeniEmlak scraper")
            await self.init_session()
            
            all_results = []
            
            # Scrape each listing type
            for listing_type in self.LISTING_TYPE_MAP.keys():
                try:
                    self.logger.info(f"Scraping listing type {listing_type}")
                    results = await self.scrape_listing_type(listing_type, pages)
                    all_results.extend(results)
                except Exception as e:
                    self.logger.error(f"Error scraping listing type {listing_type}: {str(e)}")
                    continue
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        finally:
            await self.close_session()
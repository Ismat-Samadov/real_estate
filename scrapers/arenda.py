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

class OptimizedArendaScraper:
    """Optimized scraper for arenda.az"""
    
    BASE_URL = "https://arenda.az"
    LISTINGS_URL = "https://arenda.az/filtirli-axtaris/"
    
    def __init__(self):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None
    
    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            # Common browser headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"macOS"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1'
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
        
        self.logger.info(f"Attempting to fetch URL: {url}")
        start_time = time.time()
        
        # Add request-specific headers
        headers = {
            'Referer': 'https://arenda.az/',
            'Origin': 'https://arenda.az',
            'Host': 'arenda.az'
        }
        
        # Add cookies and additional parameters
        cookies = {
            'lang': '1',
            'arenda': '1',
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                self.logger.info(f"Attempt {attempt + 1} of {MAX_RETRIES}")
                # Add random delay between 1-3 seconds
                await asyncio.sleep(DELAY + random.random() * 2)
                
                async with self.session.get(
                    url, 
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    timeout=aiohttp.ClientTimeout(total=10)  # 10 second timeout
                ) as response:
                    self.logger.info(f"Got response with status: {response.status}")
                    
                    if response.status == 200:
                        try:
                            content = await response.text(encoding='utf-8')
                            elapsed = time.time() - start_time
                            self.logger.info(f"Successfully fetched content in {elapsed:.2f} seconds")
                            return content
                        except UnicodeDecodeError:
                            content = await response.read()
                            return content.decode('utf-8', errors='replace')
                    elif response.status == 403:
                        self.logger.warning(f"Access forbidden (403) on attempt {attempt + 1}. Might be rate-limited.")
                        await asyncio.sleep(DELAY * (attempt + 2))
                    else:
                        self.logger.warning(f"Failed to fetch {url}, status: {response.status}")
                        
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout on attempt {attempt + 1}")
            except Exception as e:
                self.logger.error(f"Error fetching {url}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                
            await asyncio.sleep(DELAY * (attempt + 1))  # Exponential backoff
        
        elapsed = time.time() - start_time
        self.logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts. Total time: {elapsed:.2f} seconds")
        raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} attempts")

    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('li.new_elan_box'):
            try:
                # Get listing URL and ID
                listing_url = listing.select_one('a')['href']
                listing_id = listing.get('id', '').replace('elan_', '')
                
                if not listing_id or not listing_url:
                    continue
                
                # Determine listing type from URL
                listing_type = 'sale'  # default
                url_lower = listing_url.lower()
                if 'kiraye-gunluk' in url_lower:
                    listing_type = 'daily'
                elif 'kiraye-ayliq' in url_lower:
                    listing_type = 'monthly'
                elif 'satilir' in url_lower:
                    listing_type = 'sale'
                
                # Extract price
                price_elem = listing.select_one('.elan_price')
                price_text = price_elem.text.strip() if price_elem else None
                price = self.extract_price(price_text) if price_text else None
                
                # Basic data from listing card
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'arenda.az',
                    'price': price,
                    'currency': 'AZN',
                    'listing_type': listing_type,  # Set the listing type here
                    'created_at': datetime.datetime.now()
                }
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
                
        return listings

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse the detailed listing page with enhanced data validation"""
        soup = BeautifulSoup(html, 'lxml')
        try:
            listing_data = {
                'listing_id': listing_id,
                'source_website': 'arenda.az',
                'created_at': datetime.datetime.now(),
                'updated_at': datetime.datetime.now()
            }

            # Extract title and description
            title_elem = soup.select_one('h2.elan_main_title')
            if title_elem and title_elem.text:
                listing_data['title'] = title_elem.text.strip()
                
            desc_elem = soup.select_one('.elan_info_txt')
            if desc_elem:
                # Remove "factDisplay" text if present
                fact_display = desc_elem.select_one('#factDisplay')
                if fact_display:
                    fact_display.decompose()
                listing_data['description'] = desc_elem.text.strip()
                
            # Extract price
            price_box = soup.select_one('.elan_new_price_box')
            if price_box:
                price_text = price_box.text.strip()
                price = self.extract_price(price_text)
                if price is not None and price > 0:
                    listing_data['price'] = price
                    listing_data['currency'] = 'AZN'
            
            # Extract and validate coordinates
            lat_elem = soup.select_one('#lat')
            lon_elem = soup.select_one('#lon')
            if lat_elem and lon_elem:
                try:
                    lat = float(lat_elem.get('value', 0))
                    lon = float(lon_elem.get('value', 0))
                    
                    # Only include coordinates if they're in valid ranges
                    if -90 <= lat <= 90 and -180 <= lon <= 180:
                        listing_data['latitude'] = round(lat, 8)
                        listing_data['longitude'] = round(lon, 8)
                    else:
                        self.logger.warning(f"Invalid coordinate ranges for listing {listing_id}: lat={lat}, lon={lon}")
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Error parsing coordinates for listing {listing_id}: {str(e)}")
            
            # Extract location information
            address_elem = soup.select_one('.elan_unvan_txt')
            if address_elem:
                listing_data['address'] = address_elem.text.strip()
            
            # Get metro/district/location
            location_elems = soup.select('.elan_adr_list li a')
            for elem in location_elems:
                text = elem.text.strip().lower()
                if 'metro' in text:
                    listing_data['metro_station'] = text.replace('metro', '').strip()
                elif 'r.' in text:
                    listing_data['district'] = text.replace('r.', '').strip()
                else:
                    listing_data['location'] = text
            
            # Extract property details and amenities
            property_lists = soup.select('.property_lists li')
            amenities = [item.text.strip() for item in property_lists] if property_lists else []
            if amenities:
                listing_data['amenities'] = json.dumps(amenities)
            
            # Extract repair status
            listing_data['has_repair'] = 'Təmirli' in ' '.join(amenities) if amenities else False
            
            # Parse room/area/floor information
            for prop_item in soup.select('.elan_property_list li a'):
                text = prop_item.text.strip().lower()
                try:
                    if 'otaq' in text:
                        rooms = int(text.split()[0])
                        if 0 < rooms <= 50:  # Reasonable range check
                            listing_data['rooms'] = rooms
                    elif any(x in text for x in ['m²', 'm2']):
                        area = float(re.sub(r'[^\d.]', '', text))
                        if 5 <= area <= 10000:  # Reasonable range check
                            listing_data['area'] = area
                    elif 'mərtəbə' in text:
                        floor_info = text.split('/')
                        if len(floor_info) == 2:
                            floor = int(floor_info[0].strip())
                            total_floors = int(floor_info[1].split()[0].strip())
                            if 0 <= floor <= total_floors <= 200:  # Reasonable range check
                                listing_data['floor'] = floor
                                listing_data['total_floors'] = total_floors
                except (ValueError, IndexError) as e:
                    self.logger.warning(f"Error parsing property detail '{text}': {str(e)}")
            
            # Extract contact information
            user_info = soup.select_one('.new_elan_user_info')
            if user_info:
                contact_text = user_info.select_one('p')
                if contact_text:
                    contact_type = contact_text.text.strip()
                    if '(' in contact_type and ')' in contact_type:
                        listing_data['contact_type'] = contact_type.split('(')[1].split(')')[0]
                        
                phone_elem = user_info.select_one('.elan_in_tel')
                if phone_elem:
                    listing_data['contact_phone'] = phone_elem.text.strip()
                    listing_data['whatsapp_available'] = bool(phone_elem.select_one('.wp_status_ico'))
            
            # Extract timestamp and views
            date_box = soup.select_one('.elan_date_box')
            if date_box:
                date_elem = date_box.select_one('p:-soup-contains("tarixi")')
                if date_elem:
                    try:
                        date_str = date_elem.text.split(':')[1].strip()
                        listing_data['listing_date'] = datetime.datetime.strptime(date_str, '%d.%m.%Y').date()
                    except (ValueError, IndexError):
                        pass
                        
                views_elem = date_box.select_one('p:-soup-contains("Baxış")')
                if views_elem:
                    try:
                        views_text = views_elem.text.split(':')[1].strip()
                        views_count = int(views_text)
                        if views_count >= 0:
                            listing_data['views_count'] = views_count
                    except (ValueError, IndexError):
                        pass
            
            # Extract photos
            photos = []
            photo_containers = soup.select('.elan_img_box img')
            for photo in photo_containers:
                src = photo.get('data-src') or photo.get('src')
                if src and 'load.gif' not in src:
                    photos.append(src)
            
            if photos:
                listing_data['photos'] = json.dumps(photos)
            
            # Determine listing type from URL if not already set
            url = listing_data.get('source_url', '').lower()
            if 'kiraye-gunluk' in url:
                listing_data['listing_type'] = 'daily'
            elif 'kiraye-ayliq' in url:
                listing_data['listing_type'] = 'monthly'
            elif 'satilir' in url:
                listing_data['listing_type'] = 'sale'
            else:
                listing_data['listing_type'] = 'sale'  # default
                
            # Determine property type
            if 'heyet-evi' in url or 'villa' in url:
                listing_data['property_type'] = 'house'
            elif 'yeni-tikili' in url:
                listing_data['property_type'] = 'new'
            elif 'kohne-tikili' in url:
                listing_data['property_type'] = 'old'
            elif 'ofis' in url:
                listing_data['property_type'] = 'office'
            elif 'obyekt' in url:
                listing_data['property_type'] = 'commercial'
            elif 'torpaq' in url:
                listing_data['property_type'] = 'land'
            else:
                listing_data['property_type'] = 'apartment'  # default
                
            return listing_data
                
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise

    def extract_price(self, price_text: str) -> float:
        """Extract numeric price from price text"""
        if not price_text:
            return None
            
        try:
            # Remove currency and convert to float
            price = re.sub(r'[^\d.]', '', price_text)
            return float(price)
        except:
            return None

    async def run(self, pages: int = 1):
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Initializing scraper session")
            await self.init_session()
            results = []
            
            for page in range(1, pages + 1):
                self.logger.info(f"Processing page {page} of {pages}")
                start_time = time.time()
                
                try:
                    params = {
                        'home_search': '1',
                        'lang': '1',
                        'site': '1',
                        'home_s': '1',
                        'page': str(page)
                    }
                    
                    # Fetch and parse listings page
                    self.logger.info(f"Fetching listings page {page}")
                    html = await self.get_page_content(self.LISTINGS_URL, params)
                    listings = await self.parse_listing_page(html)
                    self.logger.info(f"Found {len(listings)} listings on page {page}")
                    
                    # Fetch and parse each listing detail
                    for idx, listing in enumerate(listings, 1):
                        try:
                            self.logger.info(f"Processing listing {idx}/{len(listings)} on page {page}")
                            detail_html = await self.get_page_content(listing['source_url'])
                            detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                            results.append({**listing, **detail_data})
                        except Exception as e:
                            self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                            continue
                    
                    elapsed = time.time() - start_time
                    self.logger.info(f"Completed page {page} in {elapsed:.2f} seconds")
                            
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    continue
                    
            self.logger.info(f"Scraping completed. Total listings processed: {len(results)}")
            return results
            
        finally:
            self.logger.info("Closing scraper session")
            await self.close_session()
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
from urllib.parse import urljoin

class BinaScraper:
    """Scraper for bina.az real estate listings"""
    
    BASE_URL = "https://bina.az"
    LISTING_TYPES = {
        'sale': {
            'url': "https://bina.az/alqi-satqi",
            'type': 'sale'
        },
        'rent': {
            'url': "https://bina.az/kiraye",
            'type': 'monthly'  # Default to monthly, we'll detect daily from title
        }
    }
    
    def __init__(self):
        """Initialize scraper"""
        self.logger = logging.getLogger(__name__)
        self.session = None

    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,az;q=0.8',
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
            
            # Initialize session with custom SSL context and connection pooling
            conn = aiohttp.TCPConnector(
                ssl=False,
                limit=10,  # Connection pool size
                ttl_dns_cache=300  # DNS cache TTL
            )
            
            self.session = aiohttp.ClientSession(
                headers=headers,
                connector=conn,
                timeout=aiohttp.ClientTimeout(total=30)
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
        
        # Add request-specific headers and cookies
        headers = {
            'Referer': 'https://bina.az/',
            'Origin': 'https://bina.az',
            'Host': 'bina.az'
        }
        
        cookies = {
            'language': 'az',
            '_ga': f'GA1.1.{random.randint(1000000, 9999999)}.{int(time.time())}'
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                self.logger.debug(f"Attempting to fetch {url} (Attempt {attempt + 1}/{MAX_RETRIES})")
                await asyncio.sleep(DELAY + random.random() * 2)
                
                async with self.session.get(
                    url,
                    params=params,
                    headers={**self.session.headers, **headers},
                    cookies=cookies,
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 403:
                        self.logger.warning(f"Access forbidden (403) on attempt {attempt + 1}")
                        await asyncio.sleep(DELAY * (attempt + 2))
                    else:
                        self.logger.warning(f"Failed with status {response.status}")
            
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout on attempt {attempt + 1}")
            except Exception as e:
                self.logger.error(f"Error fetching {url}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
            
            await asyncio.sleep(DELAY * (2 ** attempt) + random.random() * 2)
        
        raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} attempts")

    def extract_price(self, price_text: str) -> Optional[float]:
        """Extract numeric price from text"""
        if not price_text:
            return None
        try:
            # Remove all non-numeric characters
            price = re.sub(r'[^\d.]', '', price_text)
            return float(price) if price else None
        except (ValueError, TypeError):
            return None

    def extract_floor_info(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        """Extract floor and total floors from text"""
        if not text:
            return None, None
            
        try:
            # Handle formats like "5/9" or "5 / 9"
            parts = text.split('/')
            if len(parts) == 2:
                floor = int(re.search(r'\d+', parts[0]).group())
                total = int(re.search(r'\d+', parts[1]).group())
                return floor, total
        except (AttributeError, ValueError, IndexError):
            pass
            
        return None, None

    def detect_listing_type(self, title: Optional[str], base_type: str) -> str:
        """Detect specific listing type from title"""
        if not title:
            return base_type
            
        title_lower = title.lower()
        if base_type == 'monthly':
            if 'günlük' in title_lower:
                return 'daily'
            return 'monthly'
        return 'sale'

    async def parse_listing_page(self, html: str, listing_type: str) -> List[Dict]:
        """Parse the listings page to extract basic listing info"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        self.logger.debug("Parsing listings page")
        
        # Find all listing cards
        for listing in soup.select('.items_list .items-i'):
            try:
                # Extract listing URL and ID
                listing_id = listing.get('data-item-id')
                link = listing.select_one('a.item_link')
                
                if not link or not listing_id:
                    continue
                    
                listing_url = urljoin(self.BASE_URL, link.get('href', ''))
                
                # Extract price 
                price = None
                price_elem = listing.select_one('.price-val')
                if price_elem:
                    price = self.extract_price(price_elem.text.strip())
                
                # Extract title for listing type detection
                title_elem = listing.select_one('.card-title')
                title = title_elem.text.strip() if title_elem else None
                
                # Basic data from listing card
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'bina.az',
                    'price': price,
                    'currency': 'AZN',
                    'listing_type': self.detect_listing_type(title, listing_type),
                    'created_at': datetime.datetime.now(),
                    'title': title
                }
                
                # Extract location
                location = listing.select_one('.location')
                if location:
                    listing_data['location'] = location.text.strip()
                
                # Extract property details
                name_items = listing.select('.name li')
                for item in name_items:
                    text = item.text.strip().lower()
                    
                    # Extract room count
                    if 'otaq' in text:
                        try:
                            rooms = int(re.search(r'\d+', text).group())
                            listing_data['rooms'] = rooms
                        except (ValueError, AttributeError):
                            pass
                            
                    # Extract area
                    elif 'm²' in text:
                        try:
                            area = float(re.sub(r'[^\d.]', '', text))
                            listing_data['area'] = area
                        except ValueError:
                            pass
                            
                    # Extract floor info
                    elif 'mərtəbə' in text:
                        floor, total_floors = self.extract_floor_info(text)
                        if floor is not None:
                            listing_data['floor'] = floor
                        if total_floors is not None:
                            listing_data['total_floors'] = total_floors
                
                # Extract repair status and other features
                features = []
                if listing.select_one('.repair'):
                    listing_data['has_repair'] = True
                    features.append('təmirli')
                
                if listing.select_one('.bill_of_sale'):
                    features.append('kupçalı')
                    
                if listing.select_one('.mortgage'):
                    features.append('ipoteka var')
                
                if features:
                    listing_data['features'] = features
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
        
        return listings

    def validate_coordinates(self, lat: float, lon: float) -> Tuple[Optional[float], Optional[float]]:
        """
        Validate and format coordinates to match database schema constraints
        DECIMAL(10,8) means max 10 digits total with 8 after decimal point
        Valid range for coordinates:
        Latitude: -90 to 90
        Longitude: -180 to 180
        """
        try:
            if lat is not None and lon is not None:
                # Check if coordinates are within valid ranges
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    # Format to 8 decimal places to match schema
                    return (
                        round(float(lat), 8),
                        round(float(lon), 8)
                    )
            return None, None
        except (ValueError, TypeError):
            return None, None

    async def get_phone_numbers(self, listing_id: str) -> List[str]:
        """Fetch phone numbers for a listing"""
        try:
            # Construct the phone API URL
            phone_url = f"{self.BASE_URL}/items/{listing_id}/phones"
            
            # Required headers for the phone API request
            headers = {
                'Accept': 'application/json',
                'Referer': f'https://bina.az/items/{listing_id}',
                'X-Requested-With': 'XMLHttpRequest',
                'DNT': '1',
                'Sec-Ch-Ua': '"Google Chrome"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"macOS"'
            }
            
            params = {
                'source_link': f'https://bina.az/items/{listing_id}',
                'trigger_button': 'main'
            }
            
            async with self.session.get(
                phone_url,
                headers=headers,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('phones', [])
                return []
        except Exception as e:
            self.logger.error(f"Error fetching phone numbers for listing {listing_id}: {str(e)}")
            return []

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse detailed listing page and fetch phone numbers"""
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'bina.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract title
            title = soup.select_one('h1.product-title')
            if title:
                data['title'] = title.text.strip()
            
            # Extract description
            desc = soup.select_one('.product-description__content')
            if desc:
                data['description'] = desc.text.strip()
            
            # Extract and validate coordinates
            map_elem = soup.select_one('#item_map')
            if map_elem:
                try:
                    raw_lat = float(map_elem.get('data-lat', 0))
                    raw_lon = float(map_elem.get('data-lng', 0))
                    lat, lon = self.validate_coordinates(raw_lat, raw_lon)
                    if lat is not None and lon is not None:
                        data['latitude'] = lat
                        data['longitude'] = lon
                except (ValueError, TypeError, AttributeError):
                    self.logger.warning(f"Invalid coordinates for listing {listing_id}")

            # Extract location info from breadcrumbs or meta info
            location_info = soup.select('.product-map__controls')
            for info in location_info:
                text = info.text.strip().lower()
                if 'metro' in text:
                    data['metro_station'] = text.replace('metro', '').strip()
                elif 'rayon' in text:
                    data['district'] = text.replace('rayon', '').strip()
                else:
                    data['location'] = text

            # Extract phone numbers
            phones = await self.get_phone_numbers(listing_id)
            if phones:
                data['contact_phone'] = phones[0] if phones else None
                data['whatsapp_available'] = bool(soup.select_one('.wp_status_ico'))

            # Extract timestamps
            date_elem = soup.select_one(':-soup-contains("Yeniləndi")')
            if date_elem:
                try:
                    date_str = date_elem.text.split(':')[1].strip()
                    data['updated_at'] = datetime.datetime.strptime(date_str, '%d.%m.%Y, %H:%M')
                except (ValueError, IndexError):
                    pass

            # Extract photos
            photos = []
            photo_elems = soup.select('.product-photos__slider-top img[src]')
            for img in photo_elems:
                src = img.get('src')
                if src and not src.endswith('load.gif'):
                    photos.append(src)

            if photos:
                data['photos'] = json.dumps(photos)
            
            # Extract features/amenities
            features = []
            if soup.select_one('.repair'):
                features.append('təmirli')
            if soup.select_one('.bill_of_sale'):
                features.append('kupçalı')
            if soup.select_one('.mortgage'):
                features.append('ipoteka var')
                
            if features:
                data['amenities'] = json.dumps(features)
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise


    async def run(self, pages: int = 1) -> List[Dict]:
        """Run scraper for specified number of pages for both sale and rental listings"""
        try:
            self.logger.info("Starting Bina.az scraper")
            await self.init_session()
            
            all_results = []
            
            # Iterate through each listing type
            for listing_category, config in self.LISTING_TYPES.items():
                self.logger.info(f"Scraping {listing_category} listings, {pages} pages")
                
                for page in range(1, pages + 1):
                    try:
                        self.logger.info(f"Processing {listing_category} page {page}")
                        
                        # Get page HTML
                        url = f"{config['url']}?page={page}"
                        html = await self.get_page_content(url)
                        
                        # Parse listings
                        listings = await self.parse_listing_page(html, config['type'])
                        self.logger.info(f"Found {len(listings)} {listing_category} listings on page {page}")
                        
                        # Get details for each listing
                        for listing in listings:
                            try:
                                detail_html = await self.get_page_content(listing['source_url'])
                                detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                                
                                # Update listing type if it was refined from title
                                if detail_data.get('title'):
                                    detail_data['listing_type'] = self.detect_listing_type(
                                        detail_data['title'], 
                                        listing['listing_type']
                                    )
                                
                                all_results.append({**listing, **detail_data})
                            except Exception as e:
                                self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                                continue
                                
                    except Exception as e:
                        self.logger.error(f"Error processing {listing_category} page {page}: {str(e)}")
                        continue
                    
                    # Add small delay between pages
                    await asyncio.sleep(random.uniform(1, 2))
                
                # Add delay between listing types
                await asyncio.sleep(random.uniform(2, 3))
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        finally:
            self.logger.info("Closing scraper session")
            await self.close_session()

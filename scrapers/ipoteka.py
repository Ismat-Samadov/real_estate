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

    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('.place_list .col-xs-6 .item'):
            try:
                # Get listing URL and ID
                listing_url = listing.get('href')
                if not listing_url:
                    continue
                    
                listing_id = re.search(r'/elan/(\d+)-', listing_url).group(1)
                listing_url = self.BASE_URL + listing_url

                # Extract price
                price_elem = listing.select_one('span.price')
                price_text = price_elem.text.strip() if price_elem else None
                price = self.extract_number(price_text) if price_text else None
                
                # Get address from title (new)
                title = listing.select_one('span.title')
                address = title.text.strip() if title else None
                
                # Get area and rooms from description
                area = None
                rooms = None
                listing_date = None
                location = None
                
                desc_spans = listing.select('span.desc')
                for desc in desc_spans:
                    desc_text = desc.text.strip().lower()
                    
                    # Extract area
                    if 'sahəsi:' in desc_text:
                        area_match = re.search(r'sahəsi:\s*([\d.]+)', desc_text)
                        if area_match:
                            area = float(area_match.group(1))
                            
                    # Extract rooms
                    if 'otaq sayı:' in desc_text:
                        rooms_match = re.search(r'otaq sayı:\s*(\d+)', desc_text)
                        if rooms_match:
                            rooms = int(rooms_match.group(1))
                            
                    # Extract date and location (new)
                    date_location_match = re.search(r'bakı,\s*(.+?)$', desc_text)
                    if date_location_match:
                        listing_date = date_location_match.group(1).strip()
                
                # Basic listing data
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'ipoteka.az',
                    'address': address,  # New field
                    'price': price,
                    'currency': 'AZN',
                    'created_at': datetime.datetime.now(),
                    'area': area,
                    'rooms': rooms,
                    'listing_date': listing_date  # New field
                }
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
                
        return listings

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse the detailed listing page with enhanced field extraction"""
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'ipoteka.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract title/address and description
            title = soup.select_one('h2.title') or soup.select_one('div.desc_block .title')
            if title:
                data['address'] = title.text.strip()
                
            desc = soup.select_one('.desc_block .text')
            if desc:
                desc_text = desc.text.strip()
                data['description'] = desc_text
                
                # Extract district and metro from description
                for line in desc_text.split(','):
                    line = line.strip().lower()
                    
                    # Extract district (r. pattern)
                    district_match = re.search(r'(\w+)\s*r\.', line)
                    if district_match:
                        data['district'] = district_match.group(1).title()
                    
                    # Extract metro station (m. pattern)
                    metro_match = re.search(r'(\w+)\s*m\.', line)
                    if metro_match:
                        data['metro_station'] = metro_match.group(1).title()
            
            # Extract price
            price_elem = soup.select_one('.desc_block span.price')
            if price_elem:
                data['price'] = self.extract_number(price_elem.text)
                data['currency'] = 'AZN'
            
            # Extract coordinates
            map_elem = soup.select_one('#map')
            if map_elem:
                try:
                    data['latitude'] = float(map_elem.get('data-lat', 0))
                    data['longitude'] = float(map_elem.get('data-lng', 0))
                except (ValueError, TypeError):
                    pass
            
            # Extract property details
            params = soup.select('.params_block .params .rw')
            for param in params:
                divs = param.select('div')
                if len(divs) != 2:
                    continue
                    
                label_text = divs[0].text.strip().lower()
                value_text = divs[1].text.strip()
                
                if 'sahə' in label_text:
                    area = self.extract_number(value_text)
                    if area:
                        data['area'] = area
                elif 'mərtəbə' in label_text:
                    try:
                        floor, total_floors = value_text.split('/')
                        data['floor'] = int(floor)
                        data['total_floors'] = int(total_floors)
                    except:
                        pass
                elif 'otaq sayı' in label_text:
                    rooms = self.extract_number(value_text)
                    if rooms:
                        data['rooms'] = int(rooms)
                elif 'təmir' in label_text:
                    data['has_repair'] = 'təmirli' in value_text.lower()
                elif 'sənədin tipi' in label_text:
                    data['document_type'] = value_text
            
            # Extract listing date (new)
            date_elem = soup.select_one(':-soup-contains("Yeniləndi:")')
            if date_elem:
                date_match = re.search(r'Yeniləndi:\s*(\d{2}\.\d{2}\.\d{4})', date_elem.text)
                if date_match:
                    try:
                        data['listing_date'] = datetime.datetime.strptime(date_match.group(1), '%d.%m.%Y').date()
                    except ValueError:
                        pass
            
            # Extract contact info
            contact_elem = soup.select_one('.contact .user')
            if contact_elem:
                data['contact_type'] = 'owner'
                data['contact_name'] = contact_elem.text.strip()
            
            # Phone number extraction
            phone_div = soup.select_one('.links .active')
            if phone_div:
                phone = phone_div.get('number') or phone_div.text.strip()
                if phone:
                    data['contact_phone'] = phone
                    
            # Extract photos
            photos = []
            photo_elems = soup.select('.img_thumb a[data-fancybox="gallery_ads_view"]')
            for photo in photo_elems:
                src = photo.get('href')
                if src and not src.endswith('load.gif'):
                    photos.append(self.BASE_URL + src)
            
            if photos:
                data['photos'] = json.dumps(photos)
            
            # Determine listing type from description/title
            text = ' '.join(filter(None, [data.get('title', ''), data.get('description', '')])).lower()
            if 'günlük' in text:
                data['listing_type'] = 'daily'
            elif 'aylıq' in text or 'kirayə' in text:
                data['listing_type'] = 'monthly'
            else:
                data['listing_type'] = 'sale'
            
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
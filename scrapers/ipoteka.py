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
                
                listing_id = re.search(r'/elan/(\d+)-', listing_url).group(1)
                listing_url = self.BASE_URL + listing_url
                
                # Initialize basic data
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'ipoteka.az',
                    'created_at': datetime.datetime.now(),
                    'updated_at': datetime.datetime.now()
                }
                
                # Extract price
                price_elem = anchor.select_one('span.img span.price')
                if price_elem:
                    price = self.extract_number(price_elem.text)
                    if price:
                        listing_data.update({
                            'price': price,
                            'currency': 'AZN'
                        })
                
                # Check if document exists
                listing_data['has_document'] = bool(anchor.select_one('span.reg[data-title="Sənəd var"]'))
                
                # Process description spans
                desc_spans = anchor.select('span.desc')
                for desc in desc_spans:
                    desc_text = desc.text.strip()
                    
                    # Extract area
                    if 'Sahəsi:' in desc_text:
                        area_match = re.search(r'Sahəsi:\s*([\d.]+)', desc_text)
                        if area_match:
                            listing_data['area'] = float(area_match.group(1))
                    
                    # Extract rooms from the room count span
                    rooms_span = desc.select_one('span:not([style])')
                    if rooms_span and 'Otaq sayı:' in rooms_span.text:
                        rooms_match = re.search(r'Otaq sayı:\s*(\d+)', rooms_span.text)
                        if rooms_match:
                            listing_data['rooms'] = int(rooms_match.group(1))
                    
                    # Extract date from the right-aligned span
                    date_span = desc.select_one('span[style*="float: right"]')
                    if date_span:
                        date_text = date_span.text.strip()
                        # Handle current date ("Bu gün")
                        if 'Bu gün' in date_text:
                            listing_data['listing_date'] = datetime.datetime.now().date()
                        else:
                            try:
                                city, date_str = date_text.split(',', 1)
                                listing_data['listing_date'] = datetime.datetime.strptime(
                                    date_str.strip(), '%d.%m.%Y'
                                ).date()
                            except ValueError:
                                pass
                
                # Extract location information from title
                title_elem = anchor.select_one('span.title')
                if title_elem:
                    title = title_elem.text.strip()
                    listing_data['address'] = title
                    
                    # Look for district and metro in the address
                    district_match = re.search(r'(\w+)\s*r\.', title)
                    if district_match:
                        listing_data['district'] = district_match.group(1).title()
                        
                        # Look for location after district
                        location_match = re.search(r'r\.,\s*([^,]+)', title)
                        if location_match:
                            listing_data['location'] = location_match.group(1).strip()
                    
                    metro_match = re.search(r'(\w+)\s*m\.', title)
                    if metro_match:
                        listing_data['metro_station'] = metro_match.group(1).title()
                
                # Compose canonical title
                title_components = []
                if listing_data.get('rooms'):
                    title_components.append(f"{listing_data['rooms']} Otaq")
                if listing_data.get('property_type'):
                    type_text = {
                        'new': 'Yeni tikili',
                        'old': 'Köhnə tikili',
                        'house': 'Evlər/Villalar',
                        'land': 'Torpaq'
                    }.get(listing_data['property_type'])
                    if type_text:
                        title_components.append(type_text)
                if listing_data.get('district'):
                    title_components.append(f"{listing_data['district']} r.")
                if listing_data.get('location'):
                    title_components.append(listing_data['location'])
                
                if title_components:
                    listing_data['title'] = ", ".join(title_components)
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
                
        return listings

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse the detailed listing page"""
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'ipoteka.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract title and parse its components
            title = soup.select_one('.desc_block .title')
            if title:
                title_text = title.text.strip()
                data['title'] = title_text
                
                # Parse components from title
                for part in (p.strip() for p in title_text.split(',')):
                    part_lower = part.lower()
                    
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
                    
                    # Extract district and metro
                    if 'r.' in part:
                        district_match = re.search(r'(\w+)\s*r\.', part)
                        if district_match:
                            data['district'] = district_match.group(1).title()
                        # Look for location after district
                        location_match = re.search(r'r\.,\s*([^,]+)', part)
                        if location_match:
                            data['location'] = location_match.group(1).strip()
                    if 'm.' in part:
                        metro_match = re.search(r'(\w+)\s*m\.', part)
                        if metro_match:
                            data['metro_station'] = metro_match.group(1).title()
                    
                    # Extract area
                    area_match = re.search(r'(\d+(?:\.\d+)?)\s*m²', part)
                    if area_match:
                        data['area'] = float(area_match.group(1))
            
            # Extract price
            price_elem = soup.select_one('.desc_block span.price')
            if price_elem:
                price_text = price_elem.text.strip()
                data['price'] = self.extract_number(price_text)
                data['currency'] = 'AZN'
            
            # Extract description
            desc_elem = soup.select_one('.desc_block .text p')
            if desc_elem:
                data['description'] = desc_elem.text.strip()
                # Try to extract metro from description if not found in title
                if 'metro_station' not in data:
                    metro_match = re.search(r'(\w+)\s*metro', data['description'].lower())
                    if metro_match:
                        data['metro_station'] = metro_match.group(1).title()
            
            # Extract coordinates
            map_elem = soup.select_one('#map')
            if map_elem:
                try:
                    data['latitude'] = float(map_elem.get('data-lat', 0))
                    data['longitude'] = float(map_elem.get('data-lng', 0))
                except (ValueError, TypeError):
                    pass
            
            # Extract contact info
            contact_elem = soup.select_one('.contact .user')
            if contact_elem:
                data['contact_name'] = contact_elem.text.strip()
                data['contact_type'] = 'owner'  # Default to owner
            
            # Extract phone number
            phone_elem = soup.select_one('.links .active')
            if phone_elem:
                number = phone_elem.get('number') or phone_elem.text.strip()
                if number:
                    data['contact_phone'] = re.sub(r'\s+', '', number).strip()
            
            # Extract stats (views, dates)
            for stat in soup.select('.stats .rw'):
                label = stat.select_one('div:first-child')
                value = stat.select_one('div:last-child')
                if not (label and value):
                    continue
                    
                label_text = label.text.strip().lower()
                value_text = value.text.strip()
                
                if 'yeniləndi' in label_text:
                    try:
                        data['listing_date'] = datetime.datetime.strptime(value_text, '%d.%m.%Y').date()
                    except ValueError:
                        pass
                elif 'baxış sayı' in label_text:
                    try:
                        data['views_count'] = int(re.sub(r'\D', '', value_text))
                    except ValueError:
                        pass
            
            # Extract property details from params block
            for param in soup.select('.params_block .params .rw'):
                label = param.select_one('div:first-child')
                value = param.select_one('div:last-child')
                if not (label and value):
                    continue
                    
                label_text = label.text.strip().lower()
                value_text = value.text.strip()
                
                if 'sahə' in label_text:
                    area = self.extract_number(value_text)
                    if area:
                        data['area'] = area
                elif 'mərtəbə' in label_text:
                    floor_match = re.search(r'(\d+)/(\d+)', value_text)
                    if floor_match:
                        data['floor'] = int(floor_match.group(1))
                        data['total_floors'] = int(floor_match.group(2))
                elif 'otaq sayı' in label_text:
                    rooms = self.extract_number(value_text)
                    if rooms:
                        data['rooms'] = int(rooms)
                elif 'təmir' in label_text:
                    data['has_repair'] = any(x in value_text.lower() for x in ['əla', 'təmirli', 'yaxşı'])
                elif 'sənədin tipi' in label_text:
                    data['has_document'] = any(x in value_text.lower() for x in ['çıxarış', 'kupça'])
            
            # Extract photos with deduplication
            photos = set()
            for photo in soup.select('a[data-fancybox="gallery_ads_view"]'):
                src = photo.get('href')
                if src and not src.endswith('load.gif'):
                    photos.add(self.BASE_URL + src)
            
            if photos:
                data['photos'] = json.dumps(list(photos))
            
            # Determine listing type from text
            text = ' '.join(filter(None, [data.get('title', ''), data.get('description', '')]))
            text_lower = text.lower()
            if 'kirayə' in text_lower or 'icarə' in text_lower:
                if 'günlük' in text_lower:
                    data['listing_type'] = 'daily'
                else:
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
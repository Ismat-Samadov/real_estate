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
                
                # Extract address/title
                title_elem = anchor.select_one('span.title')
                title = title_elem.text.strip() if title_elem else None
                
                # Extract price
                price_elem = anchor.select_one('span.img span.price')
                price = self.extract_number(price_elem.text) if price_elem else None
                
                # Check if document exists (Sənəd var)
                has_document = bool(anchor.select_one('span.reg[data-title="Sənəd var"]'))
                
                # Initialize fields
                area = None
                rooms = None
                listing_date = None
                property_type = None
                district = None
                metro_station = None
                location = None
                
                # Parse description spans
                desc_spans = anchor.select('span.desc')
                for desc in desc_spans:
                    desc_text = desc.text.strip()
                    
                    # Extract area
                    if 'Sahəsi:' in desc_text:
                        area_match = re.search(r'Sahəsi:\s*([\d.]+)', desc_text)
                        if area_match:
                            area = float(area_match.group(1))
                    
                    # Extract rooms
                    rooms_span = desc.select_one('span:not([style])')
                    if rooms_span and 'Otaq sayı:' in rooms_span.text:
                        rooms_match = re.search(r'Otaq sayı:\s*(\d+)', rooms_span.text)
                        if rooms_match:
                            rooms = int(rooms_match.group(1))
                    
                    # Extract date
                    date_span = desc.select_one('span[style*="float: right"]')
                    if date_span:
                        date_text = date_span.text.strip()
                        if 'Bu gün' in date_text:
                            listing_date = datetime.datetime.now().date()
                    
                    # Extract property type, district, and metro station
                    if any(x in desc_text for x in ['Otaq', 'tikili', 'Evlər']):
                        parts = [p.strip() for p in desc_text.split(',')]
                        for part in parts:
                            part = part.strip().lower()
                            # Property type
                            if 'yeni tikili' in part:
                                property_type = 'new'
                            elif 'köhnə tikili' in part:
                                property_type = 'old'
                            elif any(x in part for x in ['ev', 'villa']):
                                property_type = 'house'
                            
                            # District
                            district_match = re.search(r'(\w+)\s*r\.', part)
                            if district_match:
                                district = district_match.group(1).title()
                            
                            # Metro station
                            metro_match = re.search(r'(\w+)\s*m\.', part)
                            if metro_match:
                                metro_station = metro_match.group(1).title()
                
                # Basic listing data
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'ipoteka.az',
                    'title': title,
                    'price': price,
                    'currency': 'AZN',
                    'area': area,
                    'rooms': rooms,
                    'property_type': property_type,
                    'district': district,
                    'metro_station': metro_station,
                    'location': location,
                    'has_document': has_document,
                    'listing_date': listing_date,
                    'created_at': datetime.datetime.now()
                }
                
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
            
            # Extract title and description
            title = soup.select_one('h2.title')
            if title:
                title_text = title.text.strip()
                data['title'] = title_text
                
                # Extract from title: property type, district, metro, rooms
                parts = [p.strip() for p in title_text.split(',')]
                for part in parts:
                    part = part.strip().lower()
                    
                    # Extract rooms
                    rooms_match = re.search(r'(\d+)\s*otaq', part)
                    if rooms_match:
                        data['rooms'] = int(rooms_match.group(1))
                    
                    # Extract property type
                    if 'yeni tikili' in part:
                        data['property_type'] = 'new'
                    elif 'köhnə tikili' in part:
                        data['property_type'] = 'old'
                    elif any(x in part for x in ['ev', 'villa']):
                        data['property_type'] = 'house'
                    
                    # Extract district
                    district_match = re.search(r'(\w+)\s*r\.', part)
                    if district_match:
                        data['district'] = district_match.group(1).title()
                    
                    # Extract metro
                    metro_match = re.search(r'(\w+)\s*m\.', part)
                    if metro_match:
                        data['metro_station'] = metro_match.group(1).title()
                    
                    # Extract area
                    area_match = re.search(r'(\d+(?:\.\d+)?)\s*m²', part)
                    if area_match:
                        data['area'] = float(area_match.group(1))
            
            # Extract description
            desc = soup.select_one('.desc_block .text')
            if desc:
                data['description'] = desc.text.strip()
            
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
            
            # Extract contact info and phone
            contact_elem = soup.select_one('.contact .user')
            if contact_elem:
                data['contact_type'] = 'owner'
                data['contact_name'] = contact_elem.text.strip()
            
            # Extract phone numbers
            for phone_div in soup.select('.links .active'):
                phone = phone_div.get('number') or phone_div.text.strip()
                if phone:
                    data['contact_phone'] = phone
                    break
            
            # Extract photos
            photos = []
            photo_elems = soup.select('[data-fancybox="gallery_ads_view"]')
            for photo in photo_elems:
                src = photo.get('href')
                if src and not src.endswith('load.gif'):
                    photos.append(self.BASE_URL + src if not src.startswith('http') else src)
            
            if photos:
                data['photos'] = json.dumps(photos)
            
            # Extract date and views
            stats = soup.select('.stats .rw')
            for stat in stats:
                text = stat.text.strip().lower()
                if 'yeniləndi' in text:
                    try:
                        date_str = text.split('yeniləndi')[-1].strip()
                        data['listing_date'] = datetime.datetime.strptime(date_str, '%d.%m.%Y').date()
                    except (ValueError, IndexError):
                        pass
                elif 'baxış' in text:
                    try:
                        views = int(re.search(r'\d+', text).group())
                        data['views_count'] = views
                    except (ValueError, AttributeError):
                        pass
            
            # Extract property details
            params = soup.select('.params_block .params .rw')
            for param in params:
                divs = param.select('div')
                if len(divs) != 2:
                    continue
                
                label = divs[0].text.strip().lower()
                value = divs[1].text.strip()
                
                if 'sahə' in label:
                    area = self.extract_number(value)
                    if area:
                        data['area'] = area
                elif 'mərtəbə' in label:
                    try:
                        floor, total = value.split('/')
                        data['floor'] = int(floor)
                        data['total_floors'] = int(total)
                    except:
                        pass
                elif 'otaq sayı' in label:
                    rooms = self.extract_number(value)
                    if rooms:
                        data['rooms'] = int(rooms)
                elif 'təmir' in label:
                    data['has_repair'] = 'təmirli' in value.lower()
                elif 'sənədin tipi' in label:
                    data['has_document'] = 'çıxarış' in value.lower() or 'kupça' in value.lower()
                
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
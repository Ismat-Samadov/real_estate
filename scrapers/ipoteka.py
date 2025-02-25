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
    
    # << ADDED THESE AT CLASS LEVEL >>
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
                
                # Parse components from raw_detail_title
                title_parts = [part.strip() for part in raw_detail_title.split(',')]
                for part in title_parts:
                    part_lower = part.lower().strip()
                    
                    # Extract rooms
                    if 'otaq' in part_lower:
                        rooms_match = re.search(r'(\d+)\s*otaq', part_lower)
                        if rooms_match:
                            data['rooms'] = rooms_match.group(1)
                    
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
                        data['area'] = area_match.group(1)
                        
                    # Also detect potential metro from the detail title
                    station_pattern = re.compile(
                        r'([A-Za-zƏIıİÖöĞğŞşÇçÜü]+(?:\\s+[A-Za-zƏIıİÖöĞğŞşÇçÜü]+)*)'
                        r'\\s*(?:m/s\\.?|m\\.?|metrosu\\.?|metrosunun|metro)',
                        re.IGNORECASE
                    )
                    station_match = station_pattern.search(part)
                    if station_match:
                        data['metro_station'] = station_match.group(1).strip()
            
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
            
            # Extract description
            desc_elem = soup.select_one('.desc_block .text p')
            if desc_elem:
                data['description'] = desc_elem.text.strip()
            
            # Extract location info from map
            map_elem = soup.select_one('#map')
            if map_elem:
                data['latitude'] = map_elem.get('data-lat')
                data['longitude'] = map_elem.get('data-lng')
            
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
                        phones.append(re.sub(r'\\s+', '', phone_number))
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
            
            # Extract property details
            params_block = soup.select_one('.params_block')
            if params_block:
                for row in params_block.select('.rw'):
                    label = row.select_one('div:first-child')
                    value = row.select_one('div:last-child')
                    if not (label and value):
                        continue
                    
                    label_text = label.text.strip().lower()
                    value_text = value.text.strip()
                    
                    if 'sahə' in label_text:
                        area_match = re.search(r'([\d.]+)', value_text)
                        if area_match:
                            data['area'] = area_match.group(1)
                    elif 'mərtəbə' in label_text:
                        floor_match = re.search(r'(\\d+)/(\\d+)', value_text)
                        if floor_match:
                            data['floor'] = int(floor_match.group(1))
                            data['total_floors'] = int(floor_match.group(2))
                    elif 'otaq sayı' in label_text:
                        rooms_match = re.search(r'(\\d+)', value_text)
                        if rooms_match:
                            data['rooms'] = rooms_match.group(1)
                    elif 'təmir' in label_text:
                        data['has_repair'] = any(
                            x in value_text.lower() for x in ['əla', 'təmirli', 'yaxşı']
                        )
                    elif 'sənədin tipi' in label_text:
                        data['has_document'] = (
                            'çıxarış' in value_text.lower() or 'kupça' in value_text.lower()
                        )
            
            # Extract photos
            photos = []
            photo_links = soup.select('a[data-fancybox=\"gallery_ads_view\"]')
            for link in photo_links:
                href = link.get('href')
                if href and not href.endswith('load.gif'):
                    if not href.startswith('http'):
                        href = f"{self.BASE_URL}{href}"
                    photos.append(href)
            
            if photos:
                data['photos'] = json.dumps(photos)
            
            # If no address, fall back to truncated title
            if 'address' not in data and data.get('title'):
                data['address'] = data['title']
            
            # Default to 'sale'
            data['listing_type'] = 'sale'
            
            # Default property type if not set
            if 'property_type' not in data:
                data['property_type'] = 'apartment'
            
            return data
            
        except Exception as e:
            # self.logger.error(f\"Error parsing listing detail {listing_id}: {str(e)}\")
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

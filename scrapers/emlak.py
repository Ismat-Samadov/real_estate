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

class EmlakAzScraper:
    """Scraper for emlak.az"""
    
    BASE_URL = "https://emlak.az"
    LISTINGS_URL = "https://emlak.az/elanlar/?ann_type=3&sort_type=0"

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

    def determine_listing_type(self, price_text: str, title_text: str) -> str:
        """Determine listing type based on price text and title"""
        lower_price = price_text.lower() if price_text else ""
        lower_title = title_text.lower() if title_text else ""
        
        if any(term in lower_price for term in ['/ay', '/gün', '/gun']):
            if '/gün' in lower_price or '/gun' in lower_price:
                return 'daily'
            return 'monthly'
        elif 'icarə' in lower_title or 'kirayə' in lower_title:
            return 'monthly'
        return 'sale'

    async def get_page_content(self, url: str, params: Optional[Dict] = None) -> str:
        """Fetch page content with retry logic and anti-bot measures"""
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
        DELAY = int(os.getenv('REQUEST_DELAY', 1))
        
        self.logger.info(f"Attempting to fetch URL: {url}")
        start_time = time.time()
        
        for attempt in range(MAX_RETRIES):
            try:
                self.logger.info(f"Attempt {attempt + 1} of {MAX_RETRIES}")
                await asyncio.sleep(DELAY + random.random() * 2)
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        content = await response.text()
                        elapsed = time.time() - start_time
                        self.logger.info(f"Successfully fetched content in {elapsed:.2f} seconds")
                        return content
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
        """Extract numeric value from text with validation"""
        if not text:
            return None
        try:
            # Remove everything except digits and decimal point
            clean_text = re.sub(r'[^\d.]', '', text)
            
            # Handle multiple decimal points
            if clean_text.count('.') > 1:
                parts = clean_text.split('.')
                clean_text = parts[0] + '.' + ''.join(parts[1:])
            
            value = float(clean_text)
            
            # Validate reasonable ranges for area
            if value > 10000:  # Unreasonably large area
                return None
                
            return value
        except (ValueError, TypeError):
            return None

    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('div.ticket.clearfix'):
            try:
                # Extract listing URL and ID
                link_elem = listing.select_one('h6.title a')
                if not link_elem:
                    continue
                    
                listing_url = self.BASE_URL + link_elem['href']
                listing_id = re.search(r'/(\d+)-', listing_url).group(1)
                
                # Extract title
                title_text = link_elem.text.strip()
                
                # Extract price and determine listing type
                price_elem = listing.select_one('p.price')
                price_text = price_elem.text.strip() if price_elem else None
                price = self.extract_number(price_text) if price_text else None
                
                # Determine listing type from price text and title
                listing_type = self.determine_listing_type(price_text, title_text)
                
                # Extract basic info
                info_elem = listing.select_one('.info')
                floor = None
                if info_elem:
                    floor_match = re.search(r'Mərtəbə:\s*(\d+)', info_elem.text)
                    if floor_match:
                        floor = int(floor_match.group(1))
                
                # Extract area and rooms from title
                area = None
                rooms = None
                area_match = re.search(r'(\d+)\s*m[²2]', title_text)
                rooms_match = re.search(r'(\d+)\s*otaql[ıi]', title_text)
                
                if area_match:
                    area = float(area_match.group(1))
                if rooms_match:
                    rooms = int(rooms_match.group(1))
                
                # Basic listing data
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'emlak.az',
                    'title': title_text,
                    'price': price,
                    'currency': 'AZN',
                    'listing_type': listing_type,
                    'floor': floor,
                    'area': area,
                    'rooms': rooms,
                    'created_at': datetime.datetime.now()
                }
                
                # Extract description
                desc_elem = listing.select_one('.description p')
                if desc_elem:
                    listing_data['description'] = desc_elem.text.strip()
                
                # Extract property type from title
                lower_title = title_text.lower()
                if 'həyət evi' in lower_title:
                    listing_data['property_type'] = 'house'
                elif 'villa' in lower_title:
                    listing_data['property_type'] = 'villa'
                elif 'obyekt' in lower_title:
                    listing_data['property_type'] = 'commercial'
                elif 'yeni tikili' in lower_title:
                    listing_data['property_type'] = 'new'
                elif 'köhnə tikili' in lower_title:
                    listing_data['property_type'] = 'old'
                else:
                    listing_data['property_type'] = 'apartment'
                
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
                'source_website': 'emlak.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract title and identify metro/district
            title_elem = soup.select_one('h1.title')
            if title_elem:
                title_text = title_elem.text.strip()
                data['title'] = title_text
                
                # Extract metro station from title (pattern: "Something m.")
                metro_match = re.search(r'(\w+)\s+m\.', title_text)
                if metro_match:
                    data['metro_station'] = metro_match.group(1).strip()
                
                # Extract district from title (pattern: "Something r.")
                district_match = re.search(r'(\w+)\s+r\.', title_text)
                if district_match:
                    data['district'] = district_match.group(1).strip()

            # Extract price information
            price_div = soup.select_one('div.price')
            if price_div:
                # Extract AZN price
                price_azn = price_div.select_one('span.m')
                if price_azn:
                    price_text = price_azn.text.strip()
                    price = self.extract_number(price_text)
                    if price and 0 < price < 1000000000:
                        data['price'] = price
                        data['currency'] = 'AZN'
                
                # Extract USD price if available
                price_usd = price_div.select_one('span.d')
                if price_usd:
                    usd_text = price_usd.text.strip()
                    usd_price = self.extract_number(usd_text)
                    if usd_price:
                        data['price_usd'] = usd_price

            # Extract address and coordinates
            address_elem = soup.select_one('.map-address h4')
            if address_elem:
                # Remove 'Ünvan:' and clean up the text
                address_text = address_elem.text.replace('Ünvan:', '').strip()
                if address_text:
                    # Store the full address
                    data['address'] = address_text
                    
                    # Try to extract more specific location information
                    address_parts = [part.strip() for part in address_text.split(',')]
                    
                    # If we have multiple parts, try to determine district/location
                    if len(address_parts) > 1:
                        # Usually first part is the street name
                        data['street'] = address_parts[0]
                        
                        # Look for district markers in other parts
                        for part in address_parts[1:]:
                            part = part.strip().lower()
                            if any(marker in part for marker in ['rayonu', 'район', 'r.']):
                                data['district'] = part.split('rayonu')[0].strip() if 'rayonu' in part else part
                            elif any(marker in part for marker in ['metro', 'm.']):
                                data['metro_station'] = part.split('metro')[0].strip() if 'metro' in part else part

            # Extract coordinates
            map_elem = soup.select_one('#google_map')
            if map_elem:
                coords_value = map_elem.get('value', '')
                coords_match = re.search(r'\(([\d.]+),\s*([\d.]+)\)', coords_value)
                if coords_match:
                    try:
                        lat = float(coords_match.group(1))
                        lon = float(coords_match.group(2))
                        if -90 <= lat <= 90 and -180 <= lon <= 180:
                            data['latitude'] = round(lat, 8)
                            data['longitude'] = round(lon, 8)
                    except (ValueError, TypeError):
                        pass

            # Extract views count
            views_elem = soup.select_one('.views-count strong')
            if views_elem:
                try:
                    data['views_count'] = int(views_elem.text.strip())
                except (ValueError, TypeError):
                    pass

            # Extract listing date
            date_elem = soup.select_one('.date strong')
            if date_elem:
                try:
                    data['listing_date'] = datetime.datetime.strptime(date_elem.text.strip(), '%d.%m.%Y').date()
                except ValueError:
                    pass

            # Extract description
            desc_elem = soup.select_one('.desc p')
            if desc_elem:
                data['description'] = desc_elem.text.strip()

            # Extract technical characteristics
            tech_chars = soup.select('dl.technical-characteristics dd')
            for char in tech_chars:
                label_elem = char.select_one('.label')
                if not label_elem:
                    continue
                    
                label = label_elem.text.strip().lower()
                value = char.text.replace(label_elem.text, '').strip()

                if 'sahə' in label:
                    area = self.extract_number(value)
                    if area and 5 <= area <= 1000:
                        data['area'] = area
                elif 'otaqların sayı' in label:
                    rooms = self.extract_number(value)
                    if rooms and 1 <= rooms <= 20:
                        data['rooms'] = int(rooms)
                elif 'yerləşdiyi mərtəbə' in label:
                    floor = self.extract_number(value)
                    if floor and 0 <= floor <= 50:
                        data['floor'] = int(floor)
                elif 'mərtəbə sayı' in label:
                    total_floors = self.extract_number(value)
                    if total_floors and 1 <= total_floors <= 50:
                        data['total_floors'] = int(total_floors)
                elif 'təmiri' in label:
                    data['has_repair'] = 'təmirli' in value.lower()
                elif 'əmlakın növü' in label:
                    prop_type = value.lower()
                    if 'köhnə tikili' in prop_type:
                        data['property_type'] = 'old'
                    elif 'yeni tikili' in prop_type:
                        data['property_type'] = 'new'
                    elif 'villa' in prop_type:
                        data['property_type'] = 'villa'
                    elif 'həyət evi' in prop_type:
                        data['property_type'] = 'house'
                    elif 'obyekt' in prop_type:
                        data['property_type'] = 'commercial'
                    else:
                        data['property_type'] = 'apartment'

            # Extract contact information
            seller_data = soup.select_one('.seller-data')
            if seller_data:
                name_seller = seller_data.select_one('.name-seller')
                if name_seller:
                    contact_name = name_seller.text.strip()
                    # Extract contact type from parentheses if present
                    contact_type_match = re.search(r'\((.*?)\)', contact_name)
                    if contact_type_match:
                        data['contact_type'] = contact_type_match.group(1)
                        data['contact_name'] = contact_name.split('(')[0].strip()
                    else:
                        data['contact_name'] = contact_name

                phone_elem = seller_data.select_one('.phone')
                if phone_elem:
                    data['contact_phone'] = phone_elem.text.strip()

            # Extract photos
            photos = []
            photo_elems = soup.select('.item-slider img[src]')
            for photo in photo_elems:
                src = photo.get('src')
                if src and not src.endswith('load.gif'):
                    photos.append(src if src.startswith('http') else f"{self.BASE_URL}{src}")
            
            if photos:
                data['photos'] = json.dumps(photos)

            # Determine listing type based on title and description
            if data.get('title') or data.get('description'):
                combined_text = (data.get('title', '') + ' ' + data.get('description', '')).lower()
                if 'günlük' in combined_text or 'gunluk' in combined_text:
                    data['listing_type'] = 'daily'
                elif 'kirayə' in combined_text or 'icarə' in combined_text:
                    data['listing_type'] = 'monthly'
                else:
                    data['listing_type'] = 'sale'

            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise
    
    async def scrape_listing_type(self, base_url: str, listing_type: str, pages: int = 2) -> List[Dict]:
        """Scrape specified number of pages for a given listing type"""
        results = []
        
        for page in range(1, pages + 1):
            try:
                # Prepare URL with page number
                url = f"{base_url}&page={page}"
                
                # Fetch and parse listings page
                html = await self.get_page_content(url)
                listings = await self.parse_listing_page(html, listing_type)
                
                # Fetch and parse each listing detail
                for listing in listings:
                    try:
                        detail_html = await self.get_page_content(listing['source_url'])
                        detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                        results.append({**listing, **detail_data})
                    except Exception as e:
                        self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"Error processing page {page}: {str(e)}")
                continue
                
        return results

    async def run(self, pages: int = 2):
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Starting Emlak.az scraper")
            await self.init_session()
            
            all_results = []
            
            for page in range(1, pages + 1):
                try:
                    # Construct URL with page parameter
                    url = f"{self.LISTINGS_URL}&page={page}"
                    
                    # Fetch and parse listings page
                    html = await self.get_page_content(url)
                    listings = await self.parse_listing_page(html)
                    
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
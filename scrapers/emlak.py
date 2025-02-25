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
    """Scraper for emlak.az with photo, coordinate, and address fixes"""
    
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
            clean_text = re.sub(r'[^\d.-]', '', text)
            
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

    def extract_metro_station(self, text: str) -> Optional[str]:
        """Extract metro station from text based on 'm.' pattern"""
        if not text:
            return None
            
        # Pattern to capture the word right before "m."
        metro_match = re.search(r'(\w+)\s+m\.', text)
        if metro_match:
            return metro_match.group(1).strip()
        
        # Try alternative patterns
        alt_patterns = [
            r'(\w+)\s+metro',          # "Nizami metro"
            r'(\w+)\s+metrosu',        # "Nizami metrosu"
            r'(\w+)\s+m/st'            # "Nizami m/st"
        ]
        
        for pattern in alt_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
                
        return None
        
    def extract_district(self, text: str) -> Optional[str]:
        """Extract district from text based on 'r.' pattern"""
        if not text:
            return None
            
        # Try various district patterns
        district_patterns = [
            r'(\w+)\s+r\.',           # "Yasamal r."
            r'(\w+)\s+ray\.',         # "Yasamal ray."
            r'(\w+)\s+rayonu',        # "Yasamal rayonu"
            r'(\w+)\s+rayon'          # "Yasamal rayon"
        ]
        
        for pattern in district_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
                
        return None

    def extract_floor_info(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        """Extract floor and total floors from text with "Mərtəbə: X/Y" pattern"""
        if not text:
            return None, None
            
        # Look for the common floor pattern: number/number
        floor_match = re.search(r'Mərtəbə:\s*(\d+)/(\d+)', text)
        if floor_match:
            try:
                floor = int(floor_match.group(1))
                total_floors = int(floor_match.group(2))
                return floor, total_floors
            except (ValueError, TypeError):
                pass
                
        return None, None

    def extract_coordinates(self, html: str) -> Tuple[Optional[float], Optional[float]]:
        """Extract latitude and longitude from HTML

        Example:
        <input type="hidden" value="(40.366672407736175, 49.81875555605118)" id="google_map">
        """
        soup = BeautifulSoup(html, 'lxml')
        map_elem = soup.select_one('#google_map')
        
        if map_elem:
            coords_value = map_elem.get('value', '')
            
            # Various coordinate patterns
            patterns = [
                r'\(([\d.]+),\s*([\d.]+)\)',             # (40.123, 49.123)
                r'([\d.]+)\s*,\s*([\d.]+)',              # 40.123, 49.123
                r'latitude=([\d.]+).*longitude=([\d.]+)'  # latitude=40.123...longitude=49.123
            ]
            
            for pattern in patterns:
                coords_match = re.search(pattern, coords_value)
                if coords_match:
                    try:
                        lat = float(coords_match.group(1))
                        lon = float(coords_match.group(2))
                        
                        # Validate coordinates are in Azerbaijan
                        if (38.0 <= lat <= 42.0 and  # Azerbaijan latitude range
                            44.5 <= lon <= 51.0):    # Azerbaijan longitude range
                            return lat, lon
                    except (ValueError, TypeError):
                        pass
                    
        return None, None

    def extract_address(self, html: str) -> Optional[str]:
        """Extract address from HTML
        
        Example:
        <h4>Ünvan: Yasamal rayonu, M.Müşfiq, Ə.Ələkbərov, İ.Qutqaşınlı və S.Dağlı küçələrinin kəsişməsi</h4>
        """
        soup = BeautifulSoup(html, 'lxml')
        address_elem = soup.select_one('.map-address h4')
        
        if address_elem:
            address_text = address_elem.text.strip()
            # Remove prefix if present
            if 'Ünvan:' in address_text:
                address_text = address_text.replace('Ünvan:', '').strip()
            return address_text
            
        return None

    def extract_photos(self, html: str) -> List[str]:
        """Extract photo URLs from HTML"""
        soup = BeautifulSoup(html, 'lxml')
        photos = []
        
        # Look for photos in the item-slider section
        img_elements = soup.select('.item-slider img[src]')
        
        for img in img_elements:
            src = img.get('src')
            if src and not src.endswith('load.gif'):
                # Make sure URL is absolute
                if not src.startswith('http'):
                    src = f"{self.BASE_URL}{src}"
                photos.append(src)
        
        # If no photos found, try alternative selectors
        if not photos:
            # Try fotorama slides
            for img in soup.select('.fotorama img[src]'):
                src = img.get('src')
                if src and not src.endswith('load.gif'):
                    if not src.startswith('http'):
                        src = f"{self.BASE_URL}{src}"
                    photos.append(src)
        
        return photos

    def extract_price(self, html: str) -> Dict[str, Optional[float]]:
        """Extract AZN and USD prices from HTML
        
        Example:
        <div class="price">
            <span class="m"><i></i> 253 820</span>
            <span class="d">$ 149 305.00</span>
        </div>
        """
        result = {'price': None, 'price_usd': None, 'currency': 'AZN'}
        soup = BeautifulSoup(html, 'lxml')
        
        price_div = soup.select_one('div.price')
        if price_div:
            # Extract AZN price (span.m)
            price_azn = price_div.select_one('span.m')
            if price_azn:
                # Get text content and clean it
                azn_text = price_azn.get_text(strip=True)
                azn_price = self.extract_number(azn_text)
                if azn_price and 0 < azn_price < 1000000000:
                    result['price'] = azn_price
            
            # Extract USD price (span.d)
            price_usd = price_div.select_one('span.d')
            if price_usd:
                # Get text content and clean it
                usd_text = price_usd.get_text(strip=True)
                usd_price = self.extract_number(usd_text)
                if usd_price and 0 < usd_price < 1000000000:
                    result['price_usd'] = usd_price
        
        return result

    def extract_amenities(self, html: str) -> Dict[str, any]:
        """Extract property amenities from technical characteristics
        
        Example:
        <dl class="technical-characteristics">
            <dd><span class="label">Əmlakın növü</span>Yeni tikili</dd>
            <dd><span class="label">Sahə</span>72.52 m<sup>2</sup></dd>
            <dd><span class="label">Otaqların sayı</span>1</dd>
            <dd><span class="label">Yerləşdiyi mərtəbə</span>8</dd>
            <dd><span class="label">Mərtəbə sayı</span>14</dd>
            <dd><span class="label">Təmiri</span>Təmirsiz</dd>
            <dd><span class="label">Sənədin tipi</span>Müqavilə</dd>
        </dl>
        """
        result = {}
        amenities_list = []
        soup = BeautifulSoup(html, 'lxml')
        
        # Process all technical characteristics
        for char in soup.select('dl.technical-characteristics dd'):
            label_elem = char.select_one('.label')
            if not label_elem:
                continue
                
            label = label_elem.text.strip().lower()
            # Get value by removing label from the full text
            value = char.get_text(strip=True).replace(label_elem.get_text(strip=True), '').strip()
            
            # Add to amenities list
            amenities_list.append(f"{label_elem.text.strip()}: {value}")
            
            # Also handle specific fields
            if 'sahə' in label:
                area = self.extract_number(value)
                if area and 5 <= area <= 1000:
                    result['area'] = area
            elif 'otaqların sayı' in label:
                rooms = self.extract_number(value)
                if rooms and 1 <= rooms <= 20:
                    result['rooms'] = int(rooms)
            elif 'yerləşdiyi mərtəbə' in label:
                floor = self.extract_number(value)
                if floor and 0 <= floor <= 200:
                    result['floor'] = int(floor)
            elif 'mərtəbə sayı' in label:
                total_floors = self.extract_number(value)
                if total_floors and 1 <= total_floors <= 200:
                    result['total_floors'] = int(total_floors)
            elif 'təmiri' in label:
                result['has_repair'] = 'təmirli' in value.lower()
            elif 'sənədin tipi' in label:
                result['document_type'] = value
        
        # Store the full list of amenities
        if amenities_list:
            result['amenities'] = json.dumps(amenities_list)
        
        return result

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
                listing_id = re.search(r'/(\d+)-', listing_url)
                if not listing_id:
                    continue
                listing_id = listing_id.group(1)
                
                # Extract title
                title_text = link_elem.text.strip()
                
                # Extract price and determine listing type
                price_elem = listing.select_one('p.price')
                price_text = price_elem.text.strip() if price_elem else None
                price = self.extract_number(price_text) if price_text else None
                
                # Determine listing type from price text and title
                listing_type = self.determine_listing_type(price_text, title_text)
                
                # Extract basic info including floor information
                info_elem = listing.select_one('.info')
                floor = None
                total_floors = None
                if info_elem:
                    # Extract floor information using the method
                    floor, total_floors = self.extract_floor_info(info_elem.text)
                
                # Extract area and rooms from title
                area = None
                rooms = None
                area_match = re.search(r'(\d+(?:\.\d+)?)\s*m[²2]', title_text)
                rooms_match = re.search(r'(\d+)\s*otaql[ıi]', title_text)
                
                if area_match:
                    area = float(area_match.group(1))
                if rooms_match:
                    rooms = int(rooms_match.group(1))
                
                # Extract metro station from title
                metro_station = self.extract_metro_station(title_text)
                
                # Extract district from title
                district = self.extract_district(title_text)
                
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
                    'total_floors': total_floors,
                    'area': area,
                    'rooms': rooms,
                    'metro_station': metro_station,
                    'district': district,
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
        """Parse the detailed listing page with optimized extraction for all fields"""
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'emlak.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Use BeautifulSoup only once for performance
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract title and identify metro/district
            title_elem = soup.select_one('h1.title')
            if title_elem:
                title_text = title_elem.text.strip()
                data['title'] = title_text
                
                # Extract metro station from title
                metro_station = self.extract_metro_station(title_text)
                if metro_station:
                    data['metro_station'] = metro_station
                
                # Extract district from title
                district = self.extract_district(title_text)
                if district:
                    data['district'] = district

            # Extract address
            address = self.extract_address(html)
            if address:
                data['address'] = address

            # Extract coordinates
            lat, lon = self.extract_coordinates(html)
            if lat is not None and lon is not None:
                data['latitude'] = round(lat, 8)
                data['longitude'] = round(lon, 8)

            # Extract views count directly
            views_elem = soup.select_one('.views-count strong')
            if views_elem:
                try:
                    data['views_count'] = int(views_elem.text.strip())
                except (ValueError, TypeError):
                    self.logger.warning(f"Failed to parse views count: {views_elem.text}")

            # Extract listing date directly
            date_elem = soup.select_one('.date strong')
            if date_elem:
                try:
                    data['listing_date'] = datetime.datetime.strptime(date_elem.text.strip(), '%d.%m.%Y').date()
                except ValueError:
                    self.logger.warning(f"Failed to parse date: {date_elem.text}")

            # Extract description
            desc_elem = soup.select_one('.desc p')
            if desc_elem:
                data['description'] = desc_elem.text.strip()

            # Extract floor information directly from technical characteristics
            floor_elem = soup.select_one('dd:-soup-contains("Yerləşdiyi mərtəbə")')
            if floor_elem:
                floor_text = floor_elem.text.replace('Yerləşdiyi mərtəbə', '').strip()
                try:
                    data['floor'] = int(re.search(r'\d+', floor_text).group())
                except (ValueError, AttributeError):
                    pass
            
            total_floors_elem = soup.select_one('dd:-soup-contains("Mərtəbə sayı")')
            if total_floors_elem:
                total_text = total_floors_elem.text.replace('Mərtəbə sayı', '').strip()
                try:
                    data['total_floors'] = int(re.search(r'\d+', total_text).group())
                except (ValueError, AttributeError):
                    pass

            # Extract other technical characteristics
            for char in soup.select('dl.technical-characteristics dd'):
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
            seller_elem = soup.select_one('.seller-data')
            if seller_elem:
                name_elem = seller_elem.select_one('.name-seller')
                if name_elem:
                    # Get full text including any spans
                    contact_text = name_elem.get_text(strip=True)
                    
                    # Extract name (text before parenthesis)
                    name_match = re.search(r'^([^(]+)', contact_text)
                    if name_match:
                        data['contact_name'] = name_match.group(1).strip()
                    
                    # Extract type (text in parentheses)
                    type_match = re.search(r'\(([^)]+)\)', contact_text)
                    if type_match:
                        contact_type = type_match.group(1).strip().lower()
                        if 'mülkiyyətçi' in contact_type:
                            data['contact_type'] = 'owner'
                        elif 'vasitəçi' in contact_type or 'agent' in contact_type:
                            data['contact_type'] = 'agent'
                        else:
                            data['contact_type'] = contact_type

                # Extract phone
                phone_elem = seller_elem.select_one('.phone')
                if phone_elem:
                    data['contact_phone'] = phone_elem.text.strip()

            # Extract price information with improved method
            price_data = self.extract_price(html)
            data.update(price_data)

            # Extract property amenities and details
            amenities_data = self.extract_amenities(html)
            data.update(amenities_data)
            # Extract photos
            photos = self.extract_photos(html)
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
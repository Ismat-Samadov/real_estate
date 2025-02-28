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

class TapAzScraper:
    """Scraper for tap.az real estate listings"""
    
    BASE_URL = "https://tap.az"
    LISTINGS_URL = "https://tap.az/elanlar/dasinmaz-emlak/menziller?keywords_source=typewritten"
    
    def __init__(self):
        """Initialize scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None

    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Accept': 'text/html, */*; q=0.01',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
                'Accept-Encoding': 'gzip, deflate, br',
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"macOS"',
                'DNT': '1'
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

    async def get_page_content(self, url: str, cursor: Optional[str] = None) -> str:
        """Fetch page content with retry logic and anti-bot measures"""
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
        DELAY = int(os.getenv('REQUEST_DELAY', 1))
        
        params = {'cursor': cursor} if cursor else None
        
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
            # Remove everything except digits and decimal point
            clean_text = re.sub(r'[^\d.]', '', text)
            return float(clean_text)
        except (ValueError, TypeError):
            return None

    def extract_area(self, text: str) -> Optional[float]:
        """Extract area value from text"""
        if not text:
            return None
        match = re.search(r'(\d+(?:\.\d+)?)\s*m²', text)
        if match:
            try:
                area = float(match.group(1))
                
                # Validate reasonable bounds
                if area < 5 or area > 10000:
                    self.logger.warning(f"Area value {area} m² outside reasonable bounds (5-10000)")
                    return None
                    
                # Round to 2 decimal places
                area = round(area, 2)
                
                # Ensure total digits don't exceed 10 (including decimal places)
                str_area = f"{area:.2f}".replace('.', '')
                if len(str_area) > 10:
                    self.logger.warning(f"Area value {area} exceeds maximum digits (10)")
                    return None
                    
                return area
                
            except (ValueError, TypeError) as e:
                self.logger.error(f"Error converting area value: {text} - {str(e)}")
                return None
        return None

    def extract_rooms(self, text: str) -> Optional[int]:
        """
        Extract number of rooms from text. Returns 0 if room count exceeds 20.
        
        Args:
            text (str): Text containing room information
            
        Returns:
            Optional[int]: Number of rooms, 0 if > 20 rooms, None if no valid number found
        """
        if not text:
            return None
            
        match = re.search(r'(\d+)-otaqlı', text)
        if match:
            try:
                rooms = int(match.group(1))
                if 1 <= rooms <= 20:  # Reasonable room range
                    return rooms
                elif rooms > 20:  # Handle cases with more than 20 rooms
                    return 0
            except (ValueError, TypeError):
                pass
                
        return None
    
    def extract_floor_info(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Extract floor information from text patterns like "Mərtəbə: 2/5" or "2/5 mərtəbə"
        
        Args:
            text (str): Text containing floor information
            
        Returns:
            Tuple of (current floor, total floors) if found, (None, None) otherwise
        """
        if not text:
            return None, None
            
        # Common floor patterns
        patterns = [
            r'mərtəbə:\s*(\d+)/(\d+)',  # Mərtəbə: 2/5
            r'(\d+)/(\d+)\s*mərtəbə',   # 2/5 mərtəbə
            r'mərtəbə\s*(\d+)/(\d+)',   # mərtəbə 2/5
            r'(\d+)-ci mərtəbə\/(\d+)', # 2-ci mərtəbə/5
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    current_floor = int(match.group(1))
                    total_floors = int(match.group(2))
                    
                    # Basic validation
                    if 0 <= current_floor <= 200 and 1 <= total_floors <= 200:
                        return current_floor, total_floors
                except (ValueError, IndexError):
                    pass
                    
        return None, None
    
    def extract_coordinates(self, html: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract coordinates from the map element in the HTML.
        
        Args:
            html: HTML content of the listing detail page
            
        Returns:
            Tuple of (latitude, longitude) if found, (None, None) otherwise
        """
        # For tap.az items, try different patterns to capture coordinates
        
        # First try to look for any explicit lat/lon in the page (most common in tap.az)
        patterns = [
            # Standard patterns from various map implementations
            r'lat="([^"]+)".*?lon="([^"]+)"',
            r'data-lat="([^"]+)".*?data-lng="([^"]+)"',
            r'data-lat="([^"]+)".*?data-lon="([^"]+)"',
            # Google maps patterns
            r'google_map.*?value="\(([\d.]+),\s*([\d.]+)\)"',
            r'center=([\d.]+),([\d.]+)',
            # Leaflet patterns
            r'L\.marker\(\[([\d.]+),\s*([\d.]+)\]\)',
            # General coordinate text patterns
            r'coordinates.*?([\d.]+),\s*([\d.]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
            if match:
                try:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    # Validate reasonable bounds for Azerbaijan
                    if 38.0 <= lat <= 42.0 and 44.5 <= lon <= 51.0:
                        return lat, lon
                except (ValueError, TypeError, IndexError):
                    pass
        
        # Additional pattern for google maps embed
        iframe_match = re.search(r'google\.com/maps/embed.*?q=([\d.]+),([\d.]+)', html)
        if iframe_match:
            try:
                lat = float(iframe_match.group(1))
                lon = float(iframe_match.group(2))
                if 38.0 <= lat <= 42.0 and 44.5 <= lon <= 51.0:
                    return lat, lon
            except (ValueError, TypeError):
                pass
        
        return None, None
    
    def extract_amenities(self, html: str) -> Optional[str]:
        """
        Extract amenities from the listing HTML.
        
        Args:
            html: HTML content of the listing detail page
            
        Returns:
            JSON string of amenities if found, None otherwise
        """
        soup = BeautifulSoup(html, 'lxml')
        amenities = []
        
        # Look for property details section
        for prop in soup.select('.product-properties__i'):
            label = prop.select_one('.product-properties__i-name')
            value = prop.select_one('.product-properties__i-value')
            
            if label and value:
                amenities.append(f"{label.text.strip()}: {value.text.strip()}")
        
        # Look for other amenity sections if available
        amenity_sections = soup.select('.amenities, .features, .property-features')
        for section in amenity_sections:
            for item in section.select('li, .item'):
                text = item.text.strip()
                if text and text not in amenities:
                    amenities.append(text)
        
        # Extract features from description
        desc_elem = soup.select_one('.product-description__content')
        if desc_elem:
            desc_text = desc_elem.text.strip()
            # Look for features marked with bullet points or dashes
            bullet_items = re.findall(r'[•\-\*]\s*([^\n•\-\*]+)', desc_text)
            for item in bullet_items:
                item_text = item.strip()
                if item_text and len(item_text) < 100 and item_text not in amenities:
                    amenities.append(item_text)
        
        if amenities:
            return json.dumps(amenities)
        return None

    async def get_phone_numbers(self, listing_id: str) -> List[str]:
        """Fetch phone numbers for a listing using the tap.az API with proper proxy support"""
        try:
            url = f"https://tap.az/ads/{listing_id}/phones"
            
            # Keep the original headers exactly as they were
            headers = {
                'Accept': '*/*',
                'Origin': 'https://tap.az',
                'Referer': f'https://tap.az/elanlar/dasinmaz-emlak/menziller/{listing_id}',
                'X-Requested-With': 'XMLHttpRequest',
                'DNT': '1',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'Priority': 'u=1, i'
            }
            
            self.logger.info(f"Fetching phone numbers for listing {listing_id} with proxy: {self.proxy_url}")
            
            # The only change: explicitly pass the proxy_url to the request
            async with self.session.post(
                url,
                headers=headers,
                proxy=self.proxy_url,  # This is the only change - explicitly use the proxy
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('phones', [])
                else:
                    self.logger.warning(f"Failed to get phone numbers for listing {listing_id}: Status {response.status}")
                    return []
                    
        except Exception as e:
            self.logger.error(f"Error fetching phone numbers for listing {listing_id}: {str(e)}")
            return []

    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('.products-i'):
            try:
                # Get listing URL and ID
                link = listing.select_one('a.products-link')
                if not link:
                    continue
                    
                listing_url = self.BASE_URL + link['href']
                listing_id = link['href'].split('/')[-1]
                
                # Extract price
                price_elem = listing.select_one('.price-val')
                price = self.extract_number(price_elem.text) if price_elem else None
                
                # Extract title and metadata
                title = listing.select_one('.products-name')
                title_text = title.text.strip() if title else None
                
                # Extract area and rooms
                area = None
                rooms = None
                
                # Try to extract area and rooms from both title and description
                for text in [title_text, listing.select_one('.products-description')]:
                    if text:
                        if area is None:
                            area = self.extract_area(text)
                        if rooms is None:
                            rooms = self.extract_rooms(text)
                
                # Extract location and date
                location_elem = listing.select_one('.products-created')
                if location_elem:
                    location_parts = location_elem.text.strip().split(', ')
                    location = location_parts[0] if len(location_parts) > 0 else None
                    
                # Basic listing data
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'tap.az',
                    'title': title_text,
                    'price': price,
                    'currency': 'AZN',
                    'area': area,
                    'rooms': rooms,
                    'location': location,
                    'created_at': datetime.datetime.now()
                }
                
                # Extract listing type from URL
                if 'kiraye' in listing_url.lower():
                    listing_data['listing_type'] = 'daily' if 'gunluk' in listing_url.lower() else 'monthly'
                else:
                    listing_data['listing_type'] = 'sale'
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
                
        return listings

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse the detailed listing page and fetch additional data"""
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'tap.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract description
            desc_elem = soup.select_one('.product-description__content')
            if desc_elem:
                data['description'] = desc_elem.text.strip()
            
            # Extract property details
            for prop in soup.select('.product-properties__i'):
                label = prop.select_one('.product-properties__i-name')
                value = prop.select_one('.product-properties__i-value')
                
                if not label or not value:
                    continue
                    
                label_text = label.text.strip().lower()
                value_text = value.text.strip()
                
                if 'sahə' in label_text:
                    area = self.extract_area(value_text)
                    if area is not None:  # Only update if we got a valid area
                        data['area'] = area
                    else:
                        # Try to extract just the number if area extraction failed
                        try:
                            num = float(re.sub(r'[^\d.]', '', value_text))
                            if 5 <= num <= 10000:
                                data['area'] = round(num, 2)
                        except (ValueError, TypeError):
                            pass
                elif 'yerləşmə yeri' in label_text:
                    data['location'] = value_text
                    # For district, if location contains "qəs." (settlement), extract it as district
                    if 'qəs.' in value_text:
                        data['district'] = value_text.replace('qəs.', '').strip()
                    else:
                        # Try other district patterns
                        district_patterns = [
                            r'(\w+)\s*r\.',  # matches "Yasamal r."
                            r'(\w+)\s*ray\.',  # matches "Yasamal ray."
                            r'(\w+)\s*rayonu',  # matches "Yasamal rayonu"
                            r'(\w+)\s*rayon',   # matches "Yasamal rayon"
                            r'(\w+)\s*district' # matches "Yasamal district"
                        ]
                        
                        for pattern in district_patterns:
                            district_match = re.search(pattern, value_text, re.IGNORECASE)
                            if district_match:
                                data['district'] = district_match.group(1).strip()
                                break
                    
                    # Try to extract metro station with various patterns
                    metro_patterns = [
                        r'(\w+)\s*m\.',  # matches "Nizami m."
                        r'(\w+)\s*metro',  # matches "Nizami metro"
                        r'(\w+)\s*m/st',  # matches "Nizami m/st"
                        r'(\w+)\s*metro stansiyası'  # matches "Nizami metro stansiyası"
                    ]
                    
                    for pattern in metro_patterns:
                        metro_match = re.search(pattern, value_text, re.IGNORECASE)
                        if metro_match:
                            data['metro_station'] = metro_match.group(1).strip()
                            break
                            
                    # If location contains address-like information, update address field
                    if not any(x in value_text.lower() for x in ['metro', 'rayon', 'district']) and len(value_text) > 5:
                        data['address'] = value_text.strip()
                elif 'otaq sayı' in label_text:
                    try:
                        rooms = int(re.sub(r'[^\d]', '', value_text))
                        if 1 <= rooms <= 20:
                            data['rooms'] = rooms
                    except (ValueError, TypeError):
                        pass
                elif 'mərtəbə' in label_text:
                    # Extract floor information
                    floor_match = re.search(r'(\d+)/(\d+)', value_text)
                    if floor_match:
                        try:
                            floor = int(floor_match.group(1))
                            total_floors = int(floor_match.group(2))
                            if 0 <= floor <= 200 and 1 <= total_floors <= 200:
                                data['floor'] = floor
                                data['total_floors'] = total_floors
                        except (ValueError, IndexError):
                            pass
                elif 'elanın tipi' in label_text:
                    if 'kirayə' in value_text.lower():
                        data['listing_type'] = 'monthly'
                    elif 'satış' in value_text.lower():
                        data['listing_type'] = 'sale'
                elif 'binanın tipi' in label_text or 'əmlakın növü' in label_text:
                    if 'yeni tikili' in value_text.lower():
                        data['property_type'] = 'new'
                    elif 'köhnə tikili' in value_text.lower():
                        data['property_type'] = 'old'
                    elif 'həyət evi' in value_text.lower():
                        data['property_type'] = 'house'
                    elif 'mənzil' in value_text.lower():
                        data['property_type'] = 'apartment'
            
            # Extract floor information if not already found
            if 'floor' not in data or 'total_floors' not in data:
                for info_elem in soup.select('.product-properties, .product-description__content'):
                    text = info_elem.text.strip().lower()
                    floor, total = self.extract_floor_info(text)
                    if floor is not None and 'floor' not in data:
                        data['floor'] = floor
                    if total is not None and 'total_floors' not in data:
                        data['total_floors'] = total
            
            # Extract amenities
            amenities = self.extract_amenities(html)
            if amenities:
                data['amenities'] = amenities
            
            # Extract coordinates
            lat, lon = self.extract_coordinates(html)
            if lat and lon:
                data['latitude'] = lat
                data['longitude'] = lon
            
            # Get phone numbers from API
            phones = await self.get_phone_numbers(listing_id)
            if phones:
                # Clean up phone number format
                phone = phones[0].replace('(', '').replace(')', '').replace('-', '').replace(' ', '')
                data['contact_phone'] = phone
            
            # Check WhatsApp availability
            whatsapp_elem = soup.select_one('.wp_status_ico')
            data['whatsapp_available'] = bool(whatsapp_elem)
            
            # Get seller info
            seller_info = soup.select_one('.product-owner__info-name')
            if seller_info:
                data['contact_type'] = seller_info.text.strip()
            
            # Extract photos
            photos = []
            photo_elems = soup.select('.product-photos__slider-top img')
            for img in photo_elems:
                src = img.get('src')
                if src and not src.endswith('load.gif'):
                    photos.append(src)
            
            if photos:
                data['photos'] = json.dumps(photos)
            
            # Extract timestamps
            info_stats = soup.select('.product-info__statistics__i-text')
            for stat in info_stats:
                if 'Bugün' in stat.text:
                    data['listing_date'] = datetime.date.today()
                elif 'Baxışların sayı' in stat.text:
                    try:
                        views = int(re.search(r'\d+', stat.text).group())
                        data['views_count'] = views
                    except (ValueError, AttributeError):
                        pass
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise

    async def run(self, pages: int = 2) -> List[Dict]:
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Starting Tap.az scraper")
            await self.init_session()
            
            all_results = []
            cursor = None
            
            for page in range(pages):
                try:
                    # Fetch and parse listings page
                    html = await self.get_page_content(self.LISTINGS_URL, cursor)
                    listings = await self.parse_listing_page(html)
                    
                    # Update cursor for next page if available
                    cursor_match = re.search(r'cursor=([^"]+)', html)
                    if cursor_match:
                        cursor = cursor_match.group(1)
                    
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
                    self.logger.error(f"Error processing page {page + 1}: {str(e)}")
                    continue
                    
                # Add delay between pages
                await asyncio.sleep(random.uniform(1, 2))
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        finally:
            await self.close_session()
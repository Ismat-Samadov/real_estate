import asyncio
import aiohttp
import logging
from typing import Dict, List, Optional, Tuple
import datetime
import json
import re
import random
from bs4 import BeautifulSoup

class LalafoScraper:
    """Scraper for lalafo.az with more robust detail parsing to extract additional fields."""
    
    BASE_URL = "https://lalafo.az"
    SEARCH_URL = "https://lalafo.az/azerbaijan/nedvizhimost"
    
    def __init__(self):
        """Initialize the scraper"""
        self.logger = logging.getLogger(__name__)
        self.session = None
        self.proxy_url = None  # Will be set by proxy_handler

    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,az;q=0.8,ru;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
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
        MAX_RETRIES = 5
        
        for attempt in range(MAX_RETRIES):
            try:
                # Add delay between requests
                await asyncio.sleep(1 + random.random())
                
                self.logger.info(f"Fetching URL: {url} (Attempt {attempt+1}/{MAX_RETRIES})")
                
                # Use proxy if available (will be set by proxy_handler)
                proxy_info = f" via proxy: {self.proxy_url}" if self.proxy_url else ""
                self.logger.info(f"Making request to {url}{proxy_info}")
                
                async with self.session.get(url, params=params, proxy=self.proxy_url) as response:
                    if response.status == 200:
                        self.logger.info(f"Successfully fetched {url} (Status 200)")
                        return await response.text()
                    else:
                        self.logger.warning(f"Failed to fetch {url}, status: {response.status}")
                        # Try to get error response
                        try:
                            error_text = await response.text()
                            self.logger.error(f"Error response preview: {error_text[:200]}")
                        except:
                            pass
                            
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        
            except Exception as e:
                self.logger.error(f"Error fetching {url}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} attempts")

    def extract_price(self, text: str) -> Tuple[Optional[float], str]:
        """Extract price and currency from text"""
        if not text:
            return None, 'AZN'
            
        # Clean up text
        text = text.replace('\u00a0', ' ').strip()
        
        # Determine currency
        currency = 'AZN'  # Default
        if '$' in text or 'USD' in text:
            currency = 'USD'
        elif '€' in text or 'EUR' in text:
            currency = 'EUR'
        
        # Extract numeric part
        try:
            clean_text = re.sub(r'[^\d.]', '', text.replace(',', '.').replace(' ', ''))
            if clean_text:
                return float(clean_text), currency
        except:
            pass
            
        return None, currency

    def extract_room_count(self, text: str) -> Optional[int]:
        """Extract room count from text"""
        if not text:
            return None
            
        patterns = [
            r'(\d+)\s*otaqlı',
            r'(\d+)\s*otaq',
            r'(\d+)(?:\s|-)?otaq'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    rooms = int(match.group(1))
                    if 0 < rooms <= 50:  # Reasonable range
                        return rooms
                except (ValueError, TypeError):
                    pass
        return None

    def extract_area(self, text: str) -> Optional[float]:
        """Extract area in square meters from text"""
        if not text:
            return None
            
        patterns = [
            r'(\d+(?:[\.,]\d+)?)\s*(?:kv\.?\s*m|m²)',
            r'(\d+(?:[\.,]\d+)?)\s*m2'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    area = float(match.group(1).replace(',', '.'))
                    if 5 <= area <= 10000:  # Reasonable range
                        return area
                except (ValueError, TypeError):
                    pass
        return None

    def extract_district(self, html: str) -> Optional[str]:
        """
        Extract district information from the listing HTML.
        Look specifically for "İnzibati rayonlar" parameter.
        
        Args:
            html: HTML content of the listing detail page
            
        Returns:
            District name if found, None otherwise
        """
        soup = BeautifulSoup(html, 'lxml')
        
        # Look for district in parameters list (most reliable source)
        for param_item in soup.select('.details-page__params li'):
            label = param_item.select_one('p')
            value = param_item.select_one('a')
            
            if label and value and 'İnzibati rayonlar' in label.text:
                district_text = value.text.strip()
                # Remove trailing "r." if present
                return re.sub(r'\s*r\.$', '', district_text)
        
        # Fallback: look for district pattern in the title or description
        combined_text = ''
        title = soup.select_one('h1.LFHeading')
        if title:
            combined_text += title.text + ' '
        
        desc = soup.select_one('.description__wrap')
        if desc:
            combined_text += desc.text
        
        # Try to find district patterns
        district_patterns = [
            r'(\w+)\s+r\.',           # "Xəzər r."
            r'(\w+)\s+rayonu',        # "Xəzər rayonu"
            r'(\w+)\s+r-nu',          # "Xəzər r-nu"
            r'(\w+)\s+rayon'          # "Xəzər rayon"
        ]
        
        for pattern in district_patterns:
            match = re.search(pattern, combined_text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None

    def extract_location(self, html: str) -> Optional[str]:
        """
        Extract specific location information from HTML.
        Focuses on the "Rayon" field which contains the specific area/settlement.
        
        Args:
            html: HTML content of the listing detail page
            
        Returns:
            Location name if found, None otherwise
        """
        soup = BeautifulSoup(html, 'lxml')
        
        # First look in the parameters for "Rayon" field
        for param_item in soup.select('.details-page__params li'):
            label = param_item.select_one('p')
            value = param_item.select_one('a')
            
            if label and value and 'Rayon:' in label.text:
                return value.text.strip()
        
        # Next, try to get the city from the map marker
        city_marker = soup.select_one('.map-with-city-marker p')
        if city_marker:
            return city_marker.text.strip()
        
        # Fallback: check breadcrumbs for location info
        breadcrumbs = soup.select('.detail-breadcrumbs__item')
        if len(breadcrumbs) > 3:  # Usually the 4th breadcrumb contains area info
            return breadcrumbs[3].text.strip()
        
        return None

    def extract_coordinates(self, html: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract coordinates from the map element in the HTML.
        Uses SVG path center point or other available coordinate data.
        
        Args:
            html: HTML content of the listing detail page
            
        Returns:
            Tuple of (latitude, longitude) if found, (None, None) otherwise
        """
        # Look for SVG path which contains the map marker
        center_match = re.search(r'class="leaflet-interactive".*?d="M([^,]+),([^ ]+)a', html)
        
        # If SVG center point is found, we need to convert from pixel coordinates
        # to geographical coordinates - this is complex and requires additional 
        # map metadata, so best to use a fallback approach
        
        # Search for any explicit lat/lon in the page
        lat_match = re.search(r'lat="([^"]+)"', html) or re.search(r'data-lat="([^"]+)"', html)
        lon_match = re.search(r'lon="([^"]+)"', html) or re.search(r'data-lng="([^"]+)"', html)
        
        if lat_match and lon_match:
            try:
                lat = float(lat_match.group(1))
                lon = float(lon_match.group(1))
                # Validate reasonable bounds for Azerbaijan
                if 38.0 <= lat <= 42.0 and 44.5 <= lon <= 51.0:
                    return lat, lon
            except (ValueError, TypeError):
                pass
        
        # Search for a ViewBox that might contain map bounds
        viewbox_match = re.search(r'viewBox="([^"]+)"', html)
        if viewbox_match:
            try:
                # Parse the SVG viewBox values
                values = viewbox_match.group(1).split()
                if len(values) == 4:
                    # This isn't directly convertible to lat/lon without additional data
                    # We would need the map projection parameters
                    pass
            except (ValueError, IndexError):
                pass
        
        return None, None

    def extract_address(self, html: str) -> Optional[str]:
        """
        Extract address information from the listing HTML.
        Combines location information with address details if available.
        
        Args:
            html: HTML content of the listing detail page
            
        Returns:
            Combined address string if found, None otherwise
        """
        soup = BeautifulSoup(html, 'lxml')
        address_parts = []
        
        # Try to get district
        district = self.extract_district(html)
        if district:
            address_parts.append(f"{district} r.")
        
        # Try to get specific location/settlement
        location = self.extract_location(html)
        if location:
            address_parts.append(location)
        
        # Look for any additional address info in the description
        description = soup.select_one('.description__wrap')
        if description:
            desc_text = description.text.strip()
            
            # Look for common address indicators in the text
            address_indicators = ["ünvan", "yerləşir", "küçəsi", "prospekti"]
            for indicator in address_indicators:
                if indicator in desc_text.lower():
                    # Find the sentence containing the address indicator
                    sentences = re.split(r'[.!?]', desc_text)
                    for sentence in sentences:
                        if indicator in sentence.lower():
                            # Clean and add this to address parts
                            cleaned = sentence.strip()
                            if len(cleaned) < 100:  # Only use reasonably sized address fragments
                                address_parts.append(cleaned)
                            break
                    break
        
        if address_parts:
            return ", ".join(address_parts)
        
        return None

    def extract_listing_type(self, text: str) -> str:
        """Determine listing type (sale, monthly, daily) from text"""
        text_lower = text.lower() if text else ''
        
        if 'günlük' in text_lower or 'sutkalıq' in text_lower:
            return 'daily'
        elif any(x in text_lower for x in ['kirayə', 'icarə', 'kirayələnir', 'aylıq']):
            return 'monthly'
        return 'sale'

    def extract_property_type(self, text: str) -> str:
        """Determine property type from text"""
        text_lower = text.lower() if text else ''
        
        if 'villa' in text_lower:
            return 'villa'
        elif 'yeni tikili' in text_lower or 'novostrojka' in text_lower:
            return 'new'
        elif 'köhnə tikili' in text_lower:
            return 'old'
        elif any(x in text_lower for x in ['həyət evi', 'fərdi ev']):
            return 'house'
        elif 'ofis' in text_lower:
            return 'office'
        elif 'obyekt' in text_lower or 'kommersiya' in text_lower:
            return 'commercial'
        return 'apartment'

    #
    # If you need to parse 'floor' or 'total_floors' from something like "5/16"
    # inside the title or description, you can add a small helper:
    #
    def extract_floor_info(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Extract current floor and total floors if we see patterns like "mərtəbə 5/16"
        or "5/16 mərtəbə" or "5 / 16 mertebe" in text. Adjust the pattern as needed.
        """
        pattern = r'(\d+)\s*/\s*(\d+)\s*mərtəbə'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                current_floor = int(match.group(1))
                total_floors = int(match.group(2))
                return (current_floor, total_floors)
            except:
                pass
        return (None, None)

    #
    # For listing page parse
    #
    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page HTML and extract listing information"""
        soup = BeautifulSoup(html, 'lxml')
        listings = []
        
        listing_cards = soup.select('article.ad-tile-horizontal')
        self.logger.info(f"Found {len(listing_cards)} listing cards on page")
        
        for card in listing_cards:
            try:
                link = card.select_one('a.ad-tile-horizontal-link')
                if not link:
                    continue
                href = link.get('href')
                if not href:
                    continue
                
                listing_url = f"{self.BASE_URL}{href}"
                id_match = re.search(r'id-(\d+)', href)
                if not id_match:
                    continue
                listing_id = id_match.group(1)
                
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'lalafo.az',
                    'created_at': datetime.datetime.now()
                }
                
                # Title
                title_elem = card.select_one('p.LFSubHeading')
                if title_elem:
                    listing_data['title'] = title_elem.text.strip()
                
                # Price
                price_elem = card.select_one('p.LFSubHeading span')
                if price_elem:
                    price, currency = self.extract_price(price_elem.text)
                    if price:
                        listing_data['price'] = price
                        listing_data['currency'] = currency
                
                # Category / listing type
                category_elem = card.select_one('.ad-tile-horizontal-header-description')
                if category_elem:
                    category_text = category_elem.text.strip()
                    listing_data['listing_type'] = self.extract_listing_type(category_text)
                    listing_data['property_type'] = self.extract_property_type(category_text)
                
                # Date
                date_elem = card.select_one('.ad-meta-info-default__time')
                if date_elem:
                    date_str = date_elem.text.strip()  # e.g. "25.02.2025 / 17:10"
                    try:
                        date_part = date_str.split('/')[0].strip()
                        listing_date = datetime.datetime.strptime(date_part, '%d.%m.%Y').date()
                        listing_data['listing_date'] = listing_date
                    except (ValueError, IndexError):
                        listing_data['listing_date'] = datetime.datetime.now().date()
                
                # # District or location
                # location_elem = card.select_one('.ad-meta-info-default__location')
                # if location_elem:
                #     location_text = location_elem.text.strip()
                #     # Attempt district parse
                #     district = self.extract_district(location_text)
                #     if district:
                #         listing_data['district'] = district
                #     else:
                #         # If it's not a recognized district pattern, store in 'location' for fallback
                #         listing_data['location'] = location_text
                
                # Quick attempt at rooms + area from the short title
                if 'title' in listing_data:
                    title = listing_data['title']
                    rooms = self.extract_room_count(title)
                    if rooms:
                        listing_data['rooms'] = rooms
                    area = self.extract_area(title)
                    if area:
                        listing_data['area'] = area

                # If you want floor info from the title (like "5/16 mərtəbə"):
                current_floor, total_floors = self.extract_floor_info(listing_data.get('title', ''))
                if current_floor:
                    listing_data['floor'] = current_floor
                if total_floors:
                    listing_data['total_floors'] = total_floors

                listings.append(listing_data)
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
        
        return listings

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """
        Parse the detailed listing page to extract all available information,
        including robust fallback extraction for district, address, metro_station, 
        rooms, area, property_type, etc.
        """
        soup = BeautifulSoup(html, 'lxml')

        data = {
            'listing_id': listing_id,
            'source_website': 'lalafo.az',
            'updated_at': datetime.datetime.now()  # Fallback if no 'updated_at' found
        }

        try:
            #
            # 1. Title
            #
            title_elem = soup.select_one('h1.AdViewContent__title, h1.AdPage__title, h1.LFHeading')
            if title_elem:
                data['title'] = title_elem.text.strip()
            else:
                data['title'] = ""

            #
            # 2. Description
            #
            desc_elem = soup.select_one('.AdViewContent__description, .description__wrap, .AdPageBody__description')
            if desc_elem:
                data['description'] = desc_elem.text.strip()
            else:
                data['description'] = ""

            #
            # 3. Price
            #
            price_elem = soup.select_one(
                '.AdViewContent__price-current, .AdViewPrice__price-current, '
                '.price, .ad-detail-price-container p.LFHeading, .AdPage__price'
            )
            if price_elem:
                extracted_price, currency = self.extract_price(price_elem.text)
                if extracted_price:
                    data['price'] = extracted_price
                    data['currency'] = currency

            #
            # 4. Parse Key-Value fields (param list)
            #
            params_list = soup.select('.AdViewParameters__item, .details-page__params li')
            amenities = []

            for param in params_list:
                label_elem = param.select_one('.AdViewParameters__title, p.LFParagraph')
                value_elem = param.select_one('.AdViewParameters__value, a.LFLink, p.LFParagraph:nth-child(2)')
                if not label_elem or not value_elem:
                    continue

                label_text = label_elem.get_text(strip=True).lower()
                value_text = value_elem.get_text(strip=True)

                if 'otaqların sayı' in label_text or 'otaq' in label_text:
                    match = re.search(r'(\d+)', value_text)
                    if match:
                        data['rooms'] = int(match.group(1))

                elif 'sahə' in label_text and 'm2' in label_text:
                    match = re.search(r'(\d+)', value_text)
                    if match:
                        data['area'] = float(match.group(1))

                elif 'torpaq sahəsi' in label_text:
                    match = re.search(r'(\d+)', value_text)
                    if match:
                        data['land_area'] = float(match.group(1))

                elif 'mərtəbələrin sayı' in label_text:
                    match = re.search(r'(\d+)', value_text)
                    if match:
                        data['total_floors'] = int(match.group(1))

                elif label_text.startswith('mərtəbə') and 'mərtəbələrin' not in label_text:
                    # e.g. "Mərtəbə: 5"
                    match = re.search(r'(\d+)', value_text)
                    if match:
                        data['floor'] = int(match.group(1))

                elif 'təklifin növü' in label_text:
                    if 'makler' in value_text.lower() or 'agent' in value_text.lower():
                        data['contact_type'] = 'agent'
                    else:
                        data['contact_type'] = 'owner'

                elif 'təmir' in label_text:
                    data['has_repair'] = True

                elif 'metro stansiyası' in label_text:
                    # e.g. "m. Əhmədli"
                    data['metro_station'] = re.sub(r'^m\.\s*', '', value_text).strip()

                elif 'inzibati rayonlar' in label_text:
                    # e.g. "Suraxanı r."
                    data['district'] = value_text.strip()

                # elif label_text.startswith('rayon'):
                #     # e.g. "Hövsan qəs."
                #     data['address'] = value_text.strip()

                elif 'kredit' in label_text:
                    if 'var' in value_text.lower():
                        data['has_credit'] = True

                elif any(x in label_text for x in ['kommunal xətlər', 'evin şəraiti', 'sənədlər']):
                    items = [i.strip() for i in value_text.split(',')]
                    for itm in items:
                        if itm and itm not in amenities:
                            amenities.append(itm)

            if amenities:
                data['amenities'] = ','.join(amenities)

            #
            # 5. Views Count
            #
            impressions_elem = soup.select_one('.impressions span.LFCaption')
            if impressions_elem:
                m = re.search(r'Göstərilmə:\s*(\d+)', impressions_elem.text)
                if m:
                    data['views_count'] = int(m.group(1))

            #
            # 6. Creation / Update dates
            #
            date_elems = soup.select('.about-ad-info__date')
            month_map = {
                'yan': 1, 'fev': 2, 'mar': 3, 'apr': 4, 'may': 5, 'iyn': 6,
                'iyl': 7, 'avq': 8, 'sen': 9, 'okt': 10, 'noy': 11, 'dek': 12
            }
            for elem in date_elems:
                txt = elem.get_text(strip=True).lower()
                if 'yaradılma' in txt:  # e.g. "Yaradılma vaxtı: 24 fev 2025"
                    match = re.search(r'(\d+)\s+(\w+)\s+(\d{4})', txt)
                    if match:
                        dd, month_str, yyyy = match.groups()
                        mm = month_map.get(month_str[:3], 0)
                        data['listing_date'] = datetime.date(int(yyyy), mm, int(dd))
                elif 'yenilənmə' in txt:
                    match = re.search(r'(\d+)\s+(\w+)\s+(\d{4})', txt)
                    if match:
                        dd, month_str, yyyy = match.groups()
                        mm = month_map.get(month_str[:3], 0)
                        data['updated_at'] = datetime.datetime(int(yyyy), mm, int(dd))

            #
            # 7. Phone / WhatsApp
            #
            phone_wrap = soup.select_one('.PhoneView__number, .phone-wrap')
            if phone_wrap:
                phone_text = phone_wrap.get_text(strip=True)
                data['contact_phone'] = re.sub(r'\s+', '', phone_text)

            whatsapp_elem = soup.select_one('.PhoneView__whatsapp, .whatsapp-icon')
            data['whatsapp_available'] = 1 if whatsapp_elem else 0

            #
            # 8. Seller Info
            #
            user_name = soup.select_one('.AdViewUser__name, .userName-text')
            if user_name:
                data['contact_name'] = user_name.get_text(strip=True)
                pro_label = soup.select_one('.AdViewUser__pro, .pro-label')
                if pro_label:
                    data['contact_type'] = 'agent'

            #
            # 9. Photos (existing + slick-dots fallback)
            #
            photos = []

            # Existing approach for .AdViewGallery__img-wrap or .carousel__img-wrap
            picture_elems = soup.select('.AdViewGallery__img-wrap picture, .carousel__img-wrap picture')
            for pic in picture_elems:
                source = pic.select_one('source[type="image/webp"]') or pic.select_one('source[type="image/jpeg"]')
                if source and source.get('srcset'):
                    src = source.get('srcset')
                    if src and not src.endswith(('load.gif', 'placeholder.png')):
                        photos.append(src)
                else:
                    img = pic.select_one('img[src]')
                    if img:
                        src = img.get('src')
                        if src and not src.endswith(('load.gif', 'placeholder.png')):
                            photos.append(src)

            # New approach for slick-dots
            slick_imgs = soup.select('.slick-dots.slick-thumb li a img[src]')
            for im in slick_imgs:
                src = im.get('src', '').strip()
                if src and not src.endswith(('load.gif', 'placeholder.png')):
                    photos.append(src)

            if photos:
                # Deduplicate
                unique_photos = []
                for p in photos:
                    if p not in unique_photos:
                        unique_photos.append(p)
                data['photos'] = json.dumps(unique_photos)

            #
            # 10. Potential city name from .map-with-city-marker
            #
            city_elem = soup.select_one('.map-with-city-marker p.LFParagraph')
            if city_elem:
                data['city'] = city_elem.get_text(strip=True)

            #
            # 11. Attempt lat/long if present (likely absent)
            #
            # If Lalafo doesn’t provide real lat/lng, we can't parse them from tile or <svg>.
            # If you ever see data-lat / data-lng, parse them here.

            #
            # 12. Extended fallback: parse missing fields from Title & Description
            #
            combined_text = (data['title'] + ' ' + data['description']).lower()

            # Rooms fallback
            if 'rooms' not in data:
                r = self.extract_room_count(combined_text)
                if r:
                    data['rooms'] = r

            # Area fallback
            if 'area' not in data:
                a = self.extract_area(combined_text)
                if a:
                    data['area'] = a

            # District fallback
            # if 'district' not in data:
            #     d = self.extract_district(combined_text)
            #     if d:
            #         data['district'] = d

            # Listing type fallback
            if 'listing_type' not in data:
                data['listing_type'] = self.extract_listing_type(combined_text)

            # Property type fallback
            if 'property_type' not in data:
                data['property_type'] = self.extract_property_type(combined_text)

            # has_repair fallback
            if 'has_repair' not in data:
                data['has_repair'] = any(x in combined_text for x in ['təmirli','yeni təmir','əla təmir'])

            #
            # 13. Additional fallback for Metro if in Title
            #
            # e.g. if the title says "28 may" or "Neftçilər" or "Koroğlu" we parse that:
            if 'metro_station' not in data or not data['metro_station']:
                # Very simplistic approach: if the title or desc has '28 may'
                # you can set data['metro_station'] = '28 May'
                # or parse with a dictionary of known station synonyms.
                known_metros = ['28 may', 'nizami', 'xalqlar dostluğu', 'nərimanov',
                                'koroğlu', 'nəftçilər', 'azadlıq prospekti']
                for station in known_metros:
                    if station in combined_text:
                        data['metro_station'] = station.title()
                        break

            # Enhanced extraction of location-related fields
            data['district'] = self.extract_district(html)
            data['location'] = self.extract_location(html)
            lat, lon = self.extract_coordinates(html)
            if lat is not None and lon is not None:
                data['latitude'] = lat
                data['longitude'] = lon
            data['address'] = self.extract_address(html)
            
            return data

        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise

    async def run(self, pages: int = 2) -> List[Dict]:
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Starting Lalafo.az scraper with updated detail parsing")
            await self.init_session()
            
            all_results = []
            for page in range(1, pages + 1):
                try:
                    url = self.SEARCH_URL if page == 1 else f"{self.SEARCH_URL}?page={page}"
                    self.logger.info(f"Processing page {page} at URL: {url}")
                    
                    html = await self.get_page_content(url)
                    listings = await self.parse_listing_page(html)
                    
                    if not listings:
                        self.logger.warning(f"No listings found on page {page}")
                        continue
                    
                    self.logger.info(f"Found {len(listings)} listings on page {page}")
                    
                    for idx, listing in enumerate(listings, 1):
                        try:
                            self.logger.info(
                                f"Processing listing {idx}/{len(listings)} on page {page}: {listing['source_url']}"
                            )
                            detail_html = await self.get_page_content(listing['source_url'])
                            detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                            
                            # Merge data
                            merged_data = {**listing, **detail_data}
                            all_results.append(merged_data)
                            
                            # Add short delay
                            await asyncio.sleep(1)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                            continue
                    
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    continue
                
                await asyncio.sleep(2)
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        finally:
            await self.close_session()
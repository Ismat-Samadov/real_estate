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

    def extract_metro_station(self, text: str) -> Optional[str]:
        """
        Extract metro station name from text using a predefined list of Baku metro stations.
        
        Args:
            text: Text to search for metro station names
            
        Returns:
            Metro station name if found, None otherwise
        """
        if not text:
            return None
            
        # Comprehensive list of Baku metro stations (both Azerbaijani and common names)
        metro_stations = [
            "20 Yanvar", "20 yanvar",
            "28 May", "28 may",
            "8 Noyabr", "8 noyabr",
            "Azadlıq prospekti", "azadlıq prospekti",
            "Avtovağzal", "avtovağzal",
            "Bakmil", "bakmil",
            "Cəfər Cabbarlı", "cəfər cabbarlı",
            "Dərnəgül", "dərnəgül",
            "Elmlər Akademiyası", "elmlər akademiyası",
            "Əhmədli", "əhmədli",
            "Gənclik", "gənclik",
            "Həzi Aslanov", "həzi aslanov",
            "Xalqlar dostluğu", "xalqlar dostluğu",
            "İçərişəhər", "içərişəhər",
            "İnşaatçılar", "inşaatçılar",
            "Koroğlu", "koroğlu",
            "Qara Qarayev", "qara qarayev",
            "Memar Əcəmi", "memar əcəmi",
            "Nəsimi", "nəsimi",
            "Nərimanov", "nərimanov",
            "Neftçilər", "neftçilər",
            "Nizami", "nizami",
            "Sahil", "sahil",
            "Xətai", "xətai",
            "Xocəsən", "xocəsən",
            "Ulduz", "ulduz"
        ]
        
        # Convert text to lowercase for case-insensitive matching
        text_lower = text.lower()
        
        # First try exact matches
        for station in metro_stations:
            station_lower = station.lower()
            
            # Look for the station name with various indicators
            patterns = [
                rf'\b{re.escape(station_lower)}\b',  # Exact match
                rf'\b{re.escape(station_lower)}\s+metro\b',  # Station metro
                rf'\b{re.escape(station_lower)}\s+m\.\b',  # Station m.
                rf'metro\s+{re.escape(station_lower)}\b',  # metro Station
                rf'm\.\s+{re.escape(station_lower)}\b',  # m. Station
                rf'{re.escape(station_lower)}\s+metrosu\b'  # Station metrosu
            ]
            
            for pattern in patterns:
                if re.search(pattern, text_lower):
                    # Return the properly capitalized version
                    if station.lower() == station:  # If we matched a lowercase version
                        # Find the corresponding capitalized version
                        idx = metro_stations.index(station)
                        if idx % 2 == 1:  # If it's an odd index, get the previous item
                            return metro_stations[idx - 1]
                        return station
                    return station
        
        # If no exact match found, try more general patterns
        general_patterns = [
            r'\b([A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+(?:\s+[A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+){0,2})\s+metro\s+stansiy',
            r'\b([A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+(?:\s+[A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+){0,2})\s+metrosu',
            r'\b([A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+(?:\s+[A-Za-zƏəIıİÖöĞğŞşÇçÜü0-9]+){0,2})\s+m\.'
        ]
        
        for pattern in general_patterns:
            match = re.search(pattern, text_lower)
            if match:
                candidate = match.group(1).strip()
                # Check if the candidate is similar to any known station
                for station in metro_stations:
                    station_lower = station.lower()
                    # Check for partial matches or station names without diacritics
                    if (station_lower in candidate or 
                        candidate in station_lower or
                        self._similarity_score(station_lower, candidate) > 0.7):
                        return station
        
        return None

    def _similarity_score(self, s1: str, s2: str) -> float:
        """
        Calculate a simple similarity score between two strings.
        Used to match metro stations with slight spelling variations.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Similarity score between 0 and 1
        """
        # Remove diacritics/accents for better matching
        def remove_diacritics(text):
            return text.replace('ə', 'e').replace('ı', 'i').replace('ö', 'o').replace('ü', 'u').replace('ğ', 'g').replace('ş', 's').replace('ç', 'c')
        
        s1_clean = remove_diacritics(s1)
        s2_clean = remove_diacritics(s2)
        
        # For very short strings, require exact match
        if len(s1) <= 3 or len(s2) <= 3:
            return 1.0 if s1_clean == s2_clean else 0.0
        
        # Count matching characters
        matches = sum(c1 == c2 for c1, c2 in zip(s1_clean, s2_clean))
        
        # Calculate Jaccard similarity for longer strings
        if len(s1_clean) > 0 and len(s2_clean) > 0:
            return matches / max(len(s1_clean), len(s2_clean))
        return 0.0

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """
        Parse the detailed listing page to extract all available information,
        including robust extraction of amenities from the details-page__params section.
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
            # 4. Parse Key-Value fields (param list) - ENHANCED EXTRACTION
            #
            amenities = []
            
            # Process the details-page__params list items
            for param_item in soup.select('.details-page__params li'):
                try:
                    label = param_item.select_one('p.LFParagraph')
                    if not label:
                        continue
                    
                    label_text = label.text.strip()
                    label_key = label_text.lower().replace(':', '')
                    
                    # Get all links (these are usually feature values) or fallback to paragraph text
                    value_links = param_item.select('a.LFLink')
                    value_text = ""
                    
                    if value_links:
                        # For multi-value fields (like "Kommunal xətlər"), collect all values
                        values = [link.text.strip() for link in value_links]
                        value_text = ', '.join(values)
                        
                        # Add each feature to amenities list in format "Label: Value"
                        if len(values) == 1:
                            amenities.append(f"{label_text} {values[0]}")
                        else:
                            amenities.append(f"{label_text} {value_text}")
                    else:
                        # Single value fields using paragraph
                        value_elem = param_item.select_one('p.LFParagraph:nth-child(2)')
                        if value_elem:
                            value_text = value_elem.text.strip()
                            amenities.append(f"{label_text} {value_text}")
                    
                    # Process specific fields based on label
                    if 'otaqların sayı' in label_key:
                        rooms_match = re.search(r'(\d+)', value_text)
                        if rooms_match:
                            data['rooms'] = int(rooms_match.group(1))
                    
                    elif 'sahə (m2)' in label_key:
                        area_match = re.search(r'(\d+)', value_text)
                        if area_match:
                            data['area'] = float(area_match.group(1))
                    
                    elif 'torpaq sahəsi' in label_key:
                        land_match = re.search(r'(\d+)', value_text)
                        if land_match:
                            data['land_area'] = float(land_match.group(1))
                    
                    elif 'mərtəbələrin sayı' in label_key:
                        floors_match = re.search(r'(\d+)', value_text)
                        if floors_match:
                            data['total_floors'] = int(floors_match.group(1))
                    
                    elif label_key.startswith('mərtəbə') and 'mərtəbələrin' not in label_key:
                        floor_match = re.search(r'(\d+)', value_text)
                        if floor_match:
                            data['floor'] = int(floor_match.group(1))
                    
                    elif 'təklifin növü' in label_key:
                        if value_links and len(value_links) > 0:
                            value_text = value_links[0].text.strip()
                        data['contact_type'] = 'agent' if any(x in value_text.lower() for x in ['makler', 'agent']) else 'owner'
                    
                    elif 'təmir' in label_key:
                        data['has_repair'] = True
                    
                    elif 'metro stansiyası' in label_key:
                        metro_name = re.sub(r'^m\.\s*', '', value_text).strip()
                        data['metro_station'] = metro_name
                    
                    elif 'inzibati rayonlar' in label_key:
                        district = re.sub(r'\s*r\.$', '', value_text).strip()
                        data['district'] = district
                    
                    elif 'sənədlər' in label_key:
                        if any(x in value_text.lower() for x in ['kupça', 'çıxarış']):
                            data['has_document'] = True
                    
                    elif 'kredit' in label_key:
                        if 'var' in value_text.lower():
                            data['has_credit'] = True
                    
                    # Special handling for multi-value fields
                    elif 'kommunal xətlər' in label_key:
                        if value_links:
                            utilities = [link.text.strip() for link in value_links]
                            for utility in utilities:
                                utility_lower = utility.lower()
                                if 'qaz' in utility_lower:
                                    data['has_gas'] = True
                                elif 'su' in utility_lower:
                                    data['has_water'] = True
                                elif 'işıq' in utility_lower:
                                    data['has_electricity'] = True
                                elif 'kombi' in utility_lower:
                                    data['has_heating'] = True
                                elif 'internet' in utility_lower:
                                    data['has_internet'] = True
                    
                    elif 'evin şəraiti' in label_key:
                        if value_links:
                            features = [link.text.strip() for link in value_links]
                            for feature in features:
                                feature_lower = feature.lower()
                                if 'eyvan' in feature_lower or 'balkon' in feature_lower:
                                    data['has_balcony'] = True
                                elif 'hasar' in feature_lower:
                                    data['has_fence'] = True
                                elif 'kürsülü' in feature_lower:
                                    data['has_plinth'] = True
                                elif 'zirzəmi' in feature_lower:
                                    data['has_basement'] = True
                    
                except Exception as e:
                    self.logger.warning(f"Error parsing parameter item: {str(e)}")
                    continue

            # Store amenities in data dictionary
            if amenities:
                data['amenities'] = json.dumps(amenities)

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
            # 10. Location and address information
            #
            # Enhanced extraction of location-related fields
            data['district'] = self.extract_district(html)
            data['location'] = self.extract_location(html)
            lat, lon = self.extract_coordinates(html)
            if lat is not None and lon is not None:
                data['latitude'] = lat
                data['longitude'] = lon
            data['address'] = self.extract_address(html)
            
            #
            # 11. Listing and property type
            #
            if 'listing_type' not in data:
                data['listing_type'] = self.extract_listing_type(data.get('title', '') + ' ' + data.get('description', ''))
                
            if 'property_type' not in data:
                data['property_type'] = self.extract_property_type(data.get('title', '') + ' ' + data.get('description', ''))

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

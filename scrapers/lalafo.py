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

    def extract_district(self, text: str) -> Optional[str]:
        """Extract district from text (simple heuristics)"""
        if not text:
            return None
            
        # Example patterns: "Nizami r.", "Sabunçu rayonu"
        # You can refine or localize for Azerbaijani district naming
        district_patterns = [
            r'([A-Za-z\u0400-\u04FF]+)\s*r\.',         # e.g. 'Nizami r.'
            r'([A-Za-z\u0400-\u04FF]+)\s*rayonu'       # e.g. 'Sabunçu rayonu'
        ]
        
        for pattern in district_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                district = match.group(1).strip()
                if district:
                    return district.capitalize()
                    
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
                
                # District or location
                location_elem = card.select_one('.ad-meta-info-default__location')
                if location_elem:
                    location_text = location_elem.text.strip()
                    # Attempt district parse
                    district = self.extract_district(location_text)
                    if district:
                        listing_data['district'] = district
                    else:
                        # If it's not a recognized district pattern, store in 'location' for fallback
                        listing_data['location'] = location_text
                
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

    #
    # For detailed listing page parse
    
    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """
        Parse the detailed listing page to extract all available information
        including fields like land_area, correct views_count, expanded amenities,
        and real updated_at from 'Yenilənmə tarixi:'.
        """
        soup = BeautifulSoup(html, 'lxml')
        
        data = {
            'listing_id': listing_id,
            'source_website': 'lalafo.az',
            # We will parse a real 'updated_at' from the listing. 
            # Meanwhile we can keep the fallback as "now" and overwrite below:
            'updated_at': datetime.datetime.now()
        }

        try:
            #
            # 1. Extract Title
            #
            title_elem = soup.select_one('h1.AdViewContent__title, h1.AdPage__title, h1.LFHeading')
            if title_elem:
                data['title'] = title_elem.text.strip()

            #
            # 2. Extract Description
            #
            desc_elem = soup.select_one('.AdViewContent__description, .description__wrap, .AdPageBody__description')
            if desc_elem:
                data['description'] = desc_elem.text.strip()

            #
            # 3. Extract Price
            #
            price_elem = soup.select_one('.AdViewContent__price-current, .AdViewPrice__price-current, .price')
            if not price_elem:
                # fallback older selectors
                price_elem = soup.select_one('.ad-detail-price-container p.LFHeading, .AdPage__price')
            if price_elem:
                price, currency = self.extract_price(price_elem.text)
                if price:
                    data['price'] = price
                    data['currency'] = currency

            #
            # 4. Key-Value Parameters
            #
            params_list = soup.select('.AdViewParameters__item, .details-page__params li')
            # We'll accumulate 'amenities' in a list
            amenities = []

            for param in params_list:
                label_elem = param.select_one('.AdViewParameters__title, p.LFParagraph')
                value_elem = param.select_one('.AdViewParameters__value, a.LFLink, p.LFParagraph:nth-child(2)')
                if not label_elem or not value_elem:
                    continue
                
                label_text = label_elem.get_text(strip=True).lower()
                value_text = value_elem.get_text(strip=True)

                # Rooms
                if 'otaqların sayı' in label_text or 'otaq' in label_text:
                    match = re.search(r'(\d+)', value_text)
                    if match:
                        data['rooms'] = int(match.group(1))

                # Area
                elif 'sahə' in label_text and 'm2' in label_text:
                    # e.g. "120" or "120 m2"
                    match = re.search(r'(\d+)', value_text)
                    if match:
                        data['area'] = float(match.group(1))

                # Land area (Sot)
                elif 'torpaq sahəsi' in label_text:
                    # They might show "Torpaq sahəsi (Sot): 3"
                    match = re.search(r'(\d+)', value_text)
                    if match:
                        data['land_area'] = float(match.group(1))

                # Total floors
                elif 'mərtəbələrin sayı' in label_text:
                    match = re.search(r'(\d+)', value_text)
                    if match:
                        data['total_floors'] = int(match.group(1))

                # Floor (if it’s an apartment)
                elif label_text.startswith('mərtəbə') and 'mərtəbələrin' not in label_text:
                    # e.g. "Mərtəbə: 5"
                    match = re.search(r'(\d+)', value_text)
                    if match:
                        data['floor'] = int(match.group(1))

                # "Təklifin növü" => agent/owner
                elif 'təklifin növü' in label_text:
                    if 'makler' in value_text.lower() or 'agent' in value_text.lower():
                        data['contact_type'] = 'agent'
                    else:
                        data['contact_type'] = 'owner'

                # "Təmir" => has_repair
                elif 'təmir' in label_text:
                    # e.g. "Yeni təmirli", "Əla təmirli"
                    data['has_repair'] = True

                # "metro stansiyası"
                elif 'metro stansiyası' in label_text:
                    # e.g. "m. Əhmədli"
                    data['metro_station'] = re.sub(r'^m\.\s*', '', value_text).strip()

                # "inzibati rayonlar"
                elif 'inzibati rayonlar' in label_text:
                    # e.g. "Suraxanı r."
                    district_clean = re.sub(r'\s*r\.$', '', value_text).strip()
                    data['district'] = district_clean

                # Another "Rayon:" or "Qəs."
                elif label_text.startswith('rayon'):
                    # e.g. "Hövsan qəs."
                    data['location'] = value_text

                # "Kredit" => has_credit
                elif 'kredit' in label_text:
                    if 'var' in value_text.lower():
                        data['has_credit'] = True

                # Some listings have lines like "Kommunal xətlər:", "Evin şəraiti:", "Sənədlər:"
                # that contain multiple comma-separated items. We treat them as amenities.
                elif any(x in label_text for x in ['kommunal xətlər', 'evin şəraiti', 'sənədlər']):
                    # Typically something like "Kombi, Qaz, Su, İnternet, İşıq"
                    # or "Eyvan (Balkon), Hasar"
                    # or "Kupça (Çıxarış)"
                    # We'll split by commas and add to `amenities`.
                    items = [i.strip() for i in value_text.split(',')]
                    for itm in items:
                        # Convert to lowercase or keep original?
                        # You can store them in original form or in English.  
                        if itm and itm not in amenities:
                            amenities.append(itm)

            if amenities:
                data['amenities'] = ','.join(amenities)

            #
            # 5. Correct Views Count from "Göstərilmə: 2545"
            #
            # Instead of picking up the smaller "40" from the eye icon, 
            # let's specifically target `.impressions span.LFCaption` 
            # and look for the text "Göstərilmə:".
            #
            impressions_elem = soup.select_one('.impressions span.LFCaption')
            if impressions_elem:
                # e.g. "Göstərilmə: 2545"
                m = re.search(r'Göstərilmə:\s*(\d+)', impressions_elem.text)
                if m:
                    data['views_count'] = int(m.group(1))

            #
            # 6. Extract Creation/Update Dates from ".about-ad-info__date"
            #
            date_elems = soup.select('.about-ad-info__date')
            # They might look like:
            #  - "Yaradılma vaxtı: 24 fev 2025"
            #  - "Yenilənmə tarixi: 25 fev 2025"
            month_map = {
                'yan': 1, 'fev': 2, 'mar': 3, 'apr': 4, 'may': 5, 'iyn': 6,
                'iyl': 7, 'avq': 8, 'sen': 9, 'okt': 10, 'noy': 11, 'dek': 12
            }

            for elem in date_elems:
                txt = elem.get_text(strip=True).lower()
                # "Yaradılma vaxtı: 24 fev 2025"
                if 'yaradılma' in txt or 'yaradılma vaxtı' in txt:
                    match = re.search(r'(\d+)\s+(\w+)\s+(\d{4})', txt)
                    if match:
                        day_str, month_str, year_str = match.groups()
                        dd = int(day_str)
                        mm = month_map.get(month_str[:3], 0)  # 'fev' -> 2
                        yyyy = int(year_str)
                        data['listing_date'] = datetime.date(yyyy, mm, dd)

                # "Yenilənmə tarixi: 25 fev 2025"
                if 'yenilənmə' in txt:
                    match = re.search(r'(\d+)\s+(\w+)\s+(\d{4})', txt)
                    if match:
                        day_str, month_str, year_str = match.groups()
                        dd = int(day_str)
                        mm = month_map.get(month_str[:3], 0)
                        yyyy = int(year_str)
                        # Overwrite 'updated_at' with real date:
                        data['updated_at'] = datetime.datetime(yyyy, mm, dd)

            #
            # 7. Extract Phone / WhatsApp
            #
            phone_wrap = soup.select_one('.PhoneView__number, .phone-wrap')
            if phone_wrap:
                phone_text = phone_wrap.get_text(strip=True)
                data['contact_phone'] = re.sub(r'\s+', '', phone_text)

            # For WhatsApp detection:
            whatsapp_elem = soup.select_one('.PhoneView__whatsapp, .whatsapp-icon')
            data['whatsapp_available'] = 1 if whatsapp_elem else 0

            #
            # 8. Extract Seller Info
            #
            user_name = soup.select_one('.AdViewUser__name, .userName-text')
            if user_name:
                data['contact_name'] = user_name.get_text(strip=True)
                pro_label = soup.select_one('.AdViewUser__pro, .pro-label')
                if pro_label:
                    data['contact_type'] = 'agent'

            #
            # 9. Extract Photos
            #
            photos = []
            picture_elems = soup.select('.AdViewGallery__img-wrap picture, .carousel__img-wrap picture')
            for pic in picture_elems:
                # Look for webp or jpeg <source>
                source = pic.select_one('source[type="image/webp"]') or pic.select_one('source[type="image/jpeg"]')
                if source and source.get('srcset'):
                    src = source.get('srcset')
                    if src and not src.endswith(('load.gif', 'placeholder.png')):
                        photos.append(src)
                else:
                    # Fallback <img>
                    img = pic.select_one('img[src]')
                    if img:
                        src = img.get('src')
                        if src and not src.endswith(('load.gif', 'placeholder.png')):
                            photos.append(src)

            if photos:
                unique_photos = []
                for p in photos:
                    if p not in unique_photos:
                        unique_photos.append(p)
                data['photos'] = json.dumps(unique_photos)

            #
            # 10. Fallback text-based extraction
            #
            combined_text = (data.get('title','') + ' ' + data.get('description','')).lower()
            # If we missed 'rooms' or 'area', etc., do a last fallback:
            if 'rooms' not in data:
                r = self.extract_room_count(combined_text)
                if r:
                    data['rooms'] = r
            if 'area' not in data:
                a = self.extract_area(combined_text)
                if a:
                    data['area'] = a
            if 'district' not in data:
                d = self.extract_district(combined_text)
                if d:
                    data['district'] = d
            if 'listing_type' not in data:
                data['listing_type'] = self.extract_listing_type(combined_text)
            if 'property_type' not in data:
                data['property_type'] = self.extract_property_type(combined_text)
            if 'has_repair' not in data:
                data['has_repair'] = any(x in combined_text for x in ['təmirli','yeni təmir','əla təmir'])

            return data

        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise


    #
    # Main run method
    #
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

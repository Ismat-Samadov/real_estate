# bina.py file contains the scraper class for bina.az real estate listings
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
from urllib.parse import urljoin


class BinaScraper:
    BASE_URL = "https://bina.az"
    LISTINGS_URL = "https://bina.az/items/all"
    
    # New constants for rate limiting
    MIN_DELAY = 3  # Minimum delay between requests
    MAX_DELAY = 7  # Maximum delay between requests
    PAGE_DELAY = 10  # Delay between pages
    MAX_RETRIES = 5  # Maximum number of retries per request
    BACKOFF_FACTOR = 2  # Exponential backoff multiplier
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.session = None
        self.proxy_url = None
        self.request_count = 0
        self.last_request_time = 0
        
    def _get_random_user_agent(self):
        """Generate a random user agent string"""
        browsers = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
        ]
        return random.choice(browsers)

    async def init_session(self):
        """Initialize session with enhanced headers and configuration"""
        if not self.session:
            # Enhanced browser-like headers
            headers = {
                'User-Agent': self._get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,az;q=0.8,ru;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0'
            }
            
            # Enhanced connector settings
            conn = aiohttp.TCPConnector(
                ssl=False,
                limit=3,  # Limit concurrent connections
                ttl_dns_cache=300,
                force_close=True,
                enable_cleanup_closed=True
            )
            
            # Enhanced timeout settings
            timeout = aiohttp.ClientTimeout(
                total=30,
                connect=10,
                sock_read=10,
                sock_connect=10
            )
            
            self.session = aiohttp.ClientSession(
                headers=headers,
                connector=conn,
                timeout=timeout,
                trust_env=True,
                cookie_jar=aiohttp.CookieJar(unsafe=True)
            )

    async def _wait_between_requests(self):
        """Implement intelligent delay between requests"""
        now = time.time()
        time_since_last = now - self.last_request_time
        
        # Calculate delay based on request count
        base_delay = random.uniform(self.MIN_DELAY, self.MAX_DELAY)
        if self.request_count > 10:
            base_delay *= 1.5
        if self.request_count > 20:
            base_delay *= 2
            
        # Ensure minimum time between requests
        if time_since_last < base_delay:
            await asyncio.sleep(base_delay - time_since_last)
            
        self.last_request_time = time.time()
        self.request_count += 1

    async def get_page_content(self, url: str, params: Optional[Dict] = None) -> str:
        """Enhanced page content fetching with better rate limiting handling"""
        for attempt in range(self.MAX_RETRIES):
            try:
                await self._wait_between_requests()
                
                # Enhanced request headers
                headers = {
                    'Referer': 'https://bina.az/',
                    'Origin': 'https://bina.az',
                    'Host': 'bina.az',
                    'User-Agent': self._get_random_user_agent(),
                    'X-Requested-With': 'XMLHttpRequest'
                }
                
                # Enhanced cookies
                cookies = {
                    'language': 'az',
                    '_ga': f'GA1.1.{random.randint(1000000, 9999999)}.{int(time.time())}',
                    '_gid': f'GA1.1.{random.randint(1000000, 9999999)}.{int(time.time())}',
                    '__cf_bm': f'{random.randbytes(32).hex()}',  # Simulate Cloudflare cookie
                }
                
                async with self.session.get(
                    url,
                    params=params,
                    headers={**self.session.headers, **headers},
                    cookies=cookies,
                    proxy=self.proxy_url,
                    timeout=aiohttp.ClientTimeout(total=20),
                    allow_redirects=True,
                    verify_ssl=False
                ) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status in [403, 429, 502, 503]:
                        # Enhanced backoff for rate limiting
                        backoff_time = self.MIN_DELAY * (self.BACKOFF_FACTOR ** attempt)
                        jitter = random.uniform(0, 3)
                        wait_time = backoff_time + jitter
                        
                        self.logger.warning(
                            f"Rate limited ({response.status}) on attempt {attempt + 1}, "
                            f"waiting {wait_time:.2f}s"
                        )
                        
                        # Reset session on rate limit
                        if attempt > 1:
                            await self.session.close()
                            self.session = None
                            await self.init_session()
                            
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise Exception(f"Unexpected status code: {response.status}")
                        
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout on attempt {attempt + 1}")
                backoff_time = self.MIN_DELAY * (self.BACKOFF_FACTOR ** attempt)
                await asyncio.sleep(backoff_time)
            except Exception as e:
                self.logger.error(f"Error on attempt {attempt + 1}: {str(e)}")
                if attempt == self.MAX_RETRIES - 1:
                    raise
                await asyncio.sleep(self.MIN_DELAY * (self.BACKOFF_FACTOR ** attempt))
        
        raise Exception(f"Failed to fetch {url} after {self.MAX_RETRIES} attempts")
       
    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    def extract_price(self, price_text: str) -> Optional[float]:
        """Extract numeric price from text"""
        if not price_text:
            return None
        try:
            # Remove all non-numeric characters
            price = re.sub(r'[^\d.]', '', price_text)
            return float(price) if price else None
        except (ValueError, TypeError):
            return None

    def extract_floor_info(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        """Extract floor and total floors from text"""
        if not text:
            return None, None
            
        try:
            # Handle formats like "5/9" or "5 / 9"
            parts = text.split('/')
            if len(parts) == 2:
                floor = int(re.search(r'\d+', parts[0]).group())
                total = int(re.search(r'\d+', parts[1]).group())
                return floor, total
        except (AttributeError, ValueError, IndexError):
            pass
            
        return None, None

    def detect_listing_type(self, title: Optional[str], base_type: str) -> str:
        """Detect specific listing type from title"""
        if not title:
            return base_type
            
        title_lower = title.lower()
        if base_type == 'monthly':
            if 'günlük' in title_lower:
                return 'daily'
            return 'monthly'
        return 'sale'

    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page to extract basic listing info with listing type detection"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        self.logger.debug("Parsing listings page")
        
        # Find all listing cards
        for listing in soup.select('.items_list .items-i'):
            try:
                # Extract listing URL and ID
                listing_id = listing.get('data-item-id')
                link = listing.select_one('a.item_link')
                
                if not link or not listing_id:
                    continue
                    
                listing_url = urljoin(self.BASE_URL, link.get('href', ''))
                
                # Extract price and detect listing type
                price = None
                listing_type = 'sale'  # Default type
                
                price_elem = listing.select_one('.price-val')
                price_container = listing.select_one('.price-per')
                
                if price_elem:
                    price = self.extract_price(price_elem.text.strip())
                    
                    # Detect listing type from price format
                    if price_container:
                        price_text = price_container.text.strip().lower()
                        if '/ay' in price_text or '/aylıq' in price_text:
                            listing_type = 'monthly'
                        elif '/gün' in price_text or '/günlük' in price_text:
                            listing_type = 'daily'
                
                # Extract title
                title_elem = listing.select_one('.card-title')
                title = title_elem.text.strip() if title_elem else None
                
                # Basic data from listing card
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'bina.az',
                    'price': price,
                    'currency': 'AZN',
                    'listing_type': listing_type,
                    'created_at': datetime.datetime.now(),
                    'title': title
                }
                
                # Extract location
                location = listing.select_one('.location')
                if location:
                    listing_data['location'] = location.text.strip()
                
                # Extract property details
                name_items = listing.select('.name li')
                for item in name_items:
                    text = item.text.strip().lower()
                    
                    # Extract room count
                    if 'otaq' in text:
                        try:
                            rooms = int(re.search(r'\d+', text).group())
                            if 1 <= rooms <= 20:  # Reasonable validation
                                listing_data['rooms'] = rooms
                        except (ValueError, AttributeError):
                            pass
                            
                    # Extract area
                    elif 'm²' in text:
                        try:
                            area = float(re.sub(r'[^\d.]', '', text))
                            if 5 <= area <= 1000:  # Reasonable validation
                                listing_data['area'] = area
                        except ValueError:
                            pass
                            
                    # Extract floor info
                    elif 'mərtəbə' in text:
                        floor, total_floors = self.extract_floor_info(text)
                        if floor is not None and 0 <= floor <= 100:  # Reasonable validation
                            listing_data['floor'] = floor
                        if total_floors is not None and 1 <= total_floors <= 100:  # Reasonable validation
                            listing_data['total_floors'] = total_floors
                
                # Extract repair status and other features
                features = []
                if listing.select_one('.repair'):
                    listing_data['has_repair'] = True
                    features.append('təmirli')
                
                if listing.select_one('.bill_of_sale'):
                    features.append('kupçalı')
                    
                if listing.select_one('.mortgage'):
                    features.append('ipoteka var')
                
                # Extract property type from listing
                property_type_elem = listing.select_one('.name')
                if property_type_elem:
                    property_text = property_type_elem.text.strip().lower()
                    if 'köhnə tikili' in property_text:
                        listing_data['property_type'] = 'old'
                    elif 'yeni tikili' in property_text:
                        listing_data['property_type'] = 'new'
                    elif 'həyət evi' in property_text or 'villa' in property_text:
                        listing_data['property_type'] = 'house'
                    elif 'ofis' in property_text:
                        listing_data['property_type'] = 'office'
                    elif 'qaraj' in property_text:
                        listing_data['property_type'] = 'garage'
                    elif 'torpaq' in property_text:
                        listing_data['property_type'] = 'land'
                    else:
                        listing_data['property_type'] = 'apartment'
                
                # Extract metro station and district from location
                location_text = listing_data.get('location', '').lower()
                if location_text:
                    # Extract metro station
                    if 'm.' in location_text:
                        metro_parts = location_text.split('m.')
                        if len(metro_parts) > 1:
                            listing_data['metro_station'] = metro_parts[1].split(',')[0].strip()
                    
                    # Extract district
                    if 'r.' in location_text:
                        district_parts = location_text.split('r.')
                        if len(district_parts) > 1:
                            listing_data['district'] = district_parts[0].strip()
                
                if features:
                    listing_data['amenities'] = json.dumps(features)
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card {listing_id if listing_id else 'unknown'}: {str(e)}")
                continue
        
        return listings
        
    def validate_coordinates(self, lat: float, lon: float) -> Tuple[Optional[float], Optional[float]]:
        """
        Validate and format coordinates to match database schema constraints
        DECIMAL(10,8) means max 10 digits total with 8 after decimal point
        Valid range for coordinates:
        Latitude: -90 to 90
        Longitude: -180 to 180
        """
        try:
            if lat is not None and lon is not None:
                # Check if coordinates are within valid ranges
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    # Format to 8 decimal places to match schema
                    return (
                        round(float(lat), 8),
                        round(float(lon), 8)
                    )
            return None, None
        except (ValueError, TypeError):
            return None, None

    async def get_phone_numbers(self, listing_id: str) -> List[str]:
        try:
            detail_url = f"{self.BASE_URL}/items/{listing_id}"
            
            detail_response = await self.session.get(
                detail_url,
                headers=self.session.headers,
                timeout=aiohttp.ClientTimeout(total=10)
            )
            detail_html = await detail_response.text()
            
            # Extract CSRF token from meta tag
            soup = BeautifulSoup(detail_html, 'lxml')
            csrf_token = None
            csrf_meta = soup.select_one('meta[name="csrf-token"]')
            if csrf_meta:
                csrf_token = csrf_meta.get('content')
                
            # Get any cookies from the detail page response
            cookies = dict(detail_response.cookies)
            cookies.update({
                'language': 'az',
                '_ga': f'GA1.1.{random.randint(1000000, 9999999)}.{int(time.time())}'
            })
            
            # Construct the phone API URL
            phone_url = f"{self.BASE_URL}/items/{listing_id}/phones"
            
            # Required headers for the phone API request
            headers = {
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Referer': detail_url,
                'X-Requested-With': 'XMLHttpRequest',
                'X-CSRF-Token': csrf_token if csrf_token else '',
                'DNT': '1',
                'Origin': self.BASE_URL,
                'Sec-Ch-Ua': '"Not A(Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"macOS"',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Host': 'bina.az'
            }
            
            params = {
                'source_link': detail_url,
                'trigger_button': 'main'
            }
            
            # Add longer delay between requests
            await asyncio.sleep(random.uniform(2, 4))
            
            async with self.session.get(
                phone_url,
                headers=headers,
                params=params,
                cookies=cookies,
                timeout=aiohttp.ClientTimeout(total=10),
                allow_redirects=False
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('phones', [])
                elif response.status == 403:
                    # Add exponential backoff on 403 errors
                    retry_delay = random.uniform(5, 10)
                    self.logger.warning(f"Rate limited (403) for listing {listing_id}, waiting {retry_delay}s")
                    await asyncio.sleep(retry_delay)
                    return []
                else:
                    self.logger.error(f"Phone API failed for listing {listing_id}: Status {response.status}")
                    self.logger.error(f"Response headers: {response.headers}")
                    response_text = await response.text()
                    self.logger.error(f"Response body: {response_text[:500]}")  # Log first 500 chars
                    return []
                    
        except Exception as e:
            self.logger.error(f"Error fetching phone numbers for listing {listing_id}: {str(e)}")
            return []
        
    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse detailed listing page and fetch phone numbers"""
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'bina.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract title and description first
            title = soup.select_one('h1.product-title')
            if title:
                data['title'] = title.text.strip()
            
            desc = soup.select_one('.product-description__content')
            if desc:
                data['description'] = desc.text.strip()
            
            # Extract property type from properties section
            property_items = soup.select('.product-properties__i')
            for item in property_items:
                label = item.select_one('.product-properties__i-name')
                value = item.select_one('.product-properties__i-value')
                if label and value:
                    label_text = label.text.strip().lower()
                    value_text = value.text.strip().lower()
                    
                    if 'kateqoriya' in label_text:
                        if 'köhnə tikili' in value_text:
                            data['property_type'] = 'old'
                        elif 'yeni tikili' in value_text:
                            data['property_type'] = 'new'
                        elif 'həyət evi' in value_text or 'villa' in value_text:
                            data['property_type'] = 'house'
                        elif 'ofis' in value_text:
                            data['property_type'] = 'office'
                        elif 'qaraj' in value_text:
                            data['property_type'] = 'garage'
                        elif 'torpaq' in value_text:
                            data['property_type'] = 'land'
                        else:
                            data['property_type'] = 'apartment'
            
            # Extract timestamps and views from statistics section
            stats_container = soup.select_one('.product-statistics')
            if stats_container:
                for stat in stats_container.select('.product-statistics__i-text'):
                    text = stat.text.strip()
                    if text:
                        if 'Baxışların sayı:' in text:
                            try:
                                # Extract number after colon
                                views = int(text.split(':')[1].strip())
                                data['views_count'] = views
                            except (ValueError, IndexError):
                                pass
                        elif 'Yeniləndi:' in text:
                            try:
                                # Extract and parse date after colon
                                date_str = text.split('Yeniləndi:')[1].strip()
                                # Parse the date and time
                                parsed_datetime = datetime.datetime.strptime(date_str, '%d.%m.%Y, %H:%M')
                                data['listing_date'] = parsed_datetime.date()
                                data['updated_at'] = parsed_datetime
                            except (ValueError, IndexError) as e:
                                self.logger.warning(f"Failed to parse date from: {text}, error: {str(e)}")
            # Extract contact type from owner info
            owner_info = soup.select_one('.product-owner__info')
            if owner_info:
                contact_region = owner_info.select_one('.product-owner__info-region')
                if contact_region:
                    data['contact_type'] = contact_region.text.strip()
                contact_name = owner_info.select_one('.product-owner__info-name')
                if contact_name:
                    data['contact_name'] = contact_name.text.strip()
                
            # Extract coordinates if available
            map_elem = soup.select_one('#item_map')
            if map_elem:
                try:
                    raw_lat = float(map_elem.get('data-lat', 0))
                    raw_lon = float(map_elem.get('data-lng', 0))
                    lat, lon = self.validate_coordinates(raw_lat, raw_lon)
                    if lat is not None and lon is not None:
                        data['latitude'] = lat
                        data['longitude'] = lon
                except (ValueError, TypeError, AttributeError):
                    self.logger.warning(f"Invalid coordinates for listing {listing_id}")
            
            # Extract location info
            address_elem = soup.select_one('.product-map__left__address')
            if address_elem:
                data['address'] = address_elem.text.strip()

            # Extract metro station and district
            location_extras = soup.select('.product-extras__i a')
            for extra in location_extras:
                text = extra.text.strip()
                href = extra.get('href', '').lower()
                # Check for metro station (ending with 'm.')
                if text.lower().endswith('m.'):
                    data['metro_station'] = text.replace('m.', '').strip()
                # Or if it contains 'metro' in the text
                elif 'metro' in text.lower():
                    data['metro_station'] = text.replace('metro', '').strip()

                # Extract district
                elif 'r.' in text.lower():
                    district = text.replace('r.', '').strip()
                    data['district'] = district.split()[0] if district else None
                else:
                    data['location'] = text
            
            # Extract phone numbers and WhatsApp availability
            phones = await self.get_phone_numbers(listing_id)
            if phones:
                data['contact_phone'] = phones[0] if phones else None
                data['whatsapp_available'] = bool(soup.select_one('.wp_status_ico'))
            
            # Extract photos
            photos = []
            photo_elems = soup.select('.product-photos__slider-top img[src]')
            for img in photo_elems:
                src = img.get('src')
                if src and not src.endswith('load.gif'):
                    photos.append(src)
            if photos:
                data['photos'] = json.dumps(photos)
            
            # Extract features/amenities
            features = []
            if soup.select_one('.repair'):
                features.append('təmirli')
                data['has_repair'] = True
            if soup.select_one('.bill_of_sale'):
                features.append('kupçalı')
            if soup.select_one('.mortgage'):
                features.append('ipoteka var')
            if features:
                data['amenities'] = json.dumps(features)
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise

    async def run(self, pages: int = 1) -> List[Dict]:
        """Enhanced run method with better error handling and rate limiting"""
        try:
            start_time = time.time()
            self.logger.info("Starting Bina.az scraper")
            await self.init_session()
            
            all_results = []
            failed_pages = []
            failed_listings = []
            
            for page in range(1, pages + 1):
                page_start_time = time.time()
                try:
                    self.logger.info(f"Processing page {page}/{pages}")
                    
                    url = f"{self.LISTINGS_URL}?page={page}"
                    html = await self.get_page_content(url)
                    
                    if not html:
                        self.logger.error(f"Empty HTML content for page {page}")
                        failed_pages.append(page)
                        continue
                    
                    listings = await self.parse_listing_page(html)
                    self.logger.info(f"Found {len(listings)} listings on page {page}")
                    
                    # Process listings with enhanced error handling
                    for idx, listing in enumerate(listings, 1):
                        try:
                            self.logger.debug(f"Processing listing {idx}/{len(listings)} on page {page}")
                            
                            # Reset session periodically
                            if idx % 5 == 0:
                                await self.session.close()
                                self.session = None
                                await self.init_session()
                            
                            # Add delay between listings with jitter
                            delay = random.uniform(2, 4)
                            await asyncio.sleep(delay)
                            
                            detail_html = await self.get_page_content(listing['source_url'])
                            if not detail_html:
                                failed_listings.append(listing['listing_id'])
                                continue
                                
                            detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                            all_results.append({**listing, **detail_data})
                            
                        except Exception as e:
                            self.logger.error(f"Error processing listing {listing.get('listing_id')}: {str(e)}")
                            failed_listings.append(listing.get('listing_id'))
                            continue
                    
                    # Add longer delay between pages
                    if page < pages:
                        await asyncio.sleep(self.PAGE_DELAY + random.uniform(0, 5))
                        
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    failed_pages.append(page)
                    continue
            
            return all_results
            
        finally:
            await self.close_session()
     
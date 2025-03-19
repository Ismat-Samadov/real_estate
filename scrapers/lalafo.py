import asyncio
import aiohttp
import logging
import json
import re
import random
import time
import uuid
from typing import Dict, List, Optional, Tuple, Any
import datetime
from bs4 import BeautifulSoup

class LalafoScraper:
    """Scraper for lalafo.az using direct API integration instead of HTML scraping"""
    
    BASE_URL = "https://lalafo.az"
    API_BASE_URL = "https://lalafo.az/api/search/v3/feed/search"
    API_DETAIL_URL = "https://lalafo.az/api/v3/ads/{ad_id}"
    REAL_ESTATE_CATEGORY_ID = "2029"  # Category ID for real estate
    
    def __init__(self):
        """Initialize the scraper"""
        self.logger = logging.getLogger(__name__)
        self.session = None
        self.proxy_url = None  # Will be set by proxy_handler
        self.user_hash = self._generate_user_hash()
        self.event_session_id = self._generate_session_id()
        self.device_fingerprint = self._generate_device_fingerprint()
        self.request_id = f'react-client_{uuid.uuid4()}'  # Generate a persistent request ID

    def _generate_user_hash(self) -> str:
        """Generate a random user hash for tracking"""
        return str(uuid.uuid4())
    
    def _generate_session_id(self) -> str:
        """Generate a random session ID"""
        # Format: 621ed89ee6bdb4d140000365f113fefd
        return f"{random.randint(10000000, 99999999)}e{random.randint(10000000, 99999999)}{random.randint(10000000, 99999999)}"
    
    def _generate_device_fingerprint(self) -> str:
        """Generate a random device fingerprint"""
        # Format: e276f8511e70fea9931fbcbe0dc8f8ac
        return ''.join(random.choice('0123456789abcdef') for _ in range(32))

    async def init_session(self):
        """Initialize aiohttp session with appropriate headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'DNT': '1',
                'Sec-Ch-Ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"macOS"',
                'country-id': '13',  # Azerbaijan
                'device': 'pc',
                'language': 'az_AZ',
                'priority': 'u=1, i',
                'experiment': 'novalue',
                'User-Hash': str(uuid.uuid4()),  # Generate once at init
            }
            
            # Create connector with proper settings
            connector = aiohttp.TCPConnector(
                ssl=False,
                limit=8,  # Connection pool size
                ttl_dns_cache=300,  # DNS cache TTL
                force_close=False  # Enable connection reuse
            )
            
            # Create timeout settings
            timeout = aiohttp.ClientTimeout(
                total=30,
                connect=10,
                sock_read=15,
                sock_connect=10
            )
            
            self.session = aiohttp.ClientSession(
                headers=headers,
                timeout=timeout,
                connector=connector
            )

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    def _get_cookies(self) -> Dict[str, str]:
        """Generate cookies needed for the API requests"""
        timestamp = int(time.time())
        return {
            'event_user_hash': self.user_hash,
            '_gcl_au': f'1.1.{random.randint(100000000, 999999999)}.{timestamp}', 
            '_fbp': f'fb.1.{timestamp}863.{random.randint(10000000000000000, 99999999999999999)}',
            '_gid': f'GA1.2.{random.randint(1000000000, 9999999999)}.{timestamp}',
            'event_session_id': self.event_session_id,
            '_ga': f'GA1.1.{random.randint(100000000, 999999999)}.{timestamp}',
            '__gads': f'ID={self._random_hex(16)}:T={timestamp}:RT={timestamp}:S=ALNI_MYMOBOViYIdNf_pVzrVptFSA_0aKg',
            '__gpi': f'UID=0000{self._random_hex(11)}:T={timestamp}:RT={timestamp}:S=ALNI_MYOTMJHxte2RCtkHzpVBl21F46VoQ',
            '__eoi': f'ID={self._random_hex(16)}:T={timestamp}:RT={timestamp}:S=AA-AfjZ1dKJn79MobOvu9Dno5QZ1',
            '_ga_YZ2SWY4MX0': f'GS1.1.{timestamp}.1.1.{timestamp}.0.0.0',
            'device_fingerprint': self.device_fingerprint,
            'lastAnalyticsEvent': 'listing:feed:listing:ad:view'
        }
    
    def _random_hex(self, length: int) -> str:
        """Generate a random hex string of specified length"""
        return ''.join(random.choice('0123456789abcdef') for _ in range(length))

    async def _adaptive_delay(self):
        """Implement adaptive delay between requests"""
        base_delay = random.uniform(1.5, 3.0)  # Random delay
        jitter = random.uniform(0, 1.0)  # Add some randomness
        delay = base_delay + jitter
        self.logger.debug(f"Waiting {delay:.2f} seconds before next request")
        await asyncio.sleep(delay)

    async def fetch_listings_page(self, page: int, per_page: int = 20) -> Optional[Dict]:
        """
        Fetch listings from the API with proper parameters
        
        Args:
            page: Page number to fetch
            per_page: Number of items per page
            
        Returns:
            JSON response as a dictionary if successful, None otherwise
        """
        MAX_RETRIES = 3
        
        for attempt in range(MAX_RETRIES):
            try:
                # Add adaptive delay
                await self._adaptive_delay()
                
                # Create request parameters - exact match to what the website uses
                params = {
                    'category_id': self.REAL_ESTATE_CATEGORY_ID,
                    'expand': 'url',
                    'page': str(page),
                    'per-page': str(per_page),
                    'with_feed_banner': 'true'
                }
                
                # Add request-specific headers - these exact headers are crucial
                headers = {
                    'authority': 'lalafo.az',
                    'method': 'GET',
                    'path': f'/api/search/v3/feed/search?category_id={self.REAL_ESTATE_CATEGORY_ID}&expand=url&page={page}&per-page={per_page}&with_feed_banner=true',
                    'scheme': 'https',
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
                    'Referer': 'https://lalafo.az/azerbaijan/nedvizhimost',
                    'Request-Id': self.request_id,
                    'User-Hash': self.user_hash,
                    'country-id': '13',
                    'device': 'pc',
                    'experiment': 'novalue',
                    'language': 'az_AZ',
                    'priority': 'u=1, i',
                    'Origin': 'https://lalafo.az',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
                }
                
                self.logger.info(f"Fetching page {page} with {per_page} items per page")
                
                async with self.session.get(
                    self.API_BASE_URL, 
                    params=params,
                    headers=headers,
                    cookies=self._get_cookies(),
                    proxy=self.proxy_url
                ) as response:
                    if response.status == 200:
                        try:
                            data = await response.json()
                            items_count = len(data.get('items', []))
                            self.logger.info(f"Successfully fetched page {page}, got {items_count} listings")
                            
                            # Show some debug info about response structure
                            self.logger.debug(f"Response keys: {list(data.keys() if isinstance(data, dict) else [])}")
                            
                            # Log a preview of items if available
                            if items_count > 0:
                                sample_listing = data.get('items', [])[0]
                                self.logger.debug(f"Sample listing ID: {sample_listing.get('id', 'N/A')}")
                                
                            return data
                        except json.JSONDecodeError as e:
                            self.logger.error(f"Failed to parse JSON response: {e}")
                            response_text = await response.text()
                            self.logger.debug(f"Response content: {response_text[:500]}")
                    else:
                        response_text = await response.text()
                        self.logger.warning(f"Failed to fetch page {page}, status: {response.status}")
                        self.logger.debug(f"Request URL: {response.url}")
                        self.logger.debug(f"Error response: {response_text[:500]}")
                        
                        if response.status == 429:  # Rate limiting
                            self.logger.warning("Rate limited, waiting longer before retry")
                            await asyncio.sleep(10 + random.uniform(0, 5))  # Longer wait for rate limits
                        elif response.status >= 500:  # Server error
                            await asyncio.sleep(5)  # Wait for server errors
                        elif response.status == 400:  # Bad request
                            # Log more details about the request for debugging
                            self.logger.error(f"Bad request (400) error. Full response: {response_text}")
            
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout on attempt {attempt + 1}")
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    return None
                
                await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff
        
        return None

    async def fetch_listing_details(self, ad_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific listing
        
        Args:
            ad_id: The ID of the listing
            
        Returns:
            Detailed listing data if successful, None otherwise
        """
        try:
            # Add adaptive delay
            await self._adaptive_delay()
            
            url = self.API_DETAIL_URL.format(ad_id=ad_id)
            
            # Add request-specific headers
            headers = {
                'Referer': f'https://lalafo.az/azerbaijan/ads/{ad_id}',
                'Request-Id': f'react-client_{uuid.uuid4()}',
                'User-Hash': self.user_hash,
                'Origin': 'https://lalafo.az'
            }
            
            self.logger.info(f"Fetching details for listing {ad_id}")
            
            async with self.session.get(
                url,
                headers=headers,
                cookies=self._get_cookies(),
                proxy=self.proxy_url
            ) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                        self.logger.info(f"Successfully fetched details for listing {ad_id}")
                        return data
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Failed to parse JSON response for listing {ad_id}: {e}")
                else:
                    self.logger.warning(f"Failed to fetch details for listing {ad_id}, status: {response.status}")
                    
                    try:
                        error_text = await response.text()
                        self.logger.debug(f"Error response: {error_text[:200]}")
                    except:
                        pass
                    
        except Exception as e:
            self.logger.error(f"Error fetching details for listing {ad_id}: {str(e)}")
        
        return None

    def parse_listing_from_api(self, listing_data: Dict) -> Dict:
        """
        Parse listing data from API response into our database schema format
        
        Args:
            listing_data: Raw listing data from API
            
        Returns:
            Parsed listing data in our database schema format
        """
        try:
            # Extract listing ID
            listing_id = str(listing_data.get('id', ''))
            
            # Extract title and description
            title = listing_data.get('title', '')
            description = listing_data.get('description', '')
            
            # Extract price information
            price = listing_data.get('price')
            currency = listing_data.get('currency', 'AZN')
            
            # Determine listing type based on ad_label or category
            listing_type = 'sale'  # Default
            ad_label = listing_data.get('ad_label', '').lower()
            
            if 'günlük' in title.lower() + description.lower() + ad_label:
                listing_type = 'daily'
            elif any(term in title.lower() + description.lower() + ad_label for term in ['kirayə', 'icarə', 'kirayədir', 'kirayə verilir']):
                listing_type = 'monthly'
            
            # Determine property type
            property_type = 'apartment'  # Default
            combined_text = (title + ' ' + description + ' ' + ad_label).lower()
            
            if 'yeni tikili' in combined_text:
                property_type = 'new'
            elif 'köhnə tikili' in combined_text:
                property_type = 'old'
            elif any(term in combined_text for term in ['həyət evi', 'villa', 'bağ evi']):
                property_type = 'house'
            elif 'ofis' in combined_text:
                property_type = 'office'
            elif any(term in combined_text for term in ['torpaq', 'torpaq sahəsi']):
                property_type = 'land'
            elif any(term in combined_text for term in ['obyekt', 'kommersiya']):
                property_type = 'commercial'
                
            # Extract location information
            lat = listing_data.get('lat')
            lng = listing_data.get('lng')
            city = listing_data.get('city')
            
            # Extract district from params if available
            district = None
            params = listing_data.get('params', [])
            for param in params:
                if param.get('name') == 'İnzibati rayonlar':
                    district = param.get('value', '').replace(' r.', '')
            
            # Extract contact information
            mobile = listing_data.get('mobile', '')
            
            # Extract images
            photos = []
            for img in listing_data.get('images', []):
                if img.get('original_url'):
                    photos.append(img.get('original_url'))
            
            # Extract timestamps
            created_time = listing_data.get('created_time')
            updated_time = listing_data.get('updated_time')
            
            created_date = datetime.datetime.fromtimestamp(created_time) if created_time else datetime.datetime.now()
            updated_date = datetime.datetime.fromtimestamp(updated_time) if updated_time else datetime.datetime.now()
            
            # Extract views count
            views_count = listing_data.get('views', 0)
            
            # Extract user info for contact type
            user = listing_data.get('user', {})
            contact_type = 'agent' if user.get('pro') else 'owner'
            
            # Construct url
            url = f"{self.BASE_URL}{listing_data.get('url', '')}"
            
            # Parse basic data
            parsed_data = {
                'listing_id': listing_id,
                'title': title,
                'description': description,
                'district': district,
                'city': city,
                'address': None,  # Will extract from description
                'latitude': lat,
                'longitude': lng,
                'price': price,
                'currency': currency,
                'property_type': property_type,
                'listing_type': listing_type,
                'contact_phone': mobile,
                'contact_type': contact_type,
                'whatsapp_available': False,  # Will be updated if available
                'views_count': views_count,
                'created_at': created_date,
                'updated_at': updated_date,
                'listing_date': created_date.date(),
                'photos': json.dumps(photos) if photos else None,
                'source_url': url,
                'source_website': 'lalafo.az'
            }
            
            # Find address in description
            if description:
                # Look for common address indicators
                address_patterns = [
                    r'ünvan:([^\.]+)',
                    r'yerləşir:([^\.]+)',
                    r'yerləşir([^\.]+)'
                ]
                
                for pattern in address_patterns:
                    match = re.search(pattern, description, re.IGNORECASE)
                    if match:
                        parsed_data['address'] = match.group(1).strip()
                        break
            
            # Extract room, area and floor information from title and description
            combined_text = f"{title} {description}"
            
            # Extract rooms
            rooms_match = re.search(r'(\d+)\s*otaq', combined_text, re.IGNORECASE)
            if rooms_match:
                try:
                    rooms = int(rooms_match.group(1))
                    if 1 <= rooms <= 20:  # Reasonable range
                        parsed_data['rooms'] = rooms
                except (ValueError, TypeError):
                    pass
            
            # Extract area
            area_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:m²|kv\.m|kv\.metr)', combined_text, re.IGNORECASE)
            if area_match:
                try:
                    area = float(area_match.group(1))
                    if 5 <= area <= 10000:  # Reasonable range
                        parsed_data['area'] = area
                except (ValueError, TypeError):
                    pass
            
            # Extract floor information
            floor_match = re.search(r'(\d+)/(\d+)\s*mərtəbə', combined_text, re.IGNORECASE)
            if floor_match:
                try:
                    floor = int(floor_match.group(1))
                    total_floors = int(floor_match.group(2))
                    if 0 <= floor <= 200 and 1 <= total_floors <= 200:  # Reasonable range
                        parsed_data['floor'] = floor
                        parsed_data['total_floors'] = total_floors
                except (ValueError, TypeError):
                    pass
            
            # Extract 'has_repair' from description
            if 'təmirli' in combined_text.lower() or 'təmir olunub' in combined_text.lower():
                parsed_data['has_repair'] = True
            else:
                parsed_data['has_repair'] = False
            
            # Extract metro station
            metro_match = re.search(r'(m\.\s*\w+|metro\s*\w+|\w+\s*metrosu)', combined_text, re.IGNORECASE)
            if metro_match:
                metro_text = metro_match.group(1)
                # Clean up the metro station name
                metro_station = re.sub(r'^m\.\s*|^metro\s*|\s*metrosu$', '', metro_text, flags=re.IGNORECASE)
                if metro_station:
                    parsed_data['metro_station'] = metro_station.strip()
            
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing {listing_data.get('id')}: {str(e)}")
            return {}

    async def parse_detailed_listing(self, listing_id: str, basic_data: Dict) -> Dict:
        """
        Fetch and parse detailed listing information to enhance the basic data
        
        Args:
            listing_id: The ID of the listing
            basic_data: Basic listing data already extracted
            
        Returns:
            Enhanced listing data with details
        """
        try:
            # Fetch detailed information
            details = await self.fetch_listing_details(listing_id)
            
            if not details:
                self.logger.warning(f"Failed to fetch details for listing {listing_id}, using basic data only")
                return basic_data
                
            enhanced_data = basic_data.copy()
            
            # Extract parameters/amenities
            params = details.get('params', [])
            amenities = []
            
            for param in params:
                param_name = param.get('name', '')
                param_value = param.get('value', '')
                
                if param_name and param_value:
                    amenities.append(f"{param_name}: {param_value}")
                    
                    # Extract specific parameters
                    if 'otaqların sayı' in param_name.lower():
                        try:
                            rooms = int(re.search(r'\d+', param_value).group())
                            if 1 <= rooms <= 20:
                                enhanced_data['rooms'] = rooms
                        except (ValueError, TypeError, AttributeError):
                            pass
                    elif 'sahə' in param_name.lower():
                        try:
                            area = float(re.search(r'\d+', param_value).group())
                            if 5 <= area <= 10000:
                                enhanced_data['area'] = area
                        except (ValueError, TypeError, AttributeError):
                            pass
                    elif 'mərtəbə' in param_name.lower() and 'mərtəbələrin' not in param_name.lower():
                        try:
                            floor = int(re.search(r'\d+', param_value).group())
                            if 0 <= floor <= 200:
                                enhanced_data['floor'] = floor
                        except (ValueError, TypeError, AttributeError):
                            pass
                    elif 'mərtəbələrin sayı' in param_name.lower():
                        try:
                            total_floors = int(re.search(r'\d+', param_value).group())
                            if 1 <= total_floors <= 200:
                                enhanced_data['total_floors'] = total_floors
                        except (ValueError, TypeError, AttributeError):
                            pass
                    elif 'təmir' in param_name.lower():
                        enhanced_data['has_repair'] = True
                    elif 'metro stansiyası' in param_name.lower():
                        enhanced_data['metro_station'] = param_value.replace('m.', '').strip()
                    elif 'inzibati rayonlar' in param_name.lower():
                        enhanced_data['district'] = param_value.replace('r.', '').strip()
            
            # Add amenities if available
            if amenities:
                enhanced_data['amenities'] = json.dumps(amenities)
            
            # Update WhatsApp availability if available in details
            enhanced_data['whatsapp_available'] = details.get('has_whatsapp', False)
            
            # Update any missing location information
            if 'city' not in enhanced_data or not enhanced_data['city']:
                enhanced_data['city'] = details.get('city', '')
            
            return enhanced_data
            
        except Exception as e:
            self.logger.error(f"Error enhancing listing {listing_id}: {str(e)}")
            return basic_data

    async def run(self, pages: int = 2, items_per_page: int = 20) -> List[Dict]:
        """
        Run the scraper for the specified number of pages
        
        Args:
            pages: Number of pages to scrape
            items_per_page: Number of items per page
            
        Returns:
            List of parsed listings
        """
        try:
            self.logger.info(f"Starting Lalafo.az scraper using API integration")
            await self.init_session()
            all_results = []
            
            for page in range(1, pages + 1):
                try:
                    self.logger.info(f"Processing page {page} of {pages}")
                    
                    # Fetch listings page
                    page_data = await self.fetch_listings_page(page, items_per_page)
                    
                    if not page_data or 'items' not in page_data:
                        self.logger.warning(f"No valid data found for page {page}, skipping")
                        continue
                    
                    listings = page_data.get('items', [])
                    self.logger.info(f"Found {len(listings)} listings on page {page}")
                    
                    for idx, listing in enumerate(listings, 1):
                        try:
                            listing_id = str(listing.get('id', ''))
                            if not listing_id:
                                continue
                                
                            self.logger.info(f"Processing listing {idx}/{len(listings)} on page {page}: ID {listing_id}")
                            
                            # Parse basic data from API response
                            basic_data = self.parse_listing_from_api(listing)
                            
                            # Enhance with detailed data
                            enhanced_data = await self.parse_detailed_listing(listing_id, basic_data)
                            
                            if enhanced_data:
                                all_results.append(enhanced_data)
                                
                            # Add delay between listings to avoid rate limits
                            await asyncio.sleep(random.uniform(0.5, 1.5))
                            
                        except Exception as e:
                            self.logger.error(f"Error processing listing {listing.get('id', 'unknown')}: {str(e)}")
                            continue
                    
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    continue
                
                # Add delay between pages
                await asyncio.sleep(random.uniform(2, 4))
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        except Exception as e:
            self.logger.error(f"Error in Lalafo scraper: {str(e)}")
            return []
            
        finally:
            await self.close_session()
            
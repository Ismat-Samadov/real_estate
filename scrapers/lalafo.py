import asyncio
import aiohttp
import logging
import json
import re
import random
import time
import uuid
import base64
from typing import Dict, List, Optional, Tuple, Any
import datetime
from urllib.parse import urljoin, quote

class LalafoScraper:
    """Improved scraper for lalafo.az using direct API integration with proper headers and proxy handling"""
    
    BASE_URL = "https://lalafo.az"
    API_BASE_URL = "https://lalafo.az/api/search/v3/feed/search"
    API_DETAIL_URL = "https://lalafo.az/api/search/v3/feed/details/{ad_id}"
    LISTINGS_URL = "https://lalafo.az/azerbaijan/nedvizhimost"
    REAL_ESTATE_CATEGORY_ID = "2029"  # Category ID for real estate
    
    def __init__(self):
        """Initialize the scraper"""
        self.logger = logging.getLogger(__name__)
        self.session = None
        self.proxy_url = None  # Will be set by proxy_handler
        self.user_hash = str(uuid.uuid4())
        self.event_session_id = self._generate_session_id()
        self.device_fingerprint = self._generate_device_fingerprint()
        self.request_id = f'react-client_{uuid.uuid4()}'
        self.request_count = 0
        self.last_request_time = time.time()
        self.cf_clearance = None  # Cloudflare clearance cookie
        self.csrf_token = None  # CSRF token

    def _generate_session_id(self) -> str:
        """Generate a random session ID matching the format in examples"""
        return f"{random.randint(100000, 999999)}{random.randint(10000000, 99999999)}{random.randint(10000000, 99999999)}"
    
    def _generate_device_fingerprint(self) -> str:
        """Generate a random device fingerprint matching the format in examples"""
        return ''.join(random.choice('0123456789abcdef') for _ in range(32))
        
    async def _fetch_initial_cookies(self) -> bool:
        """
        Fetch initial cookies and tokens from the main website
        This is critical for bypassing anti-bot protection
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Fetching initial cookies from main page")
            
            # Use a temporary session with minimal headers
            temp_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            async with aiohttp.ClientSession(headers=temp_headers) as temp_session:
                async with temp_session.get(
                    self.LISTINGS_URL,
                    proxy=self.proxy_url,
                    allow_redirects=True
                ) as response:
                    if response.status != 200:
                        self.logger.error(f"Failed to fetch main page: {response.status}")
                        return False
                    
                    # Extract cookies
                    cookies = response.cookies
                    for cookie_name, cookie in cookies.items():
                        if cookie_name == 'cf_clearance':
                            self.cf_clearance = cookie.value
                            self.logger.info(f"Found cf_clearance cookie: {self.cf_clearance[:10]}...")
                    
                    # Extract page content to find CSRF token
                    content = await response.text()
                    
                    # Look for CSRF token
                    csrf_matches = re.search(r'<meta\s+name="csrf-token"\s+content="([^"]+)"', content)
                    if csrf_matches:
                        self.csrf_token = csrf_matches.group(1)
                        self.logger.info(f"Found CSRF token: {self.csrf_token[:10]}...")
                    
                    # Look for any JavaScript variables containing tokens
                    token_matches = re.search(r'token[\'"]:[\s]*[\'"]([^\'"]*)[\'"]}', content)
                    if token_matches:
                        token = token_matches.group(1)
                        self.logger.info(f"Found token in JavaScript: {token[:10]}...")
                        if not self.csrf_token:
                            self.csrf_token = token
                    
                    return bool(self.cf_clearance)
        
        except Exception as e:
            self.logger.error(f"Error fetching initial cookies: {str(e)}")
            return False

    async def init_session(self):
        """Initialize aiohttp session with appropriate headers matching the API requirements"""
        if not self.session:
            # First fetch initial cookies
            await self._fetch_initial_cookies()
            
            base_headers = {
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
                'User-Hash': self.user_hash,
                'Referer': self.LISTINGS_URL,
                'Origin': self.BASE_URL,
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
            }
            
            # Add CSRF token if found
            if self.csrf_token:
                base_headers['X-CSRF-Token'] = self.csrf_token
            
            # Create connector with proper settings
            connector = aiohttp.TCPConnector(
                ssl=False,
                limit=3,  # Reduce connection pool size to avoid rate limiting
                ttl_dns_cache=300,
                force_close=True  # Force close connections to prevent lingering
            )
            
            # Create timeout settings
            timeout = aiohttp.ClientTimeout(
                total=30,
                connect=10,
                sock_read=15,
                sock_connect=10
            )
            
            self.session = aiohttp.ClientSession(
                headers=base_headers,
                timeout=timeout,
                connector=connector
            )

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    def _get_cookies(self) -> Dict[str, str]:
        """Generate cookies needed for the API requests based on the example"""
        timestamp = int(time.time())
        cookies = {
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
        
        # Add Cloudflare clearance cookie if available
        if self.cf_clearance:
            cookies['cf_clearance'] = self.cf_clearance
            
        return cookies
    
    def _random_hex(self, length: int) -> str:
        """Generate a random hex string of specified length"""
        return ''.join(random.choice('0123456789abcdef') for _ in range(length))

    async def _adaptive_delay(self):
        """Implement adaptive delay between requests to avoid rate limiting"""
        self.request_count += 1
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        # More aggressive delays for higher success rate
        base_delay = 3.0
        if self.request_count > 5:
            base_delay = 4.0
        if self.request_count > 10:
            base_delay = 5.0
        if self.request_count > 20:
            base_delay = 6.0
        
        # Add randomized jitter to avoid detection of fixed patterns
        jitter = random.uniform(1.0, 2.0)
        delay = base_delay + jitter
        
        # If we've already waited enough since last request, reduce delay
        if elapsed > delay / 2:
            delay = max(delay / 2, 2.0)
            
        self.logger.debug(f"Waiting {delay:.2f} seconds before next request (request #{self.request_count})")
        await asyncio.sleep(delay)
        self.last_request_time = time.time()

    async def _try_html_scraping(self, page: int) -> List[Dict]:
        """
        Fallback method to scrape listing data from HTML when API fails
        
        Args:
            page: Page number to scrape
            
        Returns:
            List of basic listing data extracted from HTML
        """
        self.logger.info(f"Trying HTML scraping for page {page} as fallback")
        listings = []
        
        try:
            # Build the URL for the listings page
            url = f"{self.LISTINGS_URL}?page={page}"
            
            # Add specific headers for HTML page
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Cache-Control': 'max-age=0',
                'Referer': self.BASE_URL,
                'Upgrade-Insecure-Requests': '1',
            }
            
            # Use existing session but add HTML-specific headers
            async with self.session.get(
                url,
                headers=headers,
                cookies=self._get_cookies(),
                proxy=self.proxy_url
            ) as response:
                if response.status != 200:
                    self.logger.warning(f"HTML scraping failed with status {response.status}")
                    return []
                
                html = await response.text()
                
                # Look for listing data in JavaScript variables
                listings_data_match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*({.*?});', html, re.DOTALL)
                if listings_data_match:
                    try:
                        state_data = json.loads(listings_data_match.group(1))
                        listings_array = state_data.get('items', [])
                        if isinstance(listings_array, list) and listings_array:
                            self.logger.info(f"Found {len(listings_array)} listings in HTML JavaScript data")
                            for item in listings_array:
                                if isinstance(item, dict) and 'id' in item:
                                    listings.append(self.parse_listing_from_api(item))
                            return listings
                    except (json.JSONDecodeError, KeyError):
                        pass
                
                # If no JS data found, parse the HTML directly
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, 'html.parser')
                
                # Find listing cards
                listing_cards = soup.select('.feed-item')
                self.logger.info(f"Found {len(listing_cards)} listing cards in HTML")
                
                for card in listing_cards:
                    try:
                        # Get listing ID and URL
                        link = card.select_one('a.adTile-mainLink')
                        if not link:
                            continue
                        
                        href = link.get('href', '')
                        listing_id_match = re.search(r'id-(\d+)', href)
                        if not listing_id_match:
                            continue
                            
                        listing_id = listing_id_match.group(1)
                        url = f"{self.BASE_URL}{href}"
                        
                        # Get title
                        title_elem = card.select_one('.adTile-title')
                        title = title_elem.text.strip() if title_elem else ""
                        
                        # Get price
                        price_elem = card.select_one('.adTile-price')
                        price = None
                        currency = None
                        if price_elem:
                            price_text = price_elem.text.strip()
                            currency_match = re.search(r'([A-Z]{3})', price_text)
                            if currency_match:
                                currency = currency_match.group(1)
                            price_match = re.search(r'(\d[\d\s]*)', price_text)
                            if price_match:
                                price = float(price_match.group(1).replace(' ', ''))
                        
                        # Get basic data
                        listing_data = {
                            'listing_id': listing_id,
                            'title': title,
                            'price': price,
                            'currency': currency,
                            'source_url': url,
                            'source_website': 'lalafo.az'
                        }
                        
                        # Add to listings
                        listings.append(listing_data)
                    except Exception as e:
                        self.logger.error(f"Error parsing HTML card: {str(e)}")
                
                return listings
        
        except Exception as e:
            self.logger.error(f"HTML scraping error: {str(e)}")
            return []

    async def fetch_listings_page(self, page: int, per_page: int = 20) -> Optional[Dict]:
        """
        Fetch listings from the API with proper parameters from the example
        
        Args:
            page: Page number to fetch
            per_page: Number of items per page
            
        Returns:
            JSON response as a dictionary if successful, None otherwise
        """
        MAX_RETRIES = 5  # Increased retries
        
        for attempt in range(MAX_RETRIES):
            try:
                # Apply adaptive delay
                await self._adaptive_delay()
                
                # Create request parameters exactly as in the example
                params = {
                    'category_id': self.REAL_ESTATE_CATEGORY_ID,
                    'expand': 'url',
                    'page': str(page),
                    'per-page': str(per_page),
                    'with_feed_banner': 'true'
                }
                
                # Create request-specific headers that match the example
                headers = {
                    'authority': 'lalafo.az',
                    'method': 'GET',
                    'path': f'/api/search/v3/feed/search?category_id={self.REAL_ESTATE_CATEGORY_ID}&expand=url&page={page}&per-page={per_page}&with_feed_banner=true',
                    'scheme': 'https',
                    'Referer': 'https://lalafo.az/azerbaijan/nedvizhimost',
                    'Request-Id': f'react-client_{uuid.uuid4()}',
                    'User-Hash': self.user_hash,
                    'Origin': 'https://lalafo.az',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                }
                
                # Add CSRF token if we have it
                if self.csrf_token:
                    headers['X-CSRF-Token'] = self.csrf_token
                
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
                            return data
                        except json.JSONDecodeError as e:
                            self.logger.error(f"Failed to parse JSON response: {e}")
                            response_text = await response.text()
                            self.logger.debug(f"Response content: {response_text[:500]}")
                    elif response.status == 403:  # Forbidden - needs new cookies
                        self.logger.warning("403 Forbidden response - attempting to refresh cookies")
                        # Close current session
                        await self.session.close()
                        self.session = None
                        # Re-initialize session with new cookies
                        await self.init_session()
                        # Wait longer before retry
                        await asyncio.sleep(10 + random.uniform(0, 5))
                    else:
                        response_text = await response.text()
                        self.logger.warning(f"Failed to fetch page {page}, status: {response.status}")
                        self.logger.debug(f"Request URL: {response.url}")
                        self.logger.debug(f"Error response: {response_text[:500]}")
                        
                        if response.status == 429:  # Rate limiting
                            self.logger.warning("Rate limited, waiting longer before retry")
                            await asyncio.sleep(15 + random.uniform(5, 10))  # Longer wait for rate limits
                        elif response.status >= 500:  # Server error
                            await asyncio.sleep(10)  # Wait for server errors
            
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout on attempt {attempt + 1}")
                await asyncio.sleep(7)
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    # If all API attempts failed, try HTML scraping as fallback
                    self.logger.warning("All API attempts failed, trying HTML scraping as fallback")
                    return None
                
                await asyncio.sleep(3 * (attempt + 1))  # Exponential backoff
        
        # If we've exhausted all retries, return None
        return None

    async def fetch_listing_details(self, ad_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific listing using the second API endpoint
        
        Args:
            ad_id: The ID of the listing
            
        Returns:
            Detailed listing data if successful, None otherwise
        """
        MAX_RETRIES = 3
        
        for attempt in range(MAX_RETRIES):
            try:
                # Apply adaptive delay
                await self._adaptive_delay()
                
                url = self.API_DETAIL_URL.format(ad_id=ad_id)
                
                # Create request-specific headers matching the example
                headers = {
                    'authority': 'lalafo.az',
                    'method': 'GET',
                    'path': f'/api/search/v3/feed/details/{ad_id}?expand=url',
                    'scheme': 'https',
                    'Referer': f'https://lalafo.az/baku/ads/id-{ad_id}',
                    'Request-Id': f'react-client_{uuid.uuid4()}',
                    'User-Hash': self.user_hash,
                    'country-id': '13',
                    'device': 'pc',
                    'Origin': 'https://lalafo.az',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                    'x-cache-bypass': 'yes'  # Important header from the example
                }
                
                # Add CSRF token if we have it
                if self.csrf_token:
                    headers['X-CSRF-Token'] = self.csrf_token
                
                params = {
                    'expand': 'url'
                }
                
                self.logger.info(f"Fetching details for listing {ad_id}")
                
                async with self.session.get(
                    url,
                    params=params,
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
                    elif response.status == 403:  # Forbidden - may need new cookies
                        self.logger.warning("403 Forbidden - attempting to refresh cookies")
                        # Close current session
                        await self.session.close()
                        self.session = None
                        # Re-initialize session with new cookies
                        await self.init_session()
                        # Wait longer before retry
                        await asyncio.sleep(8 + random.uniform(0, 4))
                    else:
                        self.logger.warning(f"Failed to fetch details for listing {ad_id}, status: {response.status}")
                        
                        try:
                            error_text = await response.text()
                            self.logger.debug(f"Error response: {error_text[:200]}")
                        except:
                            pass
                        
                        if response.status == 429:  # Rate limiting
                            await asyncio.sleep(10 + random.uniform(0, 5))
                        elif response.status >= 500:  # Server error
                            await asyncio.sleep(5)
                            
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout on attempt {attempt + 1}")
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f"Error fetching details for listing {ad_id}: {str(e)}")
                await asyncio.sleep(3 * (attempt + 1))
                
        # If all API attempts failed, try HTML scraping as fallback
        return await self._fetch_listing_details_html(ad_id)
        
    async def _fetch_listing_details_html(self, ad_id: str) -> Optional[Dict]:
        """
        Fetch and parse listing details from HTML page as fallback when API fails
        
        Args:
            ad_id: The ID of the listing
            
        Returns:
            Listing details if successful, None otherwise
        """
        self.logger.info(f"Trying to fetch listing {ad_id} details from HTML as fallback")
        
        try:
            # Build the URL for the detail page
            url = f"{self.BASE_URL}/azerbaijan/ads/{ad_id}"
            
            # Add specific headers for HTML page
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Cache-Control': 'max-age=0',
                'Referer': self.LISTINGS_URL,
                'Upgrade-Insecure-Requests': '1',
            }
            
            # Use existing session but with HTML-specific headers
            async with self.session.get(
                url,
                headers=headers,
                cookies=self._get_cookies(),
                proxy=self.proxy_url
            ) as response:
                if response.status != 200:
                    self.logger.warning(f"HTML detail page fetch failed with status {response.status}")
                    return None
                
                html = await response.text()
                
                # Look for listing data in JavaScript variables
                detail_data_match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*({.*?});', html, re.DOTALL)
                if detail_data_match:
                    try:
                        state_data = json.loads(detail_data_match.group(1))
                        listing_data = state_data.get('ad', {})
                        if listing_data and isinstance(listing_data, dict) and 'id' in listing_data:
                            self.logger.info(f"Successfully extracted listing {ad_id} data from HTML JavaScript")
                            return listing_data
                    except (json.JSONDecodeError, KeyError) as e:
                        self.logger.error(f"Error parsing JavaScript data: {str(e)}")
                
                # If no JS data found, parse the HTML directly
                try:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Create a basic data structure
                    data = {
                        'id': ad_id,
                        'title': '',
                        'description': '',
                        'price': None,
                        'currency': None,
                        'images': [],
                        'params': []
                    }
                    
                    # Extract title
                    title_elem = soup.select_one('.title-container h1')
                    if title_elem:
                        data['title'] = title_elem.text.strip()
                    
                    # Extract description
                    desc_elem = soup.select_one('.description')
                    if desc_elem:
                        data['description'] = desc_elem.text.strip()
                    
                    # Extract price
                    price_elem = soup.select_one('.adPage-price')
                    if price_elem:
                        price_text = price_elem.text.strip()
                        # Extract currency
                        currency_match = re.search(r'([A-Z]{3})', price_text)
                        if currency_match:
                            data['currency'] = currency_match.group(1)
                        # Extract price value
                        price_match = re.search(r'(\d[\d\s]*)', price_text)
                        if price_match:
                            data['price'] = float(price_match.group(1).replace(' ', ''))
                    
                    # Extract parameters
                    params_list = []
                    param_elems = soup.select('.adPage-properties-i')
                    for param_elem in param_elems:
                        name_elem = param_elem.select_one('.adPage-properties-i-name')
                        value_elem = param_elem.select_one('.adPage-properties-i-value')
                        if name_elem and value_elem:
                            params_list.append({
                                'name': name_elem.text.strip(),
                                'value': value_elem.text.strip()
                            })
                    data['params'] = params_list
                    
                    # Extract images
                    image_elems = soup.select('.adPage-slider img')
                    for img in image_elems:
                        src = img.get('src')
                        if src and not src.endswith('load.gif'):
                            data['images'].append({
                                'original_url': src
                            })
                    
                    self.logger.info(f"Successfully extracted listing {ad_id} data from HTML")
                    return data
                    
                except ImportError:
                    self.logger.error("BeautifulSoup is not available for HTML parsing")
                except Exception as e:
                    self.logger.error(f"Error parsing HTML: {str(e)}")
        
        except Exception as e:
            self.logger.error(f"Error fetching HTML detail page: {str(e)}")
            
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
            
            if 'günlük' in ad_label or 'gunluk' in ad_label:
                listing_type = 'daily'
            elif any(term in ad_label for term in ['kirayə', 'icarə', 'kirayədir', 'arenda']):
                listing_type = 'monthly'
            
            # Determine property type from ad_label or title
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
            url = listing_data.get('url')
            if url:
                if not url.startswith('http'):
                    url = f"{self.BASE_URL}{url}"
            else:
                url = f"{self.BASE_URL}/azerbaijan/ads/{listing_id}"
            
            # Parse basic data
            parsed_data = {
                'listing_id': listing_id,
                'title': title,
                'description': description,
                'district': district,
                'city': city,
                'address': None,  # Will be extracted from description
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
            
            # Find address in description if available
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
            
            # Extract parameters/amenities from the params list in the details
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
                            value = param_value.split()[0]
                            rooms = int(value)
                            if 1 <= rooms <= 20:
                                enhanced_data['rooms'] = rooms
                        except (ValueError, TypeError, AttributeError, IndexError):
                            pass
                    elif 'sahə' in param_name.lower():
                        try:
                            area = float(param_value)
                            if 5 <= area <= 10000:
                                enhanced_data['area'] = area
                        except (ValueError, TypeError, AttributeError):
                            pass
                    elif 'mərtəbə' in param_name.lower() and 'mərtəbələrin' not in param_name.lower():
                        try:
                            floor = int(param_value)
                            if 0 <= floor <= 200:
                                enhanced_data['floor'] = floor
                        except (ValueError, TypeError, AttributeError):
                            pass
                    elif 'mərtəbələrin sayı' in param_name.lower():
                        try:
                            total_floors = int(param_value)
                            if 1 <= total_floors <= 200:
                                enhanced_data['total_floors'] = total_floors
                        except (ValueError, TypeError, AttributeError):
                            pass
                    elif 'təmir' in param_name.lower():
                        enhanced_data['has_repair'] = 'orta' in param_value.lower() or 'əla' in param_value.lower()
                    elif 'metro stansiyası' in param_name.lower():
                        enhanced_data['metro_station'] = param_value.replace('m.', '').strip()
                    elif 'inzibati rayonlar' in param_name.lower():
                        enhanced_data['district'] = param_value.replace('r.', '').strip()
                    
                    # Extract multiple values if the param has links
                    links = param.get('links', [])
                    if links and len(links) > 1:
                        multi_values = [link.get('value') for link in links if link.get('value')]
                        if multi_values:
                            multi_value_str = ", ".join([str(v) for v in multi_values if v])
                            amenities.append(f"{param_name}: {multi_value_str}")
            
            # Add amenities if available
            if amenities:
                enhanced_data['amenities'] = json.dumps(amenities)
            
            # Update whatsapp availability
            enhanced_data['whatsapp_available'] = details.get('has_whatsapp', False)
            
            # Update title and description if not already set
            if details.get('title') and (not enhanced_data.get('title') or len(details.get('title')) > len(enhanced_data.get('title'))):
                enhanced_data['title'] = details.get('title')
                
            if details.get('description') and (not enhanced_data.get('description') or len(details.get('description')) > len(enhanced_data.get('description'))):
                enhanced_data['description'] = details.get('description')
            
            # Update any missing location information
            if ('city' not in enhanced_data or not enhanced_data.get('city')) and details.get('city'):
                enhanced_data['city'] = details.get('city')
                
            # Update price if missing
            if ('price' not in enhanced_data or enhanced_data.get('price') is None) and details.get('price') is not None:
                enhanced_data['price'] = details.get('price')
                enhanced_data['currency'] = details.get('currency', 'AZN')
                
            # Update contact information if available
            if details.get('mobile') and not enhanced_data.get('contact_phone'):
                enhanced_data['contact_phone'] = details.get('mobile')
                
            # Update user information for contact_type
            user = details.get('user', {})
            if user:
                enhanced_data['contact_type'] = 'agent' if user.get('pro') else 'owner'
                
            # Update photos if more are available
            detail_photos = []
            for img in details.get('images', []):
                if img.get('original_url'):
                    detail_photos.append(img.get('original_url'))
                    
            if detail_photos and (not enhanced_data.get('photos') or len(detail_photos) > len(json.loads(enhanced_data.get('photos') or '[]'))):
                enhanced_data['photos'] = json.dumps(detail_photos)
                
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
            self.logger.info(f"Starting Lalafo.az scraper using API integration with HTML fallback")
            await self.init_session()
            all_results = []
            
            for page in range(1, pages + 1):
                try:
                    self.logger.info(f"Processing page {page} of {pages}")
                    
                    # First try API method
                    page_data = await self.fetch_listings_page(page, items_per_page)
                    listings = []
                    
                    if page_data and 'items' in page_data:
                        listings = page_data.get('items', [])
                        self.logger.info(f"Found {len(listings)} listings via API on page {page}")
                    else:
                        # If API fails, try HTML scraping as fallback
                        self.logger.warning("API method failed, trying HTML scraping as fallback")
                        html_listings = await self._try_html_scraping(page)
                        if html_listings:
                            listings = html_listings
                            self.logger.info(f"Found {len(listings)} listings via HTML fallback on page {page}")
                        else:
                            self.logger.warning(f"Both API and HTML methods failed for page {page}, skipping")
                            continue
                    
                    if not listings:
                        self.logger.warning(f"No listings found for page {page}, skipping")
                        continue
                    
                    for idx, listing in enumerate(listings, 1):
                        try:
                            # Ensure we have an ID
                            listing_id = str(listing.get('id', ''))
                            if not listing_id:
                                self.logger.warning("Skipping listing without ID")
                                continue
                                
                            self.logger.info(f"Processing listing {idx}/{len(listings)} on page {page}: ID {listing_id}")
                            
                            # Check if listing is already a parsed result from HTML fallback
                            if 'source_website' in listing and listing['source_website'] == 'lalafo.az':
                                # Already parsed
                                basic_data = listing
                            else:
                                # Parse basic data from API response
                                basic_data = self.parse_listing_from_api(listing)
                            
                            # Enhance with detailed data if we have a valid basic result
                            if basic_data:
                                enhanced_data = await self.parse_detailed_listing(listing_id, basic_data)
                                
                                if enhanced_data:
                                    all_results.append(enhanced_data)
                                    self.logger.debug(f"Successfully processed listing {listing_id}")
                                else:
                                    # If detailed data failed but we have basic data, use it
                                    self.logger.warning(f"Using basic data for listing {listing_id} as detail fetch failed")
                                    all_results.append(basic_data)
                            
                            # Add delay between listings to avoid rate limits
                            await asyncio.sleep(random.uniform(1.0, 2.0))
                            
                        except Exception as e:
                            self.logger.error(f"Error processing listing {listing.get('id', 'unknown')}: {str(e)}")
                            continue
                    
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    continue
                
                # Add delay between pages to avoid rate limits
                await asyncio.sleep(random.uniform(5, 8))
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        except Exception as e:
            self.logger.error(f"Error in Lalafo scraper: {str(e)}")
            return []
            
        finally:
            await self.close_session()
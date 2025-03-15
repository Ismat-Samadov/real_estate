import os
import logging
import aiohttp
import asyncio
import random
import time
from typing import Optional, Dict, List
from dotenv import load_dotenv

class DataImpulseProxyHandler:
    """DataImpulse Proxy implementation with rotation and session management"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        load_dotenv()
        
        # DataImpulse proxy configuration from environment variables
        self.username = os.getenv('DATAIMPULSE_USERNAME')
        self.password = os.getenv('DATAIMPULSE_PASSWORD')
        self.proxy_host = os.getenv('DATAIMPULSE_HOST', 'gw.dataimpulse.com')
        self.proxy_port = os.getenv('DATAIMPULSE_PORT', '823')
        
        # NEW: Support for multiple proxy zones/countries for rotation
        self.proxy_countries = os.getenv('DATAIMPULSE_COUNTRIES', 'tr,ge,az').split(',')
        self.current_country_index = 0
        
        if not self.username or not self.password:
            raise ValueError("DataImpulse credentials not found in environment variables")
        
        # Generate base proxy URL (without country code)
        self.base_proxy_url = f"http://{self.username}:{self.password}@{self.proxy_host}:{self.proxy_port}"
        
        # Get the current proxy URL with country
        self.proxy_url = self._get_proxy_url_with_country()
        
        # Rate limiting settings
        self.request_count = 0
        self.error_count = 0
        self.last_request_time = 0
        self.min_request_delay = 1
        self.max_request_delay = 3
        self.error_delay = 5
        self.max_errors = 3
        
        # Website-specific settings
        self.site_specific_settings = {
            'tap.az': {
                'min_delay': 3,
                'max_delay': 6,
                'error_delay': 10,
                'max_errors': 2,
                'rotate_on_error': True
            }
        }
        
        # Browser fingerprinting
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
        ]
        
        # Base headers
        self.base_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'az,en-US;q=0.9,en;q=0.8,ru;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'DNT': '1'
        }

    def _get_proxy_url_with_country(self) -> str:
        """Get proxy URL with current country code"""
        country = self.proxy_countries[self.current_country_index]
        # For DataImpulse, you typically add country code as a parameter
        # Adjust this based on your provider's requirements
        return f"{self.base_proxy_url}?country={country}"
    
    def _rotate_proxy(self) -> None:
        """Rotate to the next country in the list"""
        self.current_country_index = (self.current_country_index + 1) % len(self.proxy_countries)
        self.proxy_url = self._get_proxy_url_with_country()
        self.logger.info(f"Rotated proxy to country: {self.proxy_countries[self.current_country_index]}")
        
    def _get_random_user_agent(self) -> str:
        """Get a random user agent from the list"""
        return random.choice(self.user_agents)

    def _get_enhanced_headers(self) -> Dict:
        """Get enhanced headers with randomization"""
        headers = self.base_headers.copy()
        headers['User-Agent'] = self._get_random_user_agent()
        return headers

    async def create_session(self) -> aiohttp.ClientSession:
        """Create an optimized aiohttp session"""
        connector = aiohttp.TCPConnector(
            ssl=False,
            limit=5,
            ttl_dns_cache=300,
            force_close=True,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(
            total=30,
            connect=10,
            sock_read=10,
            sock_connect=10
        )
        
        return aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=self._get_enhanced_headers(),
            cookie_jar=aiohttp.CookieJar(unsafe=True),
            trust_env=True
        )

    def _get_site_settings(self, url: str) -> Dict:
        """Get site-specific settings based on URL"""
        for site, settings in self.site_specific_settings.items():
            if site in url:
                return settings
        return {}

    async def _handle_rate_limiting(self, response: aiohttp.ClientResponse, url: str) -> None:
        """Handle rate limiting with exponential backoff and conditional proxy rotation"""
        self.error_count += 1
        
        # Get site-specific settings if available
        site_settings = self._get_site_settings(url)
        error_delay = site_settings.get('error_delay', self.error_delay)
        max_errors = site_settings.get('max_errors', self.max_errors)
        rotate_on_error = site_settings.get('rotate_on_error', False)
        
        backoff_time = min(error_delay * (2 ** (self.error_count - 1)), 60)
        
        self.logger.warning(
            f"Rate limited (Status: {response.status}). "
            f"Attempt: {self.error_count}. Waiting {backoff_time}s"
        )
        
        # Rotate proxy if site settings specify to do so and we've hit our error threshold
        if rotate_on_error and self.error_count >= max_errors:
            self._rotate_proxy()
            self.error_count = 0
            self.logger.info(f"Rotated proxy due to rate limiting for {url}")
        
        await asyncio.sleep(backoff_time)

    async def _wait_between_requests(self, url: str) -> None:
        """Implement intelligent delay between requests with site-specific settings"""
        now = time.time()
        time_since_last = now - self.last_request_time
        
        # Get site-specific settings if available
        site_settings = self._get_site_settings(url)
        min_delay = site_settings.get('min_delay', self.min_request_delay)
        max_delay = site_settings.get('max_delay', self.max_request_delay)
        
        if time_since_last < min_delay:
            delay = random.uniform(min_delay, max_delay)
            await asyncio.sleep(delay - time_since_last)
        
        self.last_request_time = time.time()

    def apply_to_scraper(self, scraper_instance) -> None:
        """Apply proxy configuration to a scraper instance"""
        scraper_instance.proxy_url = self.proxy_url
        self.logger.info(f"Applied proxy URL to {scraper_instance.__class__.__name__}: {self.proxy_url}")
        
        # Keep reference to the original scraper class name for site-specific handling
        scraper_class_name = scraper_instance.__class__.__name__
        
        # Store reference to the proxy handler in the scraper instance
        scraper_instance.proxy_handler = self
        
        async def new_get_page_content(url: str, params: Optional[dict] = None) -> str:
            max_retries = 3
            last_error = None
            
            # Handle tap.az with special care
            is_tap_scraper = 'TapAzScraper' in scraper_class_name
            
            for attempt in range(max_retries):
                try:
                    await self._wait_between_requests(url)
                    
                    headers = {
                        'Referer': url,
                        'Origin': url.split('/')[0] + '//' + url.split('/')[2],
                        'Host': url.split('/')[2],
                        'User-Agent': self._get_random_user_agent()
                    }
                    
                    cookies = {
                        'language': 'az',
                        '_ga': f'GA1.1.{random.randint(1000000, 9999999)}.{int(time.time())}',
                        '_gid': f'GA1.1.{random.randint(1000000, 9999999)}.{int(time.time())}',
                        'session_id': f'{random.randbytes(16).hex()}'
                    }
                    
                    # If it's a TapAzScraper, add more specialized cookies
                    if is_tap_scraper:
                        cookies.update({
                            'tapcsrf': f'{random.randbytes(16).hex()}',
                            'tapsessid': f'{random.randbytes(16).hex()}',
                            'visitor_id': f'{random.randint(10000000, 99999999)}',
                            'referer': 'https://tap.az/elanlar/dasinmaz-emlak'
                        })
                    
                    # Make sure we're using the current proxy_url (which might have been rotated)
                    current_proxy_url = self.proxy_url
                    
                    async with scraper_instance.session.get(
                        url,
                        params=params,
                        headers={**scraper_instance.session.headers, **headers},
                        cookies=cookies,
                        proxy=current_proxy_url,
                        timeout=30,
                        allow_redirects=True,
                        verify_ssl=False
                    ) as response:
                        if response.status == 200:
                            self.error_count = 0
                            return await response.text()
                        elif response.status in [403, 429, 502, 503]:
                            await self._handle_rate_limiting(response, url)
                            if is_tap_scraper and self.error_count >= 2:
                                # For tap.az, rotate proxy more aggressively
                                self._rotate_proxy()
                                # Update scraper's proxy_url to match the rotated one
                                scraper_instance.proxy_url = self.proxy_url
                                self.error_count = 0
                            
                            if self.error_count >= self.max_errors:
                                await scraper_instance.session.close()
                                scraper_instance.session = await self.create_session()
                                self.error_count = 0
                            continue
                        else:
                            raise Exception(f"Unexpected status code: {response.status}")
                            
                except asyncio.TimeoutError:
                    last_error = "Timeout"
                    await asyncio.sleep(random.uniform(2, 5))
                    # Rotate proxy on timeout for tap.az
                    if is_tap_scraper:
                        self._rotate_proxy()
                        scraper_instance.proxy_url = self.proxy_url
                except Exception as e:
                    last_error = str(e)
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(random.uniform(1, 3))
                    
            raise Exception(f"Max retries ({max_retries}) exceeded. Last error: {last_error}")
        
        # Replace the get_page_content method
        scraper_instance.get_page_content = new_get_page_content
        
        # For TapAzScraper, enhance the get_phone_numbers method if it exists
        if hasattr(scraper_instance, 'get_phone_numbers'):
            original_get_phone_numbers = scraper_instance.get_phone_numbers
            
            async def enhanced_get_phone_numbers(listing_id: str) -> List[str]:
                """Enhanced phone number fetching with proxy rotation support"""
                try:
                    # Try the original method first
                    phones = await original_get_phone_numbers(listing_id)
                    
                    # If successful, return the phones
                    if phones:
                        return phones
                    
                    # If not successful, try rotating the proxy and trying again
                    self._rotate_proxy()
                    scraper_instance.proxy_url = self.proxy_url
                    self.logger.info(f"Rotated proxy for phone API to: {self.proxy_url}")
                    
                    # Wait before retry
                    await asyncio.sleep(random.uniform(3, 5))
                    
                    # Try again with new proxy
                    return await original_get_phone_numbers(listing_id)
                except Exception as e:
                    self.logger.error(f"Error in enhanced_get_phone_numbers: {str(e)}")
                    return []
            
            # Replace the get_phone_numbers method only for TapAzScraper
            if 'TapAzScraper' in scraper_class_name:
                scraper_instance.get_phone_numbers = enhanced_get_phone_numbers
        
        async def new_init_session():
            if not hasattr(scraper_instance, 'session') or not scraper_instance.session:
                scraper_instance.session = await self.create_session()
        
        # Replace the init_session method
        scraper_instance.init_session = new_init_session
import os
import logging
import aiohttp
import asyncio
import random
import time
from typing import Optional, Dict, List
from dotenv import load_dotenv


class ProxyHandler:
    """711 Proxy implementation with rotation and session management"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        load_dotenv()
        
        # 711 proxy configuration
        self.username = os.getenv('PROXY_USERNAME')
        self.password = os.getenv('PROXY_PASSWORD')
        self.proxy_host = 'global.711proxy.com'
        self.proxy_port = '1000'  
        
        # Generate session ID for sticky sessions
        self.session_id = random.randbytes(8).hex()
        
        if not self.username or not self.password:
            raise ValueError("711 Proxy credentials not found in environment variables")
            
        # Construct proxy URL with authentication
        self.proxy_url = f"http://{self.username}:{self.password}@{self.proxy_host}:{self.proxy_port}"
        
        # Rate limiting settings
        self.request_count = 0
        self.error_count = 0
        self.last_request_time = 0
        self.min_request_delay = 1
        self.max_request_delay = 3
        self.error_delay = 5
        self.max_errors = 3
        
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

    def _get_random_user_agent(self) -> str:
        """Get a random user agent from the list"""
        return random.choice(self.user_agents)

    def _get_enhanced_headers(self) -> Dict:
        """Get enhanced headers with randomization"""
        headers = self.base_headers.copy()
        headers['User-Agent'] = self._get_random_user_agent()
        return headers

    async def verify_proxy(self) -> bool:
        """Verify proxy connection and location"""
        verification_urls = [
            'http://ip-api.com/json',
            'https://api.myip.com'
        ]
        
        session = await self.create_session()
        try:
            async with session:
                for url in verification_urls:
                    try:
                        async with session.get(
                            url,
                            proxy=self.proxy_url,
                            timeout=30,
                            verify_ssl=False
                        ) as response:
                            if response.status == 200:
                                data = await response.json()
                                self.logger.info(f"Proxy verification successful at {url}: {data}")
                                continue
                            else:
                                self.logger.error(f"Proxy verification failed at {url} with status: {response.status}")
                                return False
                    except Exception as e:
                        self.logger.error(f"Proxy verification error at {url}: {str(e)}")
                        return False
                        
                # Test target site accessibility
                try:
                    async with session.get(
                        'https://bina.az/robots.txt',
                        proxy=self.proxy_url,
                        timeout=30,
                        verify_ssl=False
                    ) as response:
                        if response.status != 200:
                            self.logger.error(f"Target site verification failed: {response.status}")
                            return False
                except Exception as e:
                    self.logger.error(f"Target site verification error: {str(e)}")
                    return False
                    
                return True
                
        except Exception as e:
            self.logger.error(f"Proxy verification error: {str(e)}")
            return False
        finally:
            await session.close()

    async def create_session(self) -> aiohttp.ClientSession:
        """Create an optimized aiohttp session"""
        connector = aiohttp.TCPConnector(
            ssl=False,
            limit=5,  # Conservative connection limit
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

    async def _handle_rate_limiting(self, response: aiohttp.ClientResponse) -> None:
        """Handle rate limiting with exponential backoff"""
        self.error_count += 1
        backoff_time = min(self.error_delay * (2 ** (self.error_count - 1)), 60)
        self.logger.warning(
            f"Rate limited (Status: {response.status}). "
            f"Attempt: {self.error_count}. Waiting {backoff_time}s"
        )
        await asyncio.sleep(backoff_time)

    async def _wait_between_requests(self) -> None:
        """Implement intelligent delay between requests"""
        now = time.time()
        time_since_last = now - self.last_request_time
        
        if time_since_last < self.min_request_delay:
            delay = random.uniform(
                self.min_request_delay,
                self.max_request_delay
            )
            await asyncio.sleep(delay - time_since_last)
        
        self.last_request_time = time.time()

    def apply_to_scraper(self, scraper_instance) -> None:
        """Apply proxy configuration to a scraper instance"""
        scraper_instance.proxy_url = self.proxy_url
        self.logger.info(f"Applied proxy URL to {scraper_instance.__class__.__name__}")
        
        async def new_get_page_content(url: str, params: Optional[dict] = None) -> str:
            max_retries = 3
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    await self._wait_between_requests()
                    
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
                    
                    async with scraper_instance.session.get(
                        url,
                        params=params,
                        headers={**scraper_instance.session.headers, **headers},
                        cookies=cookies,
                        proxy=self.proxy_url,
                        timeout=30,
                        allow_redirects=True,
                        verify_ssl=False
                    ) as response:
                        if response.status == 200:
                            self.error_count = 0
                            return await response.text()
                        elif response.status in [403, 429, 502, 503]:
                            await self._handle_rate_limiting(response)
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
                except Exception as e:
                    last_error = str(e)
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(random.uniform(1, 3))
                    
            raise Exception(f"Max retries ({max_retries}) exceeded. Last error: {last_error}")
        
        # Replace the get_page_content method
        scraper_instance.get_page_content = new_get_page_content
        
        async def new_init_session():
            if not hasattr(scraper_instance, 'session') or not scraper_instance.session:
                scraper_instance.session = await self.create_session()
        
        # Replace the init_session method
        scraper_instance.init_session = new_init_session
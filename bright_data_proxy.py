import os
import logging
import aiohttp
from typing import Optional
from dotenv import load_dotenv
import os
import logging
import aiohttp
import asyncio
import random
from typing import Optional
from dotenv import load_dotenv

class BrightDataProxy:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        load_dotenv()
        
        self.username = os.getenv('BRIGHT_DATA_USERNAME')
        self.password = os.getenv('BRIGHT_DATA_PASSWORD')
        
        # Use country-specific residential proxy for Azerbaijan
        self.proxy_host = 'brd.superproxy.io:22225'  # Different port for residential
        self.username = f"{self.username}-country-az"  # Specific to Azerbaijan
        
        if not self.username or not self.password:
            raise ValueError("Bright Data credentials not found in environment variables")
            
        self.proxy_url = f"http://{self.username}:{self.password}@{self.proxy_host}"
        
        # Enhanced browser-like headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'az,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }

    async def verify_proxy(self) -> bool:
        """Verify proxy connection"""
        try:
            session = await self.create_session()
            async with session:
                async with session.get(
                    'https://geo.brdtest.com/mygeo.json',  # Using JSON endpoint for better verification
                    proxy=self.proxy_url,
                    timeout=30,
                    verify_ssl=False
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Log proxy details
                        self.logger.info(f"Proxy verification successful: {data}")
                        return True
                    else:
                        self.logger.error(f"Proxy verification failed with status: {response.status}")
                        return False
        except Exception as e:
            self.logger.error(f"Proxy verification error: {str(e)}")
            return False

    async def create_session(self) -> aiohttp.ClientSession:
        """Create an aiohttp session with enhanced proxy configuration"""
        # Add random delay between requests
        delay = random.uniform(1, 3)
        
        connector = aiohttp.TCPConnector(
            ssl=False,
            limit=3,  # Limit concurrent connections
            ttl_dns_cache=300,
            force_close=True  # Force new connection for each request
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
            headers=self.headers,
            cookie_jar=aiohttp.CookieJar(unsafe=True)  # Enable cookie handling
        )

    def apply_to_scraper(self, scraper_instance) -> None:
        """Apply enhanced proxy configuration to a scraper instance"""
        original_init = scraper_instance.init_session
        
        async def new_get_page_content(url: str, params: Optional[dict] = None) -> str:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Add random delay between requests
                    await asyncio.sleep(random.uniform(2, 5))
                    
                    async with scraper_instance.session.get(
                        url,
                        params=params,
                        proxy=self.proxy_url,
                        timeout=30,
                        allow_redirects=True,
                        verify_ssl=False
                    ) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status in [403, 402, 502]:
                            # Wait longer on blocking errors
                            await asyncio.sleep(random.uniform(5, 10))
                            if attempt == max_retries - 1:
                                raise Exception(f"Failed with status {response.status}")
                            continue
                        else:
                            raise Exception(f"Failed with status {response.status}")
                            
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(random.uniform(3, 7))
                    
            raise Exception("Max retries exceeded")
        
        # Replace the get_page_content method
        scraper_instance.get_page_content = new_get_page_content
        
        async def new_init_session():
            if not hasattr(scraper_instance, 'session') or not scraper_instance.session:
                scraper_instance.session = await self.create_session()
        
        # Replace the init_session method
        scraper_instance.init_session = new_init_session
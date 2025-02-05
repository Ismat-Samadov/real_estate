import os
import logging
import aiohttp
import asyncio
import random
import time
from typing import Optional, Dict, List
from dotenv import load_dotenv

class ProxyHandler:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        load_dotenv()
        
        self.username = os.getenv('PROXY_USERNAME')
        self.password = os.getenv('PROXY_PASSWORD')
        self.proxy_host = 'global.711proxy.com:10000'
        
        if not self.username or not self.password:
            raise ValueError("711 Proxy credentials not found in environment variables")
            
        self.proxy_url = f"http://{self.username}:{self.password}@{self.proxy_host}"
        
        self.logger.info(f"Initialized proxy with URL format: {self.proxy_url.replace(self.password, '****')}")

    async def verify_proxy(self) -> bool:
        return True

    async def create_session(self) -> aiohttp.ClientSession:
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
            trust_env=True
        )

    def apply_to_scraper(self, scraper_instance) -> None:
        scraper_instance.proxy_url = self.proxy_url
        self.logger.info(f"Applied proxy URL to {scraper_instance.__class__.__name__}")
        
        async def new_get_page_content(url: str, params: Optional[dict] = None) -> str:
            max_retries = 3
            last_error = None
            
            for attempt in range(max_retries):
                try:
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
                        elif response.status in [403, 429, 502, 503]:
                            await asyncio.sleep(2 ** attempt)
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
        
        scraper_instance.get_page_content = new_get_page_content
        
        async def new_init_session():
            if not hasattr(scraper_instance, 'session') or not scraper_instance.session:
                scraper_instance.session = await self.create_session()
        
        scraper_instance.init_session = new_init_session
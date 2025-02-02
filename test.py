# Import required libraries
import asyncio
import aiohttp
import random
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Optional, Tuple
import datetime
import re
import json
import time
from urllib.parse import urljoin
from asyncio import Semaphore

class OptimizedBinaScraper:
    """Optimized scraper for bina.az that maintains all original functionality"""
    
    BASE_URL = "https://bina.az"
    LISTINGS_URL = "https://bina.az/items/all"
    
    def __init__(self, max_concurrent: int = 5):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None
        self.proxy_url = None
        self.semaphore = Semaphore(max_concurrent)
        self.request_count = 0
        self.last_request_time = 0
        self.min_delay = 0.3
        self.max_delay = 0.8
        self.batch_size = 8

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
        """Initialize session with enhanced headers"""
        if not self.session:
            headers = {
                'User-Agent': self._get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,az;q=0.8,ru;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            }
            
            connector = aiohttp.TCPConnector(
                ssl=False,
                limit=8,
                ttl_dns_cache=300,
                force_close=True,
                enable_cleanup_closed=True
            )
            
            timeout = aiohttp.ClientTimeout(total=30)
            
            self.session = aiohttp.ClientSession(
                headers=headers,
                connector=connector,
                timeout=timeout,
                trust_env=True,
                cookie_jar=aiohttp.CookieJar(unsafe=True)
            )

    async def _smart_delay(self):
        """Implement adaptive delay"""
        now = time.time()
        time_since_last = now - self.last_request_time
        
        delay = self.min_delay
        if self.request_count > 30:
            delay = min(self.max_delay, delay * 1.2)
        
        if time_since_last < delay:
            await asyncio.sleep(delay - time_since_last)
        
        self.last_request_time = time.time()
        self.request_count += 1

    async def get_page_content(self, url: str, params: Optional[Dict] = None) -> str:
        """Fetch page content with retry logic"""
        async with self.semaphore:
            for attempt in range(3):
                try:
                    await self._smart_delay()
                    
                    headers = {
                        'Referer': 'https://bina.az/',
                        'Origin': 'https://bina.az',
                        'Host': 'bina.az',
                        'User-Agent': self._get_random_user_agent()
                    }
                    
                    cookies = {
                        'language': 'az',
                        '_ga': f'GA1.1.{random.randint(1000000, 9999999)}.{int(time.time())}',
                        '_gid': f'GA1.1.{random.randint(1000000, 9999999)}.{int(time.time())}'
                    }
                    
                    async with self.session.get(
                        url,
                        params=params,
                        headers={**self.session.headers, **headers},
                        cookies=cookies,
                        proxy=self.proxy_url,
                        timeout=aiohttp.ClientTimeout(total=20),
                        verify_ssl=False
                    ) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status in [403, 429]:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        
                except Exception as e:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1)
            
            raise Exception(f"Failed to fetch {url}")

    async def get_phone_numbers(self, listing_id: str) -> List[str]:
        """Fetch phone numbers from API"""
        try:
            source_link = f"https://bina.az/items/{listing_id}"
            url = f"https://bina.az/items/{listing_id}/phones"
            
            headers = {
                'authority': 'bina.az',
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
                'dnt': '1',
                'referer': source_link,
                'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'user-agent': self._get_random_user_agent(),
                'x-requested-with': 'XMLHttpRequest'
            }

            # Get CSRF token
            async with self.session.get(
                source_link,
                proxy=self.proxy_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    csrf_meta = soup.select_one('meta[name="csrf-token"]')
                    if csrf_meta:
                        headers['x-csrf-token'] = csrf_meta.get('content')

            params = {'source_link': source_link, 'trigger_button': 'main'}

            async with self.session.get(
                url,
                params=params,
                headers=headers,
                proxy=self.proxy_url,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('phones', [])
                return []
                
        except Exception as e:
            self.logger.error(f"Error fetching phones for listing {listing_id}: {str(e)}")
            return []

    def extract_price(self, price_text: str) -> Optional[float]:
        """Extract numeric price from text"""
        if not price_text:
            return None
        try:
            price = re.sub(r'[^\d.]', '', price_text)
            return float(price) if price else None
        except (ValueError, TypeError):
            return None

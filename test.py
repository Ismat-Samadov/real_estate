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
        self.logger = logging.getLogger(__name__)
        self.session = None
        self.proxy_url = None
        self.semaphore = Semaphore(max_concurrent)
        self.last_request_time = 0
        self.request_count = 0
        
        # Maintain original delay parameters with slight optimization
        self.min_delay = 0.3  # Slightly reduced but still safe
        self.max_delay = 0.8
        self.batch_size = 8   # Balanced batch size
        
    def _get_random_user_agent(self):
        """Maintain original user agent rotation"""
        browsers = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
        ]
        return random.choice(browsers)

    async def init_session(self):
        """Initialize session with original headers and enhanced connection handling"""
        if not self.session:
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
            
            connector = aiohttp.TCPConnector(
                ssl=False,
                limit=8,  # Balanced connection pool
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
            
            self.session = aiohttp.ClientSession(
                headers=headers,
                connector=connector,
                timeout=timeout,
                trust_env=True,
                cookie_jar=aiohttp.CookieJar(unsafe=True)
            )

    async def _smart_delay(self):
        """Implement adaptive delay while maintaining site courtesy"""
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
        """Fetch page content with original headers and enhanced retry logic"""
        async with self.semaphore:
            for attempt in range(3):
                try:
                    await self._smart_delay()
                    
                    # Maintain exact original request headers
                    headers = {
                        'Referer': 'https://bina.az/',
                        'Origin': 'https://bina.az',
                        'Host': 'bina.az',
                        'User-Agent': self._get_random_user_agent()
                    }
                    
                    # Keep original cookies
                    cookies = {
                        'language': 'az',
                        '_ga': f'GA1.1.{random.randint(1000000, 9999999)}.{int(time.time())}',
                        '_gid': f'GA1.1.{random.randint(1000000, 9999999)}.{int(time.time())}',
                        '__cf_bm': f'{random.randbytes(32).hex()}',  # it is a just simulating a Cloudflare cookie, nothing more, nothing less
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
                        elif response.status in [403, 429]:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        
                except Exception as e:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1)
            
            raise Exception(f"Failed to fetch {url}")

    async def get_phone_numbers(self, listing_id: str) -> List[str]:
        """Maintain exact original phone number fetching logic"""
        try:
            await asyncio.sleep(random.uniform(1, 1.5))
            
            source_link = f"https://bina.az/items/{listing_id}"
            url = f"https://bina.az/items/{listing_id}/phones"
            
            # Keep exact original API headers
            headers = {
                'authority': 'bina.az',
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'accept-encoding': 'gzip, deflate, br, zstd',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
                'dnt': '1',
                'priority': 'u=1, i',
                'referer': source_link,
                'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': self._get_random_user_agent(),
                'x-requested-with': 'XMLHttpRequest'
            }

            # First get CSRF token from listing page
            async with self.session.get(
                source_link,
                proxy=self.proxy_url,
                timeout=aiohttp.ClientTimeout(total=20),
                headers=headers
            ) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    csrf_meta = soup.select_one('meta[name="csrf-token"]')
                    if csrf_meta:
                        headers['x-csrf-token'] = csrf_meta.get('content')

            params = {
                'source_link': source_link,
                'trigger_button': 'main'
            }

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

    async def process_listing_batch(self, listings: List[Dict]) -> List[Dict]:
        """Process listings in batches while maintaining data integrity"""
        tasks = []
        for listing in listings:
            task = asyncio.create_task(self._process_single_listing(listing))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, dict)]

    async def _process_single_listing(self, listing: Dict) -> Optional[Dict]:
        """Process single listing with all original data extraction"""
        try:
            detail_html = await self.get_page_content(listing['source_url'])
            detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
            
            phones = await self.get_phone_numbers(listing['listing_id'])
            if phones:
                detail_data['contact_phone'] = phones[0]
            
            return {**listing, **detail_data}
        except Exception as e:
            self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
            return None

    async def run(self, pages: int = 1) -> List[Dict]:
        """Run scraper with optimized concurrent processing"""
        all_results = []
        try:
            await self.init_session()
            
            for page in range(1, pages + 1):
                try:
                    url = f"{self.LISTINGS_URL}?page={page}"
                    html = await self.get_page_content(url)
                    listings = await self.parse_listing_page(html)
                    
                    # Process in smaller batches
                    for i in range(0, len(listings), self.batch_size):
                        batch = listings[i:i + self.batch_size]
                        results = await self.process_listing_batch(batch)
                        all_results.extend([r for r in results if r])
                        
                        # Maintain reasonable delay between batches
                        await asyncio.sleep(0.5)
                    
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    continue
            
            return all_results
            
        finally:
            if self.session:
                await self.session.close()


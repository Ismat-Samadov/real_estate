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

class OptimizedArendaScraper:
    """Optimized scraper for arenda.az"""
    
    BASE_URL = "https://arenda.az"
    LISTINGS_URL = "https://arenda.az/filtirli-axtaris/"
    
    def __init__(self):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None
    
    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            # Common browser headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
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
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
        DELAY = int(os.getenv('REQUEST_DELAY', 1))
        
        self.logger.info(f"Attempting to fetch URL: {url}")
        start_time = time.time()
        
        # Add request-specific headers
        headers = {
            'Referer': 'https://arenda.az/',
            'Origin': 'https://arenda.az',
            'Host': 'arenda.az'
        }
        
        # Add cookies and additional parameters
        cookies = {
            'lang': '1',
            'arenda': '1',
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                self.logger.info(f"Attempt {attempt + 1} of {MAX_RETRIES}")
                # Add random delay between 1-3 seconds
                await asyncio.sleep(DELAY + random.random() * 2)
                
                async with self.session.get(
                    url, 
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    timeout=aiohttp.ClientTimeout(total=10)  # 10 second timeout
                ) as response:
                    self.logger.info(f"Got response with status: {response.status}")
                    
                    if response.status == 200:
                        try:
                            content = await response.text(encoding='utf-8')
                            elapsed = time.time() - start_time
                            self.logger.info(f"Successfully fetched content in {elapsed:.2f} seconds")
                            return content
                        except UnicodeDecodeError:
                            content = await response.read()
                            return content.decode('utf-8', errors='replace')
                    elif response.status == 403:
                        self.logger.warning(f"Access forbidden (403) on attempt {attempt + 1}. Might be rate-limited.")
                        await asyncio.sleep(DELAY * (attempt + 2))
                    else:
                        self.logger.warning(f"Failed to fetch {url}, status: {response.status}")
                        
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout on attempt {attempt + 1}")
            except Exception as e:
                self.logger.error(f"Error fetching {url}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                
            await asyncio.sleep(DELAY * (attempt + 1))  # Exponential backoff
        
        elapsed = time.time() - start_time
        self.logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts. Total time: {elapsed:.2f} seconds")
        raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} attempts")

    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('li.new_elan_box'):
            try:
                listing_url = listing.select_one('a')['href']
                listing_id = listing.get('id', '').replace('elan_', '')
                
                if not listing_id or not listing_url:
                    continue
                
                price_elem = listing.select_one('.elan_price')
                price_text = price_elem.text.strip() if price_elem else None
                price = self.extract_price(price_text) if price_text else None
                
                # Basic data from listing card
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'arenda.az',
                    'price': price,
                    'currency': 'AZN',
                    'created_at': datetime.datetime.now()
                }
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
                
        return listings

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse the detailed listing page"""
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            # Extract listing details
            title = soup.select_one('h1.elan_title')
            title = title.text.strip() if title else None
            
            description = soup.select_one('.elan_info_txt')
            description = description.text.strip() if description else None
            
            # Extract address components
            address = None
            location = None
            district = None
            metro_station = None
            
            address_elem = soup.select_one('.elan_unvan_txt')
            if address_elem:
                address = address_elem.text.strip()
            
            location_elems = soup.select('.elan_adr_list li a')
            for elem in location_elems:
                text = elem.text.strip()
                if 'metro' in text.lower():
                    metro_station = text.replace('metro', '').strip()
                elif 'r.' in text:
                    district = text.replace('r.', '').strip()
                else:
                    location = text
            
            # Extract property details
            property_details = {}
            for param_row in soup.select('.n_elan_box_botom_params tr'):
                cells = param_row.select('td')
                for cell in cells:
                    text = cell.text.strip()
                    if 'otaqlı' in text:
                        property_details['rooms'] = int(text.split()[0])
                    elif 'm²' in text:
                        property_details['area'] = float(text.replace('m²', '').strip())
                    elif 'mərtəbə' in text:
                        floor_data = text.split('/')
                        if len(floor_data) == 2:
                            property_details['floor'] = int(floor_data[0])
                            property_details['total_floors'] = int(floor_data[1].split()[0])

            # Extract listing type
            listing_type = None
            if title:
                if 'günlük' in title.lower():
                    listing_type = 'daily'
                elif 'aylıq' in title.lower():
                    listing_type = 'monthly'
                elif 'satılır' in title.lower():
                    listing_type = 'sale'

            # Extract contact information
            contact_info = soup.select_one('.elan_in_tel')
            contact_phone = contact_info.text.strip() if contact_info else None
            
            # Combine all data
            listing_data = {
                'listing_id': listing_id,
                'title': title,
                'description': description,
                'address': address,
                'location': location,
                'district': district,
                'metro_station': metro_station,
                'property_type': 'apartment',  # Default for now, can be expanded
                'listing_type': listing_type,
                'contact_phone': contact_phone,
                'whatsapp_available': bool(soup.select_one('.wp_status_ico')),
                'updated_at': datetime.datetime.now(),
                **property_details
            }

            return listing_data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise

    def extract_price(self, price_text: str) -> float:
        """Extract numeric price from price text"""
        if not price_text:
            return None
            
        try:
            # Remove currency and convert to float
            price = re.sub(r'[^\d.]', '', price_text)
            return float(price)
        except:
            return None

    async def run(self, pages: int = 1):
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Initializing scraper session")
            await self.init_session()
            results = []
            
            for page in range(1, pages + 1):
                self.logger.info(f"Processing page {page} of {pages}")
                start_time = time.time()
                
                try:
                    params = {
                        'home_search': '1',
                        'lang': '1',
                        'site': '1',
                        'home_s': '1',
                        'page': str(page)
                    }
                    
                    # Fetch and parse listings page
                    self.logger.info(f"Fetching listings page {page}")
                    html = await self.get_page_content(self.LISTINGS_URL, params)
                    listings = await self.parse_listing_page(html)
                    self.logger.info(f"Found {len(listings)} listings on page {page}")
                    
                    # Fetch and parse each listing detail
                    for idx, listing in enumerate(listings, 1):
                        try:
                            self.logger.info(f"Processing listing {idx}/{len(listings)} on page {page}")
                            detail_html = await self.get_page_content(listing['source_url'])
                            detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                            results.append({**listing, **detail_data})
                        except Exception as e:
                            self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                            continue
                    
                    elapsed = time.time() - start_time
                    self.logger.info(f"Completed page {page} in {elapsed:.2f} seconds")
                            
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    continue
                    
            self.logger.info(f"Scraping completed. Total listings processed: {len(results)}")
            return results
            
        finally:
            self.logger.info("Closing scraper session")
            await self.close_session()
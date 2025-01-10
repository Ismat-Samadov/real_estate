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

class YeniEmlakScraper:
    """Scraper for yeniemlak.az"""
    
    BASE_URL = "https://yeniemlak.az"
    SEARCH_URL = "https://yeniemlak.az/elan/axtar"
    
    # Mapping listing types to database enum values
    LISTING_TYPE_MAP = {
        "1": "sale",      # elan_nov=1
        "2": "monthly",   # elan_nov=2 
        "3": "daily"      # elan_nov=3
    }
    
    def __init__(self):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None

    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br'
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
        
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(DELAY + random.random() * 2)
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 403:
                        self.logger.warning(f"Access forbidden (403) on attempt {attempt + 1}")
                        await asyncio.sleep(DELAY * (attempt + 2))
                    else:
                        self.logger.warning(f"Failed to fetch {url}, status: {response.status}")
                        
            except Exception as e:
                self.logger.error(f"Error fetching {url}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                
            await asyncio.sleep(DELAY * (attempt + 1))
        
        raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} attempts")

    def extract_number(self, text: str) -> Optional[float]:
        """Extract numeric value from text"""
        if not text:
            return None
        try:
            return float(re.sub(r'[^\d.]', '', text))
        except:
            return None

    def extract_coordinates(self, html: str) -> Tuple[Optional[float], Optional[float]]:
        """Extract latitude and longitude from page HTML"""
        lat = None
        lon = None
        
        lat_match = re.search(r'id="lat"[^>]*value="([^"]+)"', html)
        lon_match = re.search(r'id="lon"[^>]*value="([^"]+)"', html)
        
        if lat_match and lon_match:
            try:
                lat = float(lat_match.group(1))
                lon = float(lon_match.group(1))
            except ValueError:
                pass
                
        return lat, lon

    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('table.list'):
            try:
                # Extract listing ID and URL
                detail_link = listing.select_one('a.detail')
                if not detail_link:
                    continue
                    
                listing_url = self.BASE_URL + detail_link['href']
                listing_id = re.search(r'-(\d+)$', listing_url).group(1)
                
                # Extract price
                price_elem = listing.select_one('price')
                price = self.extract_number(price_elem.text) if price_elem else None
                
                # Extract basic metadata
                title = listing.select_one('.text emlak')
                title = title.text.strip() if title else None
                
                # Basic listing data
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'yeniemlak.az',
                    'title': title,
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
            # Initialize data dictionary
            data = {
                'listing_id': listing_id,
                'source_website': 'yeniemlak.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract title and description
            title = soup.select_one('.title')
            if title:
                data['title'] = title.text.strip()
                
            description = soup.select_one('.text')
            if description:
                data['description'] = description.text.strip()
            
            # Extract price
            price_elem = soup.select_one('price')
            if price_elem:
                data['price'] = self.extract_number(price_elem.text)
                data['currency'] = 'AZN'
            
            # Extract location info
            address_elements = soup.select('.params b')
            for elem in address_elements:
                text = elem.text.strip()
                if 'metro' in text.lower():
                    data['metro_station'] = text.replace('metro.', '').strip()
                elif any(district in text.lower() for district in ['rayonu', 'район']):
                    data['district'] = text.strip()
            
            # Extract coordinates
            lat, lon = self.extract_coordinates(html)
            if lat and lon:
                data['latitude'] = lat
                data['longitude'] = lon
            
            # Extract property details
            property_info = soup.select('.params')
            for info in property_info:
                text = info.text.strip()
                if 'otaq' in text:
                    data['rooms'] = self.extract_number(text)
                elif 'm2' in text or 'm²' in text:
                    data['area'] = self.extract_number(text)
                elif 'mərtəbə' in text:
                    floor_info = text.split('/')
                    if len(floor_info) == 2:
                        data['floor'] = self.extract_number(floor_info[0])
                        data['total_floors'] = self.extract_number(floor_info[1])
            
            # Extract property type
            property_type = soup.select_one('emlak')
            if property_type:
                if 'bina evi' in property_type.text.lower():
                    data['property_type'] = 'apartment'
                elif 'həyət evi' in property_type.text.lower():
                    data['property_type'] = 'house'
                
            # Extract contact info
            contact_elem = soup.select_one('.tel img')
            if contact_elem:
                data['contact_phone'] = contact_elem.get('src', '').split('/')[-1]
            
            # Extract amenities
            amenities = []
            for check in soup.select('.check'):
                amenities.append(check.text.strip())
            if amenities:
                data['amenities'] = json.dumps(amenities)
            
            # Extract repair status
            data['has_repair'] = 'Təmirli' in amenities if amenities else False
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise

    async def scrape_listing_type(self, listing_type: str, pages: int = 2) -> List[Dict]:
        """Scrape specified number of pages for a given listing type"""
        results = []
        
        for page in range(1, pages + 1):
            try:
                # Prepare search parameters
                params = {
                    'elan_nov': listing_type,
                    'emlak': '0',
                    'page': str(page)
                }
                
                # Fetch and parse listings page
                html = await self.get_page_content(self.SEARCH_URL, params)
                listings = await self.parse_listing_page(html)
                
                # Fetch and parse each listing detail
                for listing in listings:
                    try:
                        detail_html = await self.get_page_content(listing['source_url'])
                        detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                        
                        # Map listing type to database enum
                        detail_data['listing_type'] = self.LISTING_TYPE_MAP[listing_type]
                        
                        results.append({**listing, **detail_data})
                        
                    except Exception as e:
                        self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"Error processing page {page}: {str(e)}")
                continue
                
        return results

    async def run(self, pages: int = 2):
        """Run the scraper for all listing types"""
        try:
            self.logger.info("Starting YeniEmlak scraper")
            await self.init_session()
            
            all_results = []
            
            # Scrape each listing type
            for listing_type in self.LISTING_TYPE_MAP.keys():
                try:
                    self.logger.info(f"Scraping listing type {listing_type}")
                    results = await self.scrape_listing_type(listing_type, pages)
                    all_results.extend(results)
                except Exception as e:
                    self.logger.error(f"Error scraping listing type {listing_type}: {str(e)}")
                    continue
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        finally:
            await self.close_session()
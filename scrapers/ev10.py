import os
import logging
import aiohttp 
import asyncio
import json
import datetime
from typing import Dict, List, Optional, Any

class EV10Scraper:
    """Scraper for ev10.az"""
    
    BASE_URL = "https://ev10.az"
    LISTINGS_URL = "https://ev10.az/ru/kiraye"
    
    def __init__(self):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None
        
    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
                'Next-Router-State-Tree': '%5B%22%22%2C%7B%22children%22%3A%5B%5B%22lang%22%2C%22ru%22%2C%22d%22%5D%2C%7B%22children%22%3A%5B%22(root)%22%2C%7B%22children%22%3A%5B%22(home)%22%2C%7B%22children%22%3A%5B%5B%22type%22%2C%22kiraye%22%2C%22d%22%5D%2C%7B%22children%22%3A%5B%22__PAGE__%22%2C%7B%7D%2C%22%2Fru%2Fkiraye%22%2C%22refresh%22%5D%7D%5D%7D%5D%7D%5D%7D%2Cnull%2Cnull%2Ctrue%5D%7D%5D',
                'RSC': '1',
                'Referer': 'https://ev10.az/ru/kiraye',
                'Cookie': 'lang=1; arenda=1'
            }
            
            self.session = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            )

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    async def get_page_content(self, page: int = 1) -> Dict:
        """Fetch page content with retry logic"""
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
        DELAY = int(os.getenv('REQUEST_DELAY', 1))
        
        params = {
            'page_number': str(page),
            'sort_by': 'date_desc',
            '_rsc': '1qpjo'
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(DELAY)
                
                async with self.session.get(
                    self.LISTINGS_URL,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        data = await response.text()
                        lines = data.split('\n')
                        listings = []
                        
                        for line in lines:
                            if ":" in line:
                                try:
                                    key, content = line.split(':', 1)
                                    if content.strip().startswith('{') or content.strip().startswith('['):
                                        parsed = json.loads(content)
                                        if isinstance(parsed, dict) and 'id' in parsed:
                                            listings.append(parsed)
                                        elif isinstance(parsed, list) and len(parsed) > 2:
                                            for item in parsed:
                                                if isinstance(item, dict) and 'initialPostingsData' in item:
                                                    return item['initialPostingsData']
                                except json.JSONDecodeError:
                                    continue
                                    
                        if listings:
                            return {'postings': listings}
                            
                    else:
                        self.logger.warning(f"Failed to fetch page {page}, status: {response.status}")
                        
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                
            await asyncio.sleep(DELAY * (attempt + 1))
            
        raise Exception(f"Failed to fetch page {page} after {MAX_RETRIES} attempts")

    def parse_listing(self, listing: Dict) -> Dict:
        """Parse listing data into database schema format"""
        try:
            # Map listing fields to database schema
            subway_station = listing.get('subway_station', {})
            station_name = subway_station.get('name') if isinstance(subway_station, dict) else None
            
            parsed = {
                'listing_id': str(listing.get('id')),
                'title': listing.get('address'),
                'metro_station': station_name,
                'district': listing.get('district'),
                'address': listing.get('address'),
                'location': listing.get('suburban'),
                'latitude': listing.get('location_lat'),
                'longitude': listing.get('location_lng'),
                'rooms': listing.get('rooms'),
                'area': listing.get('area'),
                'floor': listing.get('floor'),
                'total_floors': listing.get('total_floors'),
                'property_type': listing.get('property_type'),
                'listing_type': 'monthly' if listing.get('lease_type') == 'MONTHLY' else 'daily',
                'price': listing.get('price'),
                'currency': listing.get('currency', 'AZN'),
                'contact_type': None,
                'contact_phone': listing.get('phone_number'),
                'whatsapp_available': False,
                'description': None,
                'views_count': None,
                'created_at': datetime.datetime.now(),
                'updated_at': datetime.datetime.strptime(listing.get('renewed_at', datetime.datetime.now().isoformat()), "%Y-%m-%dT%H:%M:%S"),
                'listing_date': datetime.datetime.strptime(listing.get('renewed_at', datetime.datetime.now().isoformat()), "%Y-%m-%dT%H:%M:%S").date(),
                'has_repair': listing.get('renovated', False),
                'amenities': None,
                'photos': json.dumps(listing.get('images', [])),
                'source_url': f"{self.BASE_URL}/elan/{listing.get('id')}",
                'source_website': 'ev10.az'
            }
            return parsed
        except Exception as e:
            self.logger.error(f"Error parsing listing {listing.get('id')}: {str(e)}")
            return None

    async def run(self, pages: int = 1) -> List[Dict]:
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Initializing scraper session")
            await self.init_session()
            all_listings = []
            
            for page in range(1, pages + 1):
                try:
                    self.logger.info(f"Processing page {page}")
                    
                    # Get page data
                    page_data = await self.get_page_content(page)
                    
                    if page_data and 'postings' in page_data:
                        # Parse each listing
                        for raw_listing in page_data['postings']:
                            parsed = self.parse_listing(raw_listing)
                            if parsed:
                                all_listings.append(parsed)
                                
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    continue
                    
            self.logger.info(f"Scraping completed. Total listings: {len(all_listings)}")
            return all_listings
            
        finally:
            await self.close_session()
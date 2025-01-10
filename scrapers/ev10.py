import os
import logging
import aiohttp 
import asyncio
import json
import datetime
import random
from typing import Dict, List, Optional, Any

class EV10Scraper:
    """Scraper for ev10.az with updated API endpoints"""
    
    BASE_URL = "https://ev10.az"
    API_BASE_URL = "https://ev10.az/api/v1.0/postings"
    DETAIL_API_URL = "https://ev10.az/api/v1.0/postings/{listing_id}"
    
    # Define listing types mapping
    LISTING_TYPES = [
        {"sale_type": "HOME_SHARING", "db_type": "monthly"},
        {"sale_type": "LEASE", "lease_type": "DAILY", "db_type": "daily"},
        {"sale_type": "LEASE", "lease_type": "MONTHLY", "db_type": "monthly"},
        {"sale_type": "PURCHASE", "db_type": "sale"}
    ]
    
    def __init__(self):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None
        
    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'DNT': '1',
                'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"macOS"',
                'Origin': 'https://ev10.az',
                'Referer': 'https://ev10.az/'
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

    def get_request_params(self, page: int, listing_type: Dict) -> Dict:
        """Generate request parameters based on listing type"""
        params = {
            'page_number': str(page),
            'sort_by': 'date_desc',
            'media_type': 'image',
            'page_size': '24',
            'sponsor_seed': str(random.randint(1, 999999)),
            'sponsor_skip': str((page - 1) * 6),
            'sponsor_limit': '6',
            'sale_type': listing_type['sale_type']
        }
        
        if 'lease_type' in listing_type:
            params['lease_type'] = listing_type['lease_type']
            
        return params

    async def get_page_content(self, page: int, listing_type: Dict) -> Dict:
        """Fetch page content with retry logic"""
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
        DELAY = int(os.getenv('REQUEST_DELAY', 1))
        
        params = self.get_request_params(page, listing_type)
        
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(DELAY + random.random())
                
                url = self.API_BASE_URL
                self.logger.debug(f"Requesting URL: {url} with params: {params}")
                
                async with self.session.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        self.logger.debug(f"Raw API response: {json.dumps(response_data)[:1000]}...")
                        return response_data
                    else:
                        self.logger.warning(f"Failed to fetch page {page}, status: {response.status}")
                        self.logger.debug(f"Response content: {await response.text()}")
                        
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                
            await asyncio.sleep(DELAY * (attempt + 1))
            
        raise Exception(f"Failed to fetch page {page} after {MAX_RETRIES} attempts")

    async def get_listing_details(self, listing_id: str) -> Optional[Dict]:
        """Fetch detailed information for a single listing"""
        try:
            url = self.DETAIL_API_URL.format(listing_id=listing_id)
            self.logger.debug(f"Fetching details for listing {listing_id} from {url}")
            
            await asyncio.sleep(random.random())
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    self.logger.debug(f"Raw listing details: {json.dumps(data)[:1000]}...")
                    return data
                else:
                    self.logger.warning(f"Failed to fetch details for listing {listing_id}, status: {response.status}")
                    self.logger.debug(f"Response content: {await response.text()}")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Error fetching details for listing {listing_id}: {str(e)}")
            return None

    def parse_listing(self, listing: Dict, listing_type: Dict) -> Optional[Dict]:
        """Parse listing data into database schema format"""
        try:
            # Log the raw listing data for debugging
            self.logger.debug(f"Parsing listing data: {json.dumps(listing)[:1000]}...")
            
            # Extract listing ID
            listing_id = listing.get('id')
            if not listing_id:
                self.logger.warning("Skipping listing without ID")
                return None
                
            # Handle nested objects safely
            subway = listing.get('subway_station', {})
            district = listing.get('district', {})
            
            # Parse dates
            created_at = datetime.datetime.now()
            updated_at = None
            listing_date = None
            
            if 'renewed_at' in listing:
                try:
                    timestamp = datetime.datetime.strptime(
                        listing['renewed_at'].split('.')[0], 
                        "%Y-%m-%dT%H:%M:%S"
                    )
                    updated_at = timestamp
                    listing_date = timestamp.date()
                except (ValueError, TypeError, AttributeError) as e:
                    self.logger.warning(f"Error parsing date for listing {listing_id}: {e}")
            
            # Parse amenities and images
            amenities = listing.get('amenities', [])
            if isinstance(amenities, str):
                try:
                    amenities = json.loads(amenities)
                except json.JSONDecodeError:
                    amenities = []
            
            images = listing.get('images', [])
            if isinstance(images, str):
                try:
                    images = json.loads(images)
                except json.JSONDecodeError:
                    images = []
            
            parsed = {
                'listing_id': str(listing_id),
                'title': listing.get('title') or listing.get('address'),
                'metro_station': subway.get('name') if isinstance(subway, dict) else subway,
                'district': district.get('name') if isinstance(district, dict) else district,
                'address': listing.get('address'),
                'location': listing.get('suburban'),
                'latitude': listing.get('location_lat'),
                'longitude': listing.get('location_lng'),
                'rooms': listing.get('rooms'),
                'area': listing.get('area'),
                'floor': listing.get('floor'),
                'total_floors': listing.get('total_floors'),
                'property_type': listing.get('property_type', 'apartment'),
                'listing_type': listing_type['db_type'],
                'price': float(listing.get('price', 0)),
                'currency': listing.get('currency', 'AZN'),
                'contact_phone': listing.get('phone_number'),
                'whatsapp_available': bool(listing.get('has_whatsapp', False)),
                'description': listing.get('description'),
                'views_count': listing.get('view_count'),
                'created_at': created_at,
                'updated_at': updated_at,
                'listing_date': listing_date,
                'has_repair': listing.get('renovated', False),
                'amenities': json.dumps(amenities),
                'photos': json.dumps(images),
                'source_url': f"{self.BASE_URL}/elan/{listing_id}",
                'source_website': 'ev10.az'
            }
            
            self.logger.debug(f"Successfully parsed listing {listing_id}")
            return parsed
            
        except Exception as e:
            self.logger.error(f"Error parsing listing {listing.get('id', 'unknown')}: {str(e)}")
            return None

    async def run(self, pages: int = 1) -> List[Dict]:
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Initializing scraper session")
            await self.init_session()
            all_listings = []
            
            # Iterate through each listing type
            for listing_type in self.LISTING_TYPES:
                self.logger.info(f"Processing {listing_type['sale_type']} listings")
                
                for page in range(1, pages + 1):
                    try:
                        self.logger.info(f"Processing page {page} for {listing_type['sale_type']}")
                        
                        # Get page data
                        response_data = await self.get_page_content(page, listing_type)
                        
                        # Check response structure and extract listings
                        if not response_data:
                            self.logger.warning("Empty response data")
                            continue
                            
                        # Try different possible response structures
                        listings = None
                        if 'data' in response_data:
                            listings = response_data['data']
                        elif 'postings' in response_data:
                            listings = response_data['postings']
                        elif 'items' in response_data:
                            listings = response_data['items']
                        elif isinstance(response_data, list):
                            listings = response_data
                            
                        if not listings:
                            self.logger.warning(f"No listings found in response for {listing_type['sale_type']} page {page}")
                            self.logger.debug(f"Response structure: {json.dumps(list(response_data.keys()) if isinstance(response_data, dict) else 'Not a dict')}")
                            continue
                            
                        self.logger.info(f"Found {len(listings)} listings on page {page}")
                        
                        # Process each listing
                        for item in listings:
                            try:
                                # Handle both string IDs and full listing objects
                                if isinstance(item, str):
                                    listing_details = await self.get_listing_details(item)
                                    if listing_details:
                                        parsed = self.parse_listing(listing_details, listing_type)
                                        if parsed:
                                            all_listings.append(parsed)
                                    else:
                                        self.logger.warning(f"Could not fetch details for listing {item}")
                                else:
                                    parsed = self.parse_listing(item, listing_type)
                                    if parsed:
                                        all_listings.append(parsed)
                            except Exception as e:
                                self.logger.error(f"Error processing listing {item}: {str(e)}")
                                continue
                                
                    except Exception as e:
                        self.logger.error(f"Error processing page {page} for {listing_type['sale_type']}: {str(e)}")
                        continue
                    
            self.logger.info(f"Scraping completed. Total listings: {len(all_listings)}")
            return all_listings
            
        finally:
            await self.close_session()
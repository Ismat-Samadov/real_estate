import os
import logging
import aiohttp 
import asyncio
import json
import datetime
import random
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin

class EV10Scraper:
    """Scraper for ev10.az with API integration"""
    
    BASE_URL = "https://ev10.az"
    API_BASE_URL = "https://ev10.az/api/v1.0/postings"
    DETAIL_API_URL = "https://ev10.az/api/v1.0/postings/{listing_id}"
    
    # Define listing types and their configurations
    LISTING_TYPES = [
        {"sale_type": "HOME_SHARING", "db_type": "monthly"},
        {"sale_type": "PURCHASE", "db_type": "sale"},
        {
            "sale_type": "LEASE",
            "subtypes": [
                {"lease_type": "DAILY", "db_type": "daily"},
                {"lease_type": "MONTHLY", "db_type": "monthly"}
            ]
        }
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
            
            # Use connection pooling and keep-alive
            connector = aiohttp.TCPConnector(
                ssl=False,
                limit=10,  # Connection pool size
                ttl_dns_cache=300,  # DNS cache TTL
                force_close=False  # Enable connection reuse
            )
            
            self.session = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                connector=connector,
                raise_for_status=False  # Handle status codes manually
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

    async def get_page_content(self, page: int, listing_type: Dict) -> Optional[Dict]:
        """Fetch page content with retry logic and error handling"""
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
                    raise_for_status=False
                ) as response:
                    if response.status == 200:
                        try:
                            response_data = await response.json()
                            self.logger.debug(f"Raw API response: {json.dumps(response_data)[:1000]}...")
                            return response_data
                        except json.JSONDecodeError as e:
                            self.logger.error(f"Failed to parse JSON response: {e}")
                            self.logger.debug(f"Response content: {await response.text()}")
                            continue
                    else:
                        self.logger.warning(f"Failed to fetch page {page}, status: {response.status}")
                        if response.status == 429:  # Rate limit
                            await asyncio.sleep(random.uniform(1, 2))  # Delay between pages
                            
                else:
                    # Handle non-LEASE types (HOME_SHARING, PURCHASE)
                    listing_type = {
                        'sale_type': listing_config['sale_type'],
                        'db_type': listing_config['db_type']
                    }
                    self.logger.info(f"Processing {listing_type['sale_type']} listings")
                    
                    for page in range(1, pages + 1):
                        page_listings = await self.process_page(page, listing_type)
                        all_listings.extend(page_listings)
                        await asyncio.sleep(random.uniform(1, 2))  # Delay between pages
                        
                # Add delay between listing types
                await asyncio.sleep(random.uniform(2, 3))
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_listings)}")
            return all_listings
            
        except Exception as e:
            self.logger.error(f"Fatal error in EV10 scraper: {str(e)}")
            return []
            
        finally:
            await self.close_session() asyncio.sleep(30)  # Longer delay for rate limits
                        elif response.status >= 500:  # Server error
                            await asyncio.sleep(5)  # Short delay for server errors
                        continue
                        
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout on attempt {attempt + 1}")
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    return None
                
            await asyncio.sleep(DELAY * (2 ** attempt))  # Exponential backoff
            
        return None

    async def get_listing_details(self, listing_id: str) -> Optional[Dict]:
        """Fetch detailed information for a single listing"""
        try:
            url = self.DETAIL_API_URL.format(listing_id=listing_id)
            self.logger.debug(f"Fetching details for listing {listing_id} from {url}")
            
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            async with self.session.get(url, raise_for_status=False) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                        self.logger.debug(f"Raw listing details: {json.dumps(data)[:1000]}...")
                        return data
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Failed to parse JSON for listing {listing_id}: {e}")
                        return None
                else:
                    self.logger.warning(f"Failed to fetch details for listing {listing_id}, status: {response.status}")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Error fetching details for listing {listing_id}: {str(e)}")
            return None

    def parse_listing(self, listing: Dict, listing_type: Dict) -> Optional[Dict]:
        """Parse listing data into database schema format with enhanced validation"""
        try:
            listing_id = str(listing.get('id'))
            if not listing_id:
                self.logger.warning("Skipping listing without ID")
                return None
                
            # Handle nested objects safely with fallbacks
            subway = listing.get('subway_station', {})
            district = listing.get('district', {})
            
            # Parse and validate dates
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
            
            # Parse and validate numeric fields
            try:
                price = float(listing.get('price', 0))
                if price < 0 or price > 1000000000:  # Basic validation
                    self.logger.warning(f"Invalid price {price} for listing {listing_id}")
                    price = None
            except (TypeError, ValueError):
                price = None
                
            try:
                rooms = int(listing.get('rooms', 0))
                if not 0 <= rooms <= 50:  # Reasonable range
                    rooms = None
            except (TypeError, ValueError):
                rooms = None
                
            try:
                area = float(listing.get('area', 0))
                if not 5 <= area <= 10000:  # Reasonable range
                    area = None
            except (TypeError, ValueError):
                area = None
            
            # Parse amenities and images with validation
            amenities = listing.get('amenities', [])
            if isinstance(amenities, str):
                try:
                    amenities = json.loads(amenities)
                except json.JSONDecodeError:
                    amenities = []
            elif not isinstance(amenities, list):
                amenities = []
            
            images = listing.get('images', [])
            if isinstance(images, str):
                try:
                    images = json.loads(images)
                except json.JSONDecodeError:
                    images = []
            elif not isinstance(images, list):
                images = []
            
            # Clean and validate lat/lon
            lat = listing.get('location_lat')
            lon = listing.get('location_lng')
            if lat is not None and lon is not None:
                try:
                    lat = float(lat)
                    lon = float(lon)
                    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                        lat = lon = None
                except (TypeError, ValueError):
                    lat = lon = None
            
            parsed = {
                'listing_id': listing_id,
                'title': (listing.get('title') or listing.get('address', '')).strip(),
                'metro_station': (subway.get('name') if isinstance(subway, dict) else str(subway)).strip() if subway else None,
                'district': (district.get('name') if isinstance(district, dict) else str(district)).strip() if district else None,
                'address': listing.get('address', '').strip(),
                'location': listing.get('suburban', '').strip(),
                'latitude': lat,
                'longitude': lon,
                'rooms': rooms,
                'area': area,
                'floor': listing.get('floor'),
                'total_floors': listing.get('total_floors'),
                'property_type': listing.get('property_type', 'apartment'),
                'listing_type': listing_type['db_type'],
                'price': price,
                'currency': listing.get('currency', 'AZN'),
                'contact_phone': listing.get('phone_number', '').strip(),
                'whatsapp_available': bool(listing.get('has_whatsapp')),
                'description': listing.get('description', '').strip(),
                'views_count': max(0, int(listing.get('view_count', 0))),
                'created_at': created_at,
                'updated_at': updated_at,
                'listing_date': listing_date,
                'has_repair': bool(listing.get('renovated')),
                'amenities': json.dumps(amenities) if amenities else None,
                'photos': json.dumps(images) if images else None,
                'source_url': urljoin(self.BASE_URL, f"/elan/{listing_id}"),
                'source_website': 'ev10.az'
            }
            
            return parsed
            
        except Exception as e:
            self.logger.error(f"Error parsing listing {listing.get('id', 'unknown')}: {str(e)}")
            return None

    async def process_page(self, page: int, listing_type: Dict) -> List[Dict]:
        """Process a single page of listings with enhanced error handling"""
        listings = []
        try:
            self.logger.info(f"Processing page {page} for {listing_type['sale_type']}")
            response_data = await self.get_page_content(page, listing_type)
            
            if not response_data:
                self.logger.warning(f"Empty response data for page {page}")
                return listings
                
            # Try different possible response structures
            items = None
            if isinstance(response_data, dict):
                for key in ['data', 'postings', 'items']:
                    if key in response_data:
                        items = response_data[key]
                        break
            elif isinstance(response_data, list):
                items = response_data
                
            if not items:
                self.logger.warning(f"No listings found in response for page {page}")
                return listings
                
            self.logger.info(f"Found {len(items)} listings on page {page}")
            
            for item in items:
                try:
                    if isinstance(item, str):
                        listing_details = await self.get_listing_details(item)
                        if listing_details:
                            parsed = self.parse_listing(listing_details, listing_type)
                            if parsed:
                                listings.append(parsed)
                    else:
                        parsed = self.parse_listing(item, listing_type)
                        if parsed:
                            listings.append(parsed)
                except Exception as e:
                    self.logger.error(f"Error processing listing on page {page}: {str(e)}")
                    continue
                    
                # Add small delay between listings
                await asyncio.sleep(random.uniform(0.1, 0.3))
                    
        except Exception as e:
            self.logger.error(f"Error processing page {page}: {str(e)}")
            
        return listings

    async def run(self, pages: int = 1) -> List[Dict]:
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Starting EV10 scraper")
            await self.init_session()
            all_listings = []
            
            for listing_config in self.LISTING_TYPES:
                if 'subtypes' in listing_config:
                    # Handle LEASE type with its subtypes (DAILY, MONTHLY)
                    for subtype in listing_config['subtypes']:
                        listing_type = {
                            'sale_type': listing_config['sale_type'],
                            'lease_type': subtype['lease_type'],
                            'db_type': subtype['db_type']
                        }
                        self.logger.info(f"Processing {listing_type['sale_type']} - {listing_type['lease_type']} listings")
                        
                        for page in range(1, pages + 1):
                            page_listings = await self.process_page(page, listing_type)
                            all_listings.extend(page_listings)
                            await
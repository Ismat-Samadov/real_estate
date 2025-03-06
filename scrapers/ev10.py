import os
import logging
import aiohttp 
import asyncio
import json
import datetime
import random
import traceback
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urljoin

class EV10Scraper:
    """Scraper for ev10.az with API integration"""
    
    BASE_URL = "https://ev10.az"
    API_BASE_URL = "https://ev10.az/api/v1.0/postings"
    DETAIL_API_URL = "https://ev10.az/api/v1.0/postings/{listing_id}"
    
    def __init__(self):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None
        self.proxy_url = None  # Will be set by proxy handler if used
    
    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                              'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9,az;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'DNT': '1',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
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

    def get_request_params(self, page: int) -> Dict:
        """Generate request parameters"""
        return {
            'page_number': str(page),
            'sort_by': 'date_desc',
            'media_type': 'image',
            'page_size': '24',
            'sponsor_seed': str(random.randint(1, 999999)),
            'sponsor_skip': str((page - 1) * 6),
            'sponsor_limit': '6'
        }

    async def fetch_page_data(self, page: int) -> Optional[Dict]:
        """Fetch page content with retry logic and error handling"""
        # fallback defaults
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))
        # DELAY = int(os.getenv('REQUEST_DELAY', '1'))
        DELAY = float(os.getenv('REQUEST_DELAY', '1'))

        
        params = self.get_request_params(page)
        
        for attempt in range(MAX_RETRIES):
            try:
                # small random delay to reduce chance of rate-limit
                await asyncio.sleep(DELAY + random.random())
                
                url = self.API_BASE_URL
                self.logger.debug(f"Requesting URL: {url} with params: {params}")
                
                headers = {
                    'Referer': 'https://ev10.az/',
                    'Accept': 'application/json'
                }
                
                async with self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    proxy=self.proxy_url,
                    raise_for_status=False
                ) as response:
                    if response.status == 200:
                        try:
                            raw_text = await response.text()
                            self.logger.debug(f"Raw API response text: {raw_text[:500]}...")
                            response_data = json.loads(raw_text)
                            self.logger.debug(f"Successfully parsed JSON")
                            return response_data
                        except json.JSONDecodeError as e:
                            self.logger.error(f"Failed to parse JSON response: {e}")
                            self.logger.debug(f"Response content: {await response.text()[:1000]}")
                            continue
                    elif response.status == 403:
                        self.logger.warning(f"Access forbidden (403) on attempt {attempt + 1}")
                        await asyncio.sleep(DELAY * (attempt + 2))
                        continue
                    elif response.status == 429:  # Rate limit
                        self.logger.warning("Rate limit hit, waiting longer")
                        await asyncio.sleep(30)  # Longer delay for rate limits
                        continue
                    elif response.status >= 500:  # Server error
                        self.logger.warning(f"Server error {response.status}")
                        await asyncio.sleep(5)  # Short delay for server errors
                        continue
                    else:
                        self.logger.warning(
                            f"Failed to fetch page {page}, status: {response.status}, "
                            f"response: {await response.text()[:200]}"
                        )
                        continue
                        
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout on attempt {attempt + 1}")
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {str(e)}")
                self.logger.error(traceback.format_exc())
                if attempt == MAX_RETRIES - 1:
                    return None
                
            # exponential backoff if we keep failing
            await asyncio.sleep(DELAY * (2 ** attempt))
            
        return None

    async def get_listing_details(self, listing_id: str) -> Optional[Dict]:
        """Fetch detailed information for a single listing"""
        try:
            url = self.DETAIL_API_URL.format(listing_id=listing_id)
            self.logger.debug(f"Fetching details for listing {listing_id} from {url}")
            
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            headers = {
                'Referer': f'https://ev10.az/elan/{listing_id}',
                'Accept': 'application/json'
            }
            
            async with self.session.get(
                url,
                headers=headers,
                proxy=self.proxy_url,
                raise_for_status=False
            ) as response:
                if response.status == 200:
                    try:
                        raw_text = await response.text()
                        self.logger.debug(f"Raw listing details text: {raw_text[:500]}...")
                        data = json.loads(raw_text)
                        self.logger.debug(f"Successfully parsed listing details JSON")
                        return data
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Failed to parse JSON for listing {listing_id}: {e}")
                        return None
                else:
                    self.logger.warning(
                        f"Failed to fetch details for listing {listing_id}, status: {response.status}"
                    )
                    return None
                    
        except Exception as e:
            self.logger.error(f"Error fetching details for listing {listing_id}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return None

    def determine_listing_type(self, listing: Dict) -> str:
        """Determine the listing type based on the data"""
        sale_type = listing.get('sale_type')
        lease_type = listing.get('lease_type')
        
        # Default to 'sale' if unknown
        if not sale_type:
            return 'sale'
        if sale_type == 'PURCHASE':
            return 'sale'
        elif sale_type == 'HOME_SHARING':
            # Home sharing is typically monthly
            return 'monthly'
        elif sale_type == 'LEASE':
            # Check if it's daily or monthly lease
            if lease_type == 'DAILY':
                return 'daily'
            else:
                return 'monthly'
        
        # Fallback
        return 'sale'

    def parse_timestamp(self, timestamp_value: Any) -> Optional[datetime.datetime]:
        """
        Parse timestamp from various formats safely
        """
        if timestamp_value is None:
            return None
        
        # debug info
        self.logger.debug(f"Parsing timestamp: {timestamp_value} (type: {type(timestamp_value)})")
            
        # integer/float UNIX timestamps
        if isinstance(timestamp_value, (int, float)):
            try:
                return datetime.datetime.fromtimestamp(timestamp_value)
            except (ValueError, OSError, OverflowError) as e:
                self.logger.warning(f"Error parsing integer timestamp {timestamp_value}: {e}")
                return None
        
        # string timestamps
        if isinstance(timestamp_value, str):
            # Try ISO format (with optional milliseconds)
            try:
                if '.' in timestamp_value:
                    timestamp_value = timestamp_value.split('.')[0]
                return datetime.datetime.strptime(timestamp_value, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass
                
            # Try multiple date formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%d",
                "%d.%m.%Y"
            ]
            
            for fmt in formats:
                try:
                    return datetime.datetime.strptime(timestamp_value, fmt)
                except ValueError:
                    continue
        
        # If all fail
        self.logger.warning(f"Could not parse timestamp: {timestamp_value} (type: {type(timestamp_value)})")
        return None

    def parse_listing(self, listing: Dict) -> Optional[Dict]:
        """Parse listing data into database schema format with enhanced validation"""
        try:
            listing_id = str(listing.get('id'))
            if not listing_id:
                self.logger.warning("Skipping listing without ID")
                return None

            self.logger.debug(f"Parsing listing ID: {listing_id}")
            
            # figure out the listing type
            listing_type = self.determine_listing_type(listing)
            
            # Extract metro station
            metro_station = None
            subway_station = listing.get('subway_station', {})
            if subway_station:
                if isinstance(subway_station, dict) and 'name' in subway_station:
                    metro_station = subway_station['name']
                elif isinstance(subway_station, str):
                    metro_station = subway_station
            
            # District can be a str or a dict
            district = None
            if isinstance(listing.get('district'), dict):
                district = listing['district'].get('name')
            else:
                district = listing.get('district')
            
            # Handle city/location information
            location = None
            if listing.get('city'):
                location = listing.get('city')
                
            # handle date fields
            created_at = datetime.datetime.now()
            updated_at = None
            listing_date = None
            
            # parse renewed_at -> updated_at
            if 'renewed_at' in listing:
                renewed_at = listing['renewed_at']
                self.logger.debug(f"Renewed at timestamp before parsing: {renewed_at}")
                updated_at = self.parse_timestamp(renewed_at)
                self.logger.debug(f"Renewed at timestamp after parsing: {updated_at}")
                if updated_at:
                    listing_date = updated_at.date()
            
            # numeric fields
            price = None
            try:
                price_value = listing.get('price')
                if price_value is not None:
                    price = float(price_value)
                    if not (0 < price < 1_000_000_000):
                        self.logger.warning(f"Invalid price {price} for listing {listing_id}")
                        price = None
            except (TypeError, ValueError) as e:
                self.logger.warning(f"Error parsing price for listing {listing_id}: {e}")
                price = None

            rooms = None
            try:
                rooms_value = listing.get('rooms')
                if rooms_value is not None:
                    rooms = int(float(rooms_value))
                    if not (0 <= rooms <= 50):
                        rooms = None
            except (TypeError, ValueError) as e:
                self.logger.warning(f"Error parsing rooms for listing {listing_id}: {e}")
                rooms = None

            area = None
            try:
                area_value = listing.get('area')
                if area_value is not None:
                    area = float(area_value)
                    if not (5 <= area <= 10000):
                        area = None
            except (TypeError, ValueError) as e:
                self.logger.warning(f"Error parsing area for listing {listing_id}: {e}")
                area = None

            floor = None
            try:
                floor_value = listing.get('floor')
                if floor_value is not None:
                    floor = int(float(floor_value))
            except (TypeError, ValueError):
                floor = None

            total_floors = None
            try:
                total_floors_value = listing.get('total_floors')
                if total_floors_value is not None:
                    total_floors = int(float(total_floors_value))
            except (TypeError, ValueError):
                total_floors = None
            # For description handling
            description = ""
            if listing.get('description') is not None:
                # Handle different types for description
                if isinstance(listing.get('description'), str):
                    description = listing.get('description').strip()
                else:
                    # Convert non-string values to string safely
                    try:
                        description = str(listing.get('description')).strip()
                    except Exception as e:
                        self.logger.warning(f"Error converting description to string: {e}")
                        description = ""
            # For amenities handling
            amenities_json = "[]"  # Default empty array
            amenities = listing.get('amenities')

            if amenities:
                self.logger.debug(f"Raw amenities data: {type(amenities)} = {amenities}")
                
                try:
                    if isinstance(amenities, str):
                        # It's already a string, check if it's valid JSON
                        try:
                            # Validate JSON
                            json.loads(amenities)
                            amenities_json = amenities
                        except json.JSONDecodeError:
                            # Not valid JSON, make it a JSON array with one item
                            amenities_json = json.dumps([amenities])
                    elif isinstance(amenities, list):
                        # Make sure all items are strings
                        amenities_list = [str(item) for item in amenities if item is not None]
                        amenities_json = json.dumps(amenities_list)
                    elif isinstance(amenities, dict):
                        # Extract values from dictionary
                        amenities_list = []
                        for key, value in amenities.items():
                            if isinstance(value, str):
                                amenities_list.append(value)
                            elif isinstance(value, bool) and value:
                                amenities_list.append(key)
                            elif value is not None:
                                amenities_list.append(str(value))
                        amenities_json = json.dumps(amenities_list)
                    else:
                        # Unknown type, convert to string and put in array
                        amenities_json = json.dumps([str(amenities)])
                except Exception as e:
                    self.logger.warning(f"Error processing amenities: {e}")
                    amenities_json = "[]"  # Fallback to empty array
            # parse images
            photo_urls = []
            images = listing.get('images', [])
            if isinstance(images, list):
                for img in images:
                    if isinstance(img, dict) and 'medium_quality_url' in img:
                        photo_urls.append(img['medium_quality_url'])
                    elif isinstance(img, dict) and 'url' in img:
                        photo_urls.append(img['url'])
                    elif isinstance(img, str):
                        photo_urls.append(img)
            elif isinstance(images, str):
                try:
                    images_data = json.loads(images)
                    if isinstance(images_data, list):
                        photo_urls.extend(images_data)
                except json.JSONDecodeError:
                    # If not JSON, just assume it's a single URL
                    if images.startswith(('http://', 'https://')):
                        photo_urls.append(images)

            # parse coords
            lat = None
            lon = None
            location_lat = listing.get('location_lat')
            location_lng = listing.get('location_lng')
            if location_lat is not None and location_lng is not None:
                try:
                    lat = float(location_lat)
                    lon = float(location_lng)
                    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                        lat = lon = None
                except (TypeError, ValueError) as e:
                    self.logger.warning(
                        f"Error parsing coordinates for listing {listing_id}: {e}"
                    )
                    lat = lon = None

            # ensure property_type is a str
            property_type = listing.get('property_type', 'apartment')
            if not property_type or not isinstance(property_type, str):
                property_type = 'apartment'
                
            # Handle contact type based on is_agent field
            contact_type = 'agent' if listing.get('is_agent', False) else 'owner'
            self.logger.debug(f"Set contact_type to {contact_type} based on is_agent: {listing.get('is_agent')}")
            
            # finalize record
            parsed = {
                'listing_id': listing_id,
                'title': (listing.get('title') or listing.get('address', '')).strip(),
                'metro_station': metro_station,
                'district': district,
                'address': listing.get('address', '').strip(),
                'location': location or (listing.get('suburban') or '').strip(),
                'latitude': lat,
                'longitude': lon,
                'rooms': rooms,
                'area': area,
                'floor': floor,
                'total_floors': total_floors,
                'property_type': property_type,
                'listing_type': listing_type,
                'price': price,
                'currency': listing.get('currency', 'AZN'),
                'contact_phone': str(listing.get('phone_number', '')).strip(),
                'contact_type': contact_type,
                'whatsapp_available': bool(listing.get('has_whatsapp')),
                'description': description,  # IMPROVED: Better handling of description
                'views_count': max(0, int(listing.get('views_count', 0))),
                'created_at': created_at,
                'updated_at': updated_at,
                'listing_date': listing_date,
                'has_repair': bool(listing.get('renovated')),
                'amenities': amenities_json,
                'photos': json.dumps(photo_urls) if photo_urls else None,
                'source_url': urljoin(self.BASE_URL, f"/elan/{listing_id}"),
                'source_website': 'ev10.az'
            }
            
            # Final validation to ensure critical fields are never null
            for field in ['description', 'amenities']:
                if field not in parsed or parsed[field] is None:
                    if field == 'description':
                        parsed[field] = ""
                    elif field == 'amenities':
                        parsed[field] = "[]"
                    self.logger.warning(f"Had to set {field} to default value")
            
            self.logger.debug(f"Successfully parsed listing {listing_id}")
            return parsed
            
        except Exception as e:
            self.logger.error(f"Error parsing listing {listing.get('id', 'unknown')}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return None

    async def process_page(self, page: int) -> List[Dict]:
        """Process a single page of listings"""
        listings = []
        try:
            self.logger.info(f"Processing page {page}")
            response_data = await self.fetch_page_data(page)
            
            if not response_data:
                self.logger.warning(f"No response data for page {page}")
                return []
            
            self.logger.debug(f"Response data type: {type(response_data)}")
            if isinstance(response_data, dict):
                self.logger.debug(f"Response keys: {list(response_data.keys())}")
            
            postings_data = []
            
            # Attempt to extract listings from known keys
            if isinstance(response_data, dict):
                for key in ['postings', 'data', 'items']:
                    if key in response_data:
                        postings_data = response_data[key]
                        self.logger.debug(f"Found listings under key: {key}")
                        break
            elif isinstance(response_data, list):
                postings_data = response_data
                self.logger.debug("Response data is already a list")
            
            if not postings_data:
                self.logger.warning(f"Could not find listings in the response for page {page}")
                # For debugging, log a snippet of the response
                if isinstance(response_data, dict):
                    self.logger.debug(
                        f"Response sample: {json.dumps(dict(list(response_data.items())[:5]))}"
                    )
                else:
                    self.logger.debug(f"Response sample: {response_data}")
                return []
                
            self.logger.info(f"Found {len(postings_data)} listings on page {page}")
            
            # parse each posting
            for posting_idx, posting in enumerate(postings_data):
                try:
                    self.logger.debug(f"Processing posting {posting_idx+1}/{len(postings_data)}")
                    self.logger.debug(f"Posting type: {type(posting)}")
                    
                    # If the API returned only an ID, fetch details
                    if isinstance(posting, (str, int)):
                        posting_id = str(posting)
                        self.logger.debug(f"Fetching details for posting ID: {posting_id}")
                        details = await self.get_listing_details(posting_id)
                        if details:
                            parsed = self.parse_listing(details)
                            if parsed:
                                listings.append(parsed)
                    else:
                        # We already have the full data
                        parsed = self.parse_listing(posting)
                        if parsed:
                            listings.append(parsed)
                except Exception as e:
                    self.logger.error(f"Error processing posting on page {page}: {str(e)}")
                    self.logger.error(traceback.format_exc())
                    continue
                
                # small random delay to avoid rate limits
                await asyncio.sleep(random.uniform(0.1, 0.3))
                
            return listings
            
        except Exception as e:
            self.logger.error(f"Error processing page {page}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return []
                          
    async def run(self, pages: int = 1) -> List[Dict]:
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Starting EV10 scraper")
            await self.init_session()
            all_listings = []
            
            # Process each page
            for page in range(1, pages + 1):
                page_listings = await self.process_page(page)
                all_listings.extend(page_listings)
                self.logger.info(f"Added {len(page_listings)} listings from page {page}")
                
                # add a delay between pages
                if page < pages:
                    await asyncio.sleep(random.uniform(1, 2))
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_listings)}")
            return all_listings
            
        except Exception as e:
            self.logger.error(f"Fatal error in EV10 scraper: {str(e)}")
            self.logger.error(traceback.format_exc())
            return []
            
        finally:
            await self.close_session()

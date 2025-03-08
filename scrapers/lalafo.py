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
import uuid

class LalafoScraper:
    """Scraper for lalafo.az that works with HTML pages instead of direct API calls"""
    
    BASE_URL = "https://lalafo.az"
    LISTINGS_URL = "https://lalafo.az/azerbaijan/nedvizhimost"
    
    def __init__(self):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None
        self.proxy_url = None  # Will be set by proxy handler
    
    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            # If get_page_content is already provided by proxy_handler, we don't need to create a session
            if hasattr(self, 'get_page_content') and callable(self.get_page_content):
                self.logger.info("Using proxy handler's get_page_content method")
                return
                
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,az;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0'
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

    async def get_page_content_internal(self, url: str, params: Optional[Dict] = None) -> str:
        """Internal method to fetch page content using our own session
        This is only used if the proxy handler doesn't provide a get_page_content method"""
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
        DELAY = float(os.getenv('REQUEST_DELAY', 1))
        
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(DELAY + random.random() * 2)
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        self.logger.warning(f"Failed to fetch {url}, status: {response.status}")
                        
            except Exception as e:
                self.logger.error(f"Error fetching {url}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                
            await asyncio.sleep(DELAY * (attempt + 1))
        
        raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} attempts")

    async def fetch_page(self, url: str, params: Optional[Dict] = None) -> str:
        """Fetch a page using either the proxy handler's method or our internal method"""
        try:
            if hasattr(self, 'get_page_content') and callable(self.get_page_content):
                # Using proxy handler's method
                # If params, add them as query parameters to the URL
                if params:
                    query_parts = []
                    for key, value in params.items():
                        query_parts.append(f"{key}={value}")
                    query_string = "&".join(query_parts)
                    url = f"{url}?{query_string}"
                
                self.logger.info(f"Fetching page with proxy handler: {url}")
                return await self.get_page_content(url)
            else:
                # Using our internal method
                self.logger.info(f"Fetching page with internal method: {url}")
                return await self.get_page_content_internal(url, params)
        except Exception as e:
            self.logger.error(f"Error fetching page {url}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return ""

    def extract_listings_from_html(self, html: str) -> List[Dict]:
        """Extract listing data from HTML using BeautifulSoup"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            # Find all listing cards on the page
            for card in soup.select('.listing-card'):
                try:
                    # Get the listing ID and URL
                    link = card.select_one('a.listing-card-title')
                    if not link:
                        continue
                        
                    href = link.get('href', '')
                    if not href:
                        continue
                        
                    # Extract listing ID from URL
                    id_match = re.search(r'id-(\d+)', href)
                    if not id_match:
                        continue
                        
                    listing_id = id_match.group(1)
                    listing_url = self.BASE_URL + href
                    
                    # Extract title
                    title = link.text.strip() if link else ''
                    
                    # Extract price
                    price_elem = card.select_one('.listing-card-price')
                    price = None
                    currency = 'AZN'
                    if price_elem:
                        price_text = price_elem.text.strip()
                        # Extract numeric price
                        price_match = re.search(r'(\d[\d\s.,]+)', price_text)
                        if price_match:
                            price_str = re.sub(r'[^\d.]', '', price_match.group(1))
                            try:
                                price = float(price_str)
                            except (ValueError, TypeError):
                                price = None
                        
                        # Extract currency
                        if 'USD' in price_text or '$' in price_text:
                            currency = 'USD'
                    
                    # Extract location
                    location_elem = card.select_one('.listing-card-location')
                    location = location_elem.text.strip() if location_elem else ''
                    
                    # Determine property type
                    property_type = 'apartment'  # default
                    
                    # Determine listing type (sale, rent, etc.)
                    listing_type = 'sale'  # default
                    if '/prodazha-' in href:
                        listing_type = 'sale'
                    elif '/arenda-' in href:
                        if '/sutochnaya-' in href:
                            listing_type = 'daily'
                        else:
                            listing_type = 'monthly'
                    
                    # Extract thumbnail image
                    img = card.select_one('img')
                    thumbnail = img.get('src') if img else None
                    photos = [thumbnail] if thumbnail else []
                    
                    # Create the listing object
                    listing_data = {
                        'listing_id': listing_id,
                        'title': title,
                        'source_url': listing_url,
                        'source_website': 'lalafo.az',
                        'price': price,
                        'currency': currency,
                        'location': location,
                        'property_type': property_type,
                        'listing_type': listing_type,
                        'photos': json.dumps(photos) if photos else None,
                        'created_at': datetime.datetime.now()
                    }
                    
                    listings.append(listing_data)
                    
                except Exception as e:
                    self.logger.error(f"Error parsing listing card: {str(e)}")
                    continue
        except Exception as e:
            self.logger.error(f"Error extracting listings from HTML: {str(e)}")
            
        return listings

    async def extract_listing_details(self, html: str, listing_id: str) -> Dict:
        """Extract detailed information from a listing page"""
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            data = {
                'listing_id': listing_id,
                'source_website': 'lalafo.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract title
            title_elem = soup.select_one('h1.adp-title')
            if title_elem:
                data['title'] = title_elem.text.strip()
            
            # Extract description
            desc_elem = soup.select_one('.adp-description')
            if desc_elem:
                data['description'] = desc_elem.text.strip()
            
            # Extract price
            price_elem = soup.select_one('.adp-price')
            if price_elem:
                price_text = price_elem.text.strip()
                # Extract numeric price
                price_match = re.search(r'(\d[\d\s.,]+)', price_text)
                if price_match:
                    price_str = re.sub(r'[^\d.]', '', price_match.group(1))
                    try:
                        data['price'] = float(price_str)
                    except (ValueError, TypeError):
                        pass
                
                # Extract currency
                if 'USD' in price_text or '$' in price_text:
                    data['currency'] = 'USD'
                else:
                    data['currency'] = 'AZN'
            
            # Extract location
            location_elem = soup.select_one('.adp-location')
            if location_elem:
                data['location'] = location_elem.text.strip()
                
                # Try to extract district from location
                district_pattern = re.compile(r'(\w+)\s+r\.')
                district_match = district_pattern.search(location_elem.text)
                if district_match:
                    data['district'] = district_match.group(1)
            
            # Extract property parameters
            for param in soup.select('.adp-characteristics-item'):
                label = param.select_one('.adp-characteristics-label')
                value = param.select_one('.adp-characteristics-value')
                
                if not label or not value:
                    continue
                    
                label_text = label.text.strip().lower()
                value_text = value.text.strip()
                
                # Extract rooms
                if 'otaq' in label_text:
                    try:
                        rooms = int(re.search(r'\d+', value_text).group())
                        data['rooms'] = rooms
                    except (ValueError, AttributeError):
                        pass
                
                # Extract area
                elif 'sahə' in label_text:
                    try:
                        area = float(re.search(r'\d+', value_text).group())
                        data['area'] = area
                    except (ValueError, AttributeError):
                        pass
                
                # Extract floor
                elif 'mərtəbə' in label_text:
                    floor_match = re.search(r'(\d+)/(\d+)', value_text)
                    if floor_match:
                        try:
                            data['floor'] = int(floor_match.group(1))
                            data['total_floors'] = int(floor_match.group(2))
                        except (ValueError, TypeError):
                            pass
            
            # Extract photos
            photos = []
            for img in soup.select('.adp-gallery img'):
                src = img.get('src')
                if src:
                    photos.append(src)
            
            if photos:
                data['photos'] = json.dumps(photos)
            
            # Extract amenities
            amenities = []
            for item in soup.select('.adp-tags-item'):
                text = item.text.strip()
                if text:
                    amenities.append(text)
            
            if amenities:
                data['amenities'] = json.dumps(amenities)
            
            # Extract contact info
            contact_name = soup.select_one('.adp-seller-name')
            if contact_name:
                data['contact_name'] = contact_name.text.strip()
            
            # Contact phone might be hidden behind JS, so we can't easily get it
            
            # Extract property type
            if 'property_type' not in data:
                if any('yeni tikili' in tag.text.lower() for tag in soup.select('.adp-tags-item')):
                    data['property_type'] = 'new'
                elif any('köhnə tikili' in tag.text.lower() for tag in soup.select('.adp-tags-item')):
                    data['property_type'] = 'old'
                elif any(x in data.get('title', '').lower() for x in ['ev', 'villa', 'həyət']):
                    data['property_type'] = 'house'
                else:
                    data['property_type'] = 'apartment'
            
            # Determine if has repair
            data['has_repair'] = any('təmir' in tag.text.lower() for tag in soup.select('.adp-tags-item'))
            
            # Extract listing type if not already determined
            listing_path = data.get('source_url', '')
            if '/prodazha-' in listing_path:
                data['listing_type'] = 'sale'
            elif '/arenda-' in listing_path:
                if '/sutochnaya-' in listing_path:
                    data['listing_type'] = 'daily'
                else:
                    data['listing_type'] = 'monthly'
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error extracting details for listing {listing_id}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return {'listing_id': listing_id, 'source_website': 'lalafo.az'}

    async def run(self, pages: int = 1):
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Starting Lalafo.az scraper")
            await self.init_session()
            all_results = []
            
            # Page URLs in lalafo follow this pattern
            base_url = self.LISTINGS_URL
            
            for page in range(1, pages + 1):
                try:
                    # Construct page URL
                    page_url = f"{base_url}?page={page}"
                    
                    # Fetch page content
                    self.logger.info(f"Fetching page {page}")
                    html = await self.fetch_page(page_url)
                    
                    if not html:
                        self.logger.warning(f"Empty HTML received for page {page}")
                        continue
                    
                    # Extract listings from the page
                    listings = self.extract_listings_from_html(html)
                    self.logger.info(f"Found {len(listings)} listings on page {page}")
                    
                    # Process each listing to get details
                    for listing in listings:
                        try:
                            # Only process if we have a valid URL
                            if not listing.get('source_url'):
                                continue
                                
                            # Fetch and parse the detailed listing page
                            self.logger.info(f"Fetching details for listing {listing['listing_id']}")
                            detail_html = await self.fetch_page(listing['source_url'])
                            
                            if not detail_html:
                                self.logger.warning(f"Empty HTML for listing details {listing['listing_id']}")
                                all_results.append(listing)  # Add the basic listing anyway
                                continue
                                
                            details = await self.extract_listing_details(detail_html, listing['listing_id'])
                            merged_listing = {**listing, **details}
                            all_results.append(merged_listing)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing listing {listing.get('listing_id')}: {str(e)}")
                            # Add the basic listing anyway
                            all_results.append(listing)
                            continue
                            
                        # Add delay between listings
                        await asyncio.sleep(random.uniform(1, 2))
                        
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    continue
                    
                # Add delay between pages
                if page < pages:
                    await asyncio.sleep(random.uniform(2, 3))
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        except Exception as e:
            self.logger.error(f"Fatal error in Lalafo scraper: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return []
            
        finally:
            await self.close_session()
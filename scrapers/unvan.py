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

class UnvanScraper:
    """Scraper for unvan.az"""
    
    BASE_URL = "https://unvan.az"
    LISTINGS_URL = "https://unvan.az/menzil"
    
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
                'Accept-Language': 'en-US,en;q=0.9,az;q=0.8',
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
        
        # Add request-specific headers and cookies
        headers = {
            'Referer': 'https://unvan.az/',
            'Origin': 'https://unvan.az',
            'Host': 'unvan.az'
        }
        
        cookies = {
            'language': 'az'
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                self.logger.info(f"Attempt {attempt + 1} of {MAX_RETRIES}")
                await asyncio.sleep(DELAY + random.random() * 2)
                
                async with self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        content = await response.text()
                        elapsed = time.time() - start_time
                        self.logger.info(f"Successfully fetched content in {elapsed:.2f} seconds")
                        return content
                    elif response.status == 403:
                        self.logger.warning(f"Access forbidden (403) on attempt {attempt + 1}")
                        await asyncio.sleep(DELAY * (attempt + 2))
                    else:
                        self.logger.warning(f"Failed to fetch {url}, status: {response.status}")
                        
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout on attempt {attempt + 1}")
            except Exception as e:
                self.logger.error(f"Error fetching {url}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                
            await asyncio.sleep(DELAY * (attempt + 1))
        
        raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} attempts")

    async def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings page and extract basic listing information"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        for listing in soup.select('.index.prodbig'):
            try:
                # Extract listing URL and ID
                link_elem = listing.select_one('.prodname a')
                if not link_elem:
                    continue
                    
                listing_url = f"{self.BASE_URL}{link_elem.get('href')}"
                listing_id = re.search(r'-(\d+)\.html$', listing_url)
                if not listing_id:
                    continue
                listing_id = listing_id.group(1)
                
                # Extract title and parse rooms/district
                title = link_elem.text.strip()
                rooms_match = re.search(r'(\d+)\s*otaq', title)
                rooms = int(rooms_match.group(1)) if rooms_match else None
                
                district_match = re.search(r',\s*([^,]+)\s*rayonu', title)
                district = district_match.group(1) if district_match else None
                
                # Extract price
                price_elem = listing.select_one('.sprice')
                price_text = price_elem.text.strip() if price_elem else None
                price = float(re.sub(r'[^\d.]', '', price_text)) if price_text else None
                
                # Extract description
                desc_elem = listing.select_one('.prodful')
                description = desc_elem.text.strip() if desc_elem else None
                
                # Basic listing data
                listing_data = {
                    'listing_id': listing_id,
                    'title': title,
                    'description': description,
                    'district': district,
                    'rooms': rooms,
                    'price': price,
                    'currency': 'AZN',
                    'source_url': listing_url,
                    'source_website': 'unvan.az',
                    'created_at': datetime.datetime.now(),
                    'listing_type': 'sale'  # Will be refined in detail page
                }
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
                
        return listings

    def extract_address(self, html: str) -> Optional[str]:
        """Extract address from HTML

        Args:
            html: HTML content of the detail page
                
        Returns:
            Address string if found, None otherwise
        """
        soup = BeautifulSoup(html, 'lxml')

        # Check for address in the "Ünvan:" format in the linkteshow section
        address_elem = soup.select_one('.infop100.linkteshow')
        if address_elem:
            address_text = address_elem.text.strip()
            # Check for explicit address in format "Ünvan: Something"
            unvan_match = re.search(r'Ünvan:\s*(.*?)(?:\s*<br>|\s*$)', str(address_elem))
            if unvan_match:
                return unvan_match.group(1).strip()
        
        # Try alternate method - look for <p> tags containing address information
        address_p_tags = soup.select('p.infop100')
        for p_tag in address_p_tags:
            if 'Ünvan:' in p_tag.text:
                address = p_tag.text.replace('Ünvan:', '').strip()
                return address
                
        # Additional fallback for other formats
        address_sections = soup.select('.map-address h4, .address-section, .contact-address')
        for section in address_sections:
            if 'Ünvan:' in section.text:
                return section.text.replace('Ünvan:', '').strip()
                
        return None

    def extract_location_info(self, html: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Extract location information like city, district, neighborhood, and metro station
        
        Args:
            html: HTML content of the detail page
                
        Returns:
            Tuple of (city, district, neighborhood, metro_station)
        """
        soup = BeautifulSoup(html, 'lxml')
        city = None
        district = None
        neighborhood = None
        metro_station = None
        
        # Find the linkteshow section that contains location links
        location_elem = soup.select_one('.infop100.linkteshow')
        if location_elem:
            # Extract all links with their titles
            links = location_elem.select('a')
            for link in links:
                href = link.get('href', '')
                title = link.get('title', '')
                text = link.text.strip()
                
                # Categorize based on link/title content
                if 'rayonu' in href or 'rayonu' in title:
                    district = text
                elif 'mikrorayon' in href or 'qesebesi' in href:
                    neighborhood = text
                elif 'metrosu' in href or 'metro' in title:
                    metro_station = text
                elif href.startswith('/') and len(href.split('-')) == 1:
                    # Likely a city name (like /baki)
                    city = text
                    
        return city, district, neighborhood, metro_station

    def extract_amenities(self, html: str) -> List[str]:
        """
        Extract amenities and location info to add to amenities list
        
        Args:
            html: HTML content of the detail page
                
        Returns:
            List of amenity strings
        """
        soup = BeautifulSoup(html, 'lxml')
        amenities = []
        
        # Extract location information
        city, district, neighborhood, metro = self.extract_location_info(html)
        
        # Add location info to amenities
        if city:
            amenities.append(f"Şəhər: {city}")
        if district:
            amenities.append(f"Rayon: {district}")
        if neighborhood:
            amenities.append(f"Qəsəbə: {neighborhood}")
        if metro:
            amenities.append(f"Metro: {metro}")
        
        # Look for other property features
        for feature in soup.select('.property_lists li, .features li, .amenities li'):
            text = feature.text.strip()
            if text and text not in amenities:
                amenities.append(text)
                
        return amenities

    async def parse_listing_detail(self, html: str, listing_id: str) -> Dict:
        """Parse the detailed listing page"""
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'unvan.az',
                'updated_at': datetime.datetime.now()
            }
            
            # Extract address
            address = self.extract_address(html)
            if address:
                data['address'] = address
                
            # Extract location information for data fields
            city, district, neighborhood, metro_station = self.extract_location_info(html)
            if district:
                data['district'] = district
            if metro_station:
                data['metro_station'] = metro_station
            if neighborhood:
                data['location'] = neighborhood
                
            # Extract amenities including location info
            amenities = self.extract_amenities(html)
            if amenities:
                data['amenities'] = json.dumps(amenities)
            
            # Extract area
            area_elem = soup.select_one('p:-soup-contains("Sahə")')
            if area_elem:
                area_match = re.search(r'(\d+)\s*m²', area_elem.text)
                if area_match:
                    data['area'] = float(area_match.group(1))
            
            # Extract property type
            prop_type_elem = soup.select_one('p:-soup-contains("Əmlakın növü")')
            if prop_type_elem:
                if 'köhnə tikili' in prop_type_elem.text.lower():
                    data['property_type'] = 'old'
                elif 'yeni tikili' in prop_type_elem.text.lower():
                    data['property_type'] = 'new'
                elif 'həyət evi' in prop_type_elem.text.lower():
                    data['property_type'] = 'house'
                elif 'villa' in prop_type_elem.text.lower():
                    data['property_type'] = 'villa'
            
            # Extract location info
            location_elem = soup.select_one('.infop100.linkteshow')
            if location_elem:
                addresses = [a.text.strip() for a in location_elem.select('a')]
                if addresses:
                    data['district'] = next((addr for addr in addresses if 'rayonu' in addr.lower()), None)
                    data['metro_station'] = next((addr for addr in addresses if 'metro' in addr.lower()), None)
                    data['location'] = ' '.join(addresses)
            
            # Extract contact info
            contact_elem = soup.select_one('.infocontact')
            if contact_elem:
                phone_elem = contact_elem.select_one('#telshow')
                if phone_elem:
                    data['contact_phone'] = phone_elem.text.strip()
                
                contact_type = contact_elem.select_one('.glyphicon-user')
                if contact_type and 'Vastəçi' in contact_type.parent.text:
                    data['contact_type'] = 'agent'
                else:
                    data['contact_type'] = 'owner'
            
            # Extract listing type from title/description
            title = soup.select_one('h1.leftfloat')
            if title:
                title_text = title.text.lower()
                if 'kirayə' in title_text or 'icarə' in title_text:
                    if 'günlük' in title_text:
                        data['listing_type'] = 'daily'
                    else:
                        data['listing_type'] = 'monthly'
                else:
                    data['listing_type'] = 'sale'
            
            # Extract photos
            photos = []
            photo_elems = soup.select('#picsopen img[src]')
            for img in photo_elems:
                src = img.get('src')
                if src and not src.endswith('load.gif'):
                    photos.append(f"{self.BASE_URL}{src}")
            
            if photos:
                data['photos'] = json.dumps(photos)
            
            # Extract listing date
            date_elem = soup.select_one('.viewsbb')
            if date_elem:
                try:
                    date_str = re.search(r'Tarix:\s*(\d{2}\.\d{2}\.\d{4})', date_elem.text).group(1)
                    data['listing_date'] = datetime.datetime.strptime(date_str, '%d.%m.%Y').date()
                except (ValueError, AttributeError):
                    pass
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing listing detail {listing_id}: {str(e)}")
            raise

    async def run(self, pages: int = 1):
        """Run the scraper for specified number of pages"""
        try:
            self.logger.info("Starting Unvan.az scraper")
            await self.init_session()
            all_results = []
            
            for page in range(1, pages + 1):
                try:
                    # Construct URL with page parameter
                    url = f"{self.LISTINGS_URL}?satilir&start={(page-1)*10}"
                    
                    # Fetch and parse listings page
                    self.logger.info(f"Processing page {page}")
                    html = await self.get_page_content(url)
                    listings = await self.parse_listing_page(html)
                    
                    # Fetch and parse each listing detail
                    for listing in listings:
                        try:
                            detail_html = await self.get_page_content(listing['source_url'])
                            detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                            all_results.append({**listing, **detail_data})
                        except Exception as e:
                            self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                            continue
                            
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {str(e)}")
                    continue
                    
                # Add delay between pages
                await asyncio.sleep(random.uniform(1, 2))
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        finally:
            await self.close_session()
import asyncio
import aiohttp
import random
import os
import hashlib
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Optional, Tuple
import datetime
import re
import json
import time

class UnvanScraper:
    """Scraper for unvan.az with enhanced phone number extraction via Ajax"""
    
    BASE_URL = "https://unvan.az"
    LISTINGS_URL = "https://unvan.az/menzil"
    AJAX_URL = "https://unvan.az/ajax.php"
    
    def __init__(self):
        """Initialize the scraper with configuration"""
        self.logger = logging.getLogger(__name__)
        self.session = None
        self.proxy_url = None  # Will be set by proxy handler if used
    
    async def init_session(self):
        """Initialize aiohttp session with browser-like headers"""
        if not self.session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,az;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': '"Chromium";v="134", "Not_A-Brand";v="24", "Google Chrome";v="134"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"macOS"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'DNT': '1'
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
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))
        DELAY = float(os.getenv('REQUEST_DELAY', '1'))
        
        self.logger.info(f"Attempting to fetch URL: {url}")
        start_time = time.time()
        
        # Add request-specific headers and cookies
        headers = {
            'Referer': 'https://unvan.az/',
            'Origin': 'https://unvan.az',
            'Host': 'unvan.az'
        }
        
        cookies = {
            'language': 'az',
            # We can add more cookies if needed
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
                    timeout=aiohttp.ClientTimeout(total=15),
                    proxy=self.proxy_url
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

    async def get_phone_numbers_with_params(self, ajax_id: str, ajax_type: str, ajax_hash: str, ajax_rf: str, referer_url: str) -> List[str]:
        """
        Fetch phone numbers using parameters extracted from the HTML
        
        Args:
            ajax_id: The ID parameter for the Ajax request
            ajax_type: The type parameter for the Ajax request
            ajax_hash: The hash parameter for the Ajax request
            ajax_rf: The referrer parameter for the Ajax request
            referer_url: The URL of the listing page (for the Referer header)
            
        Returns:
            List of phone numbers
        """
        try:
            self.logger.info(f"Fetching phone numbers with extracted params - ID: {ajax_id}, Hash: {ajax_hash}, RF: {ajax_rf}")
            
            # Prepare the payload with the exact parameters from the HTML
            payload = {
                'act': 'telshow',
                'id': ajax_id,
                't': ajax_type,
                'h': ajax_hash,
                'rf': ajax_rf
            }
            
            self.logger.debug(f"Ajax payload: {payload}")
            
            # Set up headers for the Ajax request
            headers = {
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Origin': 'https://unvan.az',
                'Referer': referer_url,
                'X-Requested-With': 'XMLHttpRequest',
                'DNT': '1',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
            }
            
            # Add cookies from observed requests
            cookies = {
                'unv_lang': 'az'
            }
            
            # Add delay to mimic human behavior
            await asyncio.sleep(random.uniform(1, 2))
            
            # Make the POST request
            async with self.session.post(
                self.AJAX_URL,
                data=payload,
                headers=headers,
                cookies=cookies,
                proxy=self.proxy_url,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    try:
                        # Try to parse the JSON response
                        resp_text = await response.text()
                        self.logger.debug(f"Raw Ajax response: {resp_text}")
                        
                        data = json.loads(resp_text)
                        
                        # Extract phone numbers from the 'tel' field if available
                        if data and 'ok' in data and data['ok'] == 1 and 'tel' in data:
                            # Clean and format phone numbers
                            phone_numbers = data['tel'].split(',')
                            cleaned_numbers = []
                            
                            for phone in phone_numbers:
                                # Remove non-digit characters
                                cleaned = re.sub(r'\D', '', phone)
                                # Make sure it's a valid length
                                if len(cleaned) >= 9:
                                    cleaned_numbers.append(cleaned)
                            
                            self.logger.info(f"Successfully retrieved {len(cleaned_numbers)} phone numbers: {cleaned_numbers}")
                            return cleaned_numbers
                        else:
                            # If there's an error in the response
                            self.logger.warning(f"Ajax response didn't contain valid phone numbers: {data}")
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Error parsing Ajax response as JSON: {str(e)}")
                        self.logger.error(f"Response content: {await response.text()}")
                    except Exception as e:
                        self.logger.error(f"General error processing Ajax response: {str(e)}")
                else:
                    self.logger.warning(f"Ajax request failed with status {response.status}")
                    
                    # Try to log the error response
                    try:
                        error_text = await response.text()
                        self.logger.error(f"Error response content: {error_text[:200]}")
                    except:
                        pass

        except Exception as e:
            self.logger.error(f"Error retrieving phone numbers for listing with ID {ajax_id}: {str(e)}")

        return []

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
                elif any(x in href or x in title for x in ['mikrorayon', 'qesebesi', 'qəsəbəsi', 'massivi']):
                    neighborhood = text
                elif 'metrosu' in href or 'metro' in title:
                    metro_station = text
                elif href.startswith('/') and len(href.split('-')) <= 2:
                    # Likely a city name (like /baki)
                    city = text
        
        # If no location info was found in links, try parsing the text directly
        if not (city or district or neighborhood) and location_elem:
            location_text = location_elem.get_text()
            
            # Try to extract from formats like "Bakı Şəhəri, Suraxanı rayonu, Hövsan qəsəbəsi"
            city_match = re.search(r'(Bakı|Baku)\s*[Şş]əhəri', location_text, re.IGNORECASE)
            if city_match:
                city = city_match.group(0)
                
            district_match = re.search(r'(\w+)\s*rayonu', location_text, re.IGNORECASE)
            if district_match:
                district = district_match.group(1)
                
            # Look for common neighborhood identifiers
            neighborhood_patterns = [
                r'(\w+)\s*qəsəbəsi',
                r'(\w+)\s*qesebesi',
                r'(\w+)\s*qəsəbə',
                r'(\w+)\s*qesebe',
                r'(\w+)\s*massivi'
            ]
            
            for pattern in neighborhood_patterns:
                match = re.search(pattern, location_text, re.IGNORECASE)
                if match:
                    neighborhood = match.group(1)
                    break
                    
        return city, district, neighborhood, metro_station

    def extract_address(self, html: str) -> Optional[str]:
        """Extract address from HTML with improved handling of various formats

        Args:
            html: HTML content of the detail page
                
        Returns:
            Address string if found, None otherwise
        """
        soup = BeautifulSoup(html, 'lxml')
        
        # Check for address in the "Ünvan:" format in the linkteshow section
        address_elem = soup.select_one('.infop100.linkteshow')
        if address_elem:
            # Find explicit "Ünvan:" text
            unvan_match = re.search(r'Ünvan:\s*(.*?)(?:\s*<br>|\s*$)', str(address_elem))
            if unvan_match:
                return unvan_match.group(1).strip()
                
            # If no explicit "Ünvan:" found, try to extract last line after all <a> tags
            # which often contains the street address
            links = address_elem.find_all('a')
            if links:
                text_chunks = [chunk.strip() for chunk in address_elem.get_text().split('\n')]
                # Look for text chunks that aren't in any of the link texts
                link_texts = [link.get_text().strip() for link in links]
                for chunk in text_chunks:
                    chunk = chunk.strip()
                    if chunk and chunk not in link_texts and 'Ünvan:' not in chunk:
                        # Check if this looks like a street address (not a district or neighborhood)
                        if not any(x in chunk.lower() for x in ['rayonu', 'qəsəbəsi', 'şəhəri', 'massivi']):
                            return chunk
            
            # Last part may contain address after stripping "Ünvan:" if present
            address_text = address_elem.get_text().strip()
            if 'Ünvan:' in address_text:
                return address_text.split('Ünvan:')[-1].strip()
        
        # Try alternate pattern matching for addresses with street names and numbers
        # Look for common address patterns in the full page text
        full_text = soup.get_text()
        address_patterns = [
            r'(?:Ünvan:|küçəsi|prospekti|bulvarı)[\s:]*([^\n.,]+\d+[^\n.,]*)',  # Street with number
            r'([A-ZƏÇŞĞİÖÜəçşğıöü][a-zəçşğıöü]+ [A-ZƏÇŞĞİÖÜəçşğıöü][a-zəçşğıöü]+ \d+)',  # Name Name Number format
            r'([A-ZƏÇŞĞİÖÜəçşğıöü][a-zəçşğıöü]+ \d+)',  # Name Number format
        ]
        
        for pattern in address_patterns:
            match = re.search(pattern, full_text)
            if match:
                address = match.group(1).strip()
                # Validate it's not just a neighborhood or district
                if not any(x in address.lower() for x in ['rayonu', 'qəsəbəsi', 'şəhəri']):
                    return address
        
        return None

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

    async def parse_listing_detail(self, html: str, listing_id: str, listing_url: str) -> Dict:
        """
        Parse the detailed listing page
        
        Args:
            html: HTML content of the listing detail page
            listing_id: The ID of the listing
            listing_url: The URL of the listing page (needed for Ajax requests)
            
        Returns:
            Dictionary with parsed listing details
        """
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'unvan.az',
                'source_url': listing_url,
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
            
            # Get phone numbers via Ajax
            phones = await self.get_phone_numbers_with_params(listing_id, listing_url)
            if phones:
                # Store the primary phone number
                data['contact_phone'] = phones[0]
                
                # If there are multiple phone numbers, add them to amenities
                if len(phones) > 1 and 'amenities' in data:
                    try:
                        amenities_data = json.loads(data['amenities'])
                        # Add phone numbers to amenities
                        for i, phone in enumerate(phones):
                            amenities_data.append(f"Phone #{i+1}: {phone}")
                        # Update amenities
                        data['amenities'] = json.dumps(amenities_data)
                    except json.JSONDecodeError:
                        # If amenities couldn't be parsed as JSON, create a new list
                        data['amenities'] = json.dumps([f"Phone #{i+1}: {phone}" for i, phone in enumerate(phones)])
            
            # Extract contact type
            contact_elem = soup.select_one('.infocontact')
            if contact_elem:
                contact_type = contact_elem.select_one('.glyphicon-user')
                if contact_type and contact_type.parent and 'Vastəçi' in contact_type.parent.text:
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

    async def parse_listing_detail(self, html: str, listing_id: str, listing_url: str) -> Dict:
        """
        Parse the detailed listing page with enhanced phone number extraction
        
        Args:
            html: HTML content of the listing detail page
            listing_id: The ID of the listing
            listing_url: The URL of the listing page (needed for Ajax requests)
            
        Returns:
            Dictionary with parsed listing details
        """
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            data = {
                'listing_id': listing_id,
                'source_website': 'unvan.az',
                'source_url': listing_url,
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
            
            # Extract phone hash and parameters from the HTML
            phone_elem = soup.select_one('#telshow')
            if phone_elem:
                # Extract hash and other parameters directly from the HTML
                ajax_id = phone_elem.get('data-id')
                ajax_type = phone_elem.get('data-t')
                ajax_hash = phone_elem.get('data-h')
                ajax_rf = phone_elem.get('data-rf')
                
                self.logger.info(f"Extracted Ajax params from HTML - ID: {ajax_id}, Type: {ajax_type}, Hash: {ajax_hash}, RF: {ajax_rf}")
                
                # Make the Ajax request with the extracted parameters
                if ajax_id and ajax_hash and ajax_rf:
                    phones = await self.get_phone_numbers_with_params(ajax_id, ajax_type, ajax_hash, ajax_rf, listing_url)
                    if phones:
                        # Store the primary phone number
                        data['contact_phone'] = phones[0]
                        
                        # If there are multiple phone numbers, add them to amenities
                        if len(phones) > 1 and 'amenities' in data:
                            try:
                                amenities_data = json.loads(data['amenities'])
                                # Add phone numbers to amenities
                                for i, phone in enumerate(phones):
                                    amenities_data.append(f"Phone #{i+1}: {phone}")
                                # Update amenities
                                data['amenities'] = json.dumps(amenities_data)
                            except json.JSONDecodeError:
                                # If amenities couldn't be parsed as JSON, create a new list
                                data['amenities'] = json.dumps([f"Phone #{i+1}: {phone}" for i, phone in enumerate(phones)])
            
            # Extract contact type
            contact_elem = soup.select_one('.infocontact')
            if contact_elem:
                contact_type = contact_elem.select_one('.glyphicon-user')
                if contact_type and contact_type.parent and 'Vastəçi' in contact_type.parent.text:
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
            self.logger.info("Starting Unvan.az scraper with enhanced phone number extraction")
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
                            detail_data = await self.parse_listing_detail(
                                detail_html, 
                                listing['listing_id'],
                                listing['source_url']
                            )
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
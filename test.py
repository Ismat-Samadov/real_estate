class BinaScraper:
    """Scraper for bina.az real estate listings"""
    
    BASE_URL = "https://bina.az"
    LISTING_TYPES = {
        'sale': {
            'url': "https://bina.az/alqi-satqi",
            'type': 'sale'
        },
        'rent': {
            'url': "https://bina.az/kiraye",
            'type': 'monthly'  # Default to monthly, we'll detect daily from title
        }
    }
    
    def __init__(self):
        """Initialize scraper"""
        self.logger = logging.getLogger(__name__)
        self.session = None

    # [Previous methods remain the same: init_session, close_session, get_page_content, 
    # extract_price, extract_floor_info, validate_coordinates, get_phone_numbers]

    def detect_listing_type(self, title: Optional[str], base_type: str) -> str:
        """Detect specific listing type from title"""
        if not title:
            return base_type
            
        title_lower = title.lower()
        if base_type == 'monthly':
            if 'günlük' in title_lower:
                return 'daily'
            return 'monthly'
        return 'sale'

    async def parse_listing_page(self, html: str, listing_type: str) -> List[Dict]:
        """Parse the listings page to extract basic listing info"""
        listings = []
        soup = BeautifulSoup(html, 'lxml')
        
        self.logger.debug("Parsing listings page")
        
        # Find all listing cards
        for listing in soup.select('.items_list .items-i'):
            try:
                # Extract listing URL and ID
                listing_id = listing.get('data-item-id')
                link = listing.select_one('a.item_link')
                
                if not link or not listing_id:
                    continue
                    
                listing_url = urljoin(self.BASE_URL, link.get('href', ''))
                
                # Extract price 
                price = None
                price_elem = listing.select_one('.price-val')
                if price_elem:
                    price = self.extract_price(price_elem.text.strip())
                
                # Extract title for listing type detection
                title_elem = listing.select_one('.card-title')
                title = title_elem.text.strip() if title_elem else None
                
                # Basic data from listing card
                listing_data = {
                    'listing_id': listing_id,
                    'source_url': listing_url,
                    'source_website': 'bina.az',
                    'price': price,
                    'currency': 'AZN',
                    'listing_type': self.detect_listing_type(title, listing_type),
                    'created_at': datetime.datetime.now(),
                    'title': title
                }
                
                # [Rest of the parse_listing_page method remains the same]
                # Extract location, room count, area, floor info, etc.
                
                listings.append(listing_data)
                
            except Exception as e:
                self.logger.error(f"Error parsing listing card: {str(e)}")
                continue
        
        return listings

    async def run(self, pages: int = 1) -> List[Dict]:
        """Run scraper for specified number of pages for both sale and rental listings"""
        try:
            self.logger.info("Starting Bina.az scraper")
            await self.init_session()
            
            all_results = []
            
            # Iterate through each listing type
            for listing_category, config in self.LISTING_TYPES.items():
                self.logger.info(f"Scraping {listing_category} listings, {pages} pages")
                
                for page in range(1, pages + 1):
                    try:
                        self.logger.info(f"Processing {listing_category} page {page}")
                        
                        # Get page HTML
                        url = f"{config['url']}?page={page}"
                        html = await self.get_page_content(url)
                        
                        # Parse listings
                        listings = await self.parse_listing_page(html, config['type'])
                        self.logger.info(f"Found {len(listings)} {listing_category} listings on page {page}")
                        
                        # Get details for each listing
                        for listing in listings:
                            try:
                                detail_html = await self.get_page_content(listing['source_url'])
                                detail_data = await self.parse_listing_detail(detail_html, listing['listing_id'])
                                
                                # Update listing type if it was refined from title
                                if detail_data.get('title'):
                                    detail_data['listing_type'] = self.detect_listing_type(
                                        detail_data['title'], 
                                        listing['listing_type']
                                    )
                                
                                all_results.append({**listing, **detail_data})
                            except Exception as e:
                                self.logger.error(f"Error processing listing {listing['listing_id']}: {str(e)}")
                                continue
                                
                    except Exception as e:
                        self.logger.error(f"Error processing {listing_category} page {page}: {str(e)}")
                        continue
                    
                    # Add small delay between pages
                    await asyncio.sleep(random.uniform(1, 2))
                
                # Add delay between listing types
                await asyncio.sleep(random.uniform(2, 3))
            
            self.logger.info(f"Scraping completed. Total listings: {len(all_results)}")
            return all_results
            
        finally:
            self.logger.info("Closing scraper session")
            await self.close_session()
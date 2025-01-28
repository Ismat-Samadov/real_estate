import os
import asyncio
import pandas as pd
import tempfile
from telegram import Bot, InputFile
import aiofiles
from typing import Dict, List
from collections import defaultdict
import logging
from datetime import datetime

class TelegramReporter:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.bot = Bot(token=self.token)
        self.logger = logging.getLogger(__name__)
        
    async def send_report(self, scraper_stats: Dict, db_stats: Dict, listings: List[Dict]) -> None:
        """Send detailed scraping report to Telegram channel with per-website statistics"""
        try:
            # Group listings by website
            website_listings = defaultdict(list)
            for listing in listings:
                website = listing.get('source_website')
                if website:
                    website_listings[website].append(listing)

            # Calculate website-specific database stats
            website_db_stats = self._calculate_website_stats(listings, db_stats)
            
            # Create overall report
            report_parts = []
            
            # Generate report for each website
            for website in scraper_stats['success_count'].keys():
                website_report = (
                    f"📊 Real Estate Scraper Report - {website}\n"
                    f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                    f"{'='*35}\n\n"
                )

                # Website specific stats
                success = scraper_stats['success_count'][website]
                errors = scraper_stats['error_count'][website]
                total = success + errors
                success_rate = (success / total * 100) if total > 0 else 0
                
                # Get website-specific DB stats
                website_stats = website_db_stats.get(website, {
                    'new': 0,
                    'updated': 0,
                    'failed': 0,
                    'updated_fields': defaultdict(int)
                })

                # Add statistics
                website_report += (
                    f"📈 Statistics:\n"
                    f"• Listings Found: {success:,}\n"
                    f"• New Listings: {website_stats['new']:,}\n"
                    f"• Updated Listings: {website_stats['updated']:,}\n"
                    f"• Failed Operations: {website_stats['failed']:,}\n"
                    f"• Success Rate: {success_rate:.1f}%\n"
                    f"• Error Count: {errors:,}\n\n"
                )

                # Add field updates if any
                if website_stats['updated_fields']:
                    website_report += "🔄 Field Updates:\n"
                    for field, count in website_stats['updated_fields'].items():
                        website_report += f"• {field}: {count:,}\n"
                    website_report += "\n"

                # Add error details if any
                if website in scraper_stats['error_details'] and scraper_stats['error_details'][website]:
                    website_report += "❌ Error Details:\n"
                    for error_type, count in scraper_stats['error_details'][website].items():
                        website_report += f"• {error_type}: {count:,}\n"
                    website_report += "\n"

                # Add performance metrics
                duration = scraper_stats['duration']
                website_report += (
                    f"⚡ Performance Metrics:\n"
                    f"{'='*35}\n"
                    f"• Total Duration: {duration:.1f} seconds\n"
                    f"• Avg Time per Listing: {(duration/success if success > 0 else 0):.2f} seconds\n"
                )
                if success > 0:
                    website_report += f"• Processing Rate: {success/duration:.1f} listings/second\n"

                report_parts.append(website_report)

            # Send reports
            for report in report_parts:
                if len(report) > 4000:
                    chunks = [report[i:i+4000] for i in range(0, len(report), 4000)]
                    for i, chunk in enumerate(chunks, 1):
                        await self.bot.send_message(
                            chat_id=self.chat_id,
                            text=f"Part {i}/{len(chunks)}\n\n{chunk}",
                            parse_mode='HTML'
                        )
                        await asyncio.sleep(1)
                else:
                    await self.bot.send_message(
                        chat_id=self.chat_id,
                        text=report,
                        parse_mode='HTML'
                    )
                await asyncio.sleep(1)

            # Generate and send Excel report if there are listings
            if listings:
                await self._send_excel_report(listings, scraper_stats, website_db_stats)

        except Exception as e:
            self.logger.error(f"Failed to send Telegram report: {str(e)}")
            try:
                await self.bot.send_message(
                    chat_id=self.chat_id,
                    text=f"❌ Error generating report: {str(e)}",
                    parse_mode='HTML'
                )
            except:
                pass

    def _calculate_website_stats(self, listings: List[Dict], db_stats: Dict) -> Dict:
        """Calculate database statistics per website"""
        website_stats = defaultdict(lambda: {
            'new': 0,
            'updated': 0,
            'failed': 0,
            'updated_fields': defaultdict(int)
        })

        # Track which listings belong to which website
        for listing in listings:
            website = listing.get('source_website')
            if not website:
                continue

            listing_id = listing.get('listing_id')
            if not listing_id:
                website_stats[website]['failed'] += 1
                continue

            # Check if this listing was new or updated
            if listing_id in getattr(db_stats, 'new_listings', set()):
                website_stats[website]['new'] += 1
            elif listing_id in getattr(db_stats, 'updated_listings', set()):
                website_stats[website]['updated'] += 1
                # Track updated fields for this website
                for field, updates in db_stats['updated_fields'].items():
                    if listing_id in updates:
                        website_stats[website]['updated_fields'][field] += 1

        return website_stats

    async def _send_excel_report(self, listings: List[Dict], scraper_stats: Dict, website_db_stats: Dict):
        """Generate and send Excel report"""
        try:
            # Create DataFrame with key listing information
            df = pd.DataFrame(listings)
            
            # Create temp file
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                with pd.ExcelWriter(tmp.name, engine='xlsxwriter') as writer:
                    # Write main data
                    df.to_excel(writer, sheet_name='Listings', index=False)
                    
                    # Write website statistics
                    stats_data = []
                    for website in scraper_stats['success_count'].keys():
                        stats_data.append({
                            'Website': website,
                            'Listings Found': scraper_stats['success_count'][website],
                            'New Listings': website_db_stats[website]['new'],
                            'Updated Listings': website_db_stats[website]['updated'],
                            'Failed Operations': website_db_stats[website]['failed'],
                            'Error Count': scraper_stats['error_count'][website]
                        })
                    
                    pd.DataFrame(stats_data).to_excel(writer, sheet_name='Statistics', index=False)

            # Send file
            async with aiofiles.open(tmp.name, 'rb') as f:
                await self.bot.send_document(
                    chat_id=self.chat_id,
                    document=InputFile(f),
                    filename=f'real_estate_report_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx',
                    caption="📊 Detailed Scraping Report"
                )

            # Cleanup
            os.unlink(tmp.name)

        except Exception as e:
            self.logger.error(f"Failed to send Excel report: {str(e)}")
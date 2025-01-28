import os
import asyncio
from telegram import Bot
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
        
    async def send_report(self, scraper_stats: Dict, db_stats: Dict) -> None:
        """Send detailed scraping report to Telegram channel"""
        try:
            # Calculate totals
            total_listings = sum(scraper_stats['success_count'].values())
            total_errors = sum(scraper_stats['error_count'].values())
            total_inserts = db_stats['successful_inserts']
            total_updates = db_stats['successful_updates']
            
            # Build report header
            report = (
                f"📊 Real Estate Scraper Report\n"
                f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                f"{'='*35}\n\n"
                f"📈 Overall Statistics:\n"
                f"• Total Listings Processed: {total_listings:,}\n"
                f"• New Listings: {total_inserts:,}\n"
                f"• Updated Listings: {total_updates:,}\n"
                f"• Failed Operations: {db_stats['failed']:,}\n"
                f"• Error Rate: {(total_errors/total_listings*100):.1f}%\n\n"
            )
            
            # Add field update statistics
            if db_stats['updated_fields']:
                report += "🔄 Field Updates:\n"
                for field, count in db_stats['updated_fields'].items():
                    report += f"• {field}: {count:,}\n"
                report += "\n"
            
            # Detailed website statistics
            report += "🌐 Website Performance:\n"
            report += "=" * 35 + "\n\n"
            
            for website in scraper_stats['success_count'].keys():
                success = scraper_stats['success_count'][website]
                errors = scraper_stats['error_count'][website]
                total = success + errors
                success_rate = (success / total * 100) if total > 0 else 0
                
                # Determine status emoji
                status = "✅" if errors == 0 else "⚠️" if errors < success else "❌"
                
                report += f"{status} {website}\n"
                report += f"{'―'*35}\n"
                report += f"• Listings Found: {success:,}\n"
                report += f"• Success Rate: {success_rate:.1f}%\n"
                report += f"• Error Count: {errors:,}\n"
                
                # Get website-specific database stats
                website_name = website.lower().replace('.az', '')
                
                # Add error details if any
                if website in scraper_stats['error_details'] and scraper_stats['error_details'][website]:
                    report += "• Error Types:\n"
                    for error_type, count in scraper_stats['error_details'][website].items():
                        report += f"  - {error_type}: {count:,}\n"
                
                report += "\n"
            
            # Performance metrics
            report += "⚡ Performance Metrics:\n"
            report += "=" * 35 + "\n"
            report += f"• Total Duration: {scraper_stats['duration']:.1f} seconds\n"
            report += f"• Avg Time per Listing: {scraper_stats['avg_time_per_listing']:.2f} seconds\n"
            if total_listings > 0:
                report += f"• Processing Rate: {total_listings/scraper_stats['duration']:.1f} listings/second\n"
            
            # Error summary if there are any errors
            if db_stats['error_details']:
                report += "\n❌ Error Summary:\n"
                report += "=" * 35 + "\n"
                for error_type, count in db_stats['error_details'].items():
                    report += f"• {error_type}: {count:,}\n"
            
            # Split report if it's too long (Telegram has a 4096 character limit)
            if len(report) > 4000:
                parts = [report[i:i+4000] for i in range(0, len(report), 4000)]
                for i, part in enumerate(parts, 1):
                    await self.bot.send_message(
                        chat_id=self.chat_id,
                        text=f"Part {i}/{len(parts)}\n\n{part}",
                        parse_mode='HTML'
                    )
                    await asyncio.sleep(1)  # Small delay between messages
            else:
                await self.bot.send_message(
                    chat_id=self.chat_id,
                    text=report,
                    parse_mode='HTML'
                )
            
        except Exception as e:
            self.logger.error(f"Failed to send Telegram report: {str(e)}")
            # Try to send error message to Telegram
            try:
                await self.bot.send_message(
                    chat_id=self.chat_id,
                    text=f"❌ Error generating report: {str(e)}",
                    parse_mode='HTML'
                )
            except:
                pass  # If even this fails, just log it
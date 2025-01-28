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
        
    async def send_report(self, stats: Dict) -> None:
        """Send scraping report to Telegram channel"""
        try:
            total_listings = sum(stats['success_count'].values())
            total_errors = sum(stats['error_count'].values())
            
            report = (
                f"📊 Scraping Report {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
                f"Total Listings Collected: {total_listings}\n"
                f"Total Errors: {total_errors}\n\n"
                "Website Status:\n"
            )
            
            # Add per-website stats
            for website in stats['success_count'].keys():
                success = stats['success_count'][website]
                errors = stats['error_count'][website]
                status = "✅" if errors == 0 else "⚠️" if errors < success else "❌"
                
                report += f"{status} {website}:\n"
                report += f"  • Listings: {success}\n"
                report += f"  • Errors: {errors}\n"
                
                # Add error details if any
                if website in stats['error_details']:
                    report += "  • Error types:\n"
                    for error_type, count in stats['error_details'][website].items():
                        report += f"    - {error_type}: {count}\n"
                report += "\n"
            
            # Add performance stats
            report += "\nPerformance:\n"
            report += f"Total Duration: {stats['duration']:.2f} seconds\n"
            report += f"Average Time per Listing: {stats['avg_time_per_listing']:.2f} seconds\n"
            
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=report,
                parse_mode='HTML'
            )
            
        except Exception as e:
            self.logger.error(f"Failed to send Telegram report: {str(e)}")
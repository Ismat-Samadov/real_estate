import os
import logging
from telegram import Bot
from telegram.error import ChatMigrated
from typing import Dict, List
from datetime import datetime

class TelegramReporter:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.bot = Bot(token=self.token)
        self.logger = logging.getLogger(__name__)

    def format_duration(self, seconds: float) -> str:
        """Format duration in seconds to a human-readable string"""
        if seconds < 60:
            return f"{seconds:.1f}s"
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.1f}s"
    
    async def send_report(self, scraper_stats: Dict, db_stats: Dict, listings: List[Dict] = None) -> None:
        try:
            # Create report header with timestamp
            report = f"🤖 Scraper Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            report += "═" * 40 + "\n\n"

            # Performance Overview Section
            report += "⚡ <b>Performance Overview</b>\n"
            total_duration = scraper_stats.get('duration', 0)
            report += f"├─ Total Runtime: {self.format_duration(total_duration)}\n"
            report += f"└─ Overall Avg Time/Listing: {self.format_duration(scraper_stats.get('avg_time_per_listing', 0))}\n\n"

            # Per-Website Statistics
            total_scraped = 0
            total_duplicates = 0
            total_new = 0
            total_failed = 0

            for website in sorted(scraper_stats['success_count'].keys()):
                scraped = scraper_stats['success_count'][website]
                total_scraped += scraped
                
                website_stats = db_stats['website_stats'].get(website, {})
                new = website_stats.get('new', 0)
                duplicates = website_stats.get('duplicates', 0)
                failed = website_stats.get('failed', 0)
                processed = website_stats.get('total_processed', scraped)  # Fallback to scraped count
                
                total_new += new
                total_duplicates += duplicates
                total_failed += failed
                
                report += f"🌐 <b>{website}</b>\n"
                report += f"├─ Listings Found: {processed:,}\n"
                report += f"├─ New Records: {new:,}\n"
                if duplicates > 0:
                    report += f"├─ Duplicates: {duplicates:,}\n"
                if failed > 0:
                    report += f"├─ Failed: {failed:,}\n"
                
                # Performance metrics
                if processed > 0:
                    avg_time = total_duration / processed
                    report += f"├─ Avg Time/Listing: {self.format_duration(avg_time)}\n"
                
                # Error reporting
                if website in scraper_stats['error_details']:
                    errors = scraper_stats['error_details'][website]
                    if errors:
                        report += "├─ Errors:\n"
                        for error_type, count in errors.items():
                            report += f"│  ├─ {error_type}: {count:,}\n"
                
                # Success rate calculation
                report += "└─ Success Rate: "
                if processed > 0:
                    success_rate = ((new + duplicates) / processed) * 100
                    report += f"{success_rate:.1f}%"
                else:
                    report += "N/A"
                report += "\n\n"

            # Overall Summary
            report += "📊 <b>Final Summary</b>\n"
            total_processed = db_stats.get('total_processed', total_scraped)  # Fallback to scraped count
            report += f"├─ Total Listings Found: {total_processed:,}\n"
            report += f"├─ New Records Added: {total_new:,}\n"
            if total_duplicates > 0:
                report += f"├─ Duplicate Records: {total_duplicates:,}\n"
            if total_failed > 0:
                report += f"├─ Failed Operations: {total_failed:,}\n"
            
            # Overall success rate
            if total_processed > 0:
                overall_success_rate = ((total_new + total_duplicates) / total_processed) * 100
                report += f"└─ Overall Success Rate: {overall_success_rate:.1f}%"

            await self.bot.send_message(
                chat_id=self.chat_id,
                text=report,
                parse_mode='HTML'
            )

        except ChatMigrated as e:
            self.logger.warning(f"Chat migrated to new ID: {e.new_chat_id}")
            self.chat_id = str(e.new_chat_id)
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=report,
                parse_mode='HTML'
            )
        except Exception as e:
            self.logger.error(f"Error sending Telegram report: {str(e)}", exc_info=True)
            raise
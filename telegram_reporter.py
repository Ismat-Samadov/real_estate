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

    async def send_report(self, scraper_stats: Dict, db_stats: Dict, listings: List[Dict] = None) -> None:
        try:
            # Create report header with timestamp
            report = f"🤖 Scraper Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            report += "═" * 40 + "\n\n"

            # Add per-website statistics in a clean format
            total_scraped = 0
            total_new = 0
            total_updated = 0

            for website in sorted(scraper_stats['success_count'].keys()):
                scraped = scraper_stats['success_count'][website]
                new = db_stats['website_stats'][website]['new']
                updated = db_stats['website_stats'][website]['updated']
                
                total_scraped += scraped
                total_new += new
                total_updated += updated
                
                report += f"🌐 <b>{website}</b>\n"
                report += f"└─ Scraped: {scraped:,} | New: {new:,} | Updated: {updated:,}\n\n"

            # Add summary section
            report += "📊 <b>Summary</b>\n"
            report += f"├─ Total Scraped: {total_scraped:,}\n"
            report += f"├─ Total New: {total_new:,}\n"
            report += f"└─ Total Updated: {total_updated:,}\n"

            try:
                # Try sending with current chat_id
                await self.bot.send_message(
                    chat_id=self.chat_id,
                    text=report,
                    parse_mode='HTML'
                )
            except ChatMigrated as e:
                # If chat was migrated, try with new chat_id
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
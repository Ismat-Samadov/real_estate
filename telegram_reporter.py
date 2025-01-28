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

    async def send_report(self, scraper_stats: Dict, db_stats: Dict, listings: List[Dict] = None) -> None:
        try:
            # First, send the report message
            report = f"📊 Scraping Report {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            report += "=" * 35 + "\n\n"

            # Add per-website statistics
            for website in scraper_stats['success_count'].keys():
                scraped = scraper_stats['success_count'][website]
                new = db_stats['website_stats'][website]['new']
                updated = db_stats['website_stats'][website]['updated']
                
                report += f"🌐 {website}:\n"
                report += f"• Scraped: {scraped}\n"
                report += f"• New: {new}\n"
                report += f"• Updated: {updated}\n"
                report += "\n"

            # Send the report message
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=report,
                parse_mode='HTML'
            )

            # Then, send Excel file if listings exist
            if listings:
                try:
                    # Create Excel file
                    excel_file = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False).name
                    df = pd.DataFrame(listings)
                    
                    # Select and order important columns
                    columns = [
                        'listing_id', 'title', 'source_website', 'price', 'currency',
                        'rooms', 'area', 'floor', 'total_floors', 'district',
                        'metro_station', 'address', 'location'
                    ]
                    
                    # Ensure all columns exist
                    for col in columns:
                        if col not in df.columns:
                            df[col] = None
                    
                    df = df[columns]
                    
                    # Save to Excel with formatting
                    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
                        df.to_excel(writer, index=False, sheet_name='Listings')
                        
                        # Basic formatting
                        workbook = writer.book
                        worksheet = writer.sheets['Listings']
                        header_format = workbook.add_format({
                            'bold': True,
                            'bg_color': '#D3D3D3'
                        })
                        
                        # Format headers
                        for col_num, value in enumerate(df.columns.values):
                            worksheet.write(0, col_num, value, header_format)
                            worksheet.set_column(col_num, col_num, len(value) + 3)

                    # Send the Excel file
                    async with aiofiles.open(excel_file, 'rb') as f:
                        content = await f.read()
                        await self.bot.send_document(
                            chat_id=self.chat_id,
                            document=InputFile(content, f'listings_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'),
                            caption="📊 Scraped Listings Data"
                        )

                    # Cleanup
                    os.unlink(excel_file)

                except Exception as e:
                    self.logger.error(f"Failed to send Excel file: {str(e)}")

        except Exception as e:
            self.logger.error(f"Error in report generation: {str(e)}")
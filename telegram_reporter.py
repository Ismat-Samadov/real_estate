import os
import asyncio
from telegram import Bot
from typing import Dict, List, Optional
from collections import defaultdict
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import io

class TelegramReporter:
    def __init__(self):
        """Initialize TelegramReporter with configuration"""
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.bot = Bot(token=self.token)
        self.logger = logging.getLogger(__name__)
    
    def format_duration(self, seconds: float) -> str:
        """Format duration in seconds to a human-readable string"""
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    async def send_report(self, stats: Dict) -> None:
        """Send enhanced scraping report to Telegram channel"""
        try:
            total_listings = sum(stats['success_count'].values())
            total_errors = sum(stats['error_count'].values())
            new_listings = stats.get('new_listings', 0)
            updated_listings = stats.get('updated_listings', 0)
            
            # Calculate success rate
            total_attempts = total_listings + total_errors
            success_rate = (total_listings / total_attempts * 100) if total_attempts > 0 else 0
            
            # Create main report
            report = [
                f"📊 Real Estate Scraping Report",
                f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n",
                
                "📈 Summary:",
                f"• Total Listings Processed: {total_listings:,}",
                f"• New Listings: {new_listings:,} 🆕",
                f"• Updated Listings: {updated_listings:,} 🔄",
                f"• Success Rate: {success_rate:.1f}%\n",
                
                "⚡ Performance:",
                f"• Total Duration: {self.format_duration(stats['duration'])}",
                f"• Avg Time per Listing: {stats['avg_time_per_listing']:.2f}s",
                f"• Processing Rate: {int(total_listings/stats['duration'])}/sec\n",
                
                "🌐 Website Status:"
            ]
            
            # Add per-website stats with emojis and formatting
            for website in sorted(stats['success_count'].keys()):
                success = stats['success_count'][website]
                errors = stats['error_count'][website]
                success_rate = (success / (success + errors) * 100) if (success + errors) > 0 else 0
                
                status = "✅" if errors == 0 else "⚠️" if errors < success else "❌"
                report.append(f"\n{status} {website}")
                report.append(f"  └ Success: {success:,} | Errors: {errors:,} ({success_rate:.1f}%)")
                
                # Add error details if present
                if website in stats['error_details'] and stats['error_details'][website]:
                    report.append("  └ Error types:")
                    for error_type, count in stats['error_details'][website].items():
                        report.append(f"    • {error_type}: {count:,}")
            
            # Add price statistics if available
            if 'price_stats' in stats:
                report.extend([
                    "\n💰 Price Analysis:",
                    f"• Avg Price: {stats['price_stats']['avg']:,.0f} AZN",
                    f"• Min Price: {stats['price_stats']['min']:,.0f} AZN",
                    f"• Max Price: {stats['price_stats']['max']:,.0f} AZN"
                ])
            
            # Join report parts and send
            await self.bot.send_message(
                chat_id=self.chat_id,
                text="\n".join(report),
                parse_mode='HTML'
            )
            
            # If there are warnings or errors, send a separate message
            if total_errors > 0:
                warning_msg = (
                    "⚠️ Warning: Some scraping errors occurred.\n"
                    "Check logs for detailed error information."
                )
                await self.bot.send_message(
                    chat_id=self.chat_id,
                    text=warning_msg
                )
                
        except Exception as e:
            self.logger.error(f"Failed to send Telegram report: {str(e)}")
            raise
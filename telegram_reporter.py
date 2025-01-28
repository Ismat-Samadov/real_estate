import os
import aiohttp
import json
import logging
from datetime import datetime
from typing import Dict, Optional
from collections import defaultdict

class TelegramReporter:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
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

    def calculate_processing_rate(self, total_items: int, duration: float) -> float:
        """Calculate items processed per second, avoiding division by zero"""
        if duration <= 0:
            return 0
        return total_items / duration

    async def send_report(self, stats: Dict) -> None:
        """Send enhanced scraping report to Telegram channel"""
        try:
            total_listings = sum(stats['success_count'].values())
            total_errors = sum(stats['error_count'].values())
            new_listings = stats.get('new_listings', 0)
            updated_listings = stats.get('updated_listings', 0)
            
            # Calculate success rate and processing rate
            total_attempts = total_listings + total_errors
            success_rate = (total_listings / total_attempts * 100) if total_attempts > 0 else 0
            processing_rate = self.calculate_processing_rate(total_listings, stats['duration'])
            
            # Create main report
            report = [
                "🏘️ Real Estate Scraping Report",
                f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n",
                
                "📈 Summary:",
                f"• Total Listings Processed: {total_listings:,}",
                f"• New Listings Added: {new_listings:,} 🆕",
                f"• Listings Updated: {updated_listings:,} 🔄",
                f"• Failed Operations: {total_errors:,} ❌",
                f"• Success Rate: {success_rate:.1f}%\n",
                
                "⚡ Performance:",
                f"• Total Duration: {self.format_duration(stats['duration'])}",
                f"• Avg Time per Listing: {stats['avg_time_per_listing']:.2f}s",
                f"• Processing Rate: {processing_rate:.1f} items/sec\n",
                
                "🌐 Website Status:"
            ]
            
            # Add per-website stats with detailed analysis
            for website in sorted(stats['success_count'].keys()):
                success = stats['success_count'][website]
                errors = stats['error_count'][website]
                site_success_rate = (success / (success + errors) * 100) if (success + errors) > 0 else 0
                
                status = "✅" if errors == 0 else "⚠️" if errors < success else "❌"
                report.append(f"\n{status} {website}")
                report.append(f"  └ Success: {success:,} | Errors: {errors:,} ({site_success_rate:.1f}%)")
                
                # Add site-specific new/updated counts if available
                if 'site_stats' in stats and website in stats['site_stats']:
                    site_stats = stats['site_stats'][website]
                    report.append(f"  └ New: {site_stats.get('new', 0):,} | Updated: {site_stats.get('updated', 0):,}")
                
                # Add error details if present
                if website in stats['error_details'] and stats['error_details'][website]:
                    report.append("  └ Error types:")
                    for error_type, count in stats['error_details'][website].items():
                        report.append(f"    • {error_type}: {count:,}")
            
            # Add price statistics if available
            if 'price_stats' in stats:
                report.extend([
                    "\n💰 Price Analysis:",
                    f"• Average Price: {stats['price_stats'].get('avg', 0):,.0f} AZN",
                    f"• Minimum Price: {stats['price_stats'].get('min', 0):,.0f} AZN",
                    f"• Maximum Price: {stats['price_stats'].get('max', 0):,.0f} AZN"
                ])
            
            # Send report using aiohttp
            async with aiohttp.ClientSession() as session:
                url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                payload = {
                    "chat_id": self.chat_id,
                    "text": "\n".join(report),
                    "parse_mode": "HTML"
                }
                
                async with session.post(url, json=payload) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        self.logger.error(f"Failed to send Telegram message: {error_text}")
                        
            # Send warning message if there are errors
            if total_errors > 0:
                warning_msg = (
                    "⚠️ Warning: Scraping errors detected\n"
                    f"Total errors: {total_errors}\n"
                    "Check application logs for details."
                )
                
                async with aiohttp.ClientSession() as session:
                    url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                    payload = {
                        "chat_id": self.chat_id,
                        "text": warning_msg
                    }
                    await session.post(url, json=payload)
                    
        except Exception as e:
            self.logger.error(f"Failed to send Telegram report: {str(e)}")
            raise
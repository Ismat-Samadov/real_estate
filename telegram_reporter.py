import os
import aiohttp
import logging
from datetime import datetime
from typing import Dict, Optional

class TelegramReporter:
    def __init__(self):
        """Initialize TelegramReporter with configuration and validation"""
        self.logger = logging.getLogger(__name__)
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # Validate configuration
        if not self.token:
            self.logger.error("TELEGRAM_BOT_TOKEN not set in environment variables")
            raise ValueError("Telegram bot token is required")
            
        if not self.chat_id:
            self.logger.error("TELEGRAM_CHAT_ID not set in environment variables")
            raise ValueError("Telegram chat ID is required")
            
        # Log configuration (but mask sensitive data)
        self.logger.info(f"Initialized TelegramReporter with token ending in ...{self.token[-4:]}")
        self.logger.info(f"Target chat ID: {self.chat_id}")

    def format_duration(self, seconds: float) -> str:
        """Format duration in seconds to a human-readable string"""
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    async def verify_bot(self) -> bool:
        """Verify bot token and permissions"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://api.telegram.org/bot{self.token}/getMe"
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ok"):
                            self.logger.info(f"Bot verification successful: {data['result']['username']}")
                            return True
                    self.logger.error(f"Bot verification failed: {await response.text()}")
                    return False
        except Exception as e:
            self.logger.error(f"Error verifying bot: {str(e)}")
            return False

    async def send_message(self, text: str) -> bool:
        """Send message to Telegram with error handling"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                payload = {
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                }
                
                async with session.post(url, json=payload) as response:
                    response_text = await response.text()
                    if response.status != 200:
                        self.logger.error(f"Failed to send message: {response_text}")
                        return False
                        
                    data = await response.json()
                    if not data.get("ok"):
                        self.logger.error(f"Telegram API error: {data}")
                        return False
                        
                    return True
                    
        except Exception as e:
            self.logger.error(f"Error sending message: {str(e)}")
            return False

    async def send_report(self, stats: Dict) -> None:
        """Send enhanced scraping report to Telegram channel"""
        try:
            # First verify bot
            if not await self.verify_bot():
                self.logger.error("Bot verification failed, not sending report")
                return
                
            total_listings = sum(stats['success_count'].values())
            total_errors = sum(stats['error_count'].values())
            new_listings = stats.get('new_listings', 0)
            updated_listings = stats.get('updated_listings', 0)
            
            # Calculate success rate
            total_attempts = total_listings + total_errors
            success_rate = (total_listings / total_attempts * 100) if total_attempts > 0 else 0
            
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
                f"• Avg Time per Listing: {stats['avg_time_per_listing']:.2f}s\n",
                
                "🌐 Website Status:"
            ]
            
            # Add per-website stats
            for website in sorted(stats['success_count'].keys()):
                success = stats['success_count'][website]
                errors = stats['error_count'][website]
                site_success_rate = (success / (success + errors) * 100) if (success + errors) > 0 else 0
                
                status = "✅" if errors == 0 else "⚠️" if errors < success else "❌"
                report.append(f"\n{status} {website}")
                report.append(f"  └ Success: {success:,} | Errors: {errors:,} ({site_success_rate:.1f}%)")
                
                # Add site-specific new/updated counts
                if 'site_stats' in stats and website in stats['site_stats']:
                    site_stats = stats['site_stats'][website]
                    report.append(f"  └ New: {site_stats.get('new', 0):,} | Updated: {site_stats.get('updated', 0):,}")
                
                # Add error details if present
                if website in stats['error_details'] and stats['error_details'][website]:
                    report.append("  └ Error types:")
                    for error_type, count in stats['error_details'][website].items():
                        report.append(f"    • {error_type}: {count:,}")
            
            # Add price statistics if available
            if 'price_stats' in stats and stats['price_stats'].get('count', 0) > 0:
                report.extend([
                    "\n💰 Price Analysis:",
                    f"• Average Price: {stats['price_stats'].get('avg', 0):,.0f} AZN",
                    f"• Minimum Price: {stats['price_stats'].get('min', 0):,.0f} AZN",
                    f"• Maximum Price: {stats['price_stats'].get('max', 0):,.0f} AZN"
                ])
            
            # Send report
            success = await self.send_message("\n".join(report))
            if success:
                self.logger.info("Report sent successfully")
            else:
                self.logger.error("Failed to send report")
            
        except Exception as e:
            self.logger.error(f"Error preparing/sending report: {str(e)}")
            raise
import os
import aiohttp
import logging
import sys
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict

class TelegramReporter:
    def __init__(self):
        """Initialize TelegramReporter with configuration and validation"""
        # Setup logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        
        # Ensure environment variables are loaded
        env_path = Path(__file__).parent.parent / '.env'
        self.logger.debug(f"Looking for .env file at: {env_path}")
        
        # Load environment variables from .env file
        if env_path.exists():
            self.logger.debug(f".env file found at {env_path}")
            load_dotenv(dotenv_path=env_path, override=True)
        else:
            self.logger.error(f".env file not found at {env_path}")
        
        # Get environment variables
        self.token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        
        # Debug logging
        self.logger.debug(f"Current working directory: {os.getcwd()}")
        self.logger.debug(f"Python path: {sys.path}")
        self.logger.debug(f"Bot token found: {'Yes' if self.token else 'No'}")
        self.logger.debug(f"Chat ID found: {'Yes' if self.chat_id else 'No'}")
        
        # Validate configuration
        if not self.token:
            error_msg = "TELEGRAM_BOT_TOKEN not set in environment variables"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        if not self.chat_id:
            error_msg = "TELEGRAM_CHAT_ID not set in environment variables"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Log configuration (but mask sensitive data)
        self.logger.info(f"Initialized with token ending in ...{self.token[-4:]}")
        self.logger.info(f"Target chat ID: {self.chat_id}")

    async def verify_bot(self) -> bool:
        """Verify bot token and permissions"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://api.telegram.org/bot{self.token}/getMe"
                ) as response:
                    response_text = await response.text()
                    self.logger.debug(f"Bot verification response: {response_text}")
                    
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ok"):
                            self.logger.info(f"Bot verification successful: {data['result']['username']}")
                            return True
                    self.logger.error(f"Bot verification failed: {response_text}")
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
                
                self.logger.debug(f"Sending message to Telegram. URL: {url}")
                self.logger.debug(f"Payload: {payload}")
                
                async with session.post(url, json=payload) as response:
                    response_text = await response.text()
                    self.logger.debug(f"Telegram API response: {response_text}")
                    
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
                f"• Success Rate: {success_rate:.1f}%\n"
            ]
            
            # Send report
            success = await self.send_message("\n".join(report))
            if success:
                self.logger.info("Report sent successfully")
            else:
                self.logger.error("Failed to send report")
            
        except Exception as e:
            self.logger.error(f"Error preparing/sending report: {str(e)}")
            raise
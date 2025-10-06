import os
import logging
import threading
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from blockchain_scanner import BlockchainScanner

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class TelegramBlockchainBot:
    def __init__(self, token: str):
        self.token = token
        self.scanner = BlockchainScanner()
        self.application = Application.builder().token(token).build()
        self.scanning_thread = None
        
        # Setup handlers
        self.setup_handlers()

    def setup_handlers(self):
        """Setup Telegram bot handlers"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
        self.application.add_handler(CommandHandler("start_scan", self.start_scan_command))
        self.application.add_handler(CommandHandler("stop_scan", self.stop_scan_command))
        self.application.add_handler(CommandHandler("get_files", self.get_files_command))
        self.application.add_handler(CommandHandler("list_files", self.list_files_command))
        self.application.add_handler(CommandHandler("get_file", self.get_file_command))
        self.application.add_handler(CommandHandler("cleanup", self.cleanup_command))

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send welcome message"""
        welcome_text = """
🤖 **Blockchain Address Scanner Bot** (Render Edition)

**Commands:**
/start_scan - Start scanning blocks (20 blocks/run)
/stop_scan - Stop scanning
/stats - Show current statistics
/list_files - Show all file batches
/get_files - Download current batch
/get_file <number> - Download specific batch
/cleanup - Remove old files (keep last 5)

**Features:**
- Scans Bitcoin blocks for addresses
- Checks balances automatically
- Files are numbered (addresses_1.txt, etc.)
- Resume from last block
- Free hosting on Render
        """
        await update.message.reply_text(welcome_text, parse_mode='Markdown')

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show current statistics"""
        stats = self.scanner.get_stats()
        
        stats_text = f"""
📊 **Current Statistics**

• **Current Block:** {stats['current_block']}
• **Total Addresses:** {stats['total_addresses']}
• **With Balance:** {stats['addresses_with_balance']}
• **Current Batch:** {stats['current_file_index']}
• **Total Batches:** {stats['total_file_pairs']}
• **Status:** {'🟢 Running' if stats['is_running'] else '🔴 Stopped'}
        """
        await update.message.reply_text(stats_text, parse_mode='Markdown')

    async def list_files_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all available file batches"""
        files = self.scanner.get_available_files()
        
        if not files:
            await update.message.reply_text("📭 No file batches available yet!")
            return
        
        files_text = "📁 **Available File Batches:**\n\n"
        for file_info in files:
            files_text += f"**Batch {file_info['index']}:**\n"
            files_text += f"• Addresses: {file_info['address_count']}\n"
            files_text += f"• With Balance: {file_info['balance_count']}\n"
            files_text += f"• Download: /get_file {file_info['index']}\n\n"
        
        await update.message.reply_text(files_text, parse_mode='Markdown')

    async def get_files_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Download current file batch"""
        try:
            current_index = self.scanner.current_file_index
            addr_file = f"data/addresses_{current_index}.txt"
            balance_file = f"data/addresses_with_balance_{current_index}.txt"
            
            if not os.path.exists(addr_file) or os.path.getsize(addr_file) == 0:
                await update.message.reply_text("❌ No addresses collected in current batch yet!")
                return
            
            # Send address file
            with open(addr_file, 'rb') as f:
                await update.message.reply_document(
                    document=f,
                    filename=f'addresses_{current_index}.txt',
                    caption=f'📄 Addresses Batch {current_index}'
                )
            
            # Send balance file if it has content
            if os.path.exists(balance_file) and os.path.getsize(balance_file) > 0:
                with open(balance_file, 'rb') as f:
                    await update.message.reply_document(
                        document=f,
                        filename=f'addresses_with_balance_{current_index}.txt',
                        caption=f'💰 Addresses with Balance Batch {current_index}'
                    )
            
            # Create new files for next batch
            new_index = self.scanner.create_new_files()
            await update.message.reply_text(
                f"✅ Batch {current_index} downloaded!\n"
                f"🆕 New batch {new_index} created for future addresses."
            )
                
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def get_file_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Download specific file batch by number"""
        try:
            if not context.args:
                await update.message.reply_text("❌ Usage: /get_file <number>")
                return
            
            file_index = int(context.args[0])
            addr_file = f"data/addresses_{file_index}.txt"
            balance_file = f"data/addresses_with_balance_{file_index}.txt"
            
            if not os.path.exists(addr_file):
                await update.message.reply_text(f"❌ Batch {file_index} not found!")
                return
            
            # Send address file
            with open(addr_file, 'rb') as f:
                await update.message.reply_document(
                    document=f,
                    filename=f'addresses_{file_index}.txt',
                    caption=f'📄 Addresses Batch {file_index}'
                )
            
            # Send balance file if it exists
            if os.path.exists(balance_file) and os.path.getsize(balance_file) > 0:
                with open(balance_file, 'rb') as f:
                    await update.message.reply_document(
                        document=f,
                        filename=f'addresses_with_balance_{file_index}.txt',
                        caption=f'💰 Addresses with Balance Batch {file_index}'
                    )
                
        except ValueError:
            await update.message.reply_text("❌ Please provide a valid number!")
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def cleanup_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Clean up old files"""
        try:
            keep_count = int(context.args[0]) if context.args else 5
            self.scanner.cleanup_old_files(keep_count)
            
            files = self.scanner.get_available_files()
            await update.message.reply_text(
                f"🧹 Cleanup completed!\n"
                f"Keeping last {keep_count} batches.\n"
                f"Remaining: {len(files)} batches"
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    def scan_blocks_thread(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Run scanning in separate thread"""
        try:
            start_block = self.scanner.load_progress()
            
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"🚀 Starting scan from block {start_block}..."
            )
            
            result = self.scanner.scan_blocks(start_block)
            
            completion_text = f"""
✅ **Scanning Completed**

• **Last Block:** {result['current_block']}
• **Total Addresses:** {result['total_addresses']}
• **With Balance:** {result['addresses_with_balance']}
• **Current Batch:** {result['current_file_index']}

Use /start_scan to continue
Use /get_files to download
            """
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=completion_text,
                parse_mode='Markdown'
            )
        except Exception as e:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ Scanning error: {str(e)}"
            )

    async def start_scan_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start scanning blocks"""
        if self.scanner.is_running:
            await update.message.reply_text("⚠️ Already scanning!")
            return
        
        self.scanning_thread = threading.Thread(
            target=self.scan_blocks_thread,
            args=(update, context)
        )
        self.scanning_thread.daemon = True
        self.scanning_thread.start()
        
        await update.message.reply_text("🔍 Starting scan... (20 blocks)")

    async def stop_scan_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Stop scanning"""
        if not self.scanner.is_running:
            await update.message.reply_text("ℹ️ No active scanning")
            return
        
        self.scanner.stop_scanning()
        await update.message.reply_text("🛑 Stopping...")

    def run(self):
        """Start the bot"""
        print("🤖 Blockchain Bot Starting on Render...")
        print("📁 File management system active")
        print("🚀 Bot is ready!")
        self.application.run_polling()

# Start the bot
if __name__ == '__main__':
    BOT_TOKEN = os.environ.get('BOT_TOKEN')
    
    if not BOT_TOKEN:
        print("❌ ERROR: BOT_TOKEN environment variable not set!")
        print("💡 Set it in Render dashboard under Environment Variables")
        exit(1)
    
    bot = TelegramBlockchainBot(BOT_TOKEN)
    bot.run()

import requests
import time
import json
import os
import glob
from typing import List, Dict
import logging

class BlockchainScanner:
    def __init__(self):
        # Create data directory if it doesn't exist
        if not os.path.exists('data'):
            os.makedirs('data')
            
        self.progress_file = "data/progress.json"
        self.data_dir = "data"
        self.current_addresses_file = None
        self.current_balances_file = None
        self.current_block = self.load_progress()
        self.total_addresses = 0
        self.addresses_with_balance = 0
        self.is_running = False
        self.current_file_index = self.get_current_file_index()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize current files
        self.initialize_current_files()

    def get_current_file_index(self) -> int:
        """Get the highest existing file index"""
        try:
            address_files = glob.glob(os.path.join(self.data_dir, "addresses_*.txt"))
            if not address_files:
                return 1
            
            indices = []
            for file_path in address_files:
                try:
                    filename = os.path.basename(file_path)
                    index = int(filename.split('_')[1].split('.')[0])
                    indices.append(index)
                except (ValueError, IndexError):
                    continue
            
            return max(indices) if indices else 1
        except Exception as e:
            self.logger.error(f"Error getting file index: {e}")
            return 1

    def initialize_current_files(self):
        """Initialize current working files"""
        self.current_file_index = self.get_current_file_index()
        self.current_addresses_file = f"data/addresses_{self.current_file_index}.txt"
        self.current_balances_file = f"data/addresses_with_balance_{self.current_file_index}.txt"
        
        # Create files if they don't exist
        for file_path in [self.current_addresses_file, self.current_balances_file]:
            try:
                with open(file_path, 'a'):
                    pass
            except FileNotFoundError:
                with open(file_path, 'w'):
                    pass

    def create_new_files(self):
        """Create new file pair for next batch"""
        self.current_file_index += 1
        self.current_addresses_file = f"data/addresses_{self.current_file_index}.txt"
        self.current_balances_file = f"data/addresses_with_balance_{self.current_file_index}.txt"
        
        # Create new empty files
        for file_path in [self.current_addresses_file, self.current_balances_file]:
            with open(file_path, 'w'):
                pass
        
        self.logger.info(f"Created new file pair: {self.current_file_index}")
        return self.current_file_index

    def load_progress(self) -> int:
        """Load progress from file"""
        try:
            with open(self.progress_file, 'r') as f:
                data = json.load(f)
                return data.get('current_block', 0)
        except (FileNotFoundError, json.JSONDecodeError):
            return 0

    def save_progress(self, block_number: int):
        """Save current progress"""
        progress_data = {
            'current_block': block_number,
            'timestamp': time.time(),
            'total_addresses': self.total_addresses,
            'addresses_with_balance': self.addresses_with_balance,
            'current_file_index': self.current_file_index
        }
        with open(self.progress_file, 'w') as f:
            json.dump(progress_data, f)

    def get_block_data(self, block_height: int) -> Dict:
        """Get block data from blockchain APIs"""
        api_url = f"https://blockchain.info/block-height/{block_height}?format=json"
        
        try:
            response = requests.get(api_url, timeout=30)
            if response.status_code == 200:
                return response.json()
        except requests.RequestException as e:
            self.logger.warning(f"API failed: {e}")
        
        return None

    def extract_addresses_from_block(self, block_data: Dict) -> List[str]:
        """Extract all addresses from block transactions"""
        addresses = set()
        
        try:
            if 'blocks' in block_data:
                blocks = block_data['blocks']
                for block in blocks:
                    if 'tx' in block:
                        # Process only first 3 transactions to avoid timeouts
                        for tx in block['tx'][:3]:
                            for output in tx.get('out', []):
                                if 'addr' in output:
                                    addresses.add(output['addr'])
        except Exception as e:
            self.logger.error(f"Error extracting addresses: {e}")
            
        return list(addresses)

    def check_balance(self, address: str) -> float:
        """Check balance using APIs"""
        api_url = f"https://blockchain.info/balance?active={address}"
        
        try:
            response = requests.get(api_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if address in data:
                    balance_data = data[address]
                    return balance_data.get('final_balance', 0) / 100000000
        except requests.RequestException:
            pass
                
        return 0.0

    def scan_blocks(self, start_block: int, end_block: int = None) -> Dict:
        """Scan blocks for addresses and balances"""
        self.is_running = True
        current_block = start_block
        
        if end_block is None:
            end_block = start_block + 20  # Only 20 blocks per run on Render
        
        try:
            while current_block <= end_block and self.is_running:
                print(f"🔄 Scanning block {current_block}")
                
                # Get block data
                block_data = self.get_block_data(current_block)
                if not block_data:
                    print(f"⚠️ Could not fetch block {current_block}")
                    current_block += 1
                    continue
                
                # Extract addresses
                addresses = self.extract_addresses_from_block(block_data)
                print(f"📨 Found {len(addresses)} addresses in block {current_block}")
                
                # Process addresses
                for address in addresses:
                    # Save all addresses
                    with open(self.current_addresses_file, 'a') as f:
                        f.write(f"{address}\n")
                    self.total_addresses += 1
                    
                    # Check balance and save if has balance
                    balance = self.check_balance(address)
                    if balance > 0:
                        with open(self.current_balances_file, 'a') as f:
                            f.write(f"{address} - {balance:.8f} BTC\n")
                        self.addresses_with_balance += 1
                        print(f"💰 Found balance: {balance} BTC")
                
                # Save progress
                self.save_progress(current_block)
                current_block += 1
                
                # Rate limiting
                time.sleep(2)
                
        except Exception as e:
            print(f"❌ Error during scanning: {e}")
        
        self.is_running = False
        return {
            'current_block': current_block,
            'total_addresses': self.total_addresses,
            'addresses_with_balance': self.addresses_with_balance,
            'current_file_index': self.current_file_index
        }

    def get_available_files(self) -> List[Dict]:
        """Get list of all available file pairs"""
        files = []
        address_files = glob.glob(os.path.join(self.data_dir, "addresses_*.txt"))
        
        for addr_file in address_files:
            try:
                filename = os.path.basename(addr_file)
                index = int(filename.split('_')[1].split('.')[0])
                balance_file = f"data/addresses_with_balance_{index}.txt"
                
                if os.path.exists(balance_file):
                    # Count lines in address file
                    with open(addr_file, 'r') as f:
                        address_count = len(f.readlines())
                    
                    # Count lines in balance file
                    with open(balance_file, 'r') as f:
                        balance_count = len(f.readlines())
                    
                    files.append({
                        'index': index,
                        'address_file': addr_file,
                        'balance_file': balance_file,
                        'address_count': address_count,
                        'balance_count': balance_count
                    })
            except (ValueError, IndexError):
                continue
        
        return sorted(files, key=lambda x: x['index'])

    def delete_files(self, file_index: int):
        """Delete specific file pair"""
        try:
            addr_file = f"data/addresses_{file_index}.txt"
            balance_file = f"data/addresses_with_balance_{file_index}.txt"
            
            if os.path.exists(addr_file):
                os.remove(addr_file)
            if os.path.exists(balance_file):
                os.remove(balance_file)
                
            print(f"🗑️ Deleted file pair: {file_index}")
            return True
        except Exception as e:
            print(f"❌ Error deleting files {file_index}: {e}")
            return False

    def cleanup_old_files(self, keep_count: int = 5):
        """Keep only recent files and delete old ones"""
        try:
            files = self.get_available_files()
            if len(files) <= keep_count:
                return
            
            # Sort by index and keep only the newest ones
            files.sort(key=lambda x: x['index'])
            files_to_delete = files[:-keep_count]
            
            for file_info in files_to_delete:
                self.delete_files(file_info['index'])
                
            print(f"🧹 Cleaned up {len(files_to_delete)} old file pairs")
        except Exception as e:
            print(f"❌ Error cleaning up old files: {e}")

    def stop_scanning(self):
        """Stop the scanning process"""
        self.is_running = False

    def get_stats(self) -> Dict:
        """Get current statistics"""
        files = self.get_available_files()
        total_files = len(files)
        
        return {
            'current_block': self.current_block,
            'total_addresses': self.total_addresses,
            'addresses_with_balance': self.addresses_with_balance,
            'is_running': self.is_running,
            'current_file_index': self.current_file_index,
            'total_file_pairs': total_files
        }

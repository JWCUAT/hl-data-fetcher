import pandas as pd
import time
from hyperliquid.utils import constants
from hyperliquid.info import Info

# --- 2026 CONFIG ---
SYMBOL = "BTC"
INTERVAL = "5m"
DAYS = 180 
FILE_NAME = f"HL_{SYMBOL}_{INTERVAL}_6mo.csv"

# Initialize Info (Mainnet)
info = Info(constants.MAINNET_API_URL, skip_ws=True)

def fetch_data():
    all_data = []
    # Current time in ms
    end_ts = int(time.time() * 1000)
    # Start time (180 days ago)
    start_limit = end_ts - (DAYS * 24 * 60 * 60 * 1000)
    
    print(f"🚀 Initializing 6-month sync for {SYMBOL}...")

    while end_ts > start_limit:
        try:
            # UPTODATE: Using 'candles_snapshot' (plural)
            data = info.candles_snapshot(SYMBOL, INTERVAL, start_limit, end_ts)
            
            if not data or len(data) == 0:
                break
                
            all_data.extend(data)
            
            # Move the window back based on the oldest candle received
            oldest_ts = min(candle['t'] for candle in data)
            
            # Safety break if no progress is made
            if oldest_ts >= end_ts:
                break
                
            end_ts = oldest_ts - 1
            print(f"✅ Progress: {len(all_data)} candles | Current Date: {pd.to_datetime(oldest_ts, unit='ms')}")
            
            # 2026 Rate Limit Protection (Slightly longer delay for safety)
            time.sleep(0.6)
            
        except Exception as e:
            print(f"⚠️ Rate limit or Connection issue: {e}")
            time.sleep(5) # Cooldown
            continue

    # Process and Save
    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=['t']).sort_values('t')
        
        # 2026 Key Mapping
        df = df.rename(columns={'t': 'timestamp', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        
        df.to_csv(FILE_NAME, index=False)
        print(f"🏁 DONE! Total Rows: {len(df)}. Saved to {FILE_NAME}")

if __name__ == "__main__":
    fetch_data()

import pandas as pd
import time
import os
from hyperliquid.utils import constants
from hyperliquid.info import Info

# --- CONFIG ---
SYMBOL = "BTC"
INTERVAL = "5m"
DAYS_TO_FETCH = 180 
FILE_NAME = f"{SYMBOL}_{INTERVAL}_6_months.csv"

# Initialize Info API
info = Info(constants.MAINNET_API_URL, skip_ws=True)

def fetch_6_months():
    end_time = int(time.time() * 1000)
    # 6 months in milliseconds
    start_limit = end_time - (DAYS_TO_FETCH * 24 * 60 * 60 * 1000)
    
    all_candles = []
    current_end = end_time
    
    print(f"Starting download for {SYMBOL}...")

    while current_end > start_limit:
        # Fetch up to 5000 candles ending at current_end
        # The API returns [startTime, endTime]
        candles = info.candle_snapshot(SYMBOL, INTERVAL, start_limit, current_end)
        
        if not candles:
            break
            
        all_candles.extend(candles)
        
        # Update current_end to the oldest candle's time - 1ms to get the next chunk
        oldest_candle_time = min(c['t'] for c in candles)
        current_end = oldest_candle_time - 1
        
        print(f"Fetched {len(all_candles)} candles. Back to: {pd.to_datetime(oldest_candle_time, unit='ms')}")
        
        # Pause to respect rate limits (1200 weight per minute)
        time.sleep(0.5)

    # Convert to DataFrame and clean
    df = pd.DataFrame(all_candles)
    df = df.drop_duplicates(subset=['t']).sort_values('t')
    
    # Map HL keys to standard OHLCV
    df = df.rename(columns={'t': 'timestamp', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
    
    df.to_csv(FILE_NAME, index=False)
    print(f"Success! {len(df)} candles saved to {FILE_NAME}")

if __name__ == "__main__":
    fetch_6_months()

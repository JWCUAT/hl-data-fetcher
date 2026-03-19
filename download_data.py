import pandas as pd
import time
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
    # Start of the 6-month window
    start_limit = end_time - (DAYS_TO_FETCH * 24 * 60 * 60 * 1000)
    
    all_candles = []
    current_end = end_time
    
    print(f"🚀 Starting download for {SYMBOL}...")

    while current_end > start_limit:
        try:
            # FIX: Updated to 'candles_snapshot' (plural)
            # The API returns max 5000 candles per request
            candles = info.candles_snapshot(SYMBOL, INTERVAL, start_limit, current_end)
            
            if not candles or len(candles) == 0:
                print("🏁 Reached the end of available history.")
                break
                
            all_candles.extend(candles)
            
            # Find the oldest candle in this batch
            oldest_ts = min(c['t'] for c in candles)
            
            # Safety check: if we didn't move backward, stop to avoid infinite loop
            if oldest_ts >= current_end:
                break
                
            # Move the window back
            current_end = oldest_ts - 1
            
            print(f"✅ Fetched {len(all_candles)} candles. Back to: {pd.to_datetime(oldest_ts, unit='ms')}")
            
            # Rate limit protection (Hyperliquid 2026 weight: 1200/min)
            time.sleep(0.6) 
            
        except Exception as e:
            print(f"⚠️ Error: {e}")
            time.sleep(5) # Cooldown on error
            continue

    if all_candles:
        df = pd.DataFrame(all_candles)
        # Remove any overlaps and sort chronologically
        df = df.drop_duplicates(subset=['t']).sort_values('t')
        
        # Mapping HL keys (t, o, h, l, c, v) to standard names
        df = df.rename(columns={'t': 'timestamp', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        
        df.to_csv(FILE_NAME, index=False)
        print(f"💾 Success! {len(df)} candles saved to {FILE_NAME}")

if __name__ == "__main__":
    fetch_6_months()

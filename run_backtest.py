import pandas as pd
import pandas_ta as ta
import numpy as np

# --- SETTINGS ---
CSV_FILE = "BTC_5m_6_months.csv"
INITIAL_BALANCE = 10000
RISK_PER_TRADE = 0.01  # 1%
TRAILING_PCT = 0.005   # 0.5%
ADX_CUTOFF = 25

def run_performance_backtest():
    df = pd.read_csv(CSV_FILE)
    
    # Calculate Indicators
    df.ta.macd(append=True)
    df.ta.adx(append=True)
    df['swing_high'] = df['high'].rolling(100).max()
    df['swing_low'] = df['low'].rolling(100).min()
    df['f618'] = df['swing_high'] - (0.618 * (df['swing_high'] - df['swing_low']))
    df['f786'] = df['swing_high'] - (0.786 * (df['swing_high'] - df['swing_low']))

    balance = INITIAL_BALANCE
    equity_curve = [balance]
    trades = []
    in_pos = False

    for i in range(100, len(df)):
        row = df.iloc[i]
        curr_price = row['close']

        if not in_pos:
            # ENTRY: Fib Zone + Bullish MACD + Strong ADX
            if row['f786'] < curr_price < row['f618'] and \
               row['MACD_12_26_9'] > row['MACDs_12_26_9'] and \
               row['ADX_14'] > ADX_CUTOFF:
                
                entry_price = curr_price
                stop_loss = row['f786']
                risk_amt = balance * RISK_PER_TRADE
                pos_size = risk_amt / abs(entry_price - stop_loss)
                
                in_pos = True
                high_water_mark = entry_price
        
        elif in_pos:
            high_water_mark = max(high_water_mark, curr_price)
            t_stop = high_water_mark * (1 - TRAILING_PCT)
            
            # EXIT: Trailing Stop or Hard Stop
            if curr_price <= t_stop or curr_price <= stop_loss:
                pnl = (curr_price - entry_price) * pos_size
                balance += pnl
                trades.append(pnl)
                in_pos = False
        
        equity_curve.append(balance)

    # --- CALCULATE METRICS ---
    trades = np.array(trades)
    wins = trades[trades > 0]
    losses = trades[trades < 0]
    
    profit_factor = abs(wins.sum() / losses.sum()) if len(losses) > 0 else float('inf')
    win_rate = (len(wins) / len(trades)) * 100 if len(trades) > 0 else 0
    
    # Sharpe Ratio (Simplified for the 17-day period)
    returns = pd.Series(equity_curve).pct_change().dropna()
    sharpe = (returns.mean() / returns.std()) * np.sqrt(252 * 288) if len(returns) > 0 else 0 # 288 5m candles/day

    print(f"--- BACKTEST RESULTS ({len(df)} candles) ---")
    print(f"Final Balance: ${balance:.2f}")
    print(f"Win Rate: {win_rate:.2f}%")
    print(f"Profit Factor: {profit_factor:.2f}")
    print(f"Sharpe Ratio: {sharpe:.2f}")
    print(f"Total Trades: {len(trades)}")

if __name__ == "__main__":
    run_performance_backtest()

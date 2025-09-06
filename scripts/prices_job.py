# scripts/prices_job.py
# -*- coding: utf-8 -*-
import os, sys
from datetime import datetime, timezone
from typing import List

import yfinance as yf
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
TICKER_FILE = os.environ.get("TICKER_FILE", "public/tickers.txt")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    print("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY envs", file=sys.stderr)
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def read_tickers(path: str) -> List[str]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip().upper()
            if not s or s.startswith("#"):
                continue
            out.append(s)
    return out

def fetch_price_yahoo(symbol: str):
    try:
        df = yf.download(symbol, period="1d", interval="1m", progress=False)
        if df is not None and len(df.index) > 0:
            last = df.tail(1)
            price = float(last["Close"].iloc[0])
            vol = float(last.get("Volume", [0]).iloc[0]) if "Volume" in last else 0.0
            return price, vol
        info = yf.Ticker(symbol).fast_info
        price = float(info.get("last_price"))
        vol = float(info.get("last_volume") or 0)
        return price, vol
    except Exception as e:
        print(f"[WARN] fetch failed {symbol}: {e}")
        return None, None

def main():
    tickers = read_tickers(TICKER_FILE)
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    rows = []
    for t in tickers:
        yahoo = t + ".IS" if "." not in t else t  # ASELS -> ASELS.IS
        price, vol = fetch_price_yahoo(yahoo)
        if price is None:
            continue
        rows.append({
            "ticker": t,
            "ts": now.isoformat(),
            "close": price,
            "volume": vol,
        })

    if not rows:
        print("No rows to upsert.")
        return

    supabase.table("prices").upsert(rows, on_conflict="ticker,ts").execute()
    print(f"Upserted {len(rows)} rows -> prices")

if __name__ == "__main__":
    main()

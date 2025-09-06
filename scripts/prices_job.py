# scripts/prices_job.py
# -*- coding: utf-8 -*-
import os, sys, requests
from datetime import datetime, timezone
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    print("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def tickers_from_db():
    data = sb.table("companies").select("ticker").execute().data or []
    return [r["ticker"].upper() for r in data if r.get("ticker")]

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_batch(symbols):
    # Yahoo quote endpoint (daha stabil)
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    headers = {"User-Agent": "Mozilla/5.0"}
    params = {"symbols": ",".join(symbols)}
    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    j = r.json()
    out = []
    for q in j.get("quoteResponse", {}).get("result", []):
        sym = q.get("symbol", "")       # örn: ASELS.IS
        base = sym.split(".")[0]        # ASELS
        price = q.get("regularMarketPrice")
        vol = q.get("regularMarketVolume") or 0
        if price is not None:
            out.append({"ticker": base, "close": float(price), "volume": float(vol)})
    return out

def main():
    tickers = tickers_from_db()
    if not tickers:
        print("No tickers in companies table.")
        return

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
    rows = []
    symbols = [t + ".IS" for t in tickers]
    for batch in chunks(symbols, 50):
        try:
            res = fetch_batch(batch)
            for r in res:
                rows.append({"ticker": r["ticker"], "ts": now, "close": r["close"], "volume": r["volume"]})
        except Exception as e:
            print(f"[WARN] batch failed: {e}")

    if not rows:
        print("No rows to upsert.")
        return

    sb.table("prices").upsert(rows, on_conflict="ticker,ts").execute()
    print(f"Upserted {len(rows)} rows -> prices")

if __name__ == "__main__":
    main()

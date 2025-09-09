import os, json, re, time, sys
from pathlib import Path
from typing import Dict, Any, List, Optional
import gspread

# == Sabit başlıklar ==
INFO_HEADERS = [
    "ticker","full_name","description","website","sector","sector_main","sector_sub",
    "address","market","indices","shares_outstanding","free_float","market_cap",
    "kap_denetim_kurulusu","kap_sermaye_5ustu_csv","kap_yk_sayisi","kap_oy_haklari_csv","updated_at"
]
FIN_HEADERS = ["period_end","code","name_tr","name_en","value","currency","group"]
RATIOS_HEADERS = ["TTM Revenue","TTM Net Income","Equity last","P/E (TTM)","Net Margin (TTM)","ROE (TTM)"]
RATIOS_ROW = [
    '=SUM(QUERY(FIN!A:E,"select E where B=\'3C\' order by A desc limit 4",0))',
    '=SUM(QUERY(FIN!A:E,"select E where B=\'3L\' order by A desc limit 4",0))',
    '=INDEX(QUERY(FIN!A:E,"select E where B=\'2N\' order by A desc limit 1",0),1,1)',
    '=IFERROR(PRICES!C2 / RATIOS!B2,)',
    '=IFERROR(RATIOS!B2 / RATIOS!A2,)',
    '=IFERROR(RATIOS!B2 / RATIOS!C2,)',
]

def get_client():
    creds = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds:
        print("ERROR: GOOGLE_CREDENTIALS is missing.", file=sys.stderr); sys.exit(1)
    return gspread.service_account_from_dict(json.loads(creds))

def list_tickers(root: Path) -> List[str]:
    txt = root / "tickers.txt"
    if txt.exists():
        out = []
        for line in txt.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#"): continue
            out.append(re.sub(r"\s+","",s).upper())
        return sorted(set(out))
    kap = {p.stem.upper() for p in (root/"kap_json").glob("*.json")}
    fin = {p.stem.upper() for p in (root/"bilanco_json").glob("*.json")}
    return sorted(kap & fin)

def period_key_to_date(pk: str) -> str:
    m = re.match(r"^(\d{4})[/-]Q?([1-4])$", pk.strip(), re.I)
    if m:
        y, q = int(m.group(1)), int(m.group(2))
        mmdd = {1:"03-31",2:"06-30",3:"09-30",4:"12-31"}[q]
        return f"{y}-{mmdd}"
    return pk  # zaten YYYY-MM-DD ise/diff formatta ise olduğu gibi bırak

def ensure_spreadsheet(gc, title: str, share_with: Optional[str]):
    try:
        sp = gc.open(title); created = False
    except gspread.SpreadsheetNotFound:
        sp = gc.create(title); created = True
        sp.batch_update({"requests":[{"updateSpreadsheetProperties":{
            "properties":{"timeZone":"Europe/Istanbul","locale":"tr_TR"},
            "fields":"timeZone,locale"}}]})
        if share_with:
            try: sp.share(share_with, perm_type="user", role="writer", notify=False)
            except Exception as e: print(f"[WARN] share failed for {title}: {e}")
    return sp, created

def get_or_create(sp, title: str, rows=1000, cols=26):
    try: return sp.worksheet(title), False
    except gspread.WorksheetNotFound: return sp.add_worksheet(title, rows, cols), True

def init_prices_ratios(sp):
    # PRICES
    ws, newp = get_or_create(sp, "PRICES", rows=50, cols=4)
    if newp or not ws.acell("A1").value:
        ws.update("A1:C1", [["", "last_price", "market_cap"]])
        ws.update_acell("A2", "=INFO!A2")
        ws.update_acell("B2", '=IFERROR(INDEX(GOOGLEFINANCE("BIST:"&INFO!A2,"price"),2,2),)')
        ws.update_acell("C2", "=IFERROR(B2 * INFO!K2,)")

    # RATIOS
    wr, newr = get_or_create(sp, "RATIOS", rows=20, cols=8)
    if newr or not wr.acell("A1").value:
        wr.update("A1:F1", [RATIOS_HEADERS])
        wr.update("A2:F2", [RATIOS_ROW])

def upsert_INFO(sp, ticker: str):
    ws, _ = get_or_create(sp, "INFO", rows=200, cols=20)
    if not ws.acell("A1").value:
        ws.update("A1:R1", [INFO_HEADERS])
    ws.update_acell("A2", ticker)   # ticker
    if not ws.acell("I2").value:
        ws.update_acell("I2", "BIST")  # market varsayılan

def upsert_FIN(sp, fin: Dict[str,Any]):
    group = fin.get("group",""); currency = fin.get("currency","")
    pkeys = fin.get("periodKeys") or fin.get("period_keys") or []
    items = fin.get("items") or {}
    rows: List[List[Any]] = []
    for code, meta in items.items():
        tr = meta.get("tr") or meta.get("name_tr") or ""
        en = meta.get("en") or meta.get("name_en") or ""
        values = meta.get("values") or {}
        for pk in pkeys:
            if pk in values and values[pk] is not None:
                rows.append([period_key_to_date(pk), code, tr, en, values[pk], currency, group])
    rows.sort(key=lambda r: r[0], reverse=True)
    ws, _ = get_or_create(sp, "FIN", rows=max(2000, len(rows)+10), cols=8)
    ws.clear()
    ws.update("A1:G1", [FIN_HEADERS])
    if rows:
        # büyük data için parça parça
        step = 4000
        for i in range(0, len(rows), step):
            chunk = rows[i:i+step]
            ws.update(f"A{2+i}:G{1+i+len(chunk)}", chunk)

def run_one(gc, root: Path, ticker: str, share_with: Optional[str]):
    kap_path = root/"kap_json"/f"{ticker}.json"
    fin_path = root/"bilanco_json"/f"{ticker}.json"
    if not fin_path.exists():
        print(f"[SKIP] {ticker}: bilanco_json yok"); return
    # KAP JSON'u şu an Sheets'e yazmıyoruz; INFO alanlarına ileride map edebiliriz.
    fin = json.loads(fin_path.read_text(encoding="utf-8"))

    sp, created = ensure_spreadsheet(gc, ticker, share_with)
    if created: init_prices_ratios(sp)
    upsert_INFO(sp, ticker)
    upsert_FIN(sp, fin)

def main():
    root = Path(".").resolve()
    gc = get_client()
    share = os.environ.get("SHARE_WITH_EMAIL")
    tickers = list_tickers(root)
    if not tickers:
        print("No tickers found (kap_json & bilanco_json)."); sys.exit(0)

    print(f"Total tickers: {len(tickers)}")
    for i, t in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] {t}")
        run_one(gc, root, t, share)
        time.sleep(1.0)  # rate limit dostu

if __name__ == "__main__":
    main()

# scripts/merge_kap_bilanco.py
# -*- coding: utf-8 -*-
"""
Tek dosya:
- KAP + bilanco JSON'larını birleştirip final/<TICKER>.json üretir
- Supabase'e upsert eder (ENV var'lar set ise)
Kullanım:
  python3 scripts/merge_kap_bilanco.py          # tickers.txt'den okur
  python3 scripts/merge_kap_bilanco.py TUPRS    # komut satırından tek/çok sembol
Gereken ENV (DB yazmak için):
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
Bağımlılıklar:
  pip install "supabase==2.*" python-dateutil
"""

import os, sys, json, re, hashlib
from typing import List, Dict, Any, Optional

try:
    from dateutil import parser as dtparser
except Exception:
    dtparser = None

# ---------- KLASÖRLER ----------
KAP_DIR     = "kap_json"
BILANCO_DIR = "bilanco_json"
OUT_DIR     = "final"

CANDIDATE_TICKER_FILES = [
    "ticker.txt",
    "tickers.txt",
    os.path.join("public", "tickers.txt"),
    os.path.join("public", "ticker.txt"),
]

# ---------- SUPABASE ----------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

def supabase_client_or_none():
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        from supabase import create_client
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"⚠ Supabase client yüklenemedi: {e}")
        return None

# ---------- YARDIMCILAR ----------
def ensure_dir(p): os.makedirs(p, exist_ok=True)

def read_tickers_from_first_existing():
    for path in CANDIDATE_TICKER_FILES:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                tickers = [ln.strip().upper() for ln in f if ln.strip() and not ln.strip().startswith("#")]
            print(f"→ Ticker kaynağı: {path} ({len(tickers)} adet)")
            return tickers
    raise FileNotFoundError("ticker listesi bulunamadı: " + " | ".join(CANDIDATE_TICKER_FILES))

def load_json_safe(path) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"⚠ JSON okunamadı: {path} -> {e}")
        return None

def atomic_write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def turkish_to_number(s):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return s
    s = str(s).strip()
    if not s:
        return None
    s = s.replace(".", "").replace("\u00A0"," ")
    s = s.replace(",", ".")
    s = re.sub(r"\s+", "", s)
    try:
        if "." in s:
            return float(s)
        return int(s)
    except Exception:
        try:
            return float(s)
        except Exception:
            return None

def parse_date_ddmmyyyy(s):
    if not s:
        return None
    s = str(s).strip()
    try:
        d, m, y = s.split("/")
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    except Exception:
        if dtparser:
            try:
                return dtparser.parse(s).date().isoformat()
            except Exception:
                return None
        return None

def period_to_date(period_key: str) -> str:
    # '2025/6' -> '2025-06-30'
    year, month = period_key.split("/")
    y = int(year); m = int(month)
    day = 31 if m in (3,) else (30 if m in (6,9) else 31)
    return f"{y:04d}-{m:02d}-{day:02d}"

# ---------- DB YAZIM ----------
def upsert(sb, table: str, rows: List[Dict[str, Any]], on_conflict: str):
    if sb is None or not rows:
        return
    sb.table(table).upsert(rows, on_conflict=on_conflict).execute()

def import_merged_to_db(sb, merged: Dict[str, Any]):
    """final/<T>.json yapısındaki objeyi Supabase'e yazar."""
    if sb is None:
        return

    ticker = merged.get("ticker")
    kap    = merged.get("kap") or {}
    bil    = merged.get("bilanco") or {}

    # 1) raw_company_json
    payload = {"ticker": ticker, "kap": kap, "bilanco": bil}
    jhash = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    fetched_at = (bil.get("meta") or {}).get("fetchedAt")
    upsert(sb, "raw_company_json", [{
        "ticker": ticker,
        "source": "kap",
        "payload": payload,
        "fetched_at": fetched_at,
        "json_hash": jhash,
    }], on_conflict="ticker")

    # 2) companies (özet)
    summary   = (kap.get("summary") or {})
    general   = (kap.get("general") or {})
    ownership = (kap.get("ownership") or {})

    indices           = summary.get("dahil_oldugu_endeksler") or []
    free_float_ratio  = turkish_to_number(ownership.get("fiili_dolasim_oran"))
    free_float_mcap   = turkish_to_number(ownership.get("fiili_dolasim_tutar_tl"))
    website           = summary.get("internet_adresi")
    market            = summary.get("islem_gordugu_pazar")
    sector_main       = summary.get("sektor_ana")
    sector_sub        = summary.get("sektor_alt")
    address           = general.get("merkez_adresi")
    listing_date      = parse_date_ddmmyyyy(general.get("kotasyon_tarihi"))

    shares_outstanding = None
    for row in (ownership.get("sermaye_5ustu") or []):
        if str(row.get("Ortağın Adı-Soyadı/Ticaret Ünvanı","")).strip().upper() == "TOPLAM":
            shares_outstanding = turkish_to_number(row.get("Sermayedeki Payı(TL)"))
            break

    upsert(sb, "companies", [{
        "ticker": ticker,
        "website": website,
        "sector_main": sector_main,
        "sector_sub": sector_sub,
        "market": market,
        "indices": indices,
        "address": address,
        "listing_date": listing_date,
        "free_float_ratio": free_float_ratio,
        "free_float_mcap": free_float_mcap,
        "shares_outstanding": shares_outstanding
    }], on_conflict="ticker")

    # 3) board_members
    bm_rows = []
    for m in kap.get("board_members", []) or []:
        bm_rows.append({
            "ticker": ticker,
            "name": m.get("Adı-Soyadı"),
            "gender": m.get("Cinsiyeti"),
            "role": m.get("Görevi"),
            "profession": m.get("Mesleği"),
            "first_elected": parse_date_ddmmyyyy(m.get("Yönetim Kuruluna İlk Seçilme Tarihi")),
            "is_executive": None if m.get("İcrada Görevli Olup Olmadığı") is None else (str(m.get("İcrada Görevli Olup Olmadığı")).strip().lower() == "evet"),
            "duties_last5y": m.get("Son 5 Yılda Ortaklıkta Üstlendiği Görevler"),
            "outside_roles": m.get("Son Durum itibariyle Ortaklık Dışında Aldığı Görevler"),
            "has_fin_exp": None if not m.get("Denetim, Muhasebe ve/veya Finans Alanında En Az 5 Yıllık Deneyime Sahip Olup Olmadığı") else (str(m.get("Denetim, Muhasebe ve/veya Finans Alanında En Az 5 Yıllık Deneyime Sahip Olup Olmadığı")).strip().lower() == "evet"),
            "equity_pct": turkish_to_number(m.get("Sermayedeki Payı (%)")),
            "represented_share_group": m.get("Temsil Ettiği Pay Grubu"),
        })
    if bm_rows:
        sb.table("kap_board_members").delete().eq("ticker", ticker).execute()
        upsert(sb, "kap_board_members", bm_rows, on_conflict="ticker,name")

    # 4) ownership (>=5%)
    own_rows = []
    for o in ownership.get("sermaye_5ustu") or []:
        own_rows.append({
            "ticker": ticker,
            "holder": o.get("Ortağın Adı-Soyadı/Ticaret Ünvanı"),
            "paid_in_tl": turkish_to_number(o.get("Sermayedeki Payı(TL)")),
            "pct": turkish_to_number(o.get("Sermayedeki Payı(%)")),
            "voting_pct": turkish_to_number(o.get("Oy Hakkı Oranı(%)")),
        })
    if own_rows:
        sb.table("kap_ownership").delete().eq("ticker", ticker).execute()
        upsert(sb, "kap_ownership", own_rows, on_conflict="ticker,holder")

    # 5) subsidiaries
    sub_rows = []
    for s in ownership.get("bagli_ortakliklar") or []:
        sub_rows.append({
            "ticker": ticker,
            "company": s.get("Ticaret Ünvanı"),
            "activity": s.get("Şirketin Faaliyet Konusu"),
            "paid_in_capital": turkish_to_number(s.get("Ödenmiş/Çıkarılmış Sermayesi")),
            "share_amount": turkish_to_number(s.get("Şirketin Sermayedeki Payı")),
            "currency": s.get("Para Birimi"),
            "share_pct": turkish_to_number(s.get("Şirketin Sermayedeki Payı(%)")),
            "relation": s.get("Şirket ile Olan İlişkinin Niteliği"),
        })
    if sub_rows:
        sb.table("kap_subsidiaries").delete().eq("ticker", ticker).execute()
        upsert(sb, "kap_subsidiaries", sub_rows, on_conflict="ticker,company")

    # 6) vote rights
    vr_pairs = (kap.get("oy_haklari") or {}).get("pairs") or []
    vr_rows = [{"ticker": ticker, "field": p.get("alan"), "value": p.get("deger")} for p in vr_pairs]
    if vr_rows:
        sb.table("kap_vote_rights").delete().eq("ticker", ticker).execute()
        upsert(sb, "kap_vote_rights", vr_rows, on_conflict="ticker,field")

    # 7) katilim 4.7
    k47 = kap.get("katilim_4_7")
    if k47:
        row = {"ticker": ticker}
        for k, v in k47.items():
            row[k] = turkish_to_number(v) if isinstance(v, str) else v
        upsert(sb, "kap_katilim_4_7", [row], on_conflict="ticker")

    # 8) financials (bilanco)
    bil_meta  = (bil.get("meta") or {})
    currency  = bil_meta.get("currency")
    period_keys = bil_meta.get("periodKeys") or []
    items = bil.get("items") or {}

    # labels
    labels = []
    for code, node in items.items():
        labels.append({
            "code": code,
            "tr": node.get("tr"),
            "en": node.get("en"),
            "statement": "bilanco",
        })
    if labels:
        upsert(sb, "financial_labels", labels, on_conflict="code")

    # rows by period
    fin_rows = []
    for pk in period_keys:
        data = {}
        for code, node in items.items():
            v = (node.get("values") or {}).get(pk)
            if v is not None:
                data[code] = v
        if not data:
            continue
        fin_rows.append({
            "ticker": ticker,
            "period": period_to_date(pk),
            "freq": "Q",
            "statement": "bilanco",
            "currency": currency,
            "data": data,
        })
    if fin_rows:
        upsert(sb, "financials", fin_rows, on_conflict="ticker,period,freq,statement")

def main():
    ensure_dir(OUT_DIR)
    # semboller
    if len(sys.argv) > 1:
        tickers = [a.strip().upper() for a in sys.argv[1:] if a.strip()]
        print(f"→ Ticker kaynağı: komut satırı ({len(tickers)} adet)")
    else:
        tickers = read_tickers_from_first_existing()

    sb = supabase_client_or_none()
    if sb is None:
        print("⚠ Supabase ENV bulunamadı (ya da client açılamadı). Sadece final/*.json üretilecek.")

    total = len(tickers)
    for i, t in enumerate(tickers, 1):
        kap_fp = os.path.join(KAP_DIR, f"{t}.json")
        bil_fp = os.path.join(BILANCO_DIR, f"{t}.json")

        kap_doc = load_json_safe(kap_fp)
        bil_doc = load_json_safe(bil_fp)

        if kap_doc is None and bil_doc is None:
            print(f"• ({i}/{total}) {t}: kaynak yok (atlandı).")
            continue

        merged = {"ticker": t, "kap": kap_doc, "bilanco": bil_doc}
        out_fp = os.path.join(OUT_DIR, f"{t}.json")
        atomic_write_json(out_fp, merged)
        print(f"✓ ({i}/{total}) {t} → {out_fp}")

        # DB'ye yaz
        import_merged_to_db(sb, merged)

    print("Bitti.")

if __name__ == "__main__":
    main()

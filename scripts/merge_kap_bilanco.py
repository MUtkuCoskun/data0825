# scripts/merge_kap_bilanco.py
# -*- coding: utf-8 -*-
import os, sys, json

KAP_DIR     = "kap_json"
BILANCO_DIR = "bilanco_json"
OUT_DIR     = "final"

CANDIDATE_TICKER_FILES = [
    "ticker.txt",
    "tickers.txt",
    os.path.join("public", "tickers.txt"),
    os.path.join("public", "ticker.txt"),
]

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def read_tickers_from_first_existing():
    for path in CANDIDATE_TICKER_FILES:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                tickers = [ln.strip().upper() for ln in f if ln.strip() and not ln.strip().startswith("#")]
            print(f"→ Ticker kaynağı: {path} ({len(tickers)} adet)")
            return tickers
    raise FileNotFoundError("ticker listesi bulunamadı: " + " | ".join(CANDIDATE_TICKER_FILES))

def load_json_safe(path):
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

def main():
    ensure_dir(OUT_DIR)

    # CLI ile sembol verilmişse dosya okumadan onları kullan
    if len(sys.argv) > 1:
        tickers = [a.strip().upper() for a in sys.argv[1:] if a.strip()]
        print(f"→ Ticker kaynağı: komut satırı ({len(tickers)} adet)")
    else:
        tickers = read_tickers_from_first_existing()

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

    print("Bitti.")

if __name__ == "__main__":
    main()

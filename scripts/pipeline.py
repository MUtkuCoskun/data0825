#!/usr/bin/env python3
import subprocess, os, json

TICKERS_FILE = "public/tickers.txt"
RESULT_DIR   = "result"

def run(cmd, cwd=None):
    print(f"\n> {cmd}")
    subprocess.run(cmd, shell=True, check=True, cwd=cwd)

def main():
    # 1) isyatirim verilerini çek (JSON olarak)
    run("npx tsx scripts/isyatirim-batch-json.ts")

    # 2) KAP verilerini çek (JSON olarak)
    run("python3 scripts/kap_batch_from_tickerfile.py")

    # 3) Birleştir ve result/ klasörüne yaz
    os.makedirs(RESULT_DIR, exist_ok=True)
    with open(TICKERS_FILE, "r", encoding="utf-8") as f:
        tickers = [ln.strip().upper() for ln in f if ln.strip() and not ln.startswith("#")]

    for t in tickers:
        bil_path = f"bilanco_json/{t}.json"
        kap_path = f"kap_json/{t}.json"
        out_path = f"{RESULT_DIR}/{t}.json"

        bil = json.load(open(bil_path, encoding="utf-8")) if os.path.exists(bil_path) else None
        kap = json.load(open(kap_path, encoding="utf-8")) if os.path.exists(kap_path) else None

        merged = {"ticker": t, "bilanco": bil, "kap": kap}
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        print(f"✓ {t} → {out_path}")

    print("\nBitti ✅ Tüm JSON dosyaları 'result/' klasöründe.")

if __name__ == "__main__":
    main()

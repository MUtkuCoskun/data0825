#!/usr/bin/env python3
from pathlib import Path
import json
import shutil
import datetime

ROOT = Path(__file__).resolve().parents[1]
FINAL = ROOT / "final"
DOCS = ROOT / "docs"
OUT_FINAL = DOCS / "final"

DOCS.mkdir(exist_ok=True)
OUT_FINAL.mkdir(parents=True, exist_ok=True)
(DOCS / ".nojekyll").touch()

items = []
for p in sorted(FINAL.glob("*.json")):
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}

    ticker = p.stem.upper()
    entry = {
        "ticker": ticker,
        "unvan": data.get("unvan") or data.get("unvanı") or data.get("title"),
        "sektor": data.get("sektor") or data.get("sector"),
        "son_bilanco_tarihi": data.get("son_bilanco_tarihi") or data.get("last_balance_date"),
        "son_guncelleme": data.get("son_guncelleme"),
    }
    items.append(entry)

    # final/*.json -> docs/final/*.json
    shutil.copy2(p, OUT_FINAL / p.name)

now = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
for it in items:
    if not it["son_guncelleme"]:
        it["son_guncelleme"] = now

index = {
    "generated_at": now,
    "count": len(items),
    "items": items,
}

with (DOCS / "index.json").open("w", encoding="utf-8") as f:
    json.dump(index, f, ensure_ascii=False, indent=2)

print(f"Wrote {DOCS/'index.json'} with {len(items)} tickers.")

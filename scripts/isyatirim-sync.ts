// scripts/isyatirim-sync.ts
// tsx ile çalışır: `npx tsx scripts/isyatirim-sync.ts`
import fs from "node:fs";
import path from "node:path";
import cp from "node:child_process";

const TICKER_FILE_CANDIDATES = [
  "public/tickers.txt",
  "public/ticker.txt",
  "tickers.txt",
  "ticker.txt",
];

function readTickers(): string[] {
  for (const p of TICKER_FILE_CANDIDATES) {
    if (fs.existsSync(p)) {
      const txt = fs.readFileSync(p, "utf8");
      const arr = txt
        .split("\n")
        .map(s => s.trim().toUpperCase())
        .filter(s => s && !s.startsWith("#"));
      console.log(`Toplam ${arr.length} sembol bulundu. Kaynak: ${p}`);
      return arr;
    }
  }
  throw new Error("Ticker listesi bulunamadı.");
}

function parsePeriodKey(pk: string): { y: number; m: number } {
  // "2025/6" gibi
  const [ys, ms] = pk.split("/");
  return { y: Number(ys), m: Number(ms) };
}

function nextQuarter(y: number, m: number): { y: number; m: number } {
  if (m === 3) return { y, m: 6 };
  if (m === 6) return { y, m: 9 };
  if (m === 9) return { y, m: 12 };
  // m === 12 → sonraki yıl mart
  return { y: y + 1, m: 3 };
}

function run(cmd: string) {
  console.log(`> ${cmd}`);
  cp.execSync(cmd, { stdio: "inherit", env: { ...process.env } });
}

function main() {
  const ticks = readTickers();
  const outDir = "bilanco_json";
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  for (const t of ticks) {
    const outPath = path.join(outDir, `${t}.json`);
    const baseCmd = (year: number, month: number) =>
      `npx tsx scripts/isyatirim-all-json.ts ${t} AUTO TRY ${year} ${month}`;

    if (!fs.existsSync(outPath)) {
      console.log(`=== ${t} (FULL) ===`);
      run(baseCmd(2008, 3)); // ilk kurulumda tüm tarihçe
      continue;
    }

    // İnkremental: JSON’u oku, son periodKey'i bul, bir sonraki çeyrekten başlat
    console.log(`=== ${t} (INCREMENTAL) ===`);
    const obj = JSON.parse(fs.readFileSync(outPath, "utf8"));
    const meta = (obj?.meta ?? obj?.bilanco?.meta) || {};
    const periodKeys: string[] = meta.periodKeys || [];

    if (!periodKeys.length) {
      console.log("• periodKeys bulunamadı; güvenli tarafta kalıp full çekiyorum.");
      run(baseCmd(2008, 3));
      continue;
    }

    // Son anahtar
    const last = periodKeys[periodKeys.length - 1];
    const { y, m } = parsePeriodKey(last);
    const { y: ny, m: nm } = nextQuarter(y, m);

    // Eğer ileri tarih olduysa bile sorun değil; script müsait dönemleri çeker
    run(baseCmd(ny, nm));
  }

  console.log("Bitti. JSON’lar bilanco_json/<TICKER>.json altında güncel.");
}

main();

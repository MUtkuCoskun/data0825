import { promises as fs } from 'node:fs'
import path from 'node:path'
import { exec } from 'node:child_process'
import { promisify } from 'node:util'
const sh = promisify(exec)

const EXCHANGE  = 'TRY'
const START_Y   = 2008
const START_P   = 3
const SLEEP_MS  = 800

// XLSX yerine JSON klasörü
const OUT_ROOT = 'bilanco_json'

// Küçük yardımcılar
function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)) }

async function ensureDir(p: string) {
  await fs.mkdir(p, { recursive: true })
}

async function fileExists(p: string) {
  try { await fs.access(p); return true } catch { return false }
}

// Tekil script ne yazarsa yazsın, çıktıyı bulup bilanco_json/<T>.json'a taşı
async function normalizeOutput(T: string, stdout: string) {
  const desired = path.join(OUT_ROOT, `${T}.json`)
  await ensureDir(OUT_ROOT)

  // 1) Zaten doğru yerde mi?
  if (await fileExists(desired)) {
    console.log(`✔ Bulundu: ${desired}`)
    return
  }

  // 2) Tekil scriptin logundan kesin yol yakalamayı dene
  const m = (stdout || '').match(/✔ JSON yazıldı:\s*(.+\.json)/)
  if (m && m[1]) {
    const src = m[1].trim()
    if (src !== desired) {
      try {
        await ensureDir(path.dirname(desired))
        await fs.rename(src, desired)
        console.log(`↪ Taşındı: ${src} → ${desired}`)
        return
      } catch (e: any) {
        console.warn(`⚠ Taşıma hatası (${src} → ${desired}): ${e?.message || e}`)
      }
    } else {
      console.log(`✔ Doğrudan yazılmış: ${desired}`)
      return
    }
  }

  // 3) Logdan yakalayamadıysak — eski olası konumları tek tek dene
  const candidates = [
    path.join(process.cwd(), 'bilanco_json', `${T}.json`),
    path.join('/Users/utku/Downloads', 'isyatirim', T, `${T}.json`),
    path.join(process.cwd(), 'public', 'isyatirim', T, `${T}.json`),
  ]
  for (const src of candidates) {
    if (await fileExists(src)) {
      try {
        await fs.rename(src, desired)
        console.log(`↪ Taşındı: ${src} → ${desired}`)
        return
      } catch (e: any) {
        console.warn(`⚠ Taşıma hatası (${src} → ${desired}): ${e?.message || e}`)
      }
    }
  }

  console.warn(`⚠ Çıktı bulunamadı. Tekil script farklı bir klasöre yazmış olabilir.`)
}

async function main() {
  // Ticker'ları public/tickers.txt'den oku (boş satırlar ve # yorum satırlarını atla)
  const raw = await fs.readFile('public/tickers.txt', 'utf8')
  const tickers = raw.split(/\r?\n/).map(s => s.trim()).filter(s => s && !s.startsWith('#'))

  console.log(`Toplam ${tickers.length} sembol bulundu.\n`)
  await ensureDir(OUT_ROOT)

  for (const t of tickers) {
    const T = t.toUpperCase()
    console.log(`\n=== ${T} ===`)

    const cmd = `npx tsx scripts/isyatirim-all-json.ts ${T} AUTO ${EXCHANGE} ${START_Y} ${START_P}`
    console.log(`> ${cmd}`)

    try {
      const { stdout, stderr } = await sh(cmd, { maxBuffer: 1024 * 1024 * 80 })
      if (stdout) process.stdout.write(stdout)
      if (stderr) process.stderr.write(stderr)

      // Çıktıyı normalize et → bilanco_json/<TICKER>.json
      await normalizeOutput(T, stdout)
      console.log(`Dosya hedefi: ${path.join(OUT_ROOT, `${T}.json`)}`)
    } catch (e: any) {
      console.error(`Hata (${T}):`, e?.message || e)
    }

    await sleep(SLEEP_MS) // nazik bekleme
  }

  console.log('\nBitti. JSON dosyaları bilanco_json/<TICKER>.json altında.')
}

main().catch(err => { console.error(err); process.exit(1) })

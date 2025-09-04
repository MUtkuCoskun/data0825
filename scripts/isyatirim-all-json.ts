// scripts/isyatirim-all-json.ts
// Kullanım:
//   npx tsx scripts/isyatirim-all-json.ts SASA AUTO TRY 2008 3

import fs from 'node:fs/promises'
import path from 'node:path'

const TICKER = (process.argv[2] || 'SASA').toUpperCase()
const GROUP  = (process.argv[3] || 'AUTO').toUpperCase()    // CONSOL | XI_29 | AUTO
const EXCH   = process.argv[4] || 'TRY'
const START_YEAR   = Number(process.argv[5] || 2008)
const START_PERIOD = Number(process.argv[6] || 3) as 3|6|9|12
const OUT_DIR = "bilanco_json"

const SLEEP_MS = 600
const RETRIES  = 3

type Period = { y: number; p: 3|6|9|12 }
type ApiRow = { itemCode?: string; itemDescTr?: string; itemDescEng?: string } & Record<string, any>
type ItemRow = { code?: string; tr?: string; en?: string; values: Record<string, number|null> }
type Output = {
  meta: { ticker: string; group: string; currency: string; fetchedAt: string; periodKeys: string[] }
  items: Record<string, ItemRow>
}

function keyOf(p: Period){ return `${p.y}/${p.p}` }

function periodsAsc(startY: number, startP: 3|6|9|12): Period[] {
  const ps: Period[] = []
  const periods = [3,6,9,12] as const
  const now = new Date()
  const endY = now.getFullYear()
  const endP = ((): 3|6|9|12 => {
    const m = now.getMonth()+1
    if (m <= 3) return 3
    if (m <= 6) return 6
    if (m <= 9) return 9
    return 12
  })()
  for (let y = startY; y <= endY; y++) {
    for (const p of periods) {
      if (y === startY && p < startP) continue
      if (y === endY && p > endP) break
      ps.push({ y, p })
    }
  }
  return ps
}

function lastNPeriods(periods: Period[], n: number): Period[] {
  return periods.slice(Math.max(0, periods.length - n))
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
}

async function ensureDir(p: string) { await fs.mkdir(p, { recursive: true }) }
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

async function fetch4Raw(ticker: string, group: string, exch: string, quad: Period[]) {
  const qp: string[] = []
  quad.forEach((q, i) => {
    const n = i+1
    qp.push(`year${n}=${q.y}`, `period${n}=${q.p}`)
  })
  const url = `https://www.isyatirim.com.tr/_layouts/15/IsYatirim.Website/Common/Data.aspx/MaliTablo?companyCode=${encodeURIComponent(ticker)}&exchange=${encodeURIComponent(exch)}&financialGroup=${encodeURIComponent(group)}&${qp.join('&')}`
  const res = await fetch(url, { headers: { 'user-agent': 'Mozilla/5.0' } })
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}`)
  const json = await res.json() as any
  const value = (json?.value ?? []) as ApiRow[]
  return value
}

async function fetch4Retry(ticker: string, group: string, exch: string, quad: Period[]) {
  let lastErr: any = null
  for (let i = 1; i <= RETRIES; i++) {
    try {
      return await fetch4Raw(ticker, group, exch, quad)
    } catch (e) {
      lastErr = e
      const backoff = SLEEP_MS * i
      process.stdout.write(` (retry ${i}/${RETRIES})`)
      await sleep(backoff)
    }
  }
  throw lastErr
}

async function fetchGroupAll(group: string, periods: Period[]): Promise<Output> {
  const chunks = chunk(periods, 4)
  const items: Record<string, ItemRow> = {}

  console.log(`→ Grup=${group} için istekler (çağrı sayısı=${chunks.length})`)

  for (let ci = 0; ci < chunks.length; ci++) {
    const quad = chunks[ci]
    process.stdout.write(`  • ${group} ${ci+1}/${chunks.length} [${quad.map(keyOf).join(', ')}]… `)
    try {
      const value = await fetch4Retry(TICKER, group, EXCH, quad)
      for (const row of value) {
        const key = (row.itemCode || row.itemDescTr || row.itemDescEng || '').toString()
        if (!items[key]) items[key] = { code: row.itemCode, tr: row.itemDescTr, en: row.itemDescEng, values: {} }
        quad.forEach((q, i) => {
          const v = row[`value${i+1}`]
          items[key].values[keyOf(q)] = (v === null || v === undefined || v === '') ? null : Number(v)
        })
      }
      console.log('ok')
    } catch (e: any) {
      console.log('hata:', e?.message || e)
    }
    await sleep(SLEEP_MS)
  }

  const periodKeys = periods.map(keyOf)
  return {
    meta: { ticker: TICKER, group, currency: EXCH, fetchedAt: new Date().toISOString(), periodKeys },
    items,
  }
}

function mergeOutputs(a: Output | null, b: Output | null, preferA = true): Output {
  const ticker = a?.meta.ticker || b?.meta.ticker || TICKER
  const currency = a?.meta.currency || b?.meta.currency || EXCH
  const pset = new Set<string>([
    ...(a?.meta.periodKeys || []),
    ...(b?.meta.periodKeys || []),
  ])
  const periodKeys = Array.from(pset).sort((x, y) => {
    const [yx, px] = x.split('/').map(Number)
    const [yy, py] = y.split('/').map(Number)
    return yx - yy || px - py
  })

  const keys = new Set<string>([
    ...Object.keys(a?.items || {}),
    ...Object.keys(b?.items || {}),
  ])

  const outItems: Record<string, ItemRow> = {}
  for (const k of keys) {
    const ia = a?.items?.[k]
    const ib = b?.items?.[k]
    const row: ItemRow = {
      code: ia?.code ?? ib?.code,
      tr: ia?.tr ?? ib?.tr,
      en: ia?.en ?? ib?.en,
      values: {}
    }
    for (const p of periodKeys) {
      const va = ia?.values?.[p]
      const vb = ib?.values?.[p]
      row.values[p] = (preferA ? (va ?? vb) : (vb ?? va)) ?? null
    }
    outItems[k] = row
  }

  return {
    meta: {
      ticker,
      group: `${a?.meta.group || ''}${a && b ? '+' : ''}${b?.meta.group || ''}` || 'AUTO',
      currency,
      fetchedAt: new Date().toISOString(),
      periodKeys
    },
    items: outItems
  }
}

async function forceOverlayPeriods(base: Output, group: string, targets: Period[]) {
  const chunks = chunk(targets, 4)
  for (let ci = 0; ci < chunks.length; ci++) {
    const quad = chunks[ci]
    process.stdout.write(`  • FORCE ${group} [${quad.map(keyOf).join(', ')}]… `)
    try {
      const value = await fetch4Retry(TICKER, group, EXCH, quad)
      for (const row of value) {
        const key = (row.itemCode || row.itemDescTr || row.itemDescEng || '').toString()
        if (!base.items[key]) base.items[key] = { code: row.itemCode, tr: row.itemDescTr, en: row.itemDescEng, values: {} }
        quad.forEach((q, i) => {
          const v = row[`value${i+1}`]
          base.items[key].values[keyOf(q)] = (v === null || v === undefined || v === '') ? null : Number(v)
        })
      }
      console.log('ok')
    } catch (e: any) {
      console.log('hata:', e?.message || e)
    }
    await sleep(SLEEP_MS)
  }
}

async function writeJson(output: Output) {
  await ensureDir(OUT_DIR)
  const jsonFile = path.join(OUT_DIR, `${TICKER}.json`)
  await fs.writeFile(jsonFile, JSON.stringify(output, null, 2), "utf8")
  console.log(`✔ JSON yazıldı: ${jsonFile}`)
}

async function run() {
  const periods = periodsAsc(START_YEAR, START_PERIOD)
  const tail = lastNPeriods(periods, 4)

  if (GROUP === 'AUTO') {
    const outConsol = await fetchGroupAll('CONSOL', periods)
    const outSolo   = await fetchGroupAll('XI_29', periods)
    const merged = mergeOutputs(outConsol, outSolo, true)
    console.log('→ Son 4 dönemi zorla yenile (CONSOL)…')
    await forceOverlayPeriods(merged, 'CONSOL', tail)
    console.log('→ Son 4 dönemi zorla yenile (XI_29)…')
    await forceOverlayPeriods(merged, 'XI_29', tail)
    await writeJson(merged)
  } else {
    const out = await fetchGroupAll(GROUP, periods)
    console.log('→ Son 4 dönemi zorla yenile…')
    await forceOverlayPeriods(out, GROUP, tail)
    await writeJson(out)
  }
}

run().catch(err => { console.error(err); process.exit(1) })

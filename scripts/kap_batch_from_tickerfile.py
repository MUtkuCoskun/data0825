# scripts/kap_batch_from_tickerfile.py
# -*- coding: utf-8 -*-
import os
import re
import time
import json
import argparse
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd  # sadece tablo parse için (çıktı JSON)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

PAGELOAD_TIMEOUT = 25
WAIT_SEC = 15
OUTPUT_DIR = "kap_json"
DEFAULT_TICKER_FILE = "public/tickers.txt"

# Global TICKER (bazı fonksiyonlar tablo içinde kodu seçerken kullanıyor)
TICKER = "ARCLK"

# ---------- yardımcılar ----------
def ensure_dir(p): os.makedirs(p, exist_ok=True)

def file_exists(p: str) -> bool:
    try:
        os.access(p, os.R_OK)
        return os.path.exists(p)
    except Exception:
        return False

def read_tickers(primary: Optional[str]) -> List[str]:
    """
    Ticker dosyasını esnek oku. Sırayla aşağıdakileri dener:
    - primary (parametre)        -> ör. public/tickers.txt
    - public/tickers.txt
    - public/ticker.txt
    - tickers.txt
    - ticker.txt
    """
    candidates = [c for c in [
        primary,
        "public/tickers.txt",
        "public/ticker.txt",
        "tickers.txt",
        "ticker.txt",
    ] if c]
    for p in candidates:
        if file_exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return [ln.strip().upper() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    return []

def textify(el) -> str:
    try:
        return re.sub(r"\s+", " ", el.text).strip()
    except Exception:
        return ""

def tr_upper(s: Optional[str]) -> Optional[str]:
    """Türkçe uyumlu büyük harf (i→İ, ı→I, vb.)."""
    if s is None: return None
    s = s.replace("i", "İ").replace("ı", "I")
    return s.upper()

def make_headers_unique(headers: List[str]) -> List[str]:
    seen, out = {}, []
    for h in headers:
        h = (h or "").strip()
        seen[h] = seen.get(h, 0) + 1
        out.append(h if seen[h] == 1 else f"{h}__{seen[h]}")
    return out

def parse_table(table_el) -> pd.DataFrame:
    thead = table_el.find_elements(By.CSS_SELECTOR, "thead th")
    headers = [textify(th) for th in thead]
    rows = table_el.find_elements(By.CSS_SELECTOR, "tbody tr")
    data = [[textify(td) for td in r.find_elements(By.TAG_NAME, "td")] for r in rows]
    if not headers and data:
        headers = [f"col_{i+1}" for i in range(len(data[0]))]
    headers = make_headers_unique(headers or [])
    fixed = []
    for row in data:
        row = row + [""] * (len(headers) - len(row))
        fixed.append(row[:len(headers)])
    return pd.DataFrame(fixed, columns=headers)

def safe_click(driver, el):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    driver.execute_script("arguments[0].click();", el)

def make_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-translate")
    options.add_argument("--mute-audio")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.page_load_strategy = "eager"

    driver = webdriver.Chrome(options=options)  # Selenium Manager/PATH
    driver.set_window_size(1280, 900)
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)
    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setBlockedURLs", {
            "urls": ["*.png","*.jpg","*.jpeg","*.gif","*.webp","*.svg",
                     "*.css","*.woff","*.woff2","*.ttf","*.otf",
                     "*.mp4","*.avi","*.webm",
                     "*doubleclick.net*","*googletagmanager.com*","*google-analytics.com*"]
        })
    except Exception:
        pass
    return driver

# ---------- navigasyon ----------
def open_company_from_ticker(driver, wait, ticker: str) -> Optional[str]:
    driver.get("https://www.kap.org.tr/tr/bist-sirketler")
    # çerez
    try:
        accept = WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.ID, "acceptAllButton")))
        safe_click(driver, accept)
    except Exception:
        pass
    # arama
    try:
        search = WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.ID, "compoiners-search")))
        search.clear(); search.send_keys(ticker)
    except Exception:
        pass

    # tablo filtresi tamamlanana kadar bekle
    try:
        WebDriverWait(driver, 7).until(
            lambda d: any(ticker.upper() in (r.text or "").upper()
                          for r in d.find_elements(By.CSS_SELECTOR, "#financialTable tbody tr"))
        )
    except Exception:
        time.sleep(1.0)

    rows = driver.find_elements(By.XPATH, "//table[@id='financialTable']/tbody/tr[.//td[1]//a]")
    tU = ticker.upper()

    def row_has_ticker(r) -> bool:
        try:
            txt = (r.text or "").strip().upper()
            if tU in txt:
                return True
            td1 = r.find_element(By.XPATH, ".//td[1]")
            lines = [ln.strip().upper() for ln in td1.text.splitlines() if ln.strip()]
            if lines:
                if tU == lines[0] or tU == lines[-1]:
                    return True
            node = td1.find_elements(By.XPATH, f".//*[normalize-space()='{ticker}']")
            return bool(node)
        except Exception:
            return False

    for r in rows:
        if row_has_ticker(r):
            try:
                a = r.find_element(By.XPATH, ".//td[1]//a")
                href = a.get_attribute("href")
                if href:
                    return href
                safe_click(driver, a)
                return driver.current_url
            except Exception:
                continue

    # gevşek fallback
    for r in rows:
        if tU in (r.text or "").upper():
            try:
                a = r.find_element(By.XPATH, ".//td[1]//a")
                href = a.get_attribute("href")
                if href:
                    return href
            except Exception:
                continue
    return None

def goto_tab(driver, wait, tab_id: str, hint: str):
    try:
        el = wait.until(EC.presence_of_element_located((By.ID, tab_id)))
        href = el.get_attribute("href")
        if href and driver.current_url != href:
            driver.get(href)
        else:
            safe_click(driver, el)
        WebDriverWait(driver, 8).until(EC.url_contains(hint))
        time.sleep(1)
    except Exception:
        try:
            el = driver.find_element(By.XPATH, f"//a[contains(., '{hint.replace('/','')}') or contains(@href,'{hint}')]")
            href = el.get_attribute("href")
            if href and driver.current_url != href:
                driver.get(href)
            else:
                safe_click(driver, el)
            WebDriverWait(driver, 8).until(EC.url_contains(hint))
            time.sleep(1)
        except Exception as e:
            print(f"Sekmeye gidilemedi: {hint} - Hata: {e}")

# ---------- summary yardımcıları ----------
def h3_following_text(driver, h3_label: str) -> Optional[str]:
    try:
        el = driver.find_element(By.XPATH, f"//h3[normalize-space()='{h3_label}']/following-sibling::*[1]")
        if el.tag_name.lower() == "a":
            return el.get_attribute("href") or textify(el)
        return textify(el)
    except Exception:
        return None

def h3_following_container(driver, h3_label: str):
    try:
        return driver.find_element(By.XPATH, f"//h3[normalize-space()='{h3_label}']/following-sibling::*[1]")
    except Exception:
        return None

def h3_following_chip_links(driver, h3_label: str) -> List[str]:
    out = []
    cont = h3_following_container(driver, h3_label)
    if not cont:
        return out
    try:
        for a in cont.find_elements(By.TAG_NAME, "a"):
            t = textify(a)
            if t: out.append(t)
    except Exception:
        pass
    return out

# --- SEKTÖR OKUMA (chip-aware) ---
def parse_sector_text(raw: Optional[str]) -> Tuple[Optional[str], Optional[str], List[str]]:
    """Eski metin-temelli ayrım (fallback). Büyük harf + alt_list döner."""
    if not raw: return None, None, []
    s = re.sub(r"\s+", " ", raw).strip()
    # İMALAT özel kuralı
    if "İMALAT" in s.upper() or "IMALAT" in s.upper():
        ana = "İMALAT"
        alt = re.sub(r"[İI]MALAT\s*[-–—/,]?\s*", "", s, flags=re.IGNORECASE).strip()
        alt_u = tr_upper(alt) or ""
        alt_list = [x.strip() for x in alt_u.split("-") if x.strip()] if "-" in alt_u else ([alt_u] if alt_u else [])
        return (tr_upper(ana), alt_u, alt_list)
    # genel ayırıcılar
    parts = re.split(r"\s*[-–—/,]\s+|\s{2,}", s)
    if len(parts) >= 2:
        ana = tr_upper(parts[0].strip())
        rest = [tr_upper(p.strip()) for p in parts[1:] if p.strip()]
        return (ana, " - ".join(rest), rest)
    toks = s.split()
    if len(toks) > 1:
        ana = tr_upper(toks[0])
        alt = tr_upper(" ".join(toks[1:]))
        return (ana, alt, [alt] if alt else [])
    return (tr_upper(s), "", [])

def extract_sector(driver) -> Tuple[Optional[str], Optional[str], str, List[str]]:
    """
    'Şirketin Sektörü' bölümündeki chip/link'leri tek tek toplar.
    İlk chip ana sektör, kalanlar alt sektör (liste).
    Chip yoksa metin temelli ayrım.
    Hepsi TÜRKÇE BÜYÜK HARF döner.
    """
    cont = h3_following_container(driver, "Şirketin Sektörü")
    raw_text = textify(cont) if cont else None
    tokens: List[str] = []
    if cont:
        try:
            nodes = cont.find_elements(By.XPATH, ".//a | .//*[contains(@class,'chip')]")
            for n in nodes:
                t = textify(n)
                if t:
                    tokens.append(t)
        except Exception:
            pass

    if tokens:
        toks = [tr_upper(t.strip()) for t in tokens if t.strip()]
        ana = toks[0] if toks else None
        alt_list = []
        seen = set()
        for t in toks[1:]:
            if t not in seen:
                seen.add(t)
                alt_list.append(t)
        alt_join = " - ".join(alt_list)
        return ana, alt_join, (raw_text or ""), alt_list

    # yoksa fallback
    ana, alt_join, alt_list = parse_sector_text(raw_text)
    return ana, alt_join, (raw_text or ""), alt_list

# --- PAZAR OKUMA (Main pazar seçimi) ---
_MAIN_PAZAR_CANDIDATES = [
    "YILDIZ PAZAR",
    "ANA PAZAR",
    "ALT PAZAR",
    "YAKIN İZLEME PAZARI",
    "PİYASA ÖNCESİ İŞLEM PLATFORMU",
    "PIYASA ONCESI ISLEM PLATFORMU",   # normalize edilmemiş olasılık
    "KOLEKTİF ÜRÜNLER PAZARI",
    "KOLEKTIF URUNLER PAZARI",
]

_EXCLUDE_PHRASES = [
    "NİTELİKLİ YATIRIMCILAR ARASINDA", "NITELIKLI YATIRIMCILAR ARASINDA",
    "SERBEST İŞLEM", "SERBEST ISLEM",
    "NİTELİKLİ", "NITELIKLI",
]

def extract_main_pazar(driver) -> Optional[str]:
    for label in ["Sermaye Piyasası Aracının İşlem Gördüğü Pazar", "İşlem Gördüğü Pazar"]:
        cont = h3_following_container(driver, label)
        if not cont:
            continue
        # önce chip/link topla
        pieces: List[str] = []
        try:
            nodes = cont.find_elements(By.XPATH, ".//a | .//*[contains(@class,'chip')] | .//span | .//p")
            for n in nodes:
                t = textify(n)
                if t:
                    pieces.append(t)
        except Exception:
            pass
        raw = tr_upper(" ".join(pieces)) if pieces else tr_upper(textify(cont))

        # parçalara ayır (virgül, /, -, yeni satır, fazla boşluk)
        toks = []
        if pieces:
            for p in pieces:
                for t in re.split(r"[,\-/]+|\s{2,}|\n", p):
                    t = tr_upper(t.strip())
                    if t:
                        toks.append(t)
        else:
            toks = [raw] if raw else []

        # EXCLUDE içerenleri at
        def not_excluded(s: str) -> bool:
            U = tr_upper(s) or ""
            return all(ex not in U for ex in _EXCLUDE_PHRASES)

        toks = [t for t in toks if not_excluded(t)]

        # adaylardan ilk geçen
        for t in toks:
            for cand in _MAIN_PAZAR_CANDIDATES:
                if cand in t:
                    return cand  # standart isimle dön

        # raw içinde ara
        if raw:
            for cand in _MAIN_PAZAR_CANDIDATES:
                if cand in raw:
                    return cand

        # hiçbiri bulunamadıysa, ilk token (exclude edilmemiş) yeter
        if toks:
            return toks[0]
    return None

def extract_summary(driver) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    out["internet_adresi"] = h3_following_text(driver, "İnternet Adresi")
    out["denetim_kurulusu"] = h3_following_text(driver, "Bağımsız Denetim Kuruluşu") or h3_following_text(driver, "Denetim Kuruluşu")

    sektor_ana, sektor_alt_join, sektor_raw, sektor_alt_list = extract_sector(driver)
    out["sektoru_raw"] = sektor_raw
    out["sektor_ana"] = sektor_ana
    out["sektor_alt"] = sektor_alt_join
    out["sektor_alt_list"] = sektor_alt_list

    main_pazar = extract_main_pazar(driver)
    out["islem_gordugu_pazar"] = main_pazar

    out["dahil_oldugu_endeksler"] = h3_following_chip_links(driver, "Şirketin Dahil Olduğu Endeksler")
    return out

# ---------- genel ----------
def get_kotasyon_tarihi(driver) -> Optional[str]:
    try:
        table = driver.find_element(By.XPATH, "//table[.//th[contains(normalize-space(),'Kotasyon/İşlem Görmeye Başlama Tarihi')]]")
        df = parse_table(table)
        def col(name_part):
            for c in df.columns:
                if name_part.lower() in c.lower(): return c
        c_tur   = col("Türü") or col("Tür")
        c_tarih = col("Kotasyon/İşlem Görmeye Başlama")
        if c_tur and c_tarih:
            hisse = df[df[c_tur].str.contains("Hisse", case=False, na=False)]
            if not hisse.empty:
                return hisse.iloc[0][c_tarih]
            if not df.empty:
                return df.iloc[0][c_tarih]
    except Exception:
        pass
    return None

def extract_merkez_adresi(driver) -> Optional[str]:
    try:
        hdr = driver.find_element(By.XPATH, "//*[contains(@class,'company__sgbf-h6-title')]//*[contains(normalize-space(),'İletişim')]")
        table = hdr.find_element(By.XPATH, "./ancestor::div[contains(@class,'company__sgbf-h6-title')]/following-sibling::div//table")
        df = parse_table(table)
        if not df.empty:
            return df.iloc[0].get("Adres") or df.iloc[0].get("Adres__1") or ""
    except Exception:
        pass
    return None

def extract_uretim_adresleri(driver) -> List[str]:
    try:
        header = driver.find_element(By.XPATH, "//*[contains(@class,'company__sgbf-h6-title')]//*[contains(normalize-space(),'Üretim Tesislerinin Bulunduğu Adresler')]")
        content_container = header.find_element(By.XPATH, "./ancestor::div[contains(@class,'company__sgbf-h6-title')]/following-sibling::div[1]")
        p_tags = content_container.find_elements(By.XPATH, ".//div[contains(@class, 'html__parser-container')]//p")
        return [textify(p) for p in p_tags if textify(p)]
    except Exception:
        return []

def get_value_by_label(driver, label: str) -> Optional[str]:
    try:
        node = driver.find_element(By.XPATH, f"//*[contains(@class,'company__sgbf-h6-title')]//*[contains(normalize-space(),'{label}')]")
        container = node.find_element(By.XPATH, "./ancestor::div[contains(@class,'sgbf__accordion-container')]")
        value_span = container.find_element(By.XPATH, ".//span[contains(@class,'font-normal') and not(ancestor::table)][1]")
        return textify(value_span)
    except Exception:
        return None

def extract_fiili_dolasim_metrikleri(driver) -> Dict[str, Any]:
    out = {"fiili_dolasim_tutar_tl": None, "fiili_dolasim_oran": None}
    try:
        table = driver.find_element(By.XPATH, "//table[.//th[contains(.,'Fiili Dolaşımdaki Pay Tutarı')]]")
        df = parse_table(table)
        if not df.empty:
            row = df[df[df.columns[0]].str.contains(TICKER, na=False)]
            if row.empty:
                row = df.iloc[[0]]
            def col(name_part):
                for c in df.columns:
                    if name_part.lower() in c.lower(): return c
            c_tutar = col("Tutarı")
            c_oran  = col("Oranı")
            if c_tutar: out["fiili_dolasim_tutar_tl"] = row.iloc[0][c_tutar]
            if c_oran:  out["fiili_dolasim_oran"] = row.iloc[0][c_oran]
    except Exception:
        pass
    return out

def extract_sermaye_5ustu(driver) -> List[Dict[str, Any]]:
    try:
        hdr = driver.find_element(By.XPATH, "//*[contains(@class,'company__sgbf-h6-title')]//*[contains(normalize-space(),'Sermayede Doğrudan %5')]")
        table = hdr.find_element(By.XPATH, "./ancestor::div[contains(@class,'company__sgbf-h6-title')]/following-sibling::div//table")
        return parse_table(table).to_dict(orient="records")
    except Exception:
        return []

def extract_bagli_ortakliklar(driver) -> List[Dict[str, Any]]:
    try:
        hdr = driver.find_element(By.XPATH, "//*[contains(@class,'company__sgbf-h6-title')]//*[contains(normalize-space(),'Bağlı Ortaklıklar')]")
        table = hdr.find_element(By.XPATH, "./ancestor::div[contains(@class,'company__sgbf-h6-title')]/following-sibling::div//table")
        return parse_table(table).to_dict(orient="records")
    except Exception:
        return []

def extract_board_members(driver) -> Optional[List[Dict[str, Any]]]:
    try:
        hdr = driver.find_element(By.XPATH, "//*[contains(@class,'company__sgbf-h6-title')]//*[contains(normalize-space(),'Yönetim Kurulu Üyeleri')]")
        table = hdr.find_element(By.XPATH, "./ancestor::div[contains(@class,'company__sgbf-h6-title')]/following-sibling::div//table")
        df = parse_table(table)
        drop_contains = [
            "Bağımsız Yönetim Kurulu Üyesi", "Bağımsızlık Beyanı",
            "Aday Gösterme Komitesi", "Bağımsızlığını Kaybeden",
            "Yer Aldığı Komiteler"
        ]
        for col in list(df.columns):
            if any(key in col for key in drop_contains):
                df = df.drop(columns=[col])
        return df.to_dict(orient="records")
    except Exception:
        return None

# ---------- kurumsal: oy hakları ----------
def extract_oy_haklari(driver) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    try:
        table = driver.find_element(By.XPATH, "//table[.//th[contains(normalize-space(),'Oy Hakları')]]")
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        pairs = []
        for r in rows:
            tds = r.find_elements(By.TAG_NAME, "td")
            if len(tds) >= 2:
                key = textify(tds[0]); val = textify(tds[1])
                if key or val:
                    pairs.append({"alan": key, "deger": val})
        out["pairs"] = pairs
    except Exception:
        out["pairs"] = []
    return out

# ---------- katılım 1–7 ----------
def extract_katilim(driver) -> Dict[str, Any]:
    out = {f"m{i}": None for i in range(1, 8)}
    try:
        table = driver.find_element(By.XPATH, "//table[.//th[contains(normalize-space(),'ÖZET BİLGİLER')]]")
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        for tr in rows:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) < 2:
                continue
            left = textify(tds[0]); right = textify(tds[1])
            m = re.match(r"^\s*(\d+)\)\s*", left)
            if not m:
                continue
            k = int(m.group(1))
            if 1 <= k <= 7:
                out[f"m{k}"] = right
    except Exception:
        pass
    return out

# ---------- JSON yaz ----------
def save_json(ticker: str, data: Dict[str, Any]):
    ensure_dir(OUTPUT_DIR)
    out_path = os.path.join(OUTPUT_DIR, f"{ticker}.json")
    tmp_path = out_path + ".tmp"  # atomic write

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    os.replace(tmp_path, out_path)
    print(f"✓ {ticker}: {out_path}")

# ---------- tek şirketi aynı düzenle işle ----------
def process_one_ticker(driver, wait, ticker: str):
    global TICKER
    TICKER = ticker  # fiili dolaşım tablosu için

    print(f"\n[{ticker}] [1/7] link bulunuyor...")
    link = open_company_from_ticker(driver, wait, ticker)
    if not link:
        raise RuntimeError(f"{ticker}: şirket sayfası bulunamadı.")
    print("   →", link)

    print(f"[{ticker}] [2/7] Özet...")
    driver.get(link)
    summary = extract_summary(driver)

    print(f"[{ticker}] [3/7] Genel...")
    goto_tab(driver, wait, "general-tab", "/sirket-bilgileri/genel/")
    general = {
        "merkez_adresi": extract_merkez_adresi(driver) or h3_following_text(driver, "Merkez Adresi"),
        "uretim_tesis_adresleri": extract_uretim_adresleri(driver),
        "kotasyon_tarihi": get_kotasyon_tarihi(driver),
    }

    print(f"[{ticker}] [4/7] Sermaye...")
    ownership = {
        "odenmis_cikarilmis_sermaye": get_value_by_label(driver, "Ödenmiş/Çıkarılmış Sermaye"),
        "kayitli_sermaye_tavani": get_value_by_label(driver, "Kayıtlı Sermaye Tavanı"),
        "sermaye_5ustu": extract_sermaye_5ustu(driver),
        **extract_fiili_dolasim_metrikleri(driver),
        "bagli_ortakliklar": extract_bagli_ortakliklar(driver),
    }

    print(f"[{ticker}] [5/7] Yönetim Kurulu...")
    board_members = extract_board_members(driver)

    print(f"[{ticker}] [6/7] Kurumsal / Oy Hakları...")
    goto_tab(driver, wait, "corporate-tab", "/kurumsal")
    oy_haklari = extract_oy_haklari(driver)

    print(f"[{ticker}] [7/7] Katılım 1–7...")
    goto_tab(driver, wait, "participation-tab", "/katilim")
    katilim = extract_katilim(driver)

    data = {
        "ticker": ticker,
        "summary": summary,
        "general": general,
        "ownership": ownership,
        "board_members": board_members,
        "oy_haklari": oy_haklari,
        "katilim_4_7": katilim,
    }
    save_json(ticker, data)

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="Ticker dosyası yolu (varsayılan public/tickers.txt)", default=DEFAULT_TICKER_FILE)
    parser.add_argument("-t", "--tickers", help="Virgülle ayrılmış semboller (dosyayı bypass eder). Örn: -t ARCLK,ASELS")
    args = parser.parse_args()

    ensure_dir(OUTPUT_DIR)

    if args.tickers:
        tickers = [x.strip().upper() for x in args.tickers.split(",") if x.strip()]
    else:
        tickers = read_tickers(args.file)

    if not tickers:
        print("⚠ Hiç sembol bulunamadı. -t ile ver veya ticker dosyasını yerleştir.")
        return

    driver = make_driver()
    wait = WebDriverWait(driver, WAIT_SEC)

    try:
        print(f"\nToplam {len(tickers)} sembol bulundu.\n")
        for i, t in enumerate(tickers, 1):
            try:
                print(f"\n=== ({i}/{len(tickers)}) {t} işleniyor ===")
                process_one_ticker(driver, wait, t)
            except KeyboardInterrupt:
                print("\n↩ Kullanıcı iptal etti.")
                break
            except Exception as e:
                print(f"✗ {t}: {e}")
            time.sleep(0.2)
    finally:
        driver.quit()
        print("\nBitti.")

if __name__ == "__main__":
    main()

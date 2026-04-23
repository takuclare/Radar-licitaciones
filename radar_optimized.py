# radar.py
import re
import json
import os
import unicodedata
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Callable
from urllib.parse import urlparse, urljoin, parse_qsl, urlencode

import pandas as pd
import feedparser
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# ---------------- CONFIG ----------------
FEEDS = [
    "https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom",
    "https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044/PlataformasAgregadasSinMenores.atom",
]

KEYWORDS_BOOST = [
    "aeropuerto", "aeropuertos", "pista", "rodaje", "apron",
    "terminal", "hangares", "hangar", "atc", "torre de control", "navegación aérea",
    "oaci", "icao", "aena", "enaire", "seguridad operacional", "aviación",
    "safety", "operación aeroportuaria", "balizamiento",
    "armada", "base aérea",
    "redacción de proyecto", "redaccion de proyecto", "proyecto básico", "proyecto de ejecución", "CSS"
]

# ✅ NUEVO: keywords súper prioritarias (empujan fuerte; no excluyen nada)
# (AT/DF se detectan con regex de palabra completa en score_tenders)
KEYWORDS_SUPERBOOST = [
    "redacción de proyecto", "redaccion de proyecto",
    "coordinación de seguridad y salud", "coordinacion de seguridad y salud",
    "atdocv",
    "at",
    "df", "aeropuerto","aeroporto", "aeronáutico", "aeronautico", "helipuerto", "ATRP", "A.T.R.P",
    "aerodromo", "aeródromo"
]

# ✅ NUEVO: CPVs prioritarios (PRIORIDAD ABSOLUTA: deben salir los primeros si se detectan)
# Detectamos tanto "NNNNNNNN-X" como "NNNNNNNN"
PRIORITY_CPVS = {
    "71000000-8",
    "71242000-6",
    "71247000-1",
    "71300000-1",
    "71317200-5",
    "71200000-0",
    "71221000-3",
    "71222000-0",
    "71240000-2",
    "71245000-7",
    "71410000-5",
    "71520000-9",
}


# ✅ NUEVO: prefijos "Airia" derivados de PRIORITY_CPVS
# - Algunos textos pueden mostrar CPV sin ceros iniciales, o sin dígito de control.
# - Consideramos match si aparece un CPV cuyo inicio coincide con el prefijo (3 dígitos) de algún CPV prioritario.
def _priority_cpv_prefixes() -> set:
    prefs = set()
    for p in PRIORITY_CPVS:
        digits = re.sub(r"\D", "", str(p))[:8]  # CPV base (8 dígitos)
        if not digits:
            continue
        if len(digits) >= 3:
            prefs.add(digits[:3])
        digits_nz = digits.lstrip("0") or digits
        if len(digits_nz) >= 3:
            prefs.add(digits_nz[:3])
    return prefs

_PRIORITY_CPV_PREFIXES = _priority_cpv_prefixes()

def _has_priority_cpv_airia(text: str) -> bool:
    """True si detecta indicios de CPV prioritario en el texto."""
    if not text:
        return False

    t = str(text)

    # 1) CPV estándar: 8 dígitos (con o sin -dígito)
    for cpv8, _ in re.findall(r"\b(\d{8})(?:-(\d))?\b", t):
        if cpv8[:3] in _PRIORITY_CPV_PREFIXES:
            return True

    # 2) Caso: CPV sin ceros iniciales cerca de la palabra CPV
    tl = _normalize(t)
    if "cpv" in tl:
        for dig in re.findall(r"\bcpv\b[^\n]{0,40}?\b(\d{3,8})\b", tl, flags=re.IGNORECASE):
            if dig[:3] in _PRIORITY_CPV_PREFIXES:
                return True
            padded = dig.zfill(8)
            if padded[:3] in _PRIORITY_CPV_PREFIXES:
                return True

    return False


KEYWORDS_BLOCK = [
    "suministro",
    "limpieza",
    "seguridad privada",
    "catering",
    "mantenimiento de ascensores",
    "carpinter",
    "Rent",
    "Alquil",
    "Veh",
    "CCTV",
    "Varada",
    "poda",
    "tala",
    "Adquisicion",
    "Adquisición",
    "Suministro",
    "Pañales",
    "Ferretería",
]


# ---------------- CACHE ----------------
CACHE_DIR = "pliegos_cache"
CACHE_PATH = os.path.join(CACHE_DIR, "dates_cache.json")
CACHE_VERSION = 6  # ⬅️ subimos versión para invalidar cache anterior con status mal extraído

def _load_cache() -> dict:
    try:
        if os.path.exists(CACHE_PATH):
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save_cache(cache: dict) -> None:
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

_CACHE = _load_cache()

# Reutiliza conexiones (más rápido) — pero seguro en paralelo (session por hilo)
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

_thread_local = threading.local()

def _get_session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        _thread_local.session = s
    return s


@dataclass
class Tender:
    title: str
    summary: str
    published: str
    updated: str
    deadline: str
    link: str
    source_feed: str
    atom_importe: str = ""


# ---------------- HELPERS ----------------
def _clean_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _normalize(s: str) -> str:
    s = (s or "").lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _extract_importe_from_atom_summary(summary: str) -> str:
    """Extrae el campo literal 'Importe:' del summary del ATOM."""
    if not summary:
        return ""
    text = _clean_text(summary)
    m = re.search(r"(?:^|[;,.\s])Importe\s*:\s*([0-9][0-9.,\s]*)\s*(EUR|€)?", text, flags=re.IGNORECASE)
    if not m:
        return ""
    amount = re.sub(r"\s+", "", (m.group(1) or "").strip())
    currency = (m.group(2) or "EUR").replace("€", "EUR").strip()
    return f"{amount} {currency}".strip()

def _fetch_feed_entries_paginated(feed_url: str, max_entries: int = 2000, max_pages: int = 10) -> List:
    """Lee un feed ATOM siguiendo enlaces rel=next hasta un máximo razonable."""
    collected = []
    next_url = feed_url
    seen_urls = set()
    pages = 0

    max_entries = max(1, int(max_entries or 1))
    max_pages = max(1, int(max_pages or 1))

    while next_url and next_url not in seen_urls and pages < max_pages and len(collected) < max_entries:
        seen_urls.add(next_url)
        parsed = feedparser.parse(next_url)
        entries = list(parsed.entries or [])
        if entries:
            remaining = max_entries - len(collected)
            collected.extend(entries[:remaining])

        next_found = None
        feed_links = getattr(parsed.feed, "links", []) if getattr(parsed, "feed", None) else []
        for link in feed_links:
            rel = (getattr(link, "rel", "") or "").lower()
            href = getattr(link, "href", None) or (link.get("href") if isinstance(link, dict) else None)
            if rel == "next" and href:
                next_found = urljoin(next_url, href)
                break

        next_url = next_found
        pages += 1

    return collected


# ---------------- CATALUNYA: filtro estado en Atom ----------------
_FEED_1044 = "https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044/PlataformasAgregadasSinMenores.atom"

def _is_catalunya_tender(link: str) -> bool:
    """Detecta licitaciones de Cataluña por host del <link href> (contractaciopublica.cat)."""
    if not link:
        return False
    try:
        host = urlparse(link).netloc.lower()
    except Exception:
        return False
    return "contractaciopublica.cat" in host

def _atom_status_is_en_plazo(summary: str) -> bool:
    """True si el summary del Atom incluye 'Estado: EN PLAZO' (tolerante a espacios/mayúsculas)."""
    t = _normalize(summary or "")
    return re.search(r"\bestado\s*:?\s*en\s*plazo\b", t) is not None


# ---------------- JUNTA DE ANDALUCÍA (www.juntadeandalucia.es) ----------------
_ANDALUCIA_HOST = "www.juntadeandalucia.es"

def _is_andalucia_tender(link: str) -> bool:
    if not link:
        return False
    try:
        host = urlparse(link).netloc.lower()
    except Exception:
        return False
    return _ANDALUCIA_HOST in host

def _normalize_andalucia_link_key(link: str) -> str:
    if not link:
        return ""
    try:
        u = urlparse(link.strip())
        host = (u.netloc or "").lower()
        path = (u.path or "").rstrip("/")
        query_items = [(k, v) for k, v in parse_qsl(u.query or "", keep_blank_values=True)]
        query_items.sort()
        query = urlencode(query_items, doseq=True)
        return f"{host}{path}?{query}" if query else f"{host}{path}"
    except Exception:
        return (link or "").strip().lower()

def _extract_andalucia_end_date_map_from_atom_xml(xml: str) -> Dict[str, date]:
    """Extrae {link_href_normalizado: EndDate} de una página XML ATOM ya descargada."""
    try:
        root = ET.fromstring(xml or "")
    except Exception:
        return {}

    out: Dict[str, date] = {}

    for entry in root.findall(".//{*}entry"):
        href = ""
        for lk in entry.findall("{*}link"):
            h = (lk.get("href") or "").strip()
            if h:
                href = h
                break

        if not href:
            for el in entry.iter():
                h = (el.get("href") or "").strip()
                if h:
                    href = h
                    break

        if not href or not _is_andalucia_tender(href):
            continue

        end_txt = ""
        for el in entry.iter():
            tag = el.tag or ""
            if isinstance(tag, str) and tag.endswith("EndDate"):
                end_txt = (el.text or "").strip()
                if end_txt:
                    break

        if not end_txt:
            continue

        try:
            d = date.fromisoformat(end_txt[:10])
        except Exception:
            continue

        out[_normalize_andalucia_link_key(href)] = d

    return out

# ---------------- COMUNIDAD DE MADRID (contratos-publicos.comunidad.madrid) ----------------
_MADRID_HOST = "contratos-publicos.comunidad.madrid"

def _is_madrid_tender(link: str) -> bool:
    if not link:
        return False
    try:
        host = urlparse(link).netloc.lower()
    except Exception:
        return False
    return _MADRID_HOST in host

def _normalize_madrid_link_key(link: str) -> str:
    """Normaliza links de la Comunidad de Madrid para casar el href del ATOM con el link parseado."""
    if not link:
        return ""
    try:
        u = urlparse(link.strip())
        host = (u.netloc or "").lower()
        path = (u.path or "").rstrip("/")
        query_items = [(k, v) for k, v in parse_qsl(u.query or "", keep_blank_values=True)]
        query_items.sort()
        query = urlencode(query_items, doseq=True)
        return f"{host}{path}?{query}" if query else f"{host}{path}"
    except Exception:
        return (link or "").strip().lower()

def _extract_issue_date_map_from_atom_xml(xml: str) -> Dict[str, date]:
    """Extrae {link_href_normalizado: IssueDate} de una página XML ATOM ya descargada."""
    try:
        root = ET.fromstring(xml or "")
    except Exception:
        return {}

    out: Dict[str, date] = {}

    for entry in root.findall(".//{*}entry"):
        href = ""
        for lk in entry.findall("{*}link"):
            h = (lk.get("href") or "").strip()
            if h:
                href = h
                break

        if not href:
            for el in entry.iter():
                h = (el.get("href") or "").strip()
                if h:
                    href = h
                    break

        if not href:
            continue

        issue_txt = ""
        for el in entry.iter():
            tag = el.tag or ""
            if isinstance(tag, str) and tag.endswith("IssueDate"):
                issue_txt = (el.text or "").strip()
                if issue_txt:
                    break

        if not issue_txt:
            continue

        try:
            d = date.fromisoformat(issue_txt[:10])
        except Exception:
            continue

        out[_normalize_madrid_link_key(href)] = d

    return out


def _fetch_issue_date_map_from_atom(atom_url: str, max_entries: int = 3000, max_pages: int = 10) -> Dict[str, date]:
    """
    Devuelve {link_href_normalizado: IssueDate} recorriendo TODAS las páginas del ATOM.
    Esto evita que licitaciones de Madrid de páginas posteriores entren con una fecha reciente de update
    cuando su IssueDate real es antiguo.
    """
    out: Dict[str, date] = {}
    next_url = atom_url
    seen_urls = set()
    pages = 0
    collected = 0

    max_entries = max(1, int(max_entries or 1))
    max_pages = max(1, int(max_pages or 1))

    while next_url and next_url not in seen_urls and pages < max_pages and collected < max_entries:
        seen_urls.add(next_url)
        try:
            r = _get_session().get(next_url, timeout=25, allow_redirects=True)
            r.raise_for_status()
            xml = r.text or ""
        except Exception:
            break

        page_map = _extract_issue_date_map_from_atom_xml(xml)
        if page_map:
            out.update(page_map)
            collected += len(page_map)

        try:
            root = ET.fromstring(xml)
        except Exception:
            break

        next_found = None
        for lk in root.findall('.//{*}link'):
            rel = (lk.get('rel') or '').lower()
            href = (lk.get('href') or '').strip()
            if rel == 'next' and href:
                next_found = urljoin(next_url, href)
                break

        next_url = next_found
        pages += 1

    return out

def _today_madrid() -> date:
    try:
        return datetime.now(ZoneInfo("Europe/Madrid")).date()
    except Exception:
        # fallback a naive local si ZoneInfo no está disponible (muy raro)
        return datetime.now().date()

def _to_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if not dt:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

def _parse_atom_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _parse_es_date_any(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    fmts = [
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except Exception:
            pass
    return None

def _extract_deadline_from_text(text: str) -> Optional[datetime]:
    if not text:
        return None
    t = text.lower()
    patterns = [
        r"(fecha\s*l[ií]mite[^0-9]{0,60})(\d{1,2}[/-]\d{1,2}[/-]\d{4})([^0-9]{0,10})(\d{1,2}:\d{2})?",
        r"(plazo\s*de\s*presentaci[oó]n[^0-9]{0,60})(\d{1,2}[/-]\d{1,2}[/-]\d{4})([^0-9]{0,10})(\d{1,2}:\d{2})?",
    ]
    for p in patterns:
        m = re.search(p, t)
        if m:
            date_part = m.group(2).replace("-", "/")
            time_part = m.group(4) or ""
            if time_part:
                return _parse_es_date_any(f"{date_part} {time_part}")
            return _parse_es_date_any(date_part)
    return None

def _find_date_near_label(text: str, label_patterns: List[str]) -> Optional[datetime]:
    if not text:
        return None
    t = re.sub(r"\s+", " ", text)
    date_re_num = r"(\d{1,2}[/-]\d{1,2}[/-]\d{4})(?:\s+(\d{1,2}:\d{2})(?::(\d{2}))?)?"
    for lp in label_patterns:
        m = re.search(lp + r".{0,200}?" + date_re_num, t, flags=re.IGNORECASE)
        if m:
            d = m.group(1).replace("-", "/")
            hhmm = m.group(2) or ""
            ss = m.group(3) or ""
            if hhmm:
                if ss:
                    return _parse_es_date_any(f"{d} {hhmm}:{ss}")
                return _parse_es_date_any(f"{d} {hhmm}")
            return _parse_es_date_any(d)
    return None


# ---------------- CSP extraction ----------------
def _is_csp_host(link: str) -> bool:
    host = urlparse(link).netloc.lower()
    return ("contrataciondelsectorpublico.gob.es" in host) or ("contrataciondelestado.es" in host)

def _extract_csp_publication_from_docs_table(soup: BeautifulSoup) -> Optional[datetime]:
    """
    Devuelve la fecha REAL de publicación inicial de la licitación en CSP.

    Regla:
      - mirar solo la tabla "Anuncios y Documentos"
      - aceptar únicamente filas de "Anuncio de Licitación" (DOC_CN)
      - si no existiera, aceptar como fallback "Pliego" (DOC_CD)
      - ignorar completamente "Otros Documentos" / DOC_GEN / actas / informes

    Con esto evitamos colar expedientes antiguos que reaparecen por publicar
    informes de valoración, actas u otros documentos posteriores.
    """
    if soup is None:
        return None

    table = soup.find("table", id="myTablaDetalleVISUOE")
    if table is None:
        return None

    def _row_dt(tr) -> Optional[datetime]:
        first_td = tr.find("td")
        if first_td is None:
            return None
        txt = first_td.get_text(" ", strip=True)
        m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2})(?::(\d{2}))?", txt)
        if not m:
            return None
        d = m.group(1)
        hhmm = m.group(2)
        ss = m.group(3)
        return _parse_es_date_any(f"{d} {hhmm}:{ss}") if ss else _parse_es_date_any(f"{d} {hhmm}")

    anuncio_candidates: List[datetime] = []
    pliego_candidates: List[datetime] = []

    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        doc_txt = _normalize(tds[1].get_text(" ", strip=True))
        dt = _row_dt(tr)
        if not dt:
            continue

        if ("anuncio" in doc_txt) and ("licitaci" in doc_txt):
            anuncio_candidates.append(dt)
        elif "pliego" in doc_txt:
            pliego_candidates.append(dt)

    if anuncio_candidates:
        return min(anuncio_candidates)
    if pliego_candidates:
        return min(pliego_candidates)
    return None


def _extract_csp_status_robust(html: str, soup: BeautifulSoup) -> Optional[str]:
    """
    Extrae estado CSP de forma robusta, soportando:
      - ES: "Estado de la Licitación"
      - EN: "State of the Tender"
      - JSON embebido: estadoLicitacion / estadoExpediente / estado
    """
    labels = ["estado de la licitacion", "state of the tender"]

    # 1) Intento estructural: tabla con etiqueta en una celda y el valor en la siguiente
    if soup is not None:
        for tr in soup.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            left = _normalize(cells[0].get_text(" ", strip=True))
            if any(lbl in left for lbl in labels):
                val = cells[1].get_text(" ", strip=True)
                val = re.sub(r"\s+", " ", val).strip()
                if val:
                    return val

    # 2) JSON embebido frecuente
    if html:
        m = re.search(r'"estadoLicitacion"\s*:\s*"([^"]+)"', html, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()

        m = re.search(r'"estadoExpediente"\s*:\s*"([^"]+)"', html, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()

        m = re.search(
            r'"estado"\s*:\s*"(PUBLICADA|EN_EVALUACION|RESUELTA|ADJUDICADA|FORMALIZADA|ANULADA|DESIERTA)"',
            html,
            flags=re.IGNORECASE
        )
        if m:
            return m.group(1).strip()

    # 3) Fallback por texto plano (ES/EN)
    if soup is not None:
        text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
        m = re.search(
            r"(Estado de la Licitaci[oó]n|State of the Tender)\s+(Publicada|En\s+evaluaci[oó]n|Resuelta|Adjudicada|Formalizada|Anulada|Desierta)",
            text,
            flags=re.IGNORECASE
        )
        if m:
            return m.group(2).strip()

    return None


def _csp_status_is_publicada(status: Optional[str]) -> Optional[bool]:
    """
    Devuelve:
      True  -> seguro publicada
      False -> seguro NO publicada (incluye en evaluación, resuelta...)
      None  -> desconocido (no se pudo extraer)
    """
    if not status:
        return None

    s = _normalize(status).replace("_", " ")

    if "evaluacion" in s:
        return False
    if "resuelta" in s or "adjudic" in s or "formaliz" in s or "desierta" in s or "anulada" in s:
        return False
    if "anuncio previo" in s or "previo" in s:
        return False

    if "publicada" in s:
        return True

    return False


def _csp_failsafe_exclude_by_text(title: str, summary: str) -> bool:
    """
    Si no conseguimos estado, al menos excluimos casos obvios.
    """
    t = _normalize((title or "") + " " + (summary or ""))
    if "en evaluacion" in t or "evaluacion" in t:
        return True
    if "adjudic" in t or "formaliz" in t or "resuelta" in t or "desierta" in t or "anulada" in t:
        return True
    if "anuncio previo" in t or "previo" in t:
        return True
    return False


# ---------------- PORTAL INFO (1 request + cache) ----------------
def extract_portal_info(link: str) -> Tuple[Optional[datetime], Optional[datetime], Optional[str]]:
    """
    Devuelve (published_dt, deadline_dt, status) con 1 request.
    Cacheado.
    """
    if not link:
        return None, None, None

    cached = _CACHE.get(link)
    if isinstance(cached, dict) and int(cached.get("v", 1) or 1) >= CACHE_VERSION:
        pub_s = cached.get("published", "") or ""
        dead_s = cached.get("deadline", "") or ""
        status_s = cached.get("status", None)
        pub_dt = _parse_es_date_any(pub_s) if pub_s else None
        dead_dt = _parse_es_date_any(dead_s) if dead_s else None
        return pub_dt, dead_dt, status_s

    try:
        r = _get_session().get(link, timeout=20, allow_redirects=True)
        r.raise_for_status()
    except Exception:
        return None, None, None

    html = r.text or ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    is_csp = _is_csp_host(link)

    published_dt = None
    deadline_dt = None
    status = None

    if is_csp:
        published_dt = _extract_csp_publication_from_docs_table(soup)
        if not published_dt:
            published_dt = _find_date_near_label(
                text,
                [r"Publicación en plataforma", r"Fecha y hora de publicación en el Portal", r"Fecha de publicación"],
            )

        deadline_dt = _find_date_near_label(
            text,
            [
                r"Fecha y hora límite de presentación de ofertas",
                r"Fecha y hora límite de presentación",
                r"Fecha y hora l[ií]mite de presentaci[oó]n",
                r"Plazo de presentación",
                r"Fecha límite de presentación",
            ],
        )

        status = _extract_csp_status_robust(html, soup)

    else:
        published_dt = _find_date_near_label(text, [r"Publicación", r"Fecha de publicación"])
        deadline_dt = _find_date_near_label(text, [r"Fecha l[ií]mite", r"presentaci[oó]n de ofertas", r"plazo"])

    _CACHE[link] = {
        "v": CACHE_VERSION,
        "published": published_dt.strftime("%d/%m/%Y %H:%M:%S") if published_dt else "",
        "deadline": deadline_dt.strftime("%d/%m/%Y %H:%M:%S") if deadline_dt else "",
        "status": status or None,
        "saved_at": datetime.now().isoformat(timespec="seconds"),
    }
    _save_cache(_CACHE)

    return published_dt, deadline_dt, status


# ---------------- HISTÓRICO ----------------
def load_company_corpus(excel_path: str) -> List[str]:
    df = pd.read_excel(excel_path, sheet_name="TRABAJOS")

    if "Assignment" in df.columns:
        df = df[df["Assignment"].astype(str).str.lower() != "assignment"]

    cols = [c.strip() for c in df.columns]
    df.columns = cols

    lang_col = None
    for c in cols:
        if _normalize(c) in ["idioma", "language", "lang"]:
            lang_col = c
            break

    if lang_col and lang_col in df.columns:
        esp = df[df[lang_col].astype(str).str.upper().str.contains("ESP", na=False)]
        df_use = esp if len(esp) >= 20 else df
    else:
        df_use = df

    text_fields = [c for c in df_use.columns if c.lower() not in ["id", "fecha", "date"]]
    corpus = []
    for _, row in df_use.iterrows():
        parts = []
        for f in text_fields:
            val = row.get(f, "")
            if pd.isna(val):
                continue
            s = str(val).strip()
            if s:
                parts.append(s)
        if parts:
            corpus.append(" | ".join(parts))
    return corpus


# ---------------- FETCH + FILTROS ----------------
def fetch_tenders(
    only_last_days: int = 5,
    exclude_deadline_soon_days: int = 2,
    limit_per_feed: int = 2000,
    max_feed_pages: int = 10,
    max_workers: int = 12,
    only_priority_cpvs: bool = False,
    progress_cb: Optional[Callable] = None,
    pre_rank_corpus: Optional[List[str]] = None,
    deep_review_top_n: int = 30,
    apply_airia_filters: bool = True,
) -> List[Tender]:
    """
    Optimizado: mantiene la lógica de filtros/extracción y además expone progreso real y continuo.
    """
    tenders: List[Tender] = []

    def emit(progress: float, message: str, **meta):
        if not progress_cb:
            return
        payload = {"progress": max(0.0, min(1.0, float(progress))), "message": message}
        payload.update(meta)
        try:
            progress_cb(payload)
        except TypeError:
            progress_cb(payload["progress"], payload["message"])
        except Exception:
            pass

    now = datetime.utcnow()
    min_date = now - timedelta(days=only_last_days) if apply_airia_filters else None

    # 1) Parse feeds y prefiltrar por fecha del feed
    candidates: List[Tuple[str, str, str, str, str, Optional[datetime], Optional[datetime], Optional[datetime], str]] = []
    total_feed_entries = 0
    emit(0.02, "Iniciando lectura de feeds ATOM…", stage="Leyendo feeds ATOM", reviewed=0, total=0, detected=0, feed_entries=0, cache_hits=0)

    for feed_idx, feed_url in enumerate(FEEDS, start=1):
        issue_date_map: Dict[str, date] = {}
        andalucia_end_date_map: Dict[str, date] = {}
        emit(0.03 + 0.07 * ((feed_idx - 1) / max(1, len(FEEDS))),
             f"Leyendo feed {feed_idx}/{len(FEEDS)}…",
             stage="Leyendo feeds ATOM", reviewed=0, total=0, detected=0, feed_entries=total_feed_entries, cache_hits=0)
        if feed_url == _FEED_1044:
            issue_date_map = _fetch_issue_date_map_from_atom(feed_url, max_entries=limit_per_feed, max_pages=max_feed_pages)
            andalucia_end_date_map = _extract_andalucia_end_date_map_from_atom_xml(
                _get_session().get(feed_url, timeout=25, allow_redirects=True).text
            )
            next_url = feed_url
            seen_urls = set()
            pages = 0
            while next_url and next_url not in seen_urls and pages < max(1, int(max_feed_pages or 1)):
                seen_urls.add(next_url)
                try:
                    r = _get_session().get(next_url, timeout=25, allow_redirects=True)
                    r.raise_for_status()
                    xml = r.text or ""
                except Exception:
                    break
                andalucia_end_date_map.update(_extract_andalucia_end_date_map_from_atom_xml(xml))
                try:
                    root = ET.fromstring(xml)
                except Exception:
                    break
                next_found = None
                for lk in root.findall('.//{*}link'):
                    rel = (lk.get('rel') or '').lower()
                    href = (lk.get('href') or '').strip()
                    if rel == 'next' and href:
                        next_found = urljoin(next_url, href)
                        break
                next_url = next_found
                pages += 1

        entries = _fetch_feed_entries_paginated(
            feed_url,
            max_entries=limit_per_feed,
            max_pages=max_feed_pages,
        )

        feed_total = max(1, len(entries))
        for entry_idx, e in enumerate(entries, start=1):
            total_feed_entries += 1
            if entry_idx == 1 or entry_idx == feed_total or entry_idx % 25 == 0:
                feed_progress = (entry_idx / feed_total)
                emit(0.04 + 0.08 * ((feed_idx - 1 + feed_progress) / max(1, len(FEEDS))),
                     f"Analizando entradas ATOM del feed {feed_idx}/{len(FEEDS)}… {entry_idx}/{feed_total}",
                     stage="Leyendo feeds ATOM", reviewed=0, total=0, detected=len(candidates), feed_entries=total_feed_entries, cache_hits=0)

            title = _clean_text(getattr(e, "title", "") or "")
            summary = _clean_text(getattr(e, "summary", "") or "")
            link = getattr(e, "link", "") or ""
            if not link:
                continue

            if apply_airia_filters and _is_catalunya_tender(link) and not _atom_status_is_en_plazo(summary):
                continue

            is_madrid = (feed_url == _FEED_1044) and _is_madrid_tender(link)
            issue_d = issue_date_map.get(_normalize_madrid_link_key(link)) if is_madrid else None
            if apply_airia_filters and is_madrid:
                # Para Madrid, el filtrado SIEMPRE debe basarse en cbc:IssueDate del ATOM.
                # Si no podemos resolverlo, preferimos excluir la entrada antes que colarla con una fecha errónea.
                if not issue_d:
                    continue
                today = _today_madrid()
                if abs((issue_d - today).days) > 1:
                    continue

            is_andalucia = (feed_url == _FEED_1044) and _is_andalucia_tender(link)
            andalucia_end_d = andalucia_end_date_map.get(_normalize_andalucia_link_key(link)) if is_andalucia else None
            if apply_airia_filters and is_andalucia and andalucia_end_d:
                andalucia_end_dt = datetime(andalucia_end_d.year, andalucia_end_d.month, andalucia_end_d.day)
                if andalucia_end_dt < (now + timedelta(days=exclude_deadline_soon_days)):
                    continue

            published_raw = getattr(e, "published", "") or getattr(e, "updated", "") or ""
            updated_raw = getattr(e, "updated", "") or published_raw

            if is_madrid and issue_d:
                feed_published_dt = datetime(issue_d.year, issue_d.month, issue_d.day)
                published_raw = feed_published_dt.isoformat()
            else:
                feed_published_dt = _to_naive_utc(_parse_atom_date(published_raw))

            feed_updated_dt = _to_naive_utc(_parse_atom_date(updated_raw))

            if apply_airia_filters and feed_published_dt and min_date and feed_published_dt < min_date:
                continue

            deadline_dt = _to_naive_utc(_extract_deadline_from_text(summary))
            if apply_airia_filters and is_andalucia and andalucia_end_d:
                deadline_dt = datetime(andalucia_end_d.year, andalucia_end_d.month, andalucia_end_d.day)
            atom_importe = _extract_importe_from_atom_summary(summary)

            candidates.append((title, summary, link, published_raw, updated_raw, feed_published_dt, feed_updated_dt, deadline_dt, feed_url, atom_importe))

    if not candidates:
        emit(1.0, "No se han encontrado candidatas en los feeds", stage="Completado", reviewed=0, total=0, detected=0, feed_entries=total_feed_entries, cache_hits=0)
        return []

    # 2) Pre-ranking ATOM + revisión profunda selectiva
    total_candidates = max(1, len(candidates))
    cache_hits = 0

    preliminary_tenders: List[Tender] = []
    for title, summary, link, published_raw, updated_raw, feed_published_dt, feed_updated_dt, deadline_dt, feed_url, atom_importe in candidates:
        preliminary_tenders.append(
            Tender(
                title=title,
                summary=summary,
                published=feed_published_dt.isoformat() if feed_published_dt else (published_raw or ""),
                updated=feed_updated_dt.isoformat() if feed_updated_dt else (updated_raw or ""),
                deadline=deadline_dt.isoformat() if deadline_dt else "",
                link=link,
                source_feed=feed_url,
                atom_importe=atom_importe,
            )
        )

    deep_review_top_n = max(0, int(deep_review_top_n or 0))
    top_review_links = set()
    if deep_review_top_n > 0 and preliminary_tenders:
        emit(0.15, "Calculando pre-ranking sobre ATOM…", stage="Pre-ranking ATOM", reviewed=0, total=total_candidates, detected=len(preliminary_tenders), feed_entries=total_feed_entries, cache_hits=0)
        try:
            prelim_df = score_tenders(preliminary_tenders, pre_rank_corpus or [""], top_k=None)
            top_review_links = set(prelim_df.head(deep_review_top_n)["link"].astype(str).tolist())
        except Exception:
            top_review_links = set(t.link for t in preliminary_tenders[:deep_review_top_n])

    cached_review_links = set()
    csp_review_links = set()
    for c in candidates:
        link = c[2]
        if _is_csp_host(link):
            csp_review_links.add(link)
        cached = _CACHE.get(link)
        if isinstance(cached, dict) and int(cached.get("v", 1) or 1) >= CACHE_VERSION:
            cache_hits += 1
            cached_review_links.add(link)

    # Con filtros Airia activados, todos los expedientes CSP deben pasar revisión profunda.
    # Si los filtros están desactivados, devolvemos todo lo leído del feed sin excluir por reglas internas.
    if apply_airia_filters:
        review_links = list(cached_review_links | top_review_links | csp_review_links)
    else:
        review_links = []
    review_total = len(review_links)
    portal_map: Dict[str, Tuple[Optional[datetime], Optional[datetime], Optional[str]]] = {}
    workers = max(2, min(int(max_workers or 12), 20))

    if review_total > 0:
        emit(0.20, f"Revisión profunda selectiva… 0/{review_total}", stage="Revisión profunda selectiva", reviewed=0, total=review_total, detected=0, feed_entries=total_feed_entries, cache_hits=cache_hits)
        with ThreadPoolExecutor(max_workers=workers) as ex:
            future_to_meta = {ex.submit(extract_portal_info, link): link for link in review_links}
            done = 0
            for fut in as_completed(future_to_meta):
                link = future_to_meta[fut]
                try:
                    portal_map[link] = fut.result()
                except Exception:
                    portal_map[link] = (None, None, None)
                done += 1
                emit(0.20 + 0.45 * (done / max(1, review_total)),
                     f"Revisión profunda selectiva… {done}/{review_total}",
                     stage="Revisión profunda selectiva", reviewed=done, total=review_total, detected=0, feed_entries=total_feed_entries, cache_hits=cache_hits)
    else:
        emit(0.65, "Sin revisión profunda adicional: usando solo datos ATOM", stage="Revisión profunda selectiva", reviewed=0, total=0, detected=0, feed_entries=total_feed_entries, cache_hits=cache_hits)

    # 3) Filtros finales con progreso real
    reviewed_final = 0
    detected_valid = 0
    for title, summary, link, published_raw, updated_raw, feed_published_dt, feed_updated_dt, deadline_dt, feed_url, atom_importe in candidates:
        reviewed_deep = link in portal_map
        portal_pub, portal_dead, portal_status = portal_map.get(link, (None, None, None))

        if (feed_url == _FEED_1044) and _is_madrid_tender(link):
            published_dt = feed_published_dt
        else:
            published_dt = _to_naive_utc(portal_pub) if portal_pub else feed_published_dt
        updated_dt = feed_updated_dt

        if portal_dead:
            deadline_dt = _to_naive_utc(portal_dead)

        include = True
        is_csp = _is_csp_host(link)
        if apply_airia_filters and is_csp:
            verdict = _csp_status_is_publicada(portal_status) if reviewed_deep else None
            if verdict is False:
                include = False
            elif verdict is None and _csp_failsafe_exclude_by_text(title, summary):
                include = False

        if apply_airia_filters and include and published_dt and min_date and published_dt < min_date:
            include = False
        if apply_airia_filters and include and deadline_dt and deadline_dt < now:
            include = False
        if apply_airia_filters and include and deadline_dt and exclude_deadline_soon_days is not None:
            if deadline_dt < (now + timedelta(days=exclude_deadline_soon_days)):
                include = False

        if include:
            detected_valid += 1
            tenders.append(
                Tender(
                    title=title,
                    summary=summary,
                    published=published_dt.isoformat() if published_dt else (published_raw or ""),
                    updated=updated_dt.isoformat() if updated_dt else (updated_raw or ""),
                    deadline=deadline_dt.isoformat() if deadline_dt else "",
                    link=link,
                    source_feed=feed_url,
                    atom_importe=atom_importe,
                )
            )

        reviewed_final += 1
        if reviewed_final == 1 or reviewed_final == total_candidates or reviewed_final % 10 == 0:
            emit(0.68 + 0.24 * (reviewed_final / total_candidates),
                 f"Aplicando filtros finales… {reviewed_final}/{total_candidates}",
                 stage="Aplicando filtros finales", reviewed=reviewed_final, total=total_candidates, detected=detected_valid, feed_entries=total_feed_entries, cache_hits=cache_hits)

    uniq: Dict[str, Tender] = {}
    for t in tenders:
        if t.link not in uniq:
            uniq[t.link] = t

    emit(0.94, f"Preparando ranking de {len(uniq)} licitaciones válidas…", stage="Preparando ranking", reviewed=len(candidates), total=len(candidates), detected=len(uniq), feed_entries=total_feed_entries, cache_hits=cache_hits)
    return list(uniq.values())


# ---------------- SCORING ----------------
def score_tenders(tenders: List[Tender], company_corpus: List[str], top_k: Optional[int] = None) -> pd.DataFrame:
    if not tenders:
        return pd.DataFrame()

    tender_texts = [(t.title + " " + t.summary).strip() for t in tenders]
    corpus = company_corpus or [""]

    vectorizer = TfidfVectorizer(stop_words=None, max_features=25000, ngram_range=(1, 2))
    X = vectorizer.fit_transform(corpus + tender_texts)

    X_corpus = X[:len(corpus)]
    X_tenders = X[len(corpus):]

    sims = cosine_similarity(X_tenders, X_corpus)
    best_sim = sims.max(axis=1) if sims.size else [0.0] * len(tenders)

    rows = []
    for i, t in enumerate(tenders):
        txt = (t.title + " " + t.summary).strip()
        txt_norm = _normalize(txt)

        blocked_hits = [k for k in KEYWORDS_BLOCK if _normalize(k) in txt_norm]
        boost_hits = [k for k in KEYWORDS_BOOST if _normalize(k) in txt_norm]

        # ✅ NUEVO: super keywords (AT/DF con regex de palabra completa)
        super_hits: List[str] = []
        for k in KEYWORDS_SUPERBOOST:
            kn = _normalize(k)
            if kn in ("at", "df"):
                if re.search(rf"\b{re.escape(kn)}\b", txt_norm):
                    super_hits.append(k.upper())
            else:
                if kn in txt_norm:
                    super_hits.append(k)

        # ✅ NUEVO: detectar CPVs en title/summary (con y sin dígito de control)
        cpvs_found = set(re.findall(r"\b\d{8}-\d\b", txt))
        cpvs_found.update(re.findall(r"\b\d{8}\b", txt))

        priority_cpv_hits: List[str] = []
        for c in sorted(cpvs_found):
            if re.match(r"^\d{8}-\d$", c):
                if c in PRIORITY_CPVS:
                    priority_cpv_hits.append(c)
            elif re.match(r"^\d{8}$", c):
                if any(p.startswith(c) for p in PRIORITY_CPVS):
                    priority_cpv_hits.append(c)

        similitud = float(best_sim[i])
        bloqueada = len(blocked_hits) > 0

        score = similitud * 100.0
        score += 5.0 * len(boost_hits)
        score -= 20.0 * len(blocked_hits)

        # Mantengo un empuje extra para super keywords (no cambia filtros, solo ranking)
        score += 35.0 * len(set(super_hits))

        rows.append({
            "title": t.title,
            "summary": t.summary,
            "link": t.link,
            "updated": t.updated,

            # columnas que app.py usa
            "publicacion": t.published,
            "fecha_limite": t.deadline,

            "similitud": similitud,
            "bloqueada": bloqueada,
            "score": score,

            "boost_keywords": ", ".join(boost_hits) if boost_hits else "",
            "blocked_hits": ", ".join(blocked_hits) if blocked_hits else "",

            # columnas extra (no rompen nada)
            "super_keywords": ", ".join(sorted(set(super_hits))) if super_hits else "",
            "priority_cpvs": ", ".join(priority_cpv_hits) if priority_cpv_hits else "",

            # compatibilidad extra
            "source_feed": t.source_feed,
            "sim": similitud,
            "contract_value_no_vat": t.atom_importe,
        })

    df = pd.DataFrame(rows)

    # ✅ PRIORIDAD ABSOLUTA: si tiene CPV prioritario, va arriba sí o sí,
    # pero cualquier licitación con palabras bloqueadas debe ir SIEMPRE al final.
    df["__has_priority_cpv"] = df["priority_cpvs"].apply(lambda x: bool(x and str(x).strip()))

    # Orden jerárquico:
    # 1) NO bloqueadas primero
    # 2) dentro de las no bloqueadas, CPV prioritario arriba
    # 3) score descendente
    df = df.sort_values(
        by=["bloqueada", "__has_priority_cpv", "score"],
        ascending=[True, False, False]
    ).reset_index(drop=True)

    df = df.drop(columns=["__has_priority_cpv"], errors="ignore")

    if top_k is not None:
        try:
            top_k = int(top_k)
            if top_k > 0:
                df = df.head(top_k).reset_index(drop=True)
        except Exception:
            pass

    return df




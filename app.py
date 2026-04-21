import os
import re
import hashlib
import base64
import io
import json
import html
from datetime import datetime, timezone
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st
from PIL import Image, ImageSequence

# ✅ Usamos el radar optimizado (mismas features, más rápido por paralelización)
from radar_optimized import load_company_corpus, fetch_tenders, score_tenders
from summarizer import (
    generate_ai_summary_excel,
    download_pliegos_from_tender_page,
)

# ==============================
# CONFIG
# ==============================
st.set_page_config(page_title="Radar de Licitaciones", layout="wide")

DATA_EXCEL = os.path.join("data", "TRABAJOS AIRIA.xlsx")
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

ASSETS_DIR = "assets"
LOGO_PATH = os.path.join(ASSETS_DIR, "logo.png")
LOADING_GIF_PATH = os.path.join(ASSETS_DIR, "barra_carga_avion.gif")
FONDO_PATH = os.path.join(ASSETS_DIR, "fondo.png")

# ==============================
# Cache (no cambia funcionalidad; solo evita recomputar en reruns)
# ==============================
@st.cache_data(show_spinner=False)
def cached_company_corpus(excel_path: str):
    return load_company_corpus(excel_path)

MAX_LIMIT_FEED = 3000
MAX_FEED_PAGES = 15

def live_fetch_tenders(company_corpus=None, progress_cb=None, bypass_cache: bool = False):
    return fetch_tenders(
        limit_per_feed=MAX_LIMIT_FEED,
        max_feed_pages=MAX_FEED_PAGES,
        only_last_days=2,
        exclude_deadline_soon_days=2,
        only_priority_cpvs=False,
        progress_cb=progress_cb,
        pre_rank_corpus=company_corpus,
        deep_review_top_n=30,
        bypass_cache=bypass_cache,
    )

# ==============================
# Snapshot remoto (GitHub branch snapshot-data)
# ==============================

REMOTE_SNAPSHOT_URL_ALL = os.getenv("REMOTE_SNAPSHOT_URL_ALL", "").strip()
REMOTE_SNAPSHOT_URL_CPV = os.getenv("REMOTE_SNAPSHOT_URL_CPV", "").strip()
REMOTE_SNAPSHOT_MAX_AGE_MIN = int(os.getenv("REMOTE_SNAPSHOT_MAX_AGE_MIN", "20") or 20)

@st.cache_data(show_spinner=False, ttl=120)
def _load_remote_snapshot(url: str):
    if not url:
        return None
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    return r.json()

def _snapshot_generated_minutes_ago(snapshot: dict):
    ts = (snapshot or {}).get("generated_at_utc")
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 60.0
    except Exception:
        return None

def _snapshot_is_fresh(snapshot: dict, max_age_minutes: int = 20) -> bool:
    age = _snapshot_generated_minutes_ago(snapshot)
    return age is not None and age <= max_age_minutes

def _snapshot_to_df(snapshot: dict) -> pd.DataFrame:
    rows = (snapshot or {}).get("rows") or []
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)

# ==============================
# UI helpers
# ==============================
def _img_to_data_url(path: str) -> str:
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    ext = os.path.splitext(path)[1].lower().replace(".", "") or "png"
    return f"data:image/{ext};base64,{b64}"

@st.cache_resource(show_spinner=False)
def _load_gif_frames_as_data_urls(path: str):
    if not os.path.exists(path):
        return []

    frames = []
    try:
        with Image.open(path) as im:
            for frame in ImageSequence.Iterator(im):
                fr = frame.convert("RGBA")
                bio = io.BytesIO()
                fr.save(bio, format="PNG")
                b64 = base64.b64encode(bio.getvalue()).decode("utf-8")
                frames.append(f"data:image/png;base64,{b64}")
    except Exception:
        return []

    return frames

def _render_plane_progress(container, value: float, text: str = "") -> None:
    value = max(0.0, min(1.0, float(value or 0.0)))
    frames = _load_gif_frames_as_data_urls(LOADING_GIF_PATH)

    if not frames:
        container.progress(value, text=text or "Cargando…")
        return

    idx = min(len(frames) - 1, max(0, round(value * (len(frames) - 1))))
    frame_url = frames[idx]
    safe_text = (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    container.markdown(
        f"""
        <div style="width:100%;">
          <img src="{frame_url}" style="width:100%; display:block; image-rendering:auto;" />
          {f'<div class="muted" style="margin-top:6px;">{safe_text}</div>' if safe_text else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )

class _GifProgressBar:
    def __init__(self, container, value: float = 0.0, text: str = ""):
        self.container = container
        self.progress(value, text=text)

    def progress(self, value: float, text: str = ""):
        _render_plane_progress(self.container, value, text=text)

    def empty(self):
        self.container.empty()


background_css = ""
if os.path.exists(FONDO_PATH):
    fondo_url = _img_to_data_url(FONDO_PATH)
    background_css = f"""
      .stApp {{
        background-image:
          linear-gradient(rgba(255,255,255,0.78), rgba(255,255,255,0.78)),
          url('{fondo_url}');
        background-size: cover;
        background-position: center top;
        background-repeat: no-repeat;
        background-attachment: fixed;
      }}
      [data-testid="stAppViewContainer"] {{
        background: transparent !important;
      }}
      [data-testid="stAppViewContainer"] > .main {{
        background: transparent !important;
      }}
      header[data-testid="stHeader"] {{
        background: transparent !important;
      }}
      header[data-testid="stHeader"]::before {{
        content: "";
        position: absolute;
        inset: 0;
        background: rgba(255,255,255,0.72);
        pointer-events: none;
      }}
    """

style_css = """
<style>
  .block-container {
    max-width: 1320px;
    padding-top: 1rem;
    padding-bottom: 2.2rem;
  }
  .stApp {
    background: linear-gradient(180deg, #f7f9fc 0%, #ffffff 100%);
  }
  section[data-testid="stSidebar"] {
    border-right: 1px solid rgba(17,24,39,0.10);
    background: #f2f5fb;
  }
  section[data-testid="stSidebar"] .block-container {
    padding-top: 1rem;
  }
  .main-hero {
    display:flex;
    align-items:flex-end;
    justify-content:space-between;
    gap:16px;
    margin-bottom:16px;
    padding: 4px 0 14px 0;
    border-bottom:1px solid rgba(17,24,39,0.10);
  }
  .main-title {
    font-size: 28px;
    font-weight: 800;
    margin: 0;
    color: #122033;
    letter-spacing: -0.02em;
  }
  .main-sub {
    margin: 6px 0 0 0;
    color: #5b6b82;
    font-size: 15px;
  }
  .kpi-row {
    display:grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 12px;
    margin: 14px 0 16px 0;
  }
  .kpi {
    border: 1px solid #dbe5f2;
    border-radius: 18px;
    padding: 16px 18px;
    background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
    box-shadow: 0 8px 22px rgba(15, 23, 42, 0.05);
  }
  .kpi-label { font-size: 14px; color: #607089; margin: 0 0 10px 0; }
  .kpi-value { font-size: 30px; font-weight: 800; color: #122033; margin: 0; }
  input[type="range"] { accent-color: #2f6edb; }
  div.stButton > button {
    border-radius: 12px;
    border: 1px solid rgba(17,24,39,0.16);
    padding: 0.60rem 0.95rem;
    font-weight: 700;
  }
  div[data-testid="stAlert"] {
    border-radius: 14px;
    border: 1px solid rgba(17,24,39,0.10);
  }
  .section-title {
    font-size: 17px;
    font-weight: 800;
    color: #122033;
    margin: 18px 0 10px 0;
  }
  .meta { display:flex; flex-wrap:wrap; gap:10px; margin:8px 0 10px 0; }
  .pill {
    font-size: 12px;
    color: #31435d;
    border: 1px solid #d8e3f3;
    padding: 5px 10px;
    border-radius: 999px;
    background: #f7fbff;
    white-space: nowrap;
  }
  .muted { color: rgba(17,24,39,0.66); font-size: 12.5px; }
  .tender-list { display:flex; flex-direction:column; gap:14px; }
  .tender-shell {
    border-radius: 22px;
    padding: 14px;
    background: linear-gradient(135deg, #edf5ff 0%, #f8fbff 52%, #eef4ff 100%);
    border: 1px solid #d7e6fb;
    box-shadow: 0 10px 26px rgba(46, 111, 216, 0.09);
  }
  .tender-box {
    background: rgba(255,255,255,0.55);
    border: 1px solid rgba(255,255,255,0.70);
    border-radius: 18px;
    padding: 16px 18px 14px 18px;
  }
  .tender-title-html {
    font-size: 18px;
    font-weight: 800;
    color: #10233d;
    line-height: 1.35;
    margin: 0 0 10px 0;
  }
  .tender-link-html {
    font-size: 12.5px;
    color: #63748e;
    word-break: break-word;
    margin-bottom: 12px;
  }
  .tender-badges {
    display:flex;
    flex-wrap:wrap;
    gap: 8px;
  }
  .tender-badge {
    display:inline-flex;
    align-items:center;
    font-size: 12px;
    font-weight: 700;
    color: #27415f;
    background: rgba(255,255,255,0.82);
    border: 1px solid #d5e2f5;
    border-radius: 999px;
    padding: 6px 11px;
  }
  .tender-badge.money {
    background: #e9f7ef;
    border-color: #c7ebd2;
    color: #1e6a3c;
  }
  .tender-open-wrap div.stButton > button {
    height: 52px !important;
    margin-top: 10px;
    background: linear-gradient(180deg, #2f6edb 0%, #215fd0 100%) !important;
    color: #ffffff !important;
    border: 1px solid rgba(33,95,208,0.85) !important;
    box-shadow: 0 10px 20px rgba(47,110,219,0.18);
  }
  .tender-open-wrap div.stButton > button:hover {
    background: linear-gradient(180deg, #245fc8 0%, #1f57bd 100%) !important;
  }
  div[role="dialog"] > div {
    max-width: 980px !important;
    width: 980px !important;
    border-radius: 18px !important;
    border: 1px solid #E6EAF0 !important;
    box-shadow: 0 22px 60px rgba(15, 23, 42, 0.22) !important;
    background: #FFFFFF !important;
  }
  .tender-divider { height: 1px; background: #E6EAF0; margin: 16px 0; }
  .modal-title { font-size: 20px; font-weight: 850; color: #0F172A; margin: 0 0 10px 0; line-height: 1.25; }
  .modal-section {
    border: 1px solid #E6EAF0;
    border-radius: 16px;
    padding: 16px 16px 12px 16px;
    background: #F8FAFC;
  }
  .modal-section h4 {
    margin: 0 0 10px 0;
    font-size: 13px;
    font-weight: 800;
    color: #334155;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }
  .close-btn div.stButton > button {
    background: #FFFFFF !important;
    color: #0F172A !important;
    border: 1px solid #E6EAF0 !important;
  }
"""

if background_css:
    style_css += "\n" + background_css

style_css += "\n</style>"

st.markdown(style_css, unsafe_allow_html=True)

# ==============================
# Session state
# ==============================
if "df" not in st.session_state:
    st.session_state.df = None
if "tenders_count" not in st.session_state:
    st.session_state.tenders_count = 0
if "filtered_count" not in st.session_state:
    st.session_state.filtered_count = 0
if "msg_ok" not in st.session_state:
    st.session_state.msg_ok = ""
if "msg_err" not in st.session_state:
    st.session_state.msg_err = ""
if "company_corpus_len" not in st.session_state:
    st.session_state.company_corpus_len = 0

# ==============================
# Sidebar: logo arriba + búsqueda/filtros
# ==============================
with st.sidebar:
    st.markdown('<div class="sidebar-brand">', unsafe_allow_html=True)
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.subheader("Búsqueda")
    bypass_cache = st.checkbox(
        "Buscar sin caché",
        value=False,
        help="Si la marcas, no se usará la precarga de GitHub y se lanzará una búsqueda completa en vivo. Puede tardar hasta unos 20 minutos.",
    )

    run = st.button("🔄 Buscar licitaciones", use_container_width=True)
    search_progress_ph = st.empty()

    if st.session_state.company_corpus_len:
        st.markdown(
            f"<div class='muted' style='margin-top:14px;'>Histórico cargado: <b>{st.session_state.company_corpus_len}</b> registros útiles</div>",
            unsafe_allow_html=True
        )

# ==============================
# Main hero
# ==============================
st.markdown(
    """
    <div class="main-hero">
      <div>
        <p class="main-title">Radar de Licitaciones</p>
        <p class="main-sub">Priorización basada en histórico, descarga de pliegos y resumen IA en Excel.</p>
      </div>
    </div>
    """,
    unsafe_allow_html=True
)

# ==============================
# Carga histórico
# ==============================
if not os.path.exists(DATA_EXCEL):
    st.error(f"No encuentro el Excel en: {DATA_EXCEL}. Ponlo en la carpeta /data")
    st.stop()

with st.spinner("Leyendo vuestro histórico (Excel)…"):
    company_corpus = cached_company_corpus(DATA_EXCEL)
st.session_state.company_corpus_len = len(company_corpus)

# ==============================
# KPI row (siempre)
# ==============================
kpi_hist = st.session_state.company_corpus_len
kpi_found = st.session_state.tenders_count
kpi_filtered = len(st.session_state.df) if isinstance(st.session_state.df, pd.DataFrame) else 0
st.markdown(
    f"""
    <div class="kpi-row">
      <div class="kpi">
        <p class="kpi-label">Histórico</p>
        <p class="kpi-value">{kpi_hist}</p>
      </div>
      <div class="kpi">
        <p class="kpi-label">Licitaciones detectadas</p>
        <p class="kpi-value">{kpi_found}</p>
      </div>
      <div class="kpi">
        <p class="kpi-label">Mostradas</p>
        <p class="kpi-value">{kpi_filtered}</p>
      </div>
    </div>
    """,
    unsafe_allow_html=True
)

# ==============================
# Buscar licitaciones (optimizado)
# ==============================
if run:
    st.session_state.msg_ok = ""
    st.session_state.msg_err = ""

    progress_title_ph = st.empty()
    p = _GifProgressBar(search_progress_ph, 0, text="Preparando búsqueda…")
    progress_meta_ph = st.empty()

    def _render_meta(stage: str = "", reviewed: int = 0, total: int = 0, detected: int = 0, feed_count: int = 0, cache_hits: int = 0):
        total_txt = total if total else "—"
        reviewed_txt = reviewed if total else 0
        progress_meta_ph.markdown(
            f"""
            <div class='muted' style='margin-top:8px; line-height:1.7;'>
              <b>Proceso:</b> {stage or 'Preparando'}<br>
              <b>Licitaciones revisadas:</b> {reviewed_txt}/{total_txt}<br>
              <b>Licitaciones válidas detectadas:</b> {detected}<br>
              <b>Entradas ATOM leídas:</b> {feed_count}<br>
              <b>Consultas resueltas desde caché interna:</b> {cache_hits}
            </div>
            """,
            unsafe_allow_html=True
        )

    try:
        progress_title_ph.markdown("<div class='section-title' style='margin-top:8px;'>Progreso de búsqueda</div>", unsafe_allow_html=True)
        _render_meta()

        used_remote_snapshot = False
        remote_error = None

        if not bypass_cache:
            selected_snapshot_url = REMOTE_SNAPSHOT_URL_ALL
            if selected_snapshot_url:
                try:
                    p.progress(0.20, text="Consultando precarga externa…")
                    snapshot = _load_remote_snapshot(selected_snapshot_url)
                    if snapshot and _snapshot_is_fresh(snapshot, REMOTE_SNAPSHOT_MAX_AGE_MIN):
                        df_remote = _snapshot_to_df(snapshot)
                        if not df_remote.empty:
                            total_detected = int(snapshot.get("detected_count", len(df_remote)) or len(df_remote))
                            generated_age = _snapshot_generated_minutes_ago(snapshot)
                            shown_df = df_remote.copy()
                            st.session_state.tenders_count = total_detected
                            st.session_state.df = shown_df
                            used_remote_snapshot = True
                            p.progress(1.0, text="Resultados cargados desde precarga externa ✅")
                            _render_meta(
                                stage="Precarga externa",
                                reviewed=total_detected,
                                total=total_detected,
                                detected=total_detected,
                                feed_count=int(snapshot.get("feed_entries", 0) or 0),
                                cache_hits=total_detected,
                            )
                            age_txt = f"hace {generated_age:.1f} min" if generated_age is not None else "reciente"
                            st.session_state.msg_ok = f"Ranking cargado desde precarga externa ✅ (mostrando {len(shown_df)} de {total_detected}, generado {age_txt})"
                            st.success(st.session_state.msg_ok)
                except Exception as e:
                    remote_error = str(e)

        if not used_remote_snapshot:
            with st.spinner("Buscando licitaciones…"):
                def _cb(payload):
                    if isinstance(payload, tuple) and len(payload) == 2:
                        frac, msg = payload
                        meta = {}
                    elif isinstance(payload, dict):
                        frac = float(payload.get('progress', 0.0) or 0.0)
                        msg = str(payload.get('message', '') or '')
                        meta = payload
                    else:
                        frac = 0.0
                        msg = str(payload or '')
                        meta = {}

                    p.progress(max(0.0, min(1.0, frac)), text=msg or "Procesando…")
                    _render_meta(
                        stage=meta.get('stage', msg),
                        reviewed=int(meta.get('reviewed', 0) or 0),
                        total=int(meta.get('total', 0) or 0),
                        detected=int(meta.get('detected', 0) or 0),
                        feed_count=int(meta.get('feed_entries', 0) or 0),
                        cache_hits=int(meta.get('cache_hits', 0) or 0),
                    )

                tenders = live_fetch_tenders(company_corpus=company_corpus, progress_cb=_cb, bypass_cache=True)

            st.session_state.tenders_count = len(tenders)

            p.progress(0.96, text="Calculando ranking final…")
            _render_meta(stage="Calculando ranking final", reviewed=len(tenders), total=max(len(tenders), 1), detected=len(tenders), feed_count=0, cache_hits=0)

            with st.spinner("Calculando ranking…"):
                df = score_tenders(tenders, company_corpus, top_k=None)

            st.session_state.df = df
            p.progress(1.0, text="Ranking generado ✅")
            st.session_state.msg_ok = f"Ranking generado ✅ (mostrando {len(df)} de {st.session_state.tenders_count})"
            if remote_error:
                st.info(f"No se pudo usar la precarga externa y se hizo búsqueda normal: {remote_error}")
            st.success(st.session_state.msg_ok)
    except Exception as e:
        st.session_state.df = None
        st.session_state.msg_err = f"Error al buscar licitaciones: {e}"
        try:
            p.empty()
            progress_title_ph.empty()
            progress_meta_ph.empty()
        except Exception:
            pass
        st.error(st.session_state.msg_err)

df = st.session_state.df
if df is None:
    st.info("Usa el botón del panel izquierdo para traer licitaciones y generar el ranking.")
    st.stop()

def _parse_amount_eur(value):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("EUR", "").replace("Euros", "").replace("€", "").strip()
    s = s.replace(".", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

def _extract_domain(url: str) -> str:
    try:
        return (urlparse(str(url)).netloc or "").lower()
    except Exception:
        return ""


def _platform_label(domain: str) -> str:
    d = (domain or "").lower().strip()
    mapping = {
        "contrataciondelestado.es": "Contratación del Estado",
        "www.contrataciondelestado.es": "Contratación del Estado",
        "contrataciondelsectorpublico.gob.es": "Contratación del Estado",
        "www.contrataciondelsectorpublico.gob.es": "Contratación del Estado",
        "www.contratosdelarioja.org": "La Rioja",
        "contratosdelarioja.org": "La Rioja",
        "www.juntadeandalucia.es": "Junta de Andalucía",
        "juntadeandalucia.es": "Junta de Andalucía",
        "www.contractaciopublica.cat": "Cataluña",
        "contractaciopublica.cat": "Cataluña",
        "hacienda.navarra.es": "Navarra",
        "www.hacienda.navarra.es": "Navarra",
        "contratos-publicos.comunidad.madrid": "Comunidad de Madrid",
        "www.contratos-publicos.comunidad.madrid": "Comunidad de Madrid",
        "www.euskadi.eus": "Euskadi",
        "euskadi.eus": "Euskadi",
        "www.contratacion.gal": "Galicia",
        "contratacion.gal": "Galicia",
    }
    if d in mapping:
        return mapping[d]
    clean = d.replace('www.', '')
    if not clean:
        return 'Otra plataforma'
    return clean

def _row_matches_airia_focus(row) -> bool:
    text_parts = [
        str(row.get("priority_cpvs", "") or ""),
        str(row.get("boost_keywords", "") or ""),
        str(row.get("super_keywords", "") or ""),
        str(row.get("title", "") or ""),
        str(row.get("summary", "") or ""),
    ]
    joined = " ".join(text_parts).lower()
    needles = [
        "71000000", "71200000", "71221000", "71222000", "71240000", "71242000",
        "71247000", "71300000", "71317200",
        "redaccion de proyecto", "dirección de obra", "direccion de obra",
        "dirección de ejecución", "direccion de ejecucion",
        "coordinación de seguridad", "coordinacion de seguridad",
        "atdocv", "asistencia tecnica", "css", "df", "at"
    ]
    return any(n in joined for n in needles)

df = df.copy()
df["__amount_num"] = df.get("contract_value_no_vat", pd.Series(index=df.index)).apply(_parse_amount_eur)
df["__domain"] = df.get("link", pd.Series(index=df.index)).apply(_extract_domain)
df["__platform_label"] = df["__domain"].apply(_platform_label)

platform_options = []
seen_labels = set()
for domain in sorted([d for d in df["__domain"].dropna().unique().tolist() if d]):
    label = _platform_label(domain)
    if label not in seen_labels:
        platform_options.append((label, domain))
        seen_labels.add(label)

def _extract_detected_keywords(row):
    values = []
    for col in ["boost_keywords", "super_keywords", "keywords", "keyword_hits"]:
        raw = row.get(col, "")
        if raw is None:
            continue
        if isinstance(raw, (list, tuple, set)):
            parts = list(raw)
        else:
            parts = re.split(r"[,;|]", str(raw))
        for part in parts:
            kw = str(part).strip()
            if kw:
                values.append(kw)
    return values

keyword_options = []
seen_keywords = set()
for _, row in df.iterrows():
    for kw in _extract_detected_keywords(row):
        key_norm = kw.lower()
        if key_norm not in seen_keywords:
            keyword_options.append(kw)
            seen_keywords.add(key_norm)
keyword_options = sorted(keyword_options, key=lambda x: x.lower())

with st.sidebar:
    st.markdown("---")
    st.subheader("Filtros")
    text_query = st.text_input("Buscar por texto", placeholder="proyecto, dirección de obra, aeropuerto...")

    amount_series = df["__amount_num"].dropna()
    st.markdown("**Importe sin IVA (€)**")
    amount_min_raw = ""
    amount_max_raw = ""
    if not amount_series.empty:
        cmin, cmax = st.columns(2)
        with cmin:
            amount_min_raw = st.text_input(
                "MIN",
                value="",
                placeholder="Sin mínimo",
                key="amount_min_filter",
            )
        with cmax:
            amount_max_raw = st.text_input(
                "MAX",
                value="",
                placeholder="Sin máximo",
                key="amount_max_filter",
            )
        st.caption("Déjalo vacío para no filtrar por importe.")
    selected_amount = None

    with st.expander("Plataforma", expanded=False):
        st.caption("Todas vienen seleccionadas por defecto.")
        selected_platform_labels = []
        for label, _domain in platform_options:
            key = f"platform_filter_{hashlib.md5(label.encode('utf-8')).hexdigest()[:10]}"
            if st.checkbox(label, value=True, key=key):
                selected_platform_labels.append(label)

    with st.expander("Palabras clave detectadas", expanded=False):
        st.caption("Todas vienen seleccionadas por defecto.")
        selected_keywords = []
        for kw in keyword_options:
            key = f"keyword_filter_{hashlib.md5(kw.encode('utf-8')).hexdigest()[:10]}"
            if st.checkbox(kw, value=True, key=key):
                selected_keywords.append(kw)

filtered_df = df.copy()
if text_query:
    q = text_query.strip().lower()
    mask = filtered_df.apply(
        lambda r: q in " ".join([
            str(r.get("title", "") or ""),
            str(r.get("summary", "") or ""),
            str(r.get("boost_keywords", "") or ""),
            str(r.get("super_keywords", "") or ""),
            str(r.get("priority_cpvs", "") or ""),
            str(r.get("link", "") or ""),
        ]).lower(),
        axis=1
    )
    filtered_df = filtered_df[mask]
def _parse_sidebar_amount(value: str):
    raw = str(value or "").strip()
    if not raw:
        return None
    cleaned = raw.replace("€", "").replace("EUR", "").replace("eur", "")
    cleaned = cleaned.replace(" ", "")
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except Exception:
        return None

amount_min_value = _parse_sidebar_amount(amount_min_raw)
amount_max_value = _parse_sidebar_amount(amount_max_raw)
if amount_min_value is not None:
    filtered_df = filtered_df[filtered_df["__amount_num"].fillna(-1) >= amount_min_value]
if amount_max_value is not None:
    filtered_df = filtered_df[filtered_df["__amount_num"].fillna(-1) <= amount_max_value]
if selected_platform_labels:
    filtered_df = filtered_df[filtered_df["__platform_label"].isin(selected_platform_labels)]
else:
    filtered_df = filtered_df.iloc[0:0]
if keyword_options:
    if selected_keywords:
        selected_keywords_norm = {k.lower() for k in selected_keywords}
        filtered_df = filtered_df[
            filtered_df.apply(
                lambda r: any(k.lower() in selected_keywords_norm for k in _extract_detected_keywords(r)),
                axis=1,
            )
        ]
    else:
        filtered_df = filtered_df.iloc[0:0]

filtered_df = filtered_df.drop(columns=["__amount_num", "__domain", "__platform_label"], errors="ignore").reset_index(drop=True)
st.session_state.filtered_count = len(filtered_df)

st.markdown("<div class='section-title'>Recomendadas</div>", unsafe_allow_html=True)

# ==============================
# Carpetas y plantillas
# ==============================
template_folder = "templates_resumen"
cache_folder = "pliegos_cache"
out_folder = os.path.join("output", "resumenes_ia")
os.makedirs(cache_folder, exist_ok=True)
os.makedirs(out_folder, exist_ok=True)

def _tender_id_from_row(row) -> str:
    base = str(row.get("link", "")) + "|" + str(row.get("title", ""))
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def _safe_filename(name: str, default: str = "archivo.pdf") -> str:
    if not name:
        name = default
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", name).strip("_")
    if not safe:
        safe = default
    return safe[:140]

def _pill(text: str) -> str:
    return f"<span class='pill'>{text}</span>"


def _html_escape(value) -> str:
    return html.escape(str(value or ""), quote=True)

# Mantener expanders abiertos al pulsar botones (evita 'parpadeo' / duplicados en el primer click)
def _open_expander(open_key: str) -> None:
    """Marca el expander como abierto antes del rerun (evita parpadeos)."""
    st.session_state[open_key] = True


# ==============================
# Lista de licitaciones (feed + modal)
# ==============================
if "active_tender" not in st.session_state:
    st.session_state.active_tender = None  # tender_id activo en el modal

def _open_tender_modal(tid: str) -> None:
    st.session_state.active_tender = tid

def _close_tender_modal() -> None:
    st.session_state.active_tender = None

# Mapa rápido tender_id -> row (dict) para poder abrir modal sin depender del loop
_tender_map = {}
for _i, _row in filtered_df.iterrows():
    _tid = _tender_id_from_row(_row)
    _tender_map[_tid] = _row.to_dict()

# Estilo premium ya definido arriba

dialog_decorator = getattr(st, "dialog", None) or getattr(st, "experimental_dialog", None)
if dialog_decorator is None:
    raise RuntimeError("Tu versión de Streamlit no soporta diálogos (st.dialog). Actualiza Streamlit.")

@dialog_decorator("Detalle de licitación")
def _tender_modal(tender_id: str, row_dict: dict):
    # Keys de estado por licitación (se mantienen como antes)
    status_key = f"status_{tender_id}"
    if status_key not in st.session_state:
        st.session_state[status_key] = ""

    pliegos_key = f"pliegos_{tender_id}"
    if pliegos_key not in st.session_state:
        st.session_state[pliegos_key] = {"pcap_path": None, "ppt_path": None, "info": ""}

    xlsx_key = f"xlsx_{tender_id}"
    if xlsx_key not in st.session_state:
        st.session_state[xlsx_key] = None

    title = row_dict.get("title", "") or ""
    link = row_dict.get("link", "") or ""
    pub = row_dict.get("publicacion", "") or row_dict.get("published", "") or ""
    deadline = row_dict.get("fecha_limite", "") or row_dict.get("deadline", "") or row_dict.get("plazo", "") or ""
    boost_kw = row_dict.get("boost_keywords", "") or ""
    estimated_value = row_dict.get("estimated_value", "") or ""
    contract_value_no_vat = row_dict.get("contract_value_no_vat", "") or ""

    st.markdown(f"<div class='modal-title'>{_html_escape(title)}</div>", unsafe_allow_html=True)

    pills = []
    if pub:
        pills.append(_pill(_html_escape(f"Publicado: {pub}")))
    if deadline:
        pills.append(_pill(_html_escape(f"Plazo: {deadline}")))
    if estimated_value:
        pills.append(_pill(_html_escape(f"Valor estimado: {estimated_value}")))
    if contract_value_no_vat:
        pills.append(_pill(_html_escape(f"Importe sin IVA: {contract_value_no_vat}")))
    if boost_kw:
        pills.append(_pill(_html_escape(f"Keywords: {boost_kw}")))
    if pills:
        st.markdown("<div class='meta'>" + "".join(pills) + "</div>", unsafe_allow_html=True)

    if link:
        st.markdown(f"**Enlace oficial:** {_html_escape(link)}")

    status_box = st.empty()
    if st.session_state.get(status_key):
        status_box.info(st.session_state[status_key])

    st.markdown("<div class='tender-divider'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    # ============ DESCARGAR PLIEGOS ============
    with col1:
        st.markdown("<div class='modal-section'><h4>Pliegos</h4>", unsafe_allow_html=True)
        if st.button("📄 Descargar anuncio o pliego", key=f"dlpl_{tender_id}", use_container_width=True):
            try:
                with status_box.status("🔎 Buscando PCAP/PPT…", expanded=True) as s:
                    s.update(label="Paso 1/2: entrando al expediente…", state="running")
                    pcap_path, ppt_path, info = download_pliegos_from_tender_page(
                        tender_link=link,
                        cache_folder=cache_folder,
                        tender_id=tender_id
                    )
                    s.update(label="Paso 2/2: descarga completada", state="complete")

                st.session_state[pliegos_key] = {"pcap_path": pcap_path, "ppt_path": ppt_path, "info": info}
                st.session_state[status_key] = "✅ Descarga completada. Puedes descargarlos abajo."
                status_box.info(st.session_state[status_key])

            except Exception as e:
                st.session_state[status_key] = f"❌ Error al descargar pliegos: {e}"
                status_box.error(st.session_state[status_key])

        pl = st.session_state[pliegos_key]
        if pl.get("info"):
            st.caption(pl["info"])

        if pl.get("pcap_path") and os.path.exists(pl["pcap_path"]):
            with open(pl["pcap_path"], "rb") as f:
                st.download_button(
                    "⬇️ Descargar Anuncio",
                    data=f,
                    file_name=os.path.basename(pl["pcap_path"]),
                    key=f"dl_pcap_{tender_id}",
                    mime="application/pdf",
                    use_container_width=True,
                )

        if pl.get("ppt_path") and os.path.exists(pl["ppt_path"]):
            with open(pl["ppt_path"], "rb") as f:
                st.download_button(
                    "⬇️ Descargar PPT",
                    data=f,
                    file_name=os.path.basename(pl["ppt_path"]),
                    key=f"dl_ppt_{tender_id}",
                    mime="application/pdf",
                    use_container_width=True,
                )
        st.markdown("</div>", unsafe_allow_html=True)

    # ============ MANUAL (PCAP obligatorio, PPT opcional) ============
    with col2:
        st.markdown("<div class='modal-section'><h4>Resumen IA</h4>", unsafe_allow_html=True)

        with st.form(key=f"form_{tender_id}", clear_on_submit=False):
            uploaded_pcap = st.file_uploader(
                "Sube PCAP (PDF) - obligatorio",
                type=["pdf"],
                key=f"up_pcap_{tender_id}"
            )
            uploaded_ppt = st.file_uploader(
                "Sube PPT (PDF) - opcional",
                type=["pdf"],
                key=f"up_ppt_{tender_id}"
            )
            submitted = st.form_submit_button("🧠 Generar Excel Resumen IA")
        ai_progress_ph = st.empty()

        if submitted:
            if uploaded_pcap is None:
                st.session_state[status_key] = "⚠️ Debes subir el PCAP (obligatorio)."
                status_box.warning(st.session_state[status_key])
            else:
                pcap_name = _safe_filename(uploaded_pcap.name, "PCAP.pdf")
                pcap_path = os.path.join(cache_folder, f"manual_{tender_id}_PCAP_{pcap_name}")
                with open(pcap_path, "wb") as f:
                    f.write(uploaded_pcap.getbuffer())

                ppt_path = None
                if uploaded_ppt is not None:
                    ppt_name = _safe_filename(uploaded_ppt.name, "PPT.pdf")
                    ppt_path = os.path.join(cache_folder, f"manual_{tender_id}_PPT_{ppt_name}")
                    with open(ppt_path, "wb") as f:
                        f.write(uploaded_ppt.getbuffer())

                try:
                    pb = _GifProgressBar(ai_progress_ph, 0, text="Preparando…")

                    def _ai_cb(frac: float, msg: str):
                        try:
                            pb.progress(max(0.0, min(1.0, float(frac))), text=msg)
                        except Exception:
                            pass

                    with status_box.status("🤖 Generando Excel Resumen IA…", expanded=True) as s:
                        s.update(label="Paso 1/3: leyendo pliegos…", state="running")
                        pb.progress(0.05, text="Leyendo pliegos…")
                        s.update(label="Paso 2/3: llamando a IA…", state="running")
                        pb.progress(0.30, text="Llamando a IA…")

                        out_xlsx, info = generate_ai_summary_excel(
                            tender_title=title,
                            tender_link=link,
                            template_folder=template_folder,
                            cache_folder=cache_folder,
                            out_folder=out_folder,
                            manual_pcap_path=pcap_path,
                            manual_ppt_path=ppt_path,
                            progress_cb=_ai_cb
                        )

                        s.update(label="Paso 3/3: Excel generado ✅", state="complete")
                        pb.progress(1.0, text="Excel generado ✅")

                    st.session_state[xlsx_key] = out_xlsx
                    st.session_state[status_key] = "✅ Excel Resumen IA generado. Descárgalo abajo."
                    status_box.info(st.session_state[status_key])
                    st.caption(info)

                except Exception as e:
                    st.session_state[xlsx_key] = None
                    st.session_state[status_key] = f"❌ Error: {e}"
                    try:
                        ai_progress_ph.empty()
                    except Exception:
                        pass
                    status_box.error(st.session_state[status_key])

        xlsx_path = st.session_state.get(xlsx_key)
        if xlsx_path and os.path.exists(xlsx_path):
            with open(xlsx_path, "rb") as f:
                st.download_button(
                    "⬇️ Descargar Excel Resumen IA",
                    data=f,
                    file_name=os.path.basename(xlsx_path),
                    key=f"dl_excel_{tender_id}",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='tender-divider'></div>", unsafe_allow_html=True)
    st.markdown("<div class='close-btn'>", unsafe_allow_html=True)
    if st.button("Cerrar", key=f"close_{tender_id}", use_container_width=True):
        _close_tender_modal()
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

# Feed de licitaciones
st.markdown("<div class='tender-list'>", unsafe_allow_html=True)

if filtered_df.empty:
    st.info("No hay licitaciones que cumplan los filtros actuales.")


for i, row in filtered_df.iterrows():
    tender_id = _tender_id_from_row(row)
    row_dict = _tender_map.get(tender_id, row.to_dict())

    title = (row_dict.get("title", "") or "").strip()
    link = (row_dict.get("link", "") or "").strip()
    pub = (row_dict.get("publicacion", "") or row_dict.get("published", "") or "").strip()
    deadline = (row_dict.get("fecha_limite", "") or row_dict.get("deadline", "") or row_dict.get("plazo", "") or "").strip()
    boost_kw = (row_dict.get("boost_keywords", "") or "").strip()
    estimated_value = (row_dict.get("estimated_value", "") or "").strip()
    contract_value_no_vat = (row_dict.get("contract_value_no_vat", "") or "").strip()

    outer_left, outer_right = st.columns([0.84, 0.16], vertical_alignment="center")
    with outer_left:
        badges = []
        if pub:
            badges.append(f"<span class='tender-badge'>Publicado: {_html_escape(pub)}</span>")
        if deadline:
            badges.append(f"<span class='tender-badge'>Plazo: {_html_escape(deadline)}</span>")
        if estimated_value:
            badges.append(f"<span class='tender-badge money'>Valor estimado: {_html_escape(estimated_value)}</span>")
        if contract_value_no_vat:
            badges.append(f"<span class='tender-badge money'>Importe sin IVA: {_html_escape(contract_value_no_vat)}</span>")
        if boost_kw:
            badges.append(f"<span class='tender-badge'>Keywords: {_html_escape(boost_kw)}</span>")

        safe_title = _html_escape(title)
        safe_link = _html_escape(link)
        html_card = (
            "<div class='tender-shell'><div class='tender-box'>"
            f"<div class='tender-title-html'>{safe_title}</div>"
            + (f"<div class='tender-link-html'>{safe_link}</div>" if safe_link else "")
            + (f"<div class='tender-badges'>{''.join(badges)}</div>" if badges else "")
            + "</div></div>"
        )
        st.markdown(html_card, unsafe_allow_html=True)
    with outer_right:
        st.markdown("<div class='tender-open-wrap'>", unsafe_allow_html=True)
        if st.button("Abrir", key=f"open_btn_{tender_id}", use_container_width=True):
            _open_tender_modal(tender_id)
        st.markdown("</div>", unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)

# Abrir modal si hay uno seleccionado

_active = st.session_state.get("active_tender")
if _active and _active in _tender_map:
    _tender_modal(_active, _tender_map[_active])
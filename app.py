import os
import re
import hashlib
import base64
import streamlit as st

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

# ==============================
# Cache (no cambia funcionalidad; solo evita recomputar en reruns)
# ==============================
@st.cache_data(show_spinner=False)
def cached_company_corpus(excel_path: str):
    return load_company_corpus(excel_path)

@st.cache_data(ttl=300, show_spinner=False)
def cached_fetch_tenders(limit_feed: int, only_cpv_airia: bool):
    # mantenemos mismos parámetros que antes
    return fetch_tenders(
        limit_per_feed=limit_feed,
        only_last_days=1,
        exclude_deadline_soon_days=1,
        only_priority_cpvs=only_cpv_airia,
    )

# ==============================
# UI helpers
# ==============================
def _img_to_data_url(path: str) -> str:
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    ext = os.path.splitext(path)[1].lower().replace(".", "") or "png"
    return f"data:image/{ext};base64,{b64}"

st.markdown(
    """
    <style>
      .block-container {
        max-width: 1320px;
        padding-top: 1rem;
        padding-bottom: 2.2rem;
      }
      header[data-testid="stHeader"] { height: 0.5rem; }
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
    </style>
    """,
    unsafe_allow_html=True
)

# ==============================
# Session state
# ==============================
if "df" not in st.session_state:
    st.session_state.df = None
if "tenders_count" not in st.session_state:
    st.session_state.tenders_count = 0
if "msg_ok" not in st.session_state:
    st.session_state.msg_ok = ""
if "msg_err" not in st.session_state:
    st.session_state.msg_err = ""
if "company_corpus_len" not in st.session_state:
    st.session_state.company_corpus_len = 0

# ==============================
# Sidebar: logo arriba + parámetros
# ==============================
with st.sidebar:
    st.markdown('<div class="sidebar-brand">', unsafe_allow_html=True)
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    st.subheader("Parámetros")
    top_k = st.slider("Top N", 20, 200, 80, 10)
    limit_feed = st.slider("Límite por feed", 50, 500, 200, 50)
    st.caption("Recencia: hoy o ayer. Se excluyen plazos demasiado inminentes.")

    # Opcional: saltarse caché si quieres datos frescos al 100%
    bypass_cache = st.checkbox("Forzar actualización (sin caché)", value=False)
    only_cpv_airia = st.checkbox("Solo CPVs Airia", value=False)

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
        <p class="kpi-label">Top N seleccionado</p>
        <p class="kpi-value">{top_k}</p>
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
    # Barra de progreso (aparece solo durante la ejecución)
    p = search_progress_ph.progress(0, text="Iniciando búsqueda…")
    try:
        p.progress(0.05, text="Descargando licitaciones…")
        with st.spinner("Descargando licitaciones…"):
            if bypass_cache:
                def _cb(frac: float, msg: str):
                    v = 0.05 + 0.65 * max(0.0, min(1.0, float(frac)))
                    p.progress(v, text=msg)
                tenders = fetch_tenders(
                    limit_per_feed=limit_feed,
                    only_last_days=1,
                    exclude_deadline_soon_days=1,
                    only_priority_cpvs=only_cpv_airia,
                    progress_cb=_cb
                )
            else:
                tenders = cached_fetch_tenders(limit_feed, only_cpv_airia)
                p.progress(0.70, text="Licitaciones descargadas")
        st.session_state.tenders_count = len(tenders)

        p.progress(0.78, text="Calculando ranking…")
        with st.spinner("Calculando ranking…"):
            df = score_tenders(tenders, company_corpus, top_k=top_k)

        st.session_state.df = df
        p.progress(1.0, text="Ranking generado ✅")
        st.session_state.msg_ok = f"Ranking generado ✅ (mostrando {len(df)} de {st.session_state.tenders_count})"
        st.success(st.session_state.msg_ok)
    except Exception as e:
        st.session_state.df = None
        st.session_state.msg_err = f"Error al buscar licitaciones: {e}"
        try:
            p.empty()
        except Exception:
            pass
        st.error(st.session_state.msg_err)


df = st.session_state.df
if df is None:
    st.info("Usa el botón del panel izquierdo para traer licitaciones y generar el ranking.")
    st.stop()

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
for _i, _row in df.iterrows():
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

    st.markdown(f"<div class='modal-title'>{title}</div>", unsafe_allow_html=True)

    pills = []
    if pub:
        pills.append(_pill(f"Publicado: {pub}"))
    if deadline:
        pills.append(_pill(f"Plazo: {deadline}"))
    if estimated_value:
        pills.append(_pill(f"Valor estimado: {estimated_value}"))
    if contract_value_no_vat:
        pills.append(_pill(f"Importe sin IVA: {contract_value_no_vat}"))
    if boost_kw:
        pills.append(_pill(f"Keywords: {boost_kw}"))
    if pills:
        st.markdown("<div class='meta'>" + "".join(pills) + "</div>", unsafe_allow_html=True)

    if link:
        st.markdown(f"**Enlace oficial:** {link}")

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
                    "⬇️ Descargar PCAP",
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
                    pb = ai_progress_ph.progress(0, text="Preparando…")

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

for i, row in df.iterrows():
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
            badges.append(f"<span class='tender-badge'>Publicado: {pub}</span>")
        if deadline:
            badges.append(f"<span class='tender-badge'>Plazo: {deadline}</span>")
        if estimated_value:
            badges.append(f"<span class='tender-badge money'>Valor estimado: {estimated_value}</span>")
        if contract_value_no_vat:
            badges.append(f"<span class='tender-badge money'>Importe sin IVA: {contract_value_no_vat}</span>")
        if boost_kw:
            badges.append(f"<span class='tender-badge'>Keywords: {boost_kw}</span>")

        html = (
            "<div class='tender-shell'><div class='tender-box'>"
            f"<div class='tender-title-html'>{title}</div>"
            + (f"<div class='tender-link-html'>{link}</div>" if link else "")
            + (f"<div class='tender-badges'>{''.join(badges)}</div>" if badges else "")
            + "</div></div>"
        )
        st.markdown(html, unsafe_allow_html=True)
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
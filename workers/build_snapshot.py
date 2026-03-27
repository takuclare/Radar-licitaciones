import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from radar_optimized import load_company_corpus, fetch_tenders, score_tenders, _has_priority_cpv_airia  # noqa: E402


DATA_EXCEL = os.getenv("DATA_EXCEL", str(ROOT / "data" / "TRABAJOS AIRIA.xlsx"))
OUTPUT_DIR = Path(os.getenv("SNAPSHOT_OUTPUT_DIR", str(ROOT / "snapshot_output")))
LIMIT_PER_FEED = int(os.getenv("SNAPSHOT_LIMIT_PER_FEED", "3000") or 3000)
MAX_FEED_PAGES = int(os.getenv("SNAPSHOT_MAX_FEED_PAGES", "15") or 15)
ONLY_LAST_DAYS = int(os.getenv("SNAPSHOT_ONLY_LAST_DAYS", "2") or 2)
EXCLUDE_DEADLINE_SOON_DAYS = int(os.getenv("SNAPSHOT_EXCLUDE_DEADLINE_SOON_DAYS", "2") or 2)


def _safe_value(value: Any):
    if value is None:
        return None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _df_to_rows(df):
    rows = []
    for record in df.to_dict(orient="records"):
        rows.append({k: _safe_value(v) for k, v in record.items()})
    return rows


def build_snapshot():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[snapshot] Cargando histórico desde: {DATA_EXCEL}")
    corpus = load_company_corpus(DATA_EXCEL)
    print(f"[snapshot] Histórico útil: {len(corpus)} registros")

    progress_events = []

    def progress_cb(payload):
        if isinstance(payload, dict):
            progress_events.append(payload)
            msg = payload.get("message", "")
            prog = payload.get("progress", 0)
            print(f"[snapshot] {prog:.0%} {msg}")
        else:
            print(f"[snapshot] {payload}")

    tenders = fetch_tenders(
        only_last_days=ONLY_LAST_DAYS,
        exclude_deadline_soon_days=EXCLUDE_DEADLINE_SOON_DAYS,
        limit_per_feed=LIMIT_PER_FEED,
        max_feed_pages=MAX_FEED_PAGES,
        only_priority_cpvs=False,
        progress_cb=progress_cb,
        pre_rank_corpus=corpus,
        deep_review_top_n=30,
    )

    print(f"[snapshot] Licitaciones detectadas: {len(tenders)}")

    df_all = score_tenders(tenders, corpus, top_k=None)
    tenders_cpv = [t for t in tenders if _has_priority_cpv_airia(f"{t.title} {t.summary}")]
    df_cpv = score_tenders(tenders_cpv, corpus, top_k=None)

    common_meta = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "data_excel": os.path.basename(DATA_EXCEL),
        "company_corpus_len": len(corpus),
        "limit_per_feed": LIMIT_PER_FEED,
        "max_feed_pages": MAX_FEED_PAGES,
        "only_last_days": ONLY_LAST_DAYS,
        "exclude_deadline_soon_days": EXCLUDE_DEADLINE_SOON_DAYS,
        "feed_entries": max((evt.get("feed_entries", 0) for evt in progress_events if isinstance(evt, dict)), default=0),
    }

    snapshot_all = {
        **common_meta,
        "mode": "all",
        "detected_count": len(df_all),
        "rows": _df_to_rows(df_all),
    }
    snapshot_cpv = {
        **common_meta,
        "mode": "only_cpv_airia",
        "detected_count": len(df_cpv),
        "rows": _df_to_rows(df_cpv),
    }

    all_path = OUTPUT_DIR / "latest_tenders_snapshot.json"
    cpv_path = OUTPUT_DIR / "latest_tenders_snapshot_cpv.json"

    all_path.write_text(json.dumps(snapshot_all, ensure_ascii=False, indent=2), encoding="utf-8")
    cpv_path.write_text(json.dumps(snapshot_cpv, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[snapshot] Guardado: {all_path}")
    print(f"[snapshot] Guardado: {cpv_path}")


if __name__ == "__main__":
    build_snapshot()

"""
Amazon PPC Analyzer — web version (Vercel Python serverless).

Принимает Sponsored Products Search Term Report (xlsx или csv),
возвращает JSON с summary и embedded decisions.csv.

Логика портирована из skill amazon-ppc-analyzer:
- Детект продуктов и variation groups по ASIN из имён кампаний
- Тиры bleeders (HIGH_CLICKS / GRAY_ZONE / LOW_DATA)
- Variation conflict detection для ASIN-таргетов
- Per-row decision + reason
- CSV в формате Amazon Search Term Report по порядку колонок

POST / с multipart 'file' → JSON ответ
GET /  → HTML (только при локальном запуске; на Vercel index.html отдаёт статика)
"""

from __future__ import annotations

import csv as csv_lib
import io
import json
import os
import re
import warnings
from collections import Counter
from pathlib import Path

import pandas as pd
from flask import Flask, request, jsonify

warnings.filterwarnings("ignore", module="openpyxl")

app = Flask(__name__)

# ── Constants ────────────────────────────────────────────────────────────

ASIN_RE = re.compile(r"(?:^|[^A-Za-z0-9])(B0[A-Z0-9]{8})(?![A-Za-z0-9])", re.IGNORECASE)
ASIN_TARGET_RE = re.compile(r"^B0[A-Z0-9]{8}$", re.IGNORECASE)

NOISE_TOKENS = {
    "auto", "manual", "exact", "phrase", "broad", "kw", "kws", "kwsrel",
    "pt", "pat", "pda", "tpk", "skc", "sku",
    "close", "loose", "substitutes", "subs", "complements", "match",
    "audience", "category", "keywords", "keyword", "select", "new",
    "rest", "top", "ud", "high", "interest", "suggested", "suggestive",
}

CLICKS_THRESHOLD_HIGH = 20
CLICKS_THRESHOLD_GRAY = 10
SAFETY_FACTOR_N = 3
TARGET_ACOS_DEFAULT = 0.30

MAX_FILE_SIZE = 4 * 1024 * 1024  # 4 MB — Vercel Hobby limit ~4.5 MB

COLUMN_ALIASES = {
    "Customer Search Term": [
        "Customer Search Term", "Customer search term",
        "Término de búsqueda de cliente",
    ],
    "Match Type": ["Match Type", "Tipo de coincidencia"],
    "Campaign Name": ["Campaign Name", "Nombre de campaña"],
    "Ad Group Name": ["Ad Group Name", "Nombre del grupo de anuncios"],
    "Targeting": ["Targeting", "Keywords", "Segmentación"],
    "Impressions": ["Impressions", "Impresiones"],
    "Clicks": ["Clicks", "Clics"],
    "Spend": ["Spend", "Total cost (USD)", "Gasto"],
    "CPC": ["Cost Per Click (CPC)", "CPC (USD)", "Coste por clic (CPC)"],
    "CTR": ["Click-Thru Rate (CTR)", "CTR", "Porcentaje de clics (CTR)"],
    "Sales": [
        "7 Day Total Sales", "7 Day Total Sales ", "Sales (USD)",
        "Ventas totales de 7 días (€)", "Ventas totales de 7 días ($)",
    ],
    "Orders": [
        "7 Day Total Orders (#)", "Purchases",
        "Pedidos totales de 7 días (#)",
    ],
    "Date": ["Date", "Start Date", "Fecha de inicio"],
    "Currency": ["Currency", "Divisa"],
}

DECISIONS_CSV_COLS = [
    # Amazon report columns in original order
    "campaign", "ad_group", "targeting", "match", "search_term",
    "impressions", "clicks", "ctr", "cpc", "spend",
    "sales", "acos", "orders", "cvr",
    # Derived columns at end
    "category", "target_type", "product_label", "decision", "reason",
]

PCT_COLS = {"ctr", "cvr", "acos"}


# ── Helpers ──────────────────────────────────────────────────────────────

def _find_col(df: pd.DataFrame, canonical: str) -> str | None:
    for candidate in COLUMN_ALIASES[canonical]:
        if candidate in df.columns:
            return candidate
    return None


def _safe_div(a, b, default=0.0) -> float:
    try:
        if b == 0 or pd.isna(b) or pd.isna(a):
            return default
        return float(a) / float(b)
    except Exception:
        return default


def _format_pct(v) -> str:
    if v == "" or v is None:
        return ""
    try:
        return f"{float(v) * 100:.2f}%"
    except (ValueError, TypeError):
        return ""


def _detect_match_type(targeting: str, match: str) -> str:
    m = (match or "").strip().upper()
    if m in {"EXACT", "PHRASE", "BROAD"}:
        return m
    t = (targeting or "").strip().lower()
    if t in {"close-match", "loose-match", "substitutes", "complements"} or t.startswith("category="):
        return "AUTO"
    return "OTHER"


def _extract_asin(campaign_name: str) -> str | None:
    m = ASIN_RE.search(str(campaign_name))
    return m.group(1).upper() if m else None


def _extract_label_candidate(campaign_name: str) -> str | None:
    name = ASIN_RE.sub("", str(campaign_name))
    parts = re.split(r"\s+-\s+|_", name)
    for part in parts:
        p = part.strip()
        if not p:
            continue
        words = p.split()
        if len(words) < 2 and len(p) < 8:
            continue
        if all(w.lower().strip(".,()") in NOISE_TOKENS for w in words):
            continue
        return p
    return None


def _normalize_label(label: str) -> str:
    if not label:
        return ""
    words = [w for w in re.findall(r"[A-Za-zА-Яа-я0-9]+", label.lower())
             if w not in NOISE_TOKENS]
    return " ".join(words)


def _build_product_map(campaigns: list[str]) -> tuple[dict, dict, list]:
    campaign_to_asin: dict[str, str | None] = {}
    asin_norm_votes: dict[str, Counter] = {}
    asin_display_votes: dict[str, Counter] = {}

    for c in campaigns:
        asin = _extract_asin(c)
        campaign_to_asin[c] = asin
        if asin:
            label = _extract_label_candidate(c)
            if label:
                asin_norm_votes.setdefault(asin, Counter())[_normalize_label(label)] += 1
                asin_display_votes.setdefault(asin, Counter())[label] += 1

    asin_to_label: dict[str, str] = {}
    for asin in {a for a in campaign_to_asin.values() if a}:
        norms = asin_norm_votes.get(asin)
        if not norms:
            asin_to_label[asin] = asin
            continue
        best_norm = norms.most_common(1)[0][0]
        display_candidates = [
            (orig, cnt) for orig, cnt in asin_display_votes.get(asin, Counter()).items()
            if _normalize_label(orig) == best_norm
        ]
        if display_candidates:
            asin_to_label[asin] = max(display_candidates, key=lambda x: x[1])[0]
        else:
            asin_to_label[asin] = best_norm

    unmapped: list[str] = []
    for c, asin in list(campaign_to_asin.items()):
        if asin:
            continue
        c_norm = _normalize_label(c)
        match = None
        for known_asin, known_label in asin_to_label.items():
            known_norm = _normalize_label(known_label)
            if known_norm and known_norm in c_norm:
                match = known_asin
                break
        if match:
            campaign_to_asin[c] = match
        else:
            unmapped.append(c)

    return campaign_to_asin, asin_to_label, unmapped


def _classify_bleeder(clicks: int, spend: float, threshold_spend: float | None) -> str:
    if clicks >= CLICKS_THRESHOLD_HIGH:
        return "HIGH_CLICKS"
    if threshold_spend is not None and spend >= threshold_spend:
        return "HIGH_CLICKS"
    if clicks >= CLICKS_THRESHOLD_GRAY:
        return "GRAY_ZONE"
    return "LOW_DATA"


# ── Decision logic ──────────────────────────────────────────────────────

def _format_subordinate_list(subs: list[dict]) -> str:
    lines = []
    for s in subs:
        tag = " (winner)" if s["is_winner"] else f" ({s['clicks']} clicks, 0 sales)"
        lines.append(f"  - [{s['match']}] {s['campaign']}{tag}")
    return "\n".join(lines) if lines else "  (других source-кампаний нет — единственный)"


def _winner_decision(row: dict, target_acos: float = TARGET_ACOS_DEFAULT) -> tuple[str, str]:
    match = row.get("match", "")
    target_type = row.get("target_type", "")
    acos = float(row.get("acos") or 0)
    clicks = int(row.get("clicks") or 0)
    spend = float(row.get("spend") or 0)
    cvr = float(row.get("cvr") or 0)
    high = target_acos * 1.2

    if acos > high and spend > 10:
        if match == "EXACT":
            return "lower_bid_exact_minus10", f"ACOS {acos*100:.1f}% > {high*100:.0f}%, EXACT step −10%"
        if match == "PHRASE":
            return "lower_bid_phrase_minus5", f"ACOS {acos*100:.1f}% > {high*100:.0f}%, PHRASE step −5%"
        if match == "BROAD":
            return "lower_bid_broad_minus3", f"ACOS {acos*100:.1f}% > {high*100:.0f}%, BROAD step −3%"
        if match == "AUTO":
            return "lower_bid_auto_minus3", f"ACOS {acos*100:.1f}% > {high*100:.0f}%, AUTO step −3%"

    if row.get("skc_role") == "subordinate":
        return "cross_negate_for_skc", (
            f"Term '{row['term']}' имеет SKC migration primary в кампании "
            f"'{row.get('skc_primary_campaign', '')}'. Cross-negate как Negative Exact "
            f"в этой {match}-кампании, чтобы трафик ушёл в новый SKC."
        )

    if row.get("pt_role") == "subordinate":
        return "cross_negate_for_pt", (
            f"ASIN-target '{row['term']}' имеет PT migration primary в кампании "
            f"'{row.get('pt_primary_campaign', '')}'. Cross-negate как Negative ASIN "
            f"в этой кампании, чтобы трафик ушёл в новую PT."
        )

    if match in ("AUTO", "BROAD", "PHRASE") and acos < target_acos and clicks >= 5:
        if target_type == "KEYWORD" and row.get("exact_exists_for_same_product"):
            return "add_negative_exact_in_source", (
                f"KW-winner ACOS {acos*100:.1f}% CVR {cvr*100:.0f}% {clicks} clicks, "
                f"но Exact для этого term уже работает в этом товаре. "
                f"Не создавай новый SKC — добавь в Negative Exact в эту "
                f"{match}-кампанию, чтобы трафик шёл в существующий Exact."
            )
        if target_type == "ASIN" and row.get("pt_role") == "primary":
            subs = row.get("pt_subordinate_sources", [])
            return "create_pt_campaign", (
                f"ASIN-winner ACOS {acos*100:.1f}% CVR {cvr*100:.0f}% {clicks} clicks "
                f"→ создать отдельную PT кампанию. Negative ASIN добавить в эти "
                f"{len(subs)} source-кампании:\n{_format_subordinate_list(subs)}"
            )
        if target_type == "KEYWORD" and row.get("skc_role") == "primary":
            subs = row.get("skc_subordinate_sources", [])
            return "migrate_to_skc_exact", (
                f"KW-winner ACOS {acos*100:.1f}% CVR {cvr*100:.0f}% {clicks} clicks "
                f"→ создать SKC Exact. Negative Exact добавить в эти "
                f"{len(subs)} source-кампании:\n{_format_subordinate_list(subs)}"
            )
        if target_type == "ASIN":
            return "create_pt_campaign", (
                f"ASIN-winner ACOS {acos*100:.1f}% CVR {cvr*100:.0f}% {clicks} clicks "
                f"→ отдельная PT кампания + Negative ASIN в source"
            )
        return "migrate_to_skc_exact", (
            f"KW-winner ACOS {acos*100:.1f}% CVR {cvr*100:.0f}% {clicks} clicks "
            f"→ SKC Exact + Negative Exact во ВСЕ source-кампании"
        )

    if match in ("AUTO", "BROAD", "PHRASE") and acos < target_acos and 0 < clicks < 5:
        return "observe_low_clicks", f"ACOS {acos*100:.1f}% хороший, но clicks={clicks}<5 — ждать данных перед SKC"

    if match == "EXACT" and acos < target_acos:
        return "keep_running", f"EXACT уже на месте, ACOS {acos*100:.1f}% < {target_acos*100:.0f}% — ничего не делать"

    return "keep_running", f"ACOS {acos*100:.1f}% (target {target_acos*100:.0f}%), spend ${spend:.2f} — в норме"


def _bleeder_decision(row: dict) -> tuple[str, str]:
    tier = row.get("tier", "")
    target_type = row.get("target_type", "")
    conflict = bool(row.get("cross_campaign_winner"))
    clicks = int(row.get("clicks") or 0)
    cpc = float(row.get("cpc") or 0)

    # SKC subordinate (KW bleeder term-а у которого есть SKC migration primary)
    if row.get("skc_subordinate"):
        return "cross_negate_for_skc", (
            f"Term '{row['term']}' имеет SKC migration primary в кампании "
            f"'{row.get('skc_primary_campaign', '')}'. "
            f"Эта запись: {clicks} clicks, ${row.get('spend', 0):.2f} spend, 0 sales. "
            f"Cross-negate как Negative Exact в эту {row.get('match', '')}-кампанию "
            f"как часть consolidation в новый SKC."
        )

    # PT subordinate (ASIN bleeder для ASIN-target с PT migration primary)
    if row.get("pt_subordinate"):
        return "cross_negate_for_pt", (
            f"ASIN-target '{row['term']}' имеет PT migration primary в кампании "
            f"'{row.get('pt_primary_campaign', '')}'. "
            f"Эта запись: {clicks} clicks, ${row.get('spend', 0):.2f} spend, 0 sales. "
            f"Cross-negate как Negative ASIN как часть consolidation в новую PT."
        )

    if conflict and target_type == "ASIN":
        return "variation_conflict_review", (
            "ASIN converts в sibling-кампании variation group — реши: "
            "A) Negative тут / B) проверить листинг вариации / C) bid −15-20%"
        )

    if tier == "HIGH_CLICKS":
        if target_type == "ASIN":
            return "negate_asin_now", f"ASIN-target, {clicks} clicks 0 sales → Negative ASIN сразу"
        priority = " (CPC высокий — приоритет)" if cpc > 1.05 else ""
        return "negate_keyword_after_diagnostics", (
            f"{clicks} clicks 0 sales → Negative Exact после диагностики listing/index/relevance{priority}"
        )

    if tier == "GRAY_ZONE":
        if target_type == "ASIN":
            return "compare_competitor_then_decide", (
                f"ASIN {clicks} clicks → открой страницу: ≥4× отзывов или ≥30% дешевле → Negative ASIN"
            )
        return "review_after_diagnostics_decide_yourself", (
            f"KW {clicks} clicks (10-19) → серая зона, реши сам после диагностики"
        )

    if target_type == "ASIN":
        return "compare_competitor_check", f"ASIN {clicks} clicks (<10) → быстро сравни с конкурентом, малая статистика"
    return "wait_for_data", f"{clicks} clicks (<10) — недостаточно для решения, наблюдай"


# ── Loader ───────────────────────────────────────────────────────────────

def _load_report_bytes(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Load xlsx/csv from bytes, auto-detect header row."""
    suffix = Path(filename).suffix.lower()
    buf = io.BytesIO(file_bytes)

    if suffix == ".csv":
        df = pd.read_csv(buf)
    else:
        raw = pd.read_excel(buf, header=None, engine="openpyxl")
        header_row = 0
        for i, row in raw.iterrows():
            row_str = " ".join(str(v) for v in row.values)
            if "Customer Search Term" in row_str or "Término de búsqueda" in row_str:
                header_row = i
                break
        buf.seek(0)
        df = pd.read_excel(buf, header=header_row, engine="openpyxl")

    df.columns = df.columns.str.strip()
    return df


# ── Core analyzer ────────────────────────────────────────────────────────

def analyze_bytes(file_bytes: bytes, filename: str) -> dict:
    df = _load_report_bytes(file_bytes, filename)

    cols = {k: _find_col(df, k) for k in COLUMN_ALIASES}
    missing_required = [k for k in ("Customer Search Term", "Impressions",
                                    "Clicks", "Spend", "Sales", "Orders") if cols[k] is None]
    if missing_required:
        raise ValueError(
            f"Этот файл не похож на Search Term Report (отсутствуют колонки: {missing_required}). "
            f"Скачай отчёт в Seller Central: Advertising → Reports → Sponsored Products → "
            f"Report type 'Search Term'."
        )

    if cols["Campaign Name"] is None:
        df["_synthetic_campaign"] = "(no campaign info in this report)"
        cols["Campaign Name"] = "_synthetic_campaign"

    for k in ("Impressions", "Clicks", "Spend", "Sales", "Orders", "CPC"):
        if cols[k]:
            df[cols[k]] = pd.to_numeric(df[cols[k]], errors="coerce").fillna(0)

    df = df.dropna(subset=[cols["Customer Search Term"], cols["Campaign Name"]])
    df = df[df[cols["Customer Search Term"]].astype(str).str.strip() != ""]

    unique_campaigns = df[cols["Campaign Name"]].astype(str).unique().tolist()
    campaign_to_asin, asin_to_label, unmapped_campaign_names = _build_product_map(unique_campaigns)
    df["_asin"] = df[cols["Campaign Name"]].astype(str).map(campaign_to_asin)
    df["_product_label"] = df["_asin"].map(lambda a: asin_to_label.get(a, "unknown") if a else "unknown")

    df["_match"] = df.apply(
        lambda r: _detect_match_type(
            str(r[cols["Targeting"]]) if cols["Targeting"] else "",
            str(r[cols["Match Type"]]) if cols["Match Type"] else "",
        ), axis=1)

    date_range = ["unknown", "unknown"]
    if cols["Date"]:
        d = pd.to_datetime(df[cols["Date"]], errors="coerce").dropna()
        if not d.empty:
            date_range = [d.min().strftime("%Y-%m-%d"), d.max().strftime("%Y-%m-%d")]

    currency = "USD"
    if cols["Currency"]:
        c = df[cols["Currency"]].dropna().astype(str)
        if not c.empty:
            currency = c.iloc[0]

    # Aggregate per (term, match, asin, campaign, ad_group, targeting)
    groupby_cols = [cols["Customer Search Term"], "_match", "_asin", cols["Campaign Name"]]
    if cols["Ad Group Name"]:
        groupby_cols.append(cols["Ad Group Name"])
    if cols["Targeting"]:
        groupby_cols.append(cols["Targeting"])

    agg = df.groupby(groupby_cols, dropna=False).agg(
        impressions=(cols["Impressions"], "sum"),
        clicks=(cols["Clicks"], "sum"),
        spend=(cols["Spend"], "sum"),
        sales=(cols["Sales"], "sum"),
        orders=(cols["Orders"], "sum"),
    ).reset_index()

    rename_map = {
        cols["Customer Search Term"]: "term",
        "_match": "match",
        "_asin": "asin",
        cols["Campaign Name"]: "campaign",
    }
    if cols["Ad Group Name"]:
        rename_map[cols["Ad Group Name"]] = "ad_group"
    if cols["Targeting"]:
        rename_map[cols["Targeting"]] = "targeting"
    agg = agg.rename(columns=rename_map)
    if "ad_group" not in agg.columns:
        agg["ad_group"] = ""
    if "targeting" not in agg.columns:
        agg["targeting"] = ""
    agg["product_label"] = agg["asin"].map(lambda a: asin_to_label.get(a, "unknown") if a else "unknown")
    agg["ctr"] = agg.apply(lambda r: _safe_div(r.clicks, r.impressions), axis=1)
    agg["cvr"] = agg.apply(lambda r: _safe_div(r.orders, r.clicks), axis=1)
    agg["acos"] = agg.apply(lambda r: _safe_div(r.spend, r.sales), axis=1)
    agg["cpc"] = agg.apply(lambda r: _safe_div(r.spend, r.clicks), axis=1)

    total_spend = float(agg.spend.sum())
    total_sales = float(agg.sales.sum())
    total_orders = int(agg.orders.sum())
    total_clicks = int(agg.clicks.sum())
    total_imps = int(agg.impressions.sum())

    overall = {
        "spend": round(total_spend, 2),
        "sales": round(total_sales, 2),
        "orders": total_orders,
        "clicks": total_clicks,
        "impressions_clicked_terms": total_imps,
        "acos": round(_safe_div(total_spend, total_sales), 4),
        "cvr": round(_safe_div(total_orders, total_clicks), 4),
        "cpc": round(_safe_div(total_spend, total_clicks), 4),
    }

    avg_ctr = _safe_div(total_clicks, total_imps)

    # Winners
    winners_df = agg[agg.orders > 0].sort_values("sales", ascending=False)
    winners = [
        {
            "term": r.term, "match": r.match, "campaign": r.campaign,
            "ad_group": r.ad_group, "targeting": r.targeting,
            "asin": r.asin, "product_label": r.product_label,
            "clicks": int(r.clicks), "impressions": int(r.impressions),
            "spend": round(r.spend, 2), "cpc": round(r.cpc, 4),
            "orders": int(r.orders), "sales": round(r.sales, 2),
            "acos": round(r.acos, 4), "cvr": round(r.cvr, 4),
            "ctr": round(r.ctr, 4),
            "target_type": "ASIN" if ASIN_TARGET_RE.match(str(r.term).strip()) else "KEYWORD",
        }
        for r in winners_df.itertuples()
    ]

    # Per-product CPC and Threshold (no CVR overrides in v1 web)
    product_cpc = {}
    for asin in df["_asin"].dropna().unique():
        sub = df[df["_asin"] == asin]
        product_cpc[asin] = _safe_div(float(sub[cols["Spend"]].sum()), int(sub[cols["Clicks"]].sum()))

    product_threshold = {asin: None for asin in df["_asin"].dropna().unique()}

    # Bleeders
    bleeders_df = agg[(agg.orders == 0) & (agg.spend > 0)].copy()

    def _tier_for_row(r):
        thr = product_threshold.get(r.asin) if r.asin else None
        return _classify_bleeder(int(r.clicks), float(r.spend), thr)

    bleeders_df["tier"] = bleeders_df.apply(_tier_for_row, axis=1)

    def _make_bleeder(r):
        is_asin_target = bool(ASIN_TARGET_RE.match(str(r.term).strip()))
        return {
            "term": r.term, "match": r.match, "campaign": r.campaign,
            "ad_group": r.ad_group, "targeting": r.targeting,
            "asin": r.asin, "product_label": r.product_label,
            "clicks": int(r.clicks), "spend": round(r.spend, 2),
            "cpc": round(r.cpc, 4), "impressions": int(r.impressions),
            "ctr": round(r.ctr, 4),
            "orders": 0,  # by definition for bleeders (orders == 0)
            "tier": r.tier,
            "is_asin_target": is_asin_target,
            "target_type": "ASIN" if is_asin_target else "KEYWORD",
        }

    high_df = bleeders_df[bleeders_df["tier"] == "HIGH_CLICKS"].sort_values(
        ["cpc", "spend"], ascending=[False, False])
    gray_df_b = bleeders_df[bleeders_df["tier"] == "GRAY_ZONE"].sort_values("spend", ascending=False)
    low_df = bleeders_df[bleeders_df["tier"] == "LOW_DATA"].sort_values("spend", ascending=False)

    bleeders = {
        "tier_high_clicks": [_make_bleeder(r) for r in high_df.itertuples()],
        "tier_gray_zone": [_make_bleeder(r) for r in gray_df_b.itertuples()],
        "tier_low_data": [_make_bleeder(r) for r in low_df.itertuples()],
    }

    bleeders_flat_df = bleeders_df.sort_values("spend", ascending=False)
    wasted = float(bleeders_flat_df.spend.sum())

    # Exact-already-exists detection for winners.
    # Если для (term, product_label) уже есть winner с match=EXACT, любой winner
    # того же term в AUTO/BROAD/PHRASE НЕ должен мигрировать в новую SKC —
    # cross-negate в source кампанию вместо дубликата.
    exact_term_set = {
        (w["term"], w["product_label"]) for w in winners if w["match"] == "EXACT"
    }
    for w in winners:
        w["exact_exists_for_same_product"] = (
            (w["term"], w["product_label"]) in exact_term_set
        )

    # Cross-campaign winner detection (variation conflict)
    winner_lookup: dict[tuple, list[dict]] = {}
    for w in winners:
        key = (w["term"], w["product_label"])
        winner_lookup.setdefault(key, []).append(w)

    for tier_name in ("tier_high_clicks", "tier_gray_zone", "tier_low_data"):
        for b in bleeders[tier_name]:
            key = (b["term"], b["product_label"])
            sibling_winners = winner_lookup.get(key, [])
            b["cross_campaign_winner"] = bool(sibling_winners)

    # ── Primary/Subordinate roll-up для SKC и PT migration ──
    # Один search term может встречаться в нескольких source-кампаниях. Чтобы пользователь
    # получил ОДНУ рекомендацию по term-у, выбираем primary row (лучший winner) — все
    # остальные строки этого term становятся subordinate с decision="cross_negate_for_skc"
    # (или _for_pt). Это устраняет противоречивые рекомендации на одном keyword.
    def _row_id(r):
        return (r["term"], r["match"], r["asin"], r["campaign"],
                r["ad_group"], r["targeting"])

    skc_candidates: dict[tuple, list[dict]] = {}
    pt_candidates: dict[tuple, list[dict]] = {}
    for w in winners:
        if (w["match"] in ("AUTO", "BROAD", "PHRASE")
                and w["acos"] < TARGET_ACOS_DEFAULT
                and w["clicks"] >= 5):
            key = (w["term"], w["product_label"])
            if w["target_type"] == "KEYWORD" and not w["exact_exists_for_same_product"]:
                skc_candidates.setdefault(key, []).append(w)
            elif w["target_type"] == "ASIN":
                pt_candidates.setdefault(key, []).append(w)

    skc_primary_id: dict[tuple, tuple] = {}
    pt_primary_id: dict[tuple, tuple] = {}
    for key, rows in skc_candidates.items():
        primary = max(rows, key=lambda r: (r["orders"], r["clicks"]))
        skc_primary_id[key] = _row_id(primary)
    for key, rows in pt_candidates.items():
        primary = max(rows, key=lambda r: (r["orders"], r["clicks"]))
        pt_primary_id[key] = _row_id(primary)

    skc_subordinates: dict[tuple, list[dict]] = {}
    pt_subordinates: dict[tuple, list[dict]] = {}

    def _make_sub_info(row, is_winner):
        return {
            "campaign": row["campaign"],
            "match": row["match"],
            "clicks": row["clicks"],
            "orders": row["orders"] if is_winner else 0,
            "is_winner": is_winner,
        }

    for w in winners:
        key = (w["term"], w["product_label"])
        rid = _row_id(w)
        if key in skc_primary_id:
            if rid == skc_primary_id[key]:
                w["skc_role"] = "primary"
            else:
                w["skc_role"] = "subordinate"
                w["skc_primary_campaign"] = next(
                    (r["campaign"] for r in skc_candidates[key] if _row_id(r) == skc_primary_id[key]),
                    "",
                )
                skc_subordinates.setdefault(skc_primary_id[key], []).append(_make_sub_info(w, is_winner=True))
        else:
            w["skc_role"] = None
        if key in pt_primary_id:
            if rid == pt_primary_id[key]:
                w["pt_role"] = "primary"
            else:
                w["pt_role"] = "subordinate"
                w["pt_primary_campaign"] = next(
                    (r["campaign"] for r in pt_candidates[key] if _row_id(r) == pt_primary_id[key]),
                    "",
                )
                pt_subordinates.setdefault(pt_primary_id[key], []).append(_make_sub_info(w, is_winner=True))
        else:
            w["pt_role"] = None

    for tier_name in ("tier_high_clicks", "tier_gray_zone", "tier_low_data"):
        for b in bleeders[tier_name]:
            key = (b["term"], b["product_label"])
            if b["target_type"] == "KEYWORD" and key in skc_primary_id:
                b["skc_subordinate"] = True
                primary_id = skc_primary_id[key]
                b["skc_primary_campaign"] = next(
                    (r["campaign"] for r in skc_candidates[key] if _row_id(r) == primary_id),
                    "",
                )
                skc_subordinates.setdefault(primary_id, []).append(_make_sub_info(b, is_winner=False))
            else:
                b["skc_subordinate"] = False
            if b["target_type"] == "ASIN" and key in pt_primary_id:
                b["pt_subordinate"] = True
                primary_id = pt_primary_id[key]
                b["pt_primary_campaign"] = next(
                    (r["campaign"] for r in pt_candidates[key] if _row_id(r) == primary_id),
                    "",
                )
                pt_subordinates.setdefault(primary_id, []).append(_make_sub_info(b, is_winner=False))
            else:
                b["pt_subordinate"] = False

    for w in winners:
        rid = _row_id(w)
        if w.get("skc_role") == "primary":
            w["skc_subordinate_sources"] = skc_subordinates.get(rid, [])
        if w.get("pt_role") == "primary":
            w["pt_subordinate_sources"] = pt_subordinates.get(rid, [])

    # Products
    label_to_asins: dict[str, list[str]] = {}
    for asin, label in asin_to_label.items():
        label_to_asins.setdefault(_normalize_label(label), []).append(asin)

    products = []
    for norm_label, asin_list in label_to_asins.items():
        display_label = asin_to_label[asin_list[0]]
        prod_rows = df[df["_asin"].isin(asin_list)]
        prod_spend = float(prod_rows[cols["Spend"]].sum())
        prod_sales = float(prod_rows[cols["Sales"]].sum())
        prod_orders = int(prod_rows[cols["Orders"]].sum())
        prod_clicks = int(prod_rows[cols["Clicks"]].sum())

        prod_bleeders_by_tier = {
            t: [b for b in bleeders[t] if b["asin"] in asin_list]
            for t in ("tier_high_clicks", "tier_gray_zone", "tier_low_data")
        }
        prod_wasted = float(sum(b["spend"] for tier in prod_bleeders_by_tier.values() for b in tier))

        products.append({
            "label": display_label,
            "asins": asin_list,
            "is_variation_group": len(asin_list) > 1,
            "spend": round(prod_spend, 2),
            "sales": round(prod_sales, 2),
            "orders": prod_orders,
            "clicks": prod_clicks,
            "acos": round(_safe_div(prod_spend, prod_sales), 4),
            "cvr": round(_safe_div(prod_orders, prod_clicks), 4),
            "wasted_spend_total": round(prod_wasted, 2),
            "wasted_spend_pct": round(_safe_div(prod_wasted, prod_spend), 4),
            "num_campaigns": int(prod_rows[cols["Campaign Name"]].nunique()),
        })
    products.sort(key=lambda p: p["spend"], reverse=True)

    return {
        "meta": {
            "rows": int(len(df)),
            "date_range": date_range,
            "num_campaigns": int(df[cols["Campaign Name"]].nunique()),
            "num_search_terms": int(agg.term.nunique()),
            "num_products": len(products),
            "num_asins": len(asin_to_label),
            "multi_product": len(products) > 1,
            "currency": currency,
        },
        "products": products,
        "overall": overall,
        "winners": winners,
        "bleeders": bleeders,
        "wasted_spend_total": round(wasted, 2),
        "wasted_spend_pct": round(_safe_div(wasted, total_spend), 4),
    }


# ── CSV generation ───────────────────────────────────────────────────────

def generate_decisions_csv(data: dict) -> str:
    src_for_col = {col: ("term" if col == "search_term" else col) for col in DECISIONS_CSV_COLS}

    def make_row(record, category, decision, reason):
        out = {}
        for col in DECISIONS_CSV_COLS:
            val = record.get(src_for_col[col], "")
            if col in PCT_COLS:
                val = _format_pct(val)
            out[col] = val
        out["category"] = category
        out["decision"] = decision
        out["reason"] = reason
        return out

    all_bleeders = (
        data["bleeders"]["tier_high_clicks"]
        + data["bleeders"]["tier_gray_zone"]
        + data["bleeders"]["tier_low_data"]
    )

    buf = io.StringIO()
    writer = csv_lib.DictWriter(buf, fieldnames=DECISIONS_CSV_COLS)
    writer.writeheader()
    for w_row in data["winners"]:
        d, r = _winner_decision(w_row)
        writer.writerow(make_row(w_row, "winner", d, r))
    for b_row in all_bleeders:
        d, r = _bleeder_decision(b_row)
        writer.writerow(make_row(b_row, "bleeder", d, r))

    return buf.getvalue()


# ── Summary builder ──────────────────────────────────────────────────────

def build_summary(data: dict) -> dict:
    """User-facing summary embedded in JSON response."""
    all_bleeders = (
        data["bleeders"]["tier_high_clicks"]
        + data["bleeders"]["tier_gray_zone"]
        + data["bleeders"]["tier_low_data"]
    )

    # Decision distribution
    dec_counter: Counter = Counter()
    for w in data["winners"]:
        d, _ = _winner_decision(w)
        dec_counter[d] += 1
    for b in all_bleeders:
        d, _ = _bleeder_decision(b)
        dec_counter[d] += 1

    return {
        "meta": data["meta"],
        "overall": data["overall"],
        "products": data["products"],
        "tier_counts": {
            "high_clicks": len(data["bleeders"]["tier_high_clicks"]),
            "gray_zone": len(data["bleeders"]["tier_gray_zone"]),
            "low_data": len(data["bleeders"]["tier_low_data"]),
        },
        "wasted_spend_total": data["wasted_spend_total"],
        "wasted_spend_pct": data["wasted_spend_pct"],
        "winners_count": len(data["winners"]),
        "bleeders_count": len(all_bleeders),
        "decision_counts": dict(dec_counter.most_common()),
        "top_winners": [
            {k: w[k] for k in ("term", "match", "campaign", "asin", "product_label",
                               "clicks", "spend", "orders", "sales", "acos", "cvr", "target_type")}
            for w in data["winners"][:10]
        ],
        "top_bleeders_high_clicks": [
            {k: b[k] for k in ("term", "match", "campaign", "asin", "product_label",
                               "clicks", "spend", "cpc", "orders",
                               "target_type", "cross_campaign_winner")}
            for b in data["bleeders"]["tier_high_clicks"][:15]
        ],
    }


# ── HTTP routes ──────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
@app.route("/api/analyze", methods=["GET"])
def index():
    """Serve index.html on local dev. On Vercel this is handled by static asset."""
    html_path = os.path.join(os.path.dirname(__file__), "..", "index.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read(), 200, {"Content-Type": "text/html; charset=utf-8"}
    return jsonify({"status": "ok", "service": "amazon-ppc-analyzer"})


@app.route("/", methods=["POST"])
@app.route("/api/analyze", methods=["POST"])
def analyze_endpoint():
    if "file" not in request.files:
        return jsonify({"error": "Файл не приложен. Используй поле 'file' в multipart/form-data."}), 400

    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Имя файла пустое."}), 400

    file_bytes = f.read()
    if not file_bytes:
        return jsonify({"error": "Файл пустой."}), 400

    if len(file_bytes) > MAX_FILE_SIZE:
        return jsonify({
            "error": (
                f"Файл слишком большой ({len(file_bytes) // 1024} KB > "
                f"{MAX_FILE_SIZE // 1024} KB лимит). Сократи период отчёта."
            )
        }), 413

    try:
        data = analyze_bytes(file_bytes, f.filename)
        csv_content = generate_decisions_csv(data)
        summary = build_summary(data)

        stem = Path(f.filename).stem
        out_filename = f"{stem}__decisions.csv"

        return jsonify({
            "summary": summary,
            "csv_content": csv_content,
            "filename": out_filename,
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        # Распространённые user-errors отдаём как 400
        if "BadZipFile" in msg or "not a zip" in msg.lower():
            return jsonify({"error": "Файл повреждён или это не xlsx. Проверь что скачал именно Excel-файл из Amazon Ads."}), 400
        if "ParserError" in msg or "Error tokenizing" in msg:
            return jsonify({"error": "Не удалось распарсить файл. Проверь что это действительно Search Term Report (xlsx или csv)."}), 400
        return jsonify({"error": f"Внутренняя ошибка анализа: {msg}"}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000, host="0.0.0.0")

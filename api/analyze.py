from flask import Flask, request, Response
import io
import json
import re
from typing import Optional
import pandas as pd
import numpy as np
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

app = Flask(__name__)

# ── Helpers ──────────────────────────────────────────────────────────────────

def safe_div(a, b, default=0):
    if b == 0 or (isinstance(b, float) and np.isnan(b)):
        return default
    return a / b


def extract_campaign_type(name):
    u = name.upper()
    if "AUTO|" in u or "AUTO " in u or u.startswith("AUTO"):
        return "Auto"
    if "PAT|" in u:
        return "Product Targeting"
    if "EXACT|" in u:
        return "Exact Match"
    if "BROAD|" in u:
        return "Broad Match"
    if "PHRASE|" in u:
        return "Phrase Match"
    if "NICHE" in u:
        return "Niche/AMZ Audience"
    return "Other"


def analyze(file_bytes):
    df = pd.read_excel(io.BytesIO(file_bytes))
    df.columns = df.columns.str.strip()

    required = {"Campaign Name", "Date", "Impressions", "Clicks", "Spend",
                "7 Day Total Sales", "7 Day Total Orders (#)"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError("Отчёт не содержит нужных колонок: {}".format(missing))

    df["Campaign Type"] = df["Campaign Name"].apply(extract_campaign_type)

    total_spend       = df["Spend"].sum()
    total_sales       = df["7 Day Total Sales"].sum()
    total_orders      = df["7 Day Total Orders (#)"].sum()
    total_units       = df["7 Day Total Units (#)"].sum() if "7 Day Total Units (#)" in df.columns else 0
    total_impressions = df["Impressions"].sum()
    total_clicks      = df["Clicks"].sum()

    overall = {
        "date_start":    str(df["Date"].min().date()),
        "date_end":      str(df["Date"].max().date()),
        "days":          (df["Date"].max() - df["Date"].min()).days + 1,
        "spend":         round(total_spend, 2),
        "sales":         round(total_sales, 2),
        "orders":        int(total_orders),
        "units":         int(total_units),
        "impressions":   int(total_impressions),
        "clicks":        int(total_clicks),
        "acos":          round(safe_div(total_spend, total_sales) * 100, 2),
        "roas":          round(safe_div(total_sales, total_spend), 2),
        "ctr":           round(safe_div(total_clicks, total_impressions) * 100, 2),
        "cvr":           round(safe_div(total_orders, total_clicks) * 100, 2),
        "cpc":           round(safe_div(total_spend, total_clicks), 2),
        "aov":           round(safe_div(total_sales, total_orders), 2),
    }

    # Products
    asin_cols = ["Impressions", "Clicks", "Spend", "7 Day Total Sales", "7 Day Total Orders (#)"]
    for col in ["7 Day Advertised SKU Sales", "7 Day Other SKU Sales"]:
        if col in df.columns:
            asin_cols.append(col)

    group_keys = ["Advertised ASIN", "Advertised SKU"] if "Advertised ASIN" in df.columns else ["Campaign Name"]
    asin_perf = df.groupby(group_keys)[asin_cols].sum().reset_index()

    products = []
    for _, r in asin_perf.iterrows():
        if r["Spend"] > 0:
            halo = round(float(r.get("7 Day Other SKU Sales", 0)), 2)
            products.append({
                "asin":        r.get("Advertised ASIN", ""),
                "sku":         r.get("Advertised SKU", r.get("Campaign Name", "")),
                "spend":       round(r["Spend"], 2),
                "sales":       round(r["7 Day Total Sales"], 2),
                "orders":      int(r["7 Day Total Orders (#)"]),
                "impressions": int(r["Impressions"]),
                "clicks":      int(r["Clicks"]),
                "acos":        round(safe_div(r["Spend"], r["7 Day Total Sales"]) * 100, 2),
                "roas":        round(safe_div(r["7 Day Total Sales"], r["Spend"]), 2),
                "ctr":         round(safe_div(r["Clicks"], r["Impressions"]) * 100, 2),
                "cvr":         round(safe_div(r["7 Day Total Orders (#)"], r["Clicks"]) * 100, 2),
                "cpc":         round(safe_div(r["Spend"], r["Clicks"]), 2),
                "halo_sales":  halo,
            })

    # Campaign types
    type_perf = df.groupby("Campaign Type").agg({
        "Impressions": "sum", "Clicks": "sum", "Spend": "sum",
        "7 Day Total Sales": "sum", "7 Day Total Orders (#)": "sum"
    }).reset_index()

    campaign_types = []
    for _, r in type_perf.iterrows():
        campaign_types.append({
            "type":   r["Campaign Type"],
            "spend":  round(r["Spend"], 2),
            "sales":  round(r["7 Day Total Sales"], 2),
            "orders": int(r["7 Day Total Orders (#)"]),
            "acos":   round(safe_div(r["Spend"], r["7 Day Total Sales"]) * 100, 2),
            "cvr":    round(safe_div(r["7 Day Total Orders (#)"], r["Clicks"]) * 100, 2),
            "cpc":    round(safe_div(r["Spend"], r["Clicks"]), 2),
        })

    # Campaign-level
    camp_perf = df.groupby("Campaign Name").agg({
        "Impressions": "sum", "Clicks": "sum", "Spend": "sum",
        "7 Day Total Sales": "sum", "7 Day Total Orders (#)": "sum"
    }).reset_index()

    # Top performers
    profitable = camp_perf[(camp_perf["7 Day Total Sales"] > 0) & (camp_perf["Spend"] > 5)].copy()
    profitable["ACOS"] = profitable["Spend"] / profitable["7 Day Total Sales"] * 100
    profitable["CVR"]  = profitable["7 Day Total Orders (#)"] / profitable["Clicks"].replace(0, np.nan) * 100
    profitable = profitable.sort_values("ACOS").head(15)

    top_performers = []
    for _, r in profitable.iterrows():
        cvr_val = r["CVR"] if not np.isnan(r["CVR"]) else 0
        top_performers.append({
            "campaign": r["Campaign Name"],
            "spend":    round(r["Spend"], 2),
            "sales":    round(r["7 Day Total Sales"], 2),
            "orders":   int(r["7 Day Total Orders (#)"]),
            "acos":     round(r["ACOS"], 2),
            "cvr":      round(cvr_val, 2),
        })

    # Wasted spend
    wasters = camp_perf[(camp_perf["Spend"] > 5) & (camp_perf["7 Day Total Sales"] == 0)].sort_values("Spend", ascending=False)
    wasted_spend = []
    for _, r in wasters.iterrows():
        wasted_spend.append({
            "campaign": r["Campaign Name"],
            "spend":    round(r["Spend"], 2),
            "clicks":   int(r["Clicks"]),
        })

    # Daily trends
    daily = df.groupby("Date").agg({
        "Impressions": "sum", "Clicks": "sum", "Spend": "sum",
        "7 Day Total Sales": "sum", "7 Day Total Orders (#)": "sum"
    }).reset_index()

    daily_trends = []
    for _, r in daily.iterrows():
        daily_trends.append({
            "date":   str(r["Date"].date()),
            "spend":  round(r["Spend"], 2),
            "sales":  round(r["7 Day Total Sales"], 2),
            "orders": int(r["7 Day Total Orders (#)"]),
            "acos":   round(safe_div(r["Spend"], r["7 Day Total Sales"]) * 100, 2),
        })

    return {
        "overall":        overall,
        "products":       products,
        "campaign_types": campaign_types,
        "top_performers": top_performers,
        "wasted_spend":   wasted_spend,
        "daily_trends":   daily_trends,
    }


# ── Excel generation ──────────────────────────────────────────────────────────

GREEN       = PatternFill("solid", fgColor="C6EFCE")
YELLOW      = PatternFill("solid", fgColor="FFEB9C")
RED         = PatternFill("solid", fgColor="FFC7CE")
HEADER_FILL = PatternFill("solid", fgColor="1F2937")
HEADER_FONT = Font(bold=True, color="FFFFFF")
BOLD        = Font(bold=True)
thin        = Side(style="thin", color="D1D5DB")
BORDER      = Border(left=thin, right=thin, top=thin, bottom=thin)


def acos_fill(v):
    if v == 0 or v > 999:
        return None
    if v < 25:
        return GREEN
    if v < 40:
        return YELLOW
    return RED


def cvr_fill(v):
    if v >= 10:
        return GREEN
    if v >= 5:
        return YELLOW
    return RED


def style_header(ws, row_num, cols):
    for col, val in enumerate(cols, 1):
        c = ws.cell(row=row_num, column=col, value=val)
        c.fill = HEADER_FILL
        c.font = HEADER_FONT
        c.alignment = Alignment(horizontal="center")
        c.border = BORDER


def autofit(ws):
    for col in ws.columns:
        max_len = max((len(str(cell.value or "")) for cell in col), default=10)
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 4, 55)


def build_excel(data):
    wb = openpyxl.Workbook()
    o = data["overall"]

    # Sheet 1: Summary
    ws = wb.active
    ws.title = "Summary"
    ws.append(["Amazon PPC Analysis"])
    ws["A1"].font = Font(bold=True, size=14)
    ws.append(["Period: {} — {}  ({} days)".format(o["date_start"], o["date_end"], o["days"])])
    ws.append([])
    style_header(ws, 4, ["Metric", "Value", "Benchmark"])
    rows = [
        ("Total Spend",     "${:,.2f}".format(o["spend"]),  ""),
        ("Total Sales",     "${:,.2f}".format(o["sales"]),  ""),
        ("Orders",          o["orders"],                    ""),
        ("Units",           o["units"],                     ""),
        ("Impressions",     "{:,}".format(o["impressions"]),""),
        ("Clicks",          "{:,}".format(o["clicks"]),     ""),
        ("ACOS",            "{:.1f}%".format(o["acos"]),    "Good <25%  |  Warn 25-40%  |  Bad >40%"),
        ("ROAS",            "{:.2f}x".format(o["roas"]),    ""),
        ("CTR",             "{:.2f}%".format(o["ctr"]),     "Good >1%  |  Warn 0.5-1%"),
        ("CVR",             "{:.1f}%".format(o["cvr"]),     "Good >10%  |  Warn 5-10%"),
        ("Avg CPC",         "${:.2f}".format(o["cpc"]),     ""),
        ("Avg Order Value", "${:.2f}".format(o["aov"]),     ""),
    ]
    for i, (metric, val, bench) in enumerate(rows, 5):
        ws.append([metric, val, bench])
        for col in range(1, 4):
            ws.cell(row=i, column=col).border = BORDER
        if metric == "ACOS":
            f = acos_fill(o["acos"])
            if f:
                ws.cell(row=i, column=2).fill = f
        if metric == "CVR":
            f = cvr_fill(o["cvr"])
            if f:
                ws.cell(row=i, column=2).fill = f
    ws.column_dimensions["A"].width = 22
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 35

    # Sheet 2: Products
    ws2 = wb.create_sheet("Products")
    h2 = ["ASIN", "SKU", "Spend", "Sales", "Orders", "Impressions",
          "Clicks", "ACOS %", "ROAS", "CTR %", "CVR %", "CPC", "Halo Sales"]
    style_header(ws2, 1, h2)
    for i, p in enumerate(sorted(data["products"], key=lambda x: x["spend"], reverse=True), 2):
        ws2.append([p["asin"], p["sku"], p["spend"], p["sales"], p["orders"],
                    p["impressions"], p["clicks"], p["acos"], p["roas"],
                    p["ctr"], p["cvr"], p["cpc"], p["halo_sales"]])
        for col in range(1, len(h2) + 1):
            ws2.cell(row=i, column=col).border = BORDER
        f = acos_fill(p["acos"])
        if f:
            ws2.cell(row=i, column=8).fill = f
        f2 = cvr_fill(p["cvr"])
        if f2:
            ws2.cell(row=i, column=11).fill = f2
    autofit(ws2)

    # Sheet 3: Campaign Types
    ws3 = wb.create_sheet("Campaign Types")
    h3 = ["Campaign Type", "Spend", "Sales", "Orders", "ACOS %", "CVR %", "CPC", "Assessment"]
    style_header(ws3, 1, h3)
    for i, ct in enumerate(sorted(data["campaign_types"], key=lambda x: x["acos"] if x["acos"] < 999 else 999), 2):
        if ct["acos"] < 25:
            assessment = "Strong - Scale"
        elif ct["acos"] < 40:
            assessment = "Monitor closely"
        elif ct["acos"] < 60:
            assessment = "Needs optimization"
        else:
            assessment = "Review / Pause"
        ws3.append([ct["type"], ct["spend"], ct["sales"], ct["orders"],
                    ct["acos"], ct["cvr"], ct["cpc"], assessment])
        for col in range(1, len(h3) + 1):
            ws3.cell(row=i, column=col).border = BORDER
        f = acos_fill(ct["acos"])
        if f:
            ws3.cell(row=i, column=5).fill = f
    autofit(ws3)

    # Sheet 4: Top Performers
    ws4 = wb.create_sheet("Top Performers")
    h4 = ["Campaign", "Spend", "Sales", "Orders", "ACOS %", "CVR %", "Action"]
    style_header(ws4, 1, h4)
    for i, p in enumerate(data["top_performers"], 2):
        action = "Scale 3x" if p["acos"] < 15 else "Scale 2x" if p["acos"] < 25 else "Scale 1.5x"
        ws4.append([p["campaign"], p["spend"], p["sales"], p["orders"], p["acos"], p["cvr"], action])
        for col in range(1, len(h4) + 1):
            ws4.cell(row=i, column=col).border = BORDER
        f = acos_fill(p["acos"])
        if f:
            ws4.cell(row=i, column=5).fill = f
        ws4.cell(row=i, column=7).font = Font(bold=True, color="2E5C55")
    autofit(ws4)

    # Sheet 5: Wasted Spend
    ws5 = wb.create_sheet("Wasted Spend")
    total_wasted = sum(w["spend"] for w in data["wasted_spend"])
    ws5.append(["Campaigns with spend > $5 and ZERO sales — pause immediately"])
    ws5["A1"].font = Font(bold=True, color="CF4043")
    ws5.append(["Total wasted: ${:,.2f}".format(total_wasted)])
    ws5.append([])
    h5 = ["Campaign", "Spend", "Clicks", "Action"]
    style_header(ws5, 4, h5)
    for i, w in enumerate(data["wasted_spend"], 5):
        ws5.append([w["campaign"], w["spend"], w["clicks"], "PAUSE"])
        for col in range(1, 5):
            ws5.cell(row=i, column=col).border = BORDER
        ws5.cell(row=i, column=2).fill = RED
        ws5.cell(row=i, column=4).font = Font(bold=True, color="CF4043")
    autofit(ws5)
    ws5.column_dimensions["A"].width = 60

    # Sheet 6: Daily Trends
    ws6 = wb.create_sheet("Daily Trends")
    h6 = ["Date", "Spend", "Sales", "Orders", "ACOS %"]
    style_header(ws6, 1, h6)
    for i, d in enumerate(data["daily_trends"], 2):
        ws6.append([d["date"], d["spend"], d["sales"], d["orders"], d["acos"]])
        for col in range(1, 6):
            ws6.cell(row=i, column=col).border = BORDER
        f = acos_fill(d["acos"])
        if f:
            ws6.cell(row=i, column=5).fill = f
    autofit(ws6)

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


# ── Flask routes ──────────────────────────────────────────────────────────────

CORS_HEADERS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


@app.route("/", methods=["GET", "POST", "OPTIONS"])
@app.route("/api/analyze", methods=["GET", "POST", "OPTIONS"])
def analyze_endpoint():
    if request.method == "OPTIONS":
        return Response("", status=200, headers=CORS_HEADERS)

    if request.method == "GET":
        return Response(
            json.dumps({"status": "ok", "message": "PPC Analyzer is running. Send POST with file field."}),
            status=200,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"},
        )

    # POST
    try:
        if "file" not in request.files:
            return _error(400, "Поле 'file' не найдено в запросе.")

        file = request.files["file"]
        file_bytes = file.read()
        if not file_bytes:
            return _error(400, "Файл пустой.")

        data = analyze(file_bytes)
        xlsx = build_excel(data)

        headers = dict(CORS_HEADERS)
        headers["Content-Disposition"] = 'attachment; filename="ppc-analysis.xlsx"'
        return Response(
            xlsx,
            status=200,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except ValueError as e:
        return _error(400, str(e))
    except Exception as e:
        return _error(500, "Внутренняя ошибка: {}".format(str(e)))


def _error(code, message):
    body = json.dumps({"error": message})
    headers = dict(CORS_HEADERS)
    headers["Content-Type"] = "application/json"
    return Response(body, status=code, headers=headers)

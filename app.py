# Candidate Connect DEV — Final Hybrid Cloud App v21zp VOTER_LOOKUP_PERSIST_HISTORY_PDF_FIX
# Full safe filters + guarded export.
# v21p: keeps v21o phone fix and makes saved universes survive app reload/reboot via URL persistence.

import io
import json
import base64
import re
import hashlib
from datetime import datetime
from pathlib import Path

import pandas as pd
import duckdb
import requests
import streamlit as st
try:
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import inch
except Exception:
    letter = canvas = inch = None

R2 = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"
DETAIL_SHARDS = 36
EXPORT_ROW_LIMIT = 250_000

st.set_page_config(page_title="Candidate Connect DEV", layout="wide")
try:
    st.set_option("runner.magicEnabled", False)
except Exception:
    pass

GEO_FIELDS = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]
VOTER_FIELDS = ["Party", "Gender", "Age_Range", "V4A", "V4G", "V4P", "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone", "Tags"]
ALL_FILTER_FIELDS = GEO_FIELDS + VOTER_FIELDS

DISPLAY_LABELS = {
    "USC": "Congressional District",
    "STS": "State Senate District",
    "STH": "State House District",
    "Age_Range": "Age Range",
    "MB_App": "Mail Ballot Application",
    "MB_App_Status": "Application Status",
    "MB_Sent": "Ballot Sent",
    "MB_Status": "Ballot Status",
    "MB_PERM": "Permanent Mail Ballot",
    "MailBallotNewRegistrant": "Newly Registered / Current Only",
    "CalculatedParty": "Calculated Party",
    "HH-Party": "Household Party",
    "V4A": "Vote History - All Elections",
    "V4G": "Vote History - General Elections",
    "V4P": "Vote History - Primary Elections",

    "HasMobile": "Mobile Phone",
    "HasLandline": "Landline",
    "HasEmail": "Email",
    "HasApplicantPhone": "Applicant Phone",
    "Tags": "Tags",
}

LOGO_CANDIDATE_CONNECT = "candidate_connect_logo.png"
LOGO_TPTC = "TSS_Logo_Transparent.png"

def file_exists(path: str) -> bool:
    try:
        return Path(path).exists()
    except Exception:
        return False

DEFAULT_EXPORT_COLUMNS = [
    # Keep voter_id in every CSV/Excel output so street/walk/contact results can be matched later.
    "voter_id",
    "County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region",
    "FirstName", "MiddleName", "LastName", "NameSuffix", "FullName",
    "Party", "CalculatedParty", "Gender", "DOB", "Age", "Age_Range", "RegistrationDate",
    "House Number", "House Number Suffix", "Street Name", "Apartment Number", "Address Line 2", "City", "State", "Zip",
    "res_address", "res_city", "res_state", "res_zip",
    "Email", "Mobile", "Landline", "Current_ApplicantPhone",
    "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "MB_Prob_Score",
    "Current_App_Return_Date", "Current_Ballot_Sent_Date", "Current_Ballot_Returned_Date",
    "Tags",
]

st.markdown(
    """
<style>
html, body, [data-testid="stAppViewContainer"], .stApp {
    background: #000000 !important;
    color: #f8fafc !important;
}
[data-testid="stSidebar"] {
    background: #05080d !important;
    border-right: 1px solid rgba(201,31,39,.45);
}
.block-container { padding-top: 1.1rem; max-width: 1550px; }
.cc-header {
    border: 1px solid rgba(201,31,39,.85);
    border-radius: 18px;
    padding: 18px 22px;
    background: radial-gradient(circle at 80% 0%, rgba(201,31,39,.23), transparent 35%),
                linear-gradient(90deg, #03070c, #0b111a 55%, #190407);
    box-shadow: 0 14px 35px rgba(0,0,0,.45);
    margin-bottom: 16px;
}
.cc-title { font-size: 30px; font-weight: 950; color: #fff; }
.cc-sub { color: #cbd5e1; margin-top: 4px; font-size: 13px; }
.cc-card {
    border: 1px solid rgba(148,163,184,.24);
    border-radius: 16px;
    background: linear-gradient(180deg, #07101a, #03070c);
    padding: 16px;
    margin-bottom: 16px;
}
.cc-metric {
    border: 1px solid rgba(148,163,184,.22);
    border-left: 4px solid #c91f27;
    border-radius: 14px;
    background: linear-gradient(180deg, #0d1724, #07101a);
    padding: 16px;
    min-height: 96px;
}
.cc-metric.blue { border-left-color:#1d4ed8; }
.cc-metric.green { border-left-color:#4c9a2a; }
.cc-metric.gold { border-left-color:#f2b84b; }
.cc-metric .label {
    color: #94a3b8;
    font-size: 11px;
    font-weight: 900;
    letter-spacing: .05em;
    text-transform: uppercase;
}
.cc-metric .value {
    color: #fff;
    font-size: 28px;
    font-weight: 950;
    margin-top: 8px;
}
.cc-metric .sub {
    color: #cbd5e1;
    font-size: 11px;
    margin-top: 4px;
}
.cc-note {
    border: 1px solid rgba(59,130,246,.35);
    background: rgba(15,23,42,.95);
    color: #dbeafe;
    border-radius: 12px;
    padding: 12px 14px;
    margin: 12px 0 16px 0;
    font-size: 13px;
}
.cc-verify {
    border: 1px solid rgba(242,184,75,.35);
    background: rgba(48, 31, 6, .75);
    color: #fef3c7;
    border-radius: 12px;
    padding: 12px 14px;
    margin: 12px 0 16px 0;
    font-size: 13px;
}
.stButton > button, div[data-testid="stDownloadButton"] > button {
    border-radius: 10px !important;
    font-weight: 850 !important;
    background: linear-gradient(180deg, #9f151c, #6e0f14) !important;
    color: white !important;
    border: 1px solid rgba(242,184,75,.45) !important;
}
[data-baseweb="select"] > div, [data-baseweb="input"] > div, textarea, input {
    background-color: #0f172a !important;
    color: #f8fafc !important;
    border-color: #334155 !important;
}
[data-baseweb="tag"] { background: rgba(201,31,39,.30) !important; color: white !important; }
.stAlert { background: rgba(15,23,42,.95) !important; color: #f8fafc !important; }

.cc-powered {
    border: 1px solid rgba(242,184,75,.35);
    border-radius: 14px;
    padding: 10px 16px;
    background: rgba(0,0,0,.25);
    text-align: center;
}


div[data-testid="stHorizontalBlock"] .stButton > button {
    min-height: 34px !important;
    padding: 0.25rem 0.75rem !important;
    font-size: 12px !important;
}


/* v12 fixes selected-option chip clipping */
[data-baseweb="tag"] {
    padding-left: 8px !important;
    margin-left: 2px !important;
}
[data-baseweb="tag"] span {
    padding-left: 2px !important;
}
[data-baseweb="select"] input {
    padding-left: 6px !important;
}


/* v13 stronger selected-chip clipping fix */
[data-baseweb="tag"] {
    padding-left: 12px !important;
    margin-left: 6px !important;
    max-width: none !important;
}
[data-baseweb="tag"] span {
    padding-left: 4px !important;
}
[data-baseweb="select"] > div {
    padding-left: 8px !important;
}
[data-baseweb="select"] input {
    padding-left: 10px !important;
}


/* v15 stronger chip visibility */
[data-baseweb="tag"] {
    padding-left: 18px !important;
    margin-left: 8px !important;
    max-width: none !important;
}
[data-baseweb="tag"] span,
[data-baseweb="tag"] div {
    padding-left: 4px !important;
}


/* v17 top nav polish */
div[data-testid="stHorizontalBlock"] .stButton > button {
    min-height: 34px !important;
    padding: 0.25rem 0.75rem !important;
    font-size: 12px !important;
}


/* v19 sidebar roll-up section polish */
[data-testid="stSidebar"] details {
    border: 1px solid rgba(148,163,184,.22);
    border-radius: 10px;
    padding: 2px 6px;
    margin-bottom: 8px;
    background: rgba(15,23,42,.35);
}
[data-testid="stSidebar"] summary {
    font-weight: 850 !important;
    color: #f8fafc !important;
}


/* v20 sidebar readability fixes */
[data-testid="stSidebar"] details,
[data-testid="stSidebar"] details[open] {
    background: rgba(15,23,42,.55) !important;
    border: 1px solid rgba(148,163,184,.30) !important;
    border-radius: 10px !important;
    margin-bottom: 9px !important;
}
[data-testid="stSidebar"] summary,
[data-testid="stSidebar"] summary * {
    color: #f8fafc !important;
    font-weight: 900 !important;
}
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] label *,
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] p {
    color: #f8fafc !important;
    opacity: 1 !important;
}
[data-testid="stSidebar"] [data-baseweb="tag"] {
    padding-left: 18px !important;
    margin-left: 8px !important;
    max-width: none !important;
}
[data-testid="stSidebar"] [data-baseweb="tag"] span,
[data-testid="stSidebar"] [data-baseweb="tag"] div {
    padding-left: 4px !important;
}


/* v20f force dark sidebar expander headers */
[data-testid="stSidebar"] details > summary,
[data-testid="stSidebar"] details > summary:hover,
[data-testid="stSidebar"] details > summary:focus,
[data-testid="stSidebar"] details > summary:active {
    background-color: #0f172a !important;
    background: #0f172a !important;
    color: #f8fafc !important;
    border-radius: 8px !important;
    min-height: 34px !important;
    padding: 8px 10px !important;
}
[data-testid="stSidebar"] details > summary *,
[data-testid="stSidebar"] details > summary svg {
    color: #f8fafc !important;
    fill: #f8fafc !important;
}
[data-testid="stSidebar"] details[open] > summary {
    background-color: #111827 !important;
    background: #111827 !important;
    border: 1px solid rgba(201,31,39,.65) !important;
}


/* v21s restored local-style dashboard cards */
.cc-home-title { font-size: 28px; font-weight: 950; letter-spacing: .08em; text-transform: uppercase; margin: 10px 0 18px 0; color: #f8fafc; }
.cc-home-card { border: 1px solid rgba(148,163,184,.28); border-radius: 16px; background: linear-gradient(180deg, #07101a, #02060b); padding: 18px; box-shadow: 0 14px 28px rgba(0,0,0,.35); margin-bottom: 16px; }
.cc-icon-metric { display:flex; align-items:center; gap:14px; border:1px solid rgba(148,163,184,.25); border-left:4px solid #c91f27; border-radius:16px; background:linear-gradient(180deg,#0c1624,#050b12); padding:18px 16px; min-height:94px; }
.cc-icon-metric.blue { border-left-color:#2454d6; } .cc-icon-metric.green { border-left-color:#4c9a2a; } .cc-icon-metric.gold { border-left-color:#f2b84b; }
.cc-icon-dot { width:46px; height:46px; border-radius:999px; display:flex; align-items:center; justify-content:center; background:radial-gradient(circle at 35% 20%, #ff6b6b, #9f151c 72%); box-shadow:inset 0 0 0 1px rgba(255,255,255,.18), 0 8px 18px rgba(201,31,39,.25); font-size:21px; }
.cc-icon-dot.blue { background:radial-gradient(circle at 35% 20%, #60a5fa, #1d4ed8 72%); } .cc-icon-dot.green { background:radial-gradient(circle at 35% 20%, #86efac, #3f8f27 72%); } .cc-icon-dot.gold { background:radial-gradient(circle at 35% 20%, #fde68a, #b7791f 72%); }
.cc-icon-label { color:#94a3b8; font-size:11px; font-weight:900; text-transform:uppercase; letter-spacing:.08em; } .cc-icon-value { color:#fff; font-size:29px; line-height:1.15; font-weight:950; margin-top:4px; } .cc-icon-sub { color:#cbd5e1; font-size:11px; margin-top:2px; }
.cc-donut-wrap { display:flex; align-items:center; justify-content:center; gap:28px; min-height:275px; }
.cc-donut { --r:40; --d:43; --o:17; width:220px; height:220px; border-radius:50%; background:conic-gradient(#d51f2a 0 calc(var(--r)*1%), #2454d6 calc(var(--r)*1%) calc((var(--r) + var(--d))*1%), #4c9a2a calc((var(--r) + var(--d))*1%) 100%); position:relative; box-shadow:0 18px 35px rgba(0,0,0,.38), inset 0 0 0 1px rgba(255,255,255,.12); }
.cc-donut:after { content:''; position:absolute; inset:58px; border-radius:50%; background:#050b12; box-shadow:inset 0 0 0 1px rgba(148,163,184,.24); }
.cc-donut-center { position:absolute; inset:0; display:flex; flex-direction:column; align-items:center; justify-content:center; z-index:2; font-weight:950; color:#fff; }
.cc-legend-row { display:grid; grid-template-columns:16px 1fr auto; gap:10px; align-items:center; margin:12px 0; color:#f8fafc; } .cc-swatch { width:12px; height:12px; border-radius:999px; }
.cc-age-row { display:grid; grid-template-columns:80px 1fr 70px; gap:12px; align-items:center; margin:13px 0; } .cc-age-bar-bg { height:18px; border-radius:999px; background:#111827; border:1px solid rgba(148,163,184,.2); overflow:hidden; } .cc-age-bar { height:100%; border-radius:999px; background:linear-gradient(90deg,#8b0d13,#ef4444); }
.cc-home-table { width:100%; border-collapse:collapse; overflow:hidden; border-radius:12px; } .cc-home-table th { color:#f8fafc; background:#111827; padding:11px; font-size:12px; text-align:left; } .cc-home-table td { color:#e5e7eb; background:#0b1220; padding:10px 11px; border-top:1px solid rgba(148,163,184,.15); font-size:12px; }


/* v21u compact dashboard/output polish */
.cc-header { padding: 10px 16px !important; margin-bottom: 10px !important; }
.cc-title { font-size: 24px !important; }
.cc-sub { font-size: 11px !important; }
.cc-home-title { font-size: 24px !important; margin: 8px 0 12px 0 !important; }
.cc-icon-metric { min-height: 72px !important; padding: 12px !important; }
.cc-icon-dot { width: 42px !important; height: 42px !important; font-size: 20px !important; }
.cc-icon-value { font-size: 24px !important; }
.cc-icon-label, .cc-icon-sub { font-size: 10px !important; }
.cc-home-card { padding: 14px !important; margin-bottom: 12px !important; border-radius: 12px !important; }
.cc-home-card h3 { font-size: 20px !important; margin: 0 0 10px 0 !important; }
.cc-donut-wrap { gap: 18px !important; min-height: 190px !important; justify-content:flex-start !important; }
.cc-donut { width: 150px !important; height: 150px !important; }
.cc-donut:after { inset: 40px !important; }
.cc-donut-center { font-size: 12px !important; }
.cc-legend-row { font-size: 12px !important; grid-template-columns: 14px 130px 120px !important; gap: 8px !important; }
.cc-age-row { grid-template-columns: 64px 1fr 72px !important; gap: 10px !important; font-size: 12px !important; margin: 8px 0 !important; }
.cc-age-bar-bg { height: 14px !important; }
.cc-home-table { font-size: 11px !important; }
.cc-scroll-table { max-height: 245px; overflow-y: auto; border:1px solid rgba(148,163,184,.20); border-radius:10px; }
.cc-section-tabs { display:flex; gap:8px; margin: 12px 0 10px 0; }



/* v21zr table readability polish */
[data-testid="stDataFrame"] div[role="gridcell"],
[data-testid="stDataFrame"] div[role="columnheader"] {
    text-align: center !important;
}
[data-testid="stDataFrame"] div[role="columnheader"] {
    font-weight: 900 !important;
}
</style>
""",
    unsafe_allow_html=True,
)


def r2_url(key: str) -> str:
    return f"{R2}/{key.lstrip('/')}"


def sql_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def sql_lit(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def tag_contains_mask(series: pd.Series, selected_tags) -> pd.Series:
    """Match Tags safely when a row may contain comma/semicolon/pipe separated values."""
    if series is None:
        return pd.Series([], dtype=bool)
    vals = [str(v).strip().lower() for v in (selected_tags or []) if str(v).strip()]
    if not vals:
        return pd.Series(True, index=series.index)

    def has_any(raw) -> bool:
        txt = str(raw or "").strip().lower()
        if not txt:
            return False
        parts = [p.strip() for p in re.split(r"[,;|]+", txt) if p.strip()]
        if parts:
            return any(v == p for v in vals for p in parts)
        return any(v in txt for v in vals)

    return series.map(has_any).fillna(False)


def count_cube_url() -> str:
    manifest = load_manifest()
    speed = manifest.get("speed", {}).get("tables", {})
    key = speed.get("count_cube", "speed/count_cube.parquet")
    return r2_url(key)


def _count_cube_expanded_values(field: str, vals) -> list[str]:
    """Expand user-facing MB yes/no selections to the canonical values that may exist in count_cube.

    This is especially important for MB_PERM. In some SURE-derived files permanent MB
    is stored as Y/blank, in others as Y/N or Yes/No. Selecting N should mean
    "not permanent," including blank/no/false/0 variants.
    """
    raw = [str(v).strip() for v in (vals or []) if str(v).strip()]
    if not raw:
        return []

    yes = {"Y", "YES", "TRUE", "T", "1", "APPLIED", "SENT", "VOTED", "RETURNED", "PERMANENT"}
    no = {"N", "NO", "FALSE", "F", "0", "DNA", "DID NOT APPLY", "NOT APPLIED", "NOT SENT", "NOT VOTED", "NOT RETURNED", "NOT PERMANENT", "NON PERMANENT", "NON-PERMANENT"}

    expanded = []
    for v in raw:
        u = v.upper()
        if field in {"MB_PERM", "MB_App", "MB_Sent", "MB_Status"}:
            if u in yes:
                expanded.extend([v, "Y", "Yes", "YES", "True", "TRUE", "1"])
                if field == "MB_App":
                    expanded.extend(["Applied"])
                if field == "MB_Sent":
                    expanded.extend(["Sent"])
                if field == "MB_Status":
                    expanded.extend(["Voted", "Returned"])
                if field == "MB_PERM":
                    expanded.extend(["Permanent"])
            elif u in no:
                expanded.extend([v, "", "N", "No", "NO", "False", "FALSE", "0"])
                if field == "MB_App":
                    expanded.extend(["DNA", "Not Applied", "Did Not Apply"])
                if field == "MB_Sent":
                    expanded.extend(["Not Sent"])
                if field == "MB_Status":
                    expanded.extend(["Not Voted", "Not Returned"])
                if field == "MB_PERM":
                    expanded.extend(["Not Permanent", "Non Permanent", "Non-Permanent"])
            else:
                expanded.append(v)
        elif field == "MB_App_Status":
            # Application status is not a yes/no field, but keep common capitalization variants.
            expanded.extend([v, v.title(), u])
        else:
            expanded.append(v)

    out = []
    seen = set()
    for x in expanded:
        sx = str(x)
        if sx not in seen:
            seen.add(sx)
            out.append(sx)
    return out


def count_cube_where_sql(active: dict, special: dict | None = None) -> str:
    clauses = []
    for field, vals in (active or {}).items():
        if not vals:
            continue
        if field == "Tags":
            continue
        cleaned = _count_cube_expanded_values(field, vals)
        if not cleaned:
            continue
        expr = f"COALESCE(CAST({sql_ident(field)} AS VARCHAR), '')"
        # MB_PERM is a Y/blank style field in many builds. For this filter,
        # N must mean "not permanent" — not just literal N. The safest fast
        # count-cube expression is therefore NOT IN all yes/permanent variants.
        if field == "MB_PERM":
            raw_upper = {str(v).strip().upper() for v in (vals or []) if str(v).strip()}
            yes_tokens = {"Y", "YES", "TRUE", "T", "1", "PERMANENT"}
            no_tokens = {"N", "NO", "FALSE", "F", "0", "NOT PERMANENT", "NON PERMANENT", "NON-PERMANENT", ""}
            upper_expr = f"UPPER(TRIM({expr}))"
            if raw_upper and raw_upper.issubset(no_tokens):
                clauses.append(f"({upper_expr} NOT IN ('Y','YES','TRUE','T','1','PERMANENT'))")
                continue
            if raw_upper and raw_upper.issubset(yes_tokens):
                clauses.append(f"({upper_expr} IN ('Y','YES','TRUE','T','1','PERMANENT'))")
                continue
        clauses.append(f"{expr} IN (" + ",".join(sql_lit(v) for v in cleaned) + ")")

    special = special or {}
    for field, rule in special.items():
        if field == "__PhoneReach":
            mobile = "LOWER(CAST(\"HasMobile\" AS VARCHAR)) = 'yes'"
            landline = "LOWER(CAST(\"HasLandline\" AS VARCHAR)) = 'yes'"
            mode = str(rule)
            if mode == "Mobile only":
                clauses.append(f"({mobile})")
            elif mode == "Landline only":
                clauses.append(f"({landline})")
            elif mode == "Mobile OR landline":
                clauses.append(f"(({mobile}) OR ({landline}))")
            elif mode == "Mobile AND landline":
                clauses.append(f"(({mobile}) AND ({landline}))")
            elif mode == "No mobile or landline":
                clauses.append(f"(NOT (({mobile}) OR ({landline})))")
            continue
        if str(field).startswith("__"):
            continue
        if isinstance(rule, dict):
            expr = f"TRY_CAST({sql_ident(field)} AS DOUBLE)"
            if "min" in rule:
                clauses.append(f"{expr} >= {float(rule['min'])}")
            if "max" in rule:
                clauses.append(f"{expr} <= {float(rule['max'])}")

    return " WHERE " + " AND ".join(clauses) if clauses else ""


@st.cache_data(ttl=300, show_spinner=False)
def duckdb_count_cube_summary(active_json: str, special_json: str) -> dict:
    active = json.loads(active_json or "{}")
    special = json.loads(special_json or "{}")
    url = count_cube_url()
    where = count_cube_where_sql(active, special)
    query = f"""
        SELECT CAST(Party AS VARCHAR) AS Party, SUM(Voters) AS Voters
        FROM read_parquet({sql_lit(url)})
        {where}
        GROUP BY CAST(Party AS VARCHAR)
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try:
                con.execute("LOAD httpfs;")
            except Exception:
                pass
        df = con.execute(query).df()
    finally:
        try:
            con.close()
        except Exception:
            pass
    return summarize_from_df(df, row_count_mode=False)


def index_urls_from_manifest() -> list[str]:
    """Remote index shard URLs for DuckDB. Keeps counting out of Streamlit memory."""
    m = load_manifest()
    count = int(((m.get("index", {}) or {}).get("count", DETAIL_SHARDS)) or DETAIL_SHARDS)
    return [r2_url(f"index/voters_index_{i:03d}.parquet") for i in range(count)]


def detail_urls_from_manifest() -> list[str]:
    """Remote detail shard URLs for DuckDB exports/reports."""
    m = load_manifest()
    count = int(((m.get("detail", {}) or {}).get("count", DETAIL_SHARDS)) or DETAIL_SHARDS)
    return [r2_url(f"detail/voters_detail_{i:03d}.parquet") for i in range(count)]


def speed_table_key(stem: str) -> str:
    try:
        m = load_manifest()
        return (((m.get("speed", {}) or {}).get("tables", {}) or {}).get(stem, ""))
    except Exception:
        return ""


def speed_table_url(stem: str) -> str:
    key = speed_table_key(stem)
    return r2_url(key) if key else ""


def voter_search_all_urls() -> list[str]:
    urls = []
    for ch in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["OTHER"]:
        u = speed_table_url(f"voter_search_lname_{ch}")
        if u:
            urls.append(u)
    return urls


def _lookup_county_token_and_search_tokens(term: str):
    county_names = {
        "adams","allegheny","armstrong","beaver","bedford","berks","blair","bradford","bucks","butler",
        "cambria","cameron","carbon","centre","chester","clarion","clearfield","clinton","columbia","crawford",
        "cumberland","dauphin","delaware","elk","erie","fayette","forest","franklin","fulton","greene",
        "huntingdon","indiana","jefferson","juniata","lackawanna","lancaster","lawrence","lebanon","lehigh",
        "luzerne","lycoming","mckean","mercer","mifflin","monroe","montgomery","montour","northampton",
        "northumberland","perry","philadelphia","pike","potter","schuylkill","snyder","somerset","sullivan",
        "susquehanna","tioga","union","venango","warren","washington","wayne","westmoreland","wyoming","york"
    }
    raw_tokens = [t.strip() for t in re.split(r"\s+", str(term or "").strip()) if t.strip()]
    tokens_lower = [t.lower().replace("'", "''") for t in raw_tokens]
    county_token = next((t for t in tokens_lower if t in county_names), "")
    search_tokens = [t for t in tokens_lower if t != county_token]
    return county_token, search_tokens


def voter_search_urls_for_term(term: str) -> list[str]:
    """Pick the smallest useful search file set.

    Normal name searches read one last-name-letter shard. Address-only searches
    fall back to all 27 thin search shards. If Step 8 has not produced these yet,
    fall back to the regular index shards.
    """
    county_token, search_tokens = _lookup_county_token_and_search_tokens(term)
    digits = re.sub(r"\D+", "", str(term or ""))
    if len(digits) >= 6 or "@" in str(term or ""):
        return voter_search_all_urls() or index_urls_from_manifest()
    if search_tokens:
        last_t = search_tokens[-1]
        ch = last_t[:1].upper()
        if "A" <= ch <= "Z":
            u = speed_table_url(f"voter_search_lname_{ch}")
            if u:
                return [u]
    return voter_search_all_urls() or index_urls_from_manifest()


def voter_detail_hash_url(voter_id: str) -> str:
    vid = cc_text(voter_id)
    if not vid:
        return ""
    bucket = int(hashlib.md5(vid.encode("utf-8")).hexdigest(), 16) % 64
    return speed_table_url(f"voter_detail_hash_{bucket:02d}")


def voter_detail_lookup_urls_for_id(voter_id: str) -> list[str]:
    u = voter_detail_hash_url(voter_id)
    return [u] if u else detail_urls_from_manifest()


def _hh_norm(value) -> str:
    s = cc_text(value).upper().strip()
    s = re.sub(r"\bSTREET\b", "ST", s)
    s = re.sub(r"\bROAD\b", "RD", s)
    s = re.sub(r"\bDRIVE\b", "DR", s)
    s = re.sub(r"\bAVENUE\b", "AVE", s)
    s = re.sub(r"\bLANE\b", "LN", s)
    s = re.sub(r"\bCOURT\b", "CT", s)
    s = re.sub(r"\bTOWNSHIP\b", "TWP", s)
    s = re.sub(r"\bBOROUGH\b", "BORO", s)
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def household_lookup_key(row) -> str:
    parts = [
        row.get("County", ""), row.get("House Number", ""), row.get("Street Name", ""),
        row.get("Apartment Number", ""), row.get("Zip", ""),
    ]
    return "|".join(_hh_norm(x) for x in parts)

def household_hash_bucket_from_key(key: str, buckets: int = 64) -> int:
    return int(hashlib.md5(cc_text(key).encode("utf-8")).hexdigest(), 16) % int(buckets)

def voter_household_lookup_url(row) -> str:
    key = cc_text(row.get("HH_LOOKUP_KEY", "")) or household_lookup_key(row)
    if not key or key.count("|") < 4:
        return ""
    bucket = household_hash_bucket_from_key(key)
    return speed_table_url(f"voter_household_hash_{bucket:02d}")


def voter_lookup_urls_from_manifest() -> list[str]:
    """Backward-compatible helper for older lookup builds."""
    key = speed_table_key("voter_lookup")
    return [r2_url(key)] if key else []


def voter_lookup_or_detail_urls() -> list[str]:
    return voter_lookup_urls_from_manifest() or detail_urls_from_manifest()


def normalize_compare_value(value) -> str:
    s = clean_value(value).upper()
    s = s.replace("&", " AND ")
    s = re.sub(r"\bTOWNSHIP\b", "TWP", s)
    s = re.sub(r"\bTWP\.\b", "TWP", s)
    s = re.sub(r"\bBOROUGH\b", "BORO", s)
    s = re.sub(r"\bBORO\.\b", "BORO", s)
    s = re.sub(r"\bPRECINCT\b", "PRECINCT", s)
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def detail_filter_where_sql(active: dict, special: dict | None = None) -> str:
    clauses = []
    for field, vals in (active or {}).items():
        cleaned = [str(v).strip() for v in (vals or []) if str(v).strip()]
        if not cleaned:
            continue
        if field == "Tags":
            expr = f"LOWER(CAST({sql_ident(field)} AS VARCHAR))"
            clauses.append("(" + " OR ".join([f"{expr} LIKE {sql_lit('%' + v.lower().replace(chr(39), chr(39)+chr(39)) + '%')}" for v in cleaned]) + ")")
        else:
            norm_vals = [normalize_compare_value(v) for v in cleaned]
            expr = (
                "REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("
                f"UPPER(CAST({sql_ident(field)} AS VARCHAR)), "
                "'\\bTOWNSHIP\\b','TWP','g'), "
                "'\\bTWP\\.\\b','TWP','g'), "
                "'\\bBOROUGH\\b','BORO','g'), "
                "'[^A-Z0-9]+',' ','g')"
            )
            clauses.append(f"TRIM({expr}) IN (" + ",".join(sql_lit(v) for v in norm_vals) + ")")
    special = special or {}
    for field, rule in special.items():
        if field == "__ElectionFilters":
            continue
        if field == "__PhoneReach":
            phone_clause = index_phone_reach_sql(str(rule))
            if phone_clause:
                clauses.append(phone_clause)
            continue
        if str(field).startswith("__"):
            continue
        if isinstance(rule, dict):
            expr = f"TRY_CAST({sql_ident(field)} AS DOUBLE)"
            if "min" in rule:
                clauses.append(f"{expr} >= {float(rule['min'])}")
            if "max" in rule:
                clauses.append(f"{expr} <= {float(rule['max'])}")
    ef = special.get("__ElectionFilters")
    if isinstance(ef, dict):
        cols = selected_election_columns(ef.get("years") or [], ef.get("types") or [])
        if cols:
            clauses.append(election_method_sql(cols, ef.get("methods") or []))
        elif ef.get("years") or ef.get("types") or ef.get("methods"):
            clauses.append("(FALSE)")
    return " WHERE " + " AND ".join(clauses) if clauses else ""


def duckdb_detail_filtered_df(active: dict, special: dict | None, max_rows: int) -> pd.DataFrame:
    urls = detail_urls_from_manifest()
    url_list = "[" + ",".join(sql_lit(u) for u in urls) + "]"
    where = detail_filter_where_sql(active or {}, special or {})
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        q = f"SELECT * FROM read_parquet({url_list}, union_by_name=true) {where} LIMIT {int(max_rows)}"
        return con.execute(q).df()
    finally:
        try: con.close()
        except Exception: pass


def duckdb_detail_group(active: dict, special: dict | None, field: str, limit: int = 20) -> pd.DataFrame:
    urls = detail_urls_from_manifest()
    url_list = "[" + ",".join(sql_lit(u) for u in urls) + "]"
    where = detail_filter_where_sql(active or {}, special or {})
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        q = f"""
            SELECT CAST({sql_ident(field)} AS VARCHAR) AS label, COUNT(*) AS Voters
            FROM read_parquet({url_list}, union_by_name=true)
            {where}
            GROUP BY CAST({sql_ident(field)} AS VARCHAR)
            ORDER BY Voters DESC
            LIMIT {int(limit)}
        """
        return con.execute(q).df()
    except Exception:
        return pd.DataFrame(columns=["label", "Voters"])
    finally:
        try: con.close()
        except Exception: pass


def dataframe_to_excel_bytes(df: pd.DataFrame, area_level: str = "Municipality") -> bytes:
    bio = io.BytesIO()
    if df is None:
        df = pd.DataFrame()
    area_col = area_level if area_level in df.columns else ("Precinct" if "Precinct" in df.columns else ("Municipality" if "Municipality" in df.columns else None))
    if area_col and not df.empty:
        counts = df.groupby(area_col, dropna=False).size().reset_index(name="Voters").sort_values(area_col, ascending=True)
    else:
        counts = pd.DataFrame(columns=[area_level, "Voters"])
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        counts.to_excel(writer, sheet_name="Area Counts", index=False)
        df.to_excel(writer, sheet_name="Data", index=False)
    bio.seek(0)
    return bio.getvalue()


def render_group_bar(active: dict, field: str, title: str, order: list[str] | None = None):
    special = {k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}
    df = duckdb_count_cube_group_filtered(
        json.dumps(count_safe_filters(active or {}), sort_keys=True),
        json.dumps(special or {}, sort_keys=True),
        field,
        20,
    )
    if df.empty or "Voters" not in df.columns:
        return
    df["label"] = df["label"].astype(str).str.strip()
    df = df[~df["label"].str.lower().isin(["", "(blank)", "blank", "nan", "none", "null"])]
    df = df[df["Voters"].fillna(0).astype(float) > 0]
    if df.empty:
        return
    if order:
        sortmap = {v:i for i,v in enumerate(order)}
        df["_sort"] = df["label"].map(lambda x: sortmap.get(str(x), 999))
        df = df.sort_values(["_sort", "label"])
    total = float(df["Voters"].sum() or 1)
    maxv = float(df["Voters"].max() or 1)
    rows=[]
    table_rows=[]
    for _,r in df.iterrows():
        lab=str(r["label"]); val=int(r["Voters"] or 0); p=val/total*100; w=max(2,val/maxv*100)
        rows.append(f'<div class="cc-age-row"><b>{lab}</b><div class="cc-age-bar-bg"><div class="cc-age-bar" style="width:{w:.1f}%"></div></div><span>{val:,} ({p:.1f}%)</span></div>')
        table_rows.append({"Category": lab, "Voters": val, "%": f"{p:.1f}%"})
    st.markdown('<div class="cc-home-card"><h3>'+title+'</h3>'+''.join(rows)+'</div>', unsafe_allow_html=True)
    st.dataframe(pd.DataFrame(table_rows), hide_index=True, width="stretch", height=min(210, 42 + 32*len(table_rows)))


def election_method_sql(selected_cols: list[str], methods: list[str]) -> str:
    if not selected_cols:
        return "(FALSE)"
    method_vals = [str(m).strip().upper() for m in (methods or []) if str(m).strip()]
    col_checks = []
    for c in selected_cols:
        expr = f"UPPER(CAST({sql_ident(c)} AS VARCHAR))"
        if not method_vals:
            col_checks.append(f"({expr} NOT IN ('', 'NAN', 'NONE', 'NULL', '0', 'N', 'NO'))")
            continue
        tests = []
        for m in method_vals:
            if m == "VOTED":
                tests.append(f"({expr} NOT IN ('', 'NAN', 'NONE', 'NULL', '0', 'N', 'NO'))")
            elif m == "MAIL":
                tests.append(f"({expr} LIKE '%MAIL%' OR {expr} IN ('M','MB'))")
            elif m == "ABSENTEE":
                tests.append(f"({expr} LIKE '%ABS%' OR {expr} = 'A')")
            elif m == "POLLS":
                tests.append(f"({expr} LIKE '%POLL%' OR {expr} LIKE '%PERSON%' OR {expr} = 'P')")
            elif m == "PROVISIONAL":
                tests.append(f"({expr} LIKE '%PROV%')")
            else:
                tests.append(f"({expr} = {sql_lit(m)})")
        col_checks.append("(" + " OR ".join(tests) + ")")
    return "(" + " OR ".join(col_checks) + ")"


def index_contact_flag_sql(field: str, vals: list[str]) -> str | None:
    """Translate count-cube contact flags to real columns in lightweight index shards.
    Step 8 index shards include Mobile/Landline/Email/Current_ApplicantPhone,
    not HasMobile/HasLandline/HasEmail/HasApplicantPhone.
    """
    col_map = {
        "HasMobile": "Mobile",
        "HasLandline": "Landline",
        "HasEmail": "Email",
        "HasApplicantPhone": "Current_ApplicantPhone",
    }
    col = col_map.get(field)
    if not col:
        return None
    has_expr = f"NULLIF(TRIM(CAST({sql_ident(col)} AS VARCHAR)), '') IS NOT NULL"
    wanted = {str(v).strip().lower() for v in (vals or []) if str(v).strip()}
    parts = []
    if wanted & {"yes", "y", "true", "1"}:
        parts.append(f"({has_expr})")
    if wanted & {"no", "n", "false", "0"}:
        parts.append(f"(NOT ({has_expr}))")
    return "(" + " OR ".join(parts) + ")" if parts else None


def index_phone_reach_sql(mode: str) -> str | None:
    mobile = "NULLIF(TRIM(CAST(\"Mobile\" AS VARCHAR)), '') IS NOT NULL"
    landline = "NULLIF(TRIM(CAST(\"Landline\" AS VARCHAR)), '') IS NOT NULL"
    mode = str(mode or "").strip()
    if mode == "Mobile only":
        return f"(({mobile}) AND NOT ({landline}))"
    if mode == "Landline only":
        return f"(({landline}) AND NOT ({mobile}))"
    if mode == "Mobile OR landline":
        return f"(({mobile}) OR ({landline}))"
    if mode == "Mobile AND landline":
        return f"(({mobile}) AND ({landline}))"
    if mode == "No mobile or landline":
        return f"(NOT (({mobile}) OR ({landline})))"
    return None


def index_where_sql(active: dict, special: dict | None = None) -> str:
    clauses = []
    for field, vals in (active or {}).items():
        if not vals:
            continue
        cleaned = [str(v) for v in vals if str(v).strip()]
        if not cleaned:
            continue
        if field == "Tags":
            tag_expr = f"LOWER(CAST({sql_ident(field)} AS VARCHAR))"
            tag_clauses = [f"{tag_expr} LIKE {sql_lit('%' + v.lower().replace(chr(39), chr(39)+chr(39)) + '%')}" for v in cleaned]
            clauses.append("(" + " OR ".join(tag_clauses) + ")")
        else:
            contact_clause = index_contact_flag_sql(field, cleaned)
            if contact_clause:
                clauses.append(contact_clause)
            else:
                clauses.append(f"CAST({sql_ident(field)} AS VARCHAR) IN (" + ",".join(sql_lit(v) for v in cleaned) + ")")

    special = special or {}
    for field, rule in special.items():
        if field == "__ElectionFilters":
            continue
        if field == "__PhoneReach":
            phone_clause = index_phone_reach_sql(str(rule))
            if phone_clause:
                clauses.append(phone_clause)
            continue
        if str(field).startswith("__"):
            continue
        if isinstance(rule, dict):
            expr = f"TRY_CAST({sql_ident(field)} AS DOUBLE)"
            if "min" in rule:
                clauses.append(f"{expr} >= {float(rule['min'])}")
            if "max" in rule:
                clauses.append(f"{expr} <= {float(rule['max'])}")

    ef = special.get("__ElectionFilters")
    if isinstance(ef, dict):
        cols = selected_election_columns(ef.get("years") or [], ef.get("types") or [])
        if cols:
            clauses.append(election_method_sql(cols, ef.get("methods") or []))
        elif ef.get("years") or ef.get("types") or ef.get("methods"):
            clauses.append("(FALSE)")

    return " WHERE " + " AND ".join(clauses) if clauses else ""


@st.cache_data(ttl=300, show_spinner=False)
def duckdb_index_summary(active_json: str, special_json: str) -> dict:
    active = json.loads(active_json or "{}")
    special = json.loads(special_json or "{}")
    urls = index_urls_from_manifest()
    where = index_where_sql(active, special)
    url_list = "[" + ",".join(sql_lit(u) for u in urls) + "]"
    query = f"""
        SELECT CAST(Party AS VARCHAR) AS Party, COUNT(*) AS Voters
        FROM read_parquet({url_list})
        {where}
        GROUP BY CAST(Party AS VARCHAR)
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try:
                con.execute("LOAD httpfs;")
            except Exception:
                pass
        df = con.execute(query).df()
    finally:
        try:
            con.close()
        except Exception:
            pass
    return summarize_from_df(df, row_count_mode=False)


def requires_remote_index_count(active: dict, special: dict) -> bool:
    if active.get("Tags"):
        return True
    if (special or {}).get("__ElectionFilters"):
        return True
    return False


@st.cache_data(ttl=600, show_spinner=False)
def get_bytes(key: str) -> bytes:
    r = requests.get(r2_url(key), timeout=120)
    r.raise_for_status()
    return r.content


@st.cache_data(ttl=600, show_spinner=False)
def load_manifest():
    return json.loads(get_bytes("dataset_manifest.json").decode("utf-8"))


@st.cache_data(ttl=600, show_spinner=False)
def load_parquet(key: str, columns=None) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(get_bytes(key)), columns=columns)


@st.cache_data(ttl=600, show_spinner=False)
def load_filter_layer():
    """Load only the small filter layer needed to draw the UI.

    v21g startup-safe fix:
    - Load manifest + filter_options only on startup.
    - Do NOT load count_cube while the sidebar is being drawn.
    - Do NOT load geo_hierarchy until Create Universe needs geo options.
    """
    manifest = load_manifest()
    speed = manifest.get("speed", {}).get("tables", {})
    filter_options = load_parquet(speed.get("filter_options", "speed/filter_options.parquet"))
    geo_hierarchy = pd.DataFrame()
    return manifest, filter_options, geo_hierarchy


def r2_content_length(key: str) -> int:
    try:
        r = requests.head(r2_url(key), timeout=20)
        r.raise_for_status()
        return int(r.headers.get("Content-Length", "0") or 0)
    except Exception:
        return 0


@st.cache_data(ttl=600, show_spinner=False)
def load_geo_hierarchy_safe(max_bytes: int = 90_000_000) -> pd.DataFrame:
    """Optional dependent geo table.

    This keeps Create Universe from crashing: if the R2 geo_hierarchy file is too
    large or unavailable, the app falls back to flat filter_options instead of
    killing the Streamlit process.
    """
    try:
        manifest = load_manifest()
        speed = manifest.get("speed", {}).get("tables", {})
        key = speed.get("geo_hierarchy", "speed/geo_hierarchy.parquet")
        size = r2_content_length(key)
        if size and size > max_bytes:
            return pd.DataFrame()
        return load_parquet(key)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def load_count_cube_columns(cols_tuple):
    """Read only requested columns from the quick-count cube.

    v21i: never fall back to a full count_cube read. A full read can exceed
    Streamlit Cloud memory and kill the app health check when a selected field
    is missing/mismatched. Callers can catch the exception and choose a safer
    fallback.
    """
    manifest = load_manifest()
    speed = manifest.get("speed", {}).get("tables", {})
    key = speed.get("count_cube", "speed/count_cube.parquet")
    return load_parquet(key, columns=list(cols_tuple))


@st.cache_data(ttl=600, show_spinner=False)
def load_index_columns(key: str, cols_tuple):
    return load_parquet(key, columns=list(cols_tuple))


@st.cache_data(ttl=600, show_spinner=False)
def load_detail_columns(key: str, cols_tuple):
    return load_parquet(key, columns=list(cols_tuple))


def clean_value(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if s.lower() in {"", "nan", "none", "null", "(blank)"}:
        return ""
    return s


def smart_sort_key(v):
    s = str(v)
    try:
        return (0, int(float(s)))
    except Exception:
        return (1, s)


def current_filter_suffix() -> int:
    return int(st.session_state.get("filter_reset_token", 0))


def filter_key(field: str) -> str:
    return f"filter_{field}_{current_filter_suffix()}"


def special_key(name: str) -> str:
    return f"{name}_{current_filter_suffix()}"


SAVED_UNIVERSES_PARAM = "cc_saved_universes"

def _json_safe_saved_universes(saved):
    """Return saved universes as plain JSON-safe dict/list/scalar values."""
    if not isinstance(saved, dict):
        return {}
    clean = {}
    for name, data in saved.items():
        if not str(name).strip() or not isinstance(data, dict):
            continue
        clean[str(name)] = {
            "filters": data.get("filters") or {},
            "special": data.get("special") or {},
        }
    return clean


def encode_saved_universes(saved) -> str:
    try:
        payload = json.dumps(_json_safe_saved_universes(saved), separators=(",", ":"), ensure_ascii=False)
        return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")
    except Exception:
        return ""


def decode_saved_universes(raw):
    try:
        if isinstance(raw, list):
            raw = raw[0] if raw else ""
        raw = str(raw or "").strip()
        if not raw:
            return {}
        payload = base64.urlsafe_b64decode(raw.encode("ascii") + b"=" * (-len(raw) % 4)).decode("utf-8")
        data = json.loads(payload)
        return _json_safe_saved_universes(data)
    except Exception:
        return {}



CORRECTIONS_PARAM = "cc_voter_corrections"

def _json_safe_corrections(corrections):
    """Return voter corrections as JSON-safe durable data."""
    if not isinstance(corrections, dict):
        return {}
    clean = {}
    for vid, payload in corrections.items():
        vid_s = str(vid or "").strip()
        if not vid_s or not isinstance(payload, dict):
            continue
        fields = payload.get("fields") or {}
        if not isinstance(fields, dict):
            fields = {}
        clean[vid_s] = {
            "updated_at": str(payload.get("updated_at", "")),
            "fields": {str(k): cc_text(v) for k, v in fields.items()},
            "notes": cc_text(payload.get("notes", "")),
        }
    return clean

def encode_corrections(corrections) -> str:
    try:
        payload = json.dumps(_json_safe_corrections(corrections), separators=(",", ":"), ensure_ascii=False)
        return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")
    except Exception:
        return ""

def decode_corrections(raw):
    try:
        if isinstance(raw, list):
            raw = raw[0] if raw else ""
        raw = str(raw or "").strip()
        if not raw:
            return {}
        payload = base64.urlsafe_b64decode(raw.encode("ascii") + b"=" * (-len(raw) % 4)).decode("utf-8")
        return _json_safe_corrections(json.loads(payload))
    except Exception:
        return {}

def _load_remote_app_state() -> dict:
    """Read durable app_state uploaded to R2 by Pipeline Manager, when available."""
    state = {}
    try:
        r = requests.get(r2_url("app_state/saved_universes.json"), timeout=10)
        if r.ok:
            state["saved_universes"] = _json_safe_saved_universes(r.json())
    except Exception:
        pass
    try:
        r = requests.get(r2_url("app_state/voter_record_corrections.json"), timeout=10)
        if r.ok:
            raw = r.json()
            # Accept either direct correction-store JSON or a rows/list export.
            if isinstance(raw, dict):
                state["voter_corrections"] = _json_safe_corrections(raw)
    except Exception:
        pass
    return state

def _state_file_candidates():
    """Small local DEV persistence so saved universes/corrections survive app reboot.
    This is not a substitute for the later real cloud/mobile persistence layer, but it
    prevents losing work during Streamlit restarts in DEV.
    """
    out = []
    try:
        out.append(Path.cwd() / ".candidate_connect_dev_state.json")
    except Exception:
        pass
    try:
        out.append(Path.home() / ".candidate_connect_dev_state.json")
    except Exception:
        pass
    out.append(Path("/tmp/candidate_connect_dev_state.json"))
    return out


def _load_dev_state() -> dict:
    # Remote app_state is the durable baseline after rebuild/deploy. Local/browser state can override it.
    state = _load_remote_app_state()
    for path in _state_file_candidates():
        try:
            if path.exists():
                local_state = json.loads(path.read_text(encoding="utf-8")) or {}
                if isinstance(local_state, dict):
                    state.update(local_state)
                    return state
        except Exception:
            continue
    return state


def _save_dev_state(state: dict):
    for path in _state_file_candidates():
        try:
            path.write_text(json.dumps(state or {}, ensure_ascii=False, indent=2), encoding="utf-8")
            return True
        except Exception:
            continue
    return False


def _persist_dev_section(section: str, data):
    state = _load_dev_state()
    state[section] = data
    _save_dev_state(state)


def load_persistent_saved_universes():
    """Initialize session saved universes from local DEV state, then URL query params.
    Local DEV state is checked first so saved universes survive Streamlit app reboots.
    """
    if "saved_universes" not in st.session_state:
        state_saved = (_load_dev_state().get("saved_universes") or {})
        if state_saved:
            st.session_state["saved_universes"] = _json_safe_saved_universes(state_saved)
        else:
            try:
                raw = st.query_params.get(SAVED_UNIVERSES_PARAM, "")
            except Exception:
                raw = ""
            st.session_state["saved_universes"] = decode_saved_universes(raw)
    return st.session_state.setdefault("saved_universes", {})


def persist_saved_universes(saved):
    """Persist saved universes into local DEV state and the browser URL."""
    clean = _json_safe_saved_universes(saved)
    try:
        _persist_dev_section("saved_universes", clean)
    except Exception:
        pass
    try:
        encoded = encode_saved_universes(clean)
        if encoded:
            st.query_params[SAVED_UNIVERSES_PARAM] = encoded
        elif SAVED_UNIVERSES_PARAM in st.query_params:
            del st.query_params[SAVED_UNIVERSES_PARAM]
    except Exception:
        pass


def load_saved_universe_into_widgets(data):
    """Reset current widget keys, then write saved filter values into the fresh keys."""
    old_token = int(st.session_state.get("filter_reset_token", 0))
    prefixes = (
        "filter_",
        "new_reg_months_",
        "vote_score_type_",
        "vote_history_score_range_",
        "election_years_",
        "election_types_",
        "election_methods_",
        "mb_prob_score_range_",
        "phone_reach_mode_",
    )
    for key in list(st.session_state.keys()):
        if key.startswith(prefixes) or key in {"quick_summary", "count_mode", "exact_summary"} or key.startswith("prepared_"):
            st.session_state.pop(key, None)
    st.session_state["filter_reset_token"] = old_token + 1

    for f, vals in ((data or {}).get("filters") or {}).items():
        st.session_state[filter_key(f)] = vals

    sp = (data or {}).get("special") or {}
    for k, v in sp.items():
        if k == "__PhoneReach":
            st.session_state[special_key("phone_reach_mode")] = v
        elif k == "__ElectionFilters" and isinstance(v, dict):
            st.session_state[special_key("election_years")] = v.get("years", [])
            st.session_state[special_key("election_types")] = v.get("types", [])
            st.session_state[special_key("election_methods")] = v.get("methods", [])
        elif k == "RegistrationMonthsAgo" and isinstance(v, dict):
            st.session_state[special_key("new_reg_months")] = int(v.get("max", 0) or 0)
        elif k == "MB_Prob_Score" and isinstance(v, dict):
            st.session_state[special_key("mb_prob_score_range")] = (int(v.get("min", 0)), int(v.get("max", 4)))
        elif k in {"V4A", "V4G", "V4P"} and isinstance(v, dict):
            label = "All Elections" if k == "V4A" else ("General Elections" if k == "V4G" else "Primary Elections")
            st.session_state[special_key("vote_score_type")] = label
            st.session_state[special_key("vote_history_score_range")] = (int(v.get("min", 0)), int(v.get("max", 4)))

    st.session_state["left_section"] = "create_universe"
    st.session_state["view"] = "targeting"
    st.rerun()


def selected(field: str):
    return st.session_state.get(filter_key(field), [])

def clear_filter_state():
    """Reset all Create Universe widgets, saved count output, and force fresh widget keys."""
    old_token = int(st.session_state.get("filter_reset_token", 0))
    prefixes = (
        "filter_",
        "new_reg_months_",
        "vote_score_type_",
        "vote_history_score_range_",
        "election_years_",
        "election_types_",
        "election_methods_",
        "mb_prob_score_range_",
        "phone_reach_mode_",
    )
    for key in list(st.session_state.keys()):
        if key.startswith(prefixes) or key in {"quick_summary", "count_mode", "exact_summary"} or key.startswith("prepared_"):
            st.session_state.pop(key, None)
    st.session_state["filter_reset_token"] = old_token + 1
    st.session_state["left_section"] = "create_universe"
    st.session_state["view"] = "targeting"
    st.rerun()

def active_filters() -> dict:
    out = {}
    for f in ALL_FILTER_FIELDS:
        vals = selected(f)
        if vals:
            out[f] = vals
    return out


def universe_label_from_filters(filters: dict) -> str:
    """Build a short human label for the currently applied universe."""
    filters = filters or {}
    priority = ["County", "Municipality", "Precinct", "School District", "School Region", "USC", "STS", "STH", "Party", "Gender", "Age_Range"]
    parts = []
    for field in priority:
        vals = filters.get(field) or []
        if vals:
            label = DISPLAY_LABELS.get(field, field)
            shown = ", ".join(map(str, vals[:3]))
            if len(vals) > 3:
                shown += f" +{len(vals)-3} more"
            parts.append(f"{label}: {shown}")
        if len(parts) >= 3:
            break
    return " | ".join(parts) if parts else "Statewide"


def save_current_universe(filters: dict, summary: dict | None = None, source: str = "Create Universe"):
    """Persist the latest applied Create Universe so other workspaces can use it."""
    clean_filters = {str(k): list(v or []) for k, v in (filters or {}).items() if v}
    st.session_state["current_universe_filters"] = clean_filters
    st.session_state["current_universe_label"] = universe_label_from_filters(clean_filters)
    st.session_state["current_universe_source"] = source
    st.session_state["current_universe_updated"] = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
    if summary is not None:
        st.session_state["current_universe_summary"] = summary


def get_current_universe_filters() -> dict:
    return dict(st.session_state.get("current_universe_filters") or {})


def has_current_universe() -> bool:
    return bool(get_current_universe_filters())


def active_geo_filters() -> dict:
    return {k: v for k, v in active_filters().items() if k in GEO_FIELDS}

def count_safe_filters(active: dict) -> dict:
    # v21: after Step 8 v18 rebuild, Update Counts supports the full targeting count cube.
    safe = set(GEO_FIELDS + [
        "Party", "Gender", "Age_Range", "CalculatedParty", "HH-Party",
        "V4A", "V4G", "V4P",
        "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "MB_Prob_Score",
        "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone",
        "RegistrationMonthsAgo",
    ])
    return {k: v for k, v in active.items() if k in safe}

def non_count_filters(active: dict) -> dict:
    safe = set(GEO_FIELDS + [
        "Party", "Gender", "Age_Range", "CalculatedParty", "HH-Party",
        "V4A", "V4G", "V4P",
        "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "MB_Prob_Score",
        "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone",
        "RegistrationMonthsAgo",
    ])
    return {k: v for k, v in active.items() if k not in safe}


def normalize_election_method_value(value) -> str:
    s = clean_value(value).upper()
    if not s:
        return ""
    if s in {"Y", "V", "VOTED", "YES"}:
        return "Voted"
    if s in {"A", "AB", "ABS", "ABSENTEE"} or "ABS" in s:
        return "Absentee"
    if s in {"M", "MB", "MAIL", "MAIL-IN", "MAIL IN", "MAILIN"} or "MAIL" in s:
        return "Mail"
    if s in {"P", "POLL", "POLLING", "IN PERSON", "IN-PERSON", "ELECTION DAY"} or "POLL" in s or "PERSON" in s:
        return "Polls"
    if s in {"PROV", "PROVISIONAL"} or "PROV" in s:
        return "Provisional"
    return clean_value(value).title()


def election_meta_from_col(col: str):
    raw = str(col).strip()
    u = re.sub(r"[^A-Z0-9]+", "_", raw.upper()).strip("_")
    patterns = [
        r"^([GPS])_?((?:20)?\d{2})(?:_|$)",
        r"^(GENERAL|PRIMARY|SPECIAL|GEN|PRI|PRIM|SPEC)_?((?:20)?\d{2})(?:_|$)",
        r"^((?:20)?\d{2})_?(GENERAL|PRIMARY|SPECIAL|GEN|PRI|PRIM|SPEC)(?:_|$)",
        r"(?:^|_)([GPS])_?((?:20)?\d{2})(?:_|$)",
        r"(?:^|_)(GENERAL|PRIMARY|SPECIAL|GEN|PRI|PRIM|SPEC)_?((?:20)?\d{2})(?:_|$)",
        r"(?:^|_)((?:20)?\d{2})_?(GENERAL|PRIMARY|SPECIAL|GEN|PRI|PRIM|SPEC)(?:_|$)",
    ]
    for pat in patterns:
        m = re.search(pat, u)
        if not m:
            continue
        a, b = m.group(1), m.group(2)
        if a.isdigit():
            yy, typ = a, b
        else:
            typ, yy = a, b
        try:
            year = int(yy) if len(str(yy)) == 4 else 2000 + int(yy)
        except Exception:
            continue
        if not (2000 <= year <= 2030):
            continue
        typ_u = str(typ).upper()
        if typ_u.startswith("G"):
            etype = "General"
        elif typ_u.startswith("P"):
            etype = "Primary"
        elif typ_u.startswith("S"):
            etype = "Special"
        else:
            etype = str(typ).title()
        return {"column": raw, "year": str(year), "type": etype}
    return None


def election_columns_from_manifest() -> list[str]:
    try:
        m = load_manifest()
        cols = []
        for section in ["index", "schema", "detail"]:
            data = m.get(section, {}) if isinstance(m, dict) else {}
            for key in ["columns", "index_columns", "detail_columns"]:
                for c in data.get(key, []) or []:
                    if election_meta_from_col(c) and c not in cols:
                        cols.append(c)
        return cols
    except Exception:
        return []


def election_options():
    metas = [election_meta_from_col(c) for c in election_columns_from_manifest()]
    metas = [m for m in metas if m]
    years = sorted({m["year"] for m in metas}, key=lambda x: int(x), reverse=True)
    types = sorted({m["type"] for m in metas})
    methods = ["Voted", "Mail", "Absentee", "Polls", "Provisional"]
    return years, types, methods


def selected_election_columns(years=None, types=None) -> list[str]:
    years = set(years or [])
    types = set(types or [])
    cols = []
    for c in election_columns_from_manifest():
        meta = election_meta_from_col(c)
        if not meta:
            continue
        if years and meta["year"] not in years:
            continue
        if types and meta["type"] not in types:
            continue
        cols.append(c)
    return cols

def vote_score_field_from_selection() -> str:
    choice = st.session_state.get(special_key("vote_score_type"), "All Elections")
    if choice == "General Elections":
        return "V4G"
    if choice == "Primary Elections":
        return "V4P"
    return "V4A"


def active_special_filters() -> dict:
    special = {}

    # Newly registered slider, expressed against RegistrationMonthsAgo from Step 8 v18.
    new_reg_months = st.session_state.get(special_key("new_reg_months"), 0)
    if new_reg_months and int(new_reg_months) > 0:
        special["RegistrationMonthsAgo"] = {"max": int(new_reg_months)}

    vh_range = st.session_state.get(special_key("vote_history_score_range"), (0, 4))
    vh_field = vote_score_field_from_selection() if "vote_score_field_from_selection" in globals() else "V4A"
    if vh_range != (0, 4):
        special[vh_field] = {"min": int(vh_range[0]), "max": int(vh_range[1])}

    mb_prob = st.session_state.get(special_key("mb_prob_score_range"), (0, 4))
    if mb_prob != (0, 4):
        special["MB_Prob_Score"] = {"min": int(mb_prob[0]), "max": int(mb_prob[1])}

    phone_mode = st.session_state.get(special_key("phone_reach_mode"), "No phone filter")
    if phone_mode and phone_mode != "No phone filter":
        special["__PhoneReach"] = phone_mode

    election_years = st.session_state.get(special_key("election_years"), [])
    election_types = st.session_state.get(special_key("election_types"), [])
    election_methods = st.session_state.get(special_key("election_methods"), [])
    if election_years or election_types or election_methods:
        special["__ElectionFilters"] = {
            "years": list(election_years or []),
            "types": list(election_types or []),
            "methods": list(election_methods or []),
        }

    return special

def apply_special_filters(df: pd.DataFrame, special: dict) -> pd.DataFrame:
    out = df
    for field, rule in (special or {}).items():
        if out.empty:
            return out

        if field == "__PhoneReach":
            mobile = out["HasMobile"].astype(str).str.lower().eq("yes") if "HasMobile" in out.columns else pd.Series(False, index=out.index)
            landline = out["HasLandline"].astype(str).str.lower().eq("yes") if "HasLandline" in out.columns else pd.Series(False, index=out.index)
            mode = str(rule)
            if mode == "Mobile only":
                out = out[mobile]
            elif mode == "Landline only":
                out = out[landline]
            elif mode == "Mobile OR landline":
                out = out[mobile | landline]
            elif mode == "Mobile AND landline":
                out = out[mobile & landline]
            elif mode == "No mobile or landline":
                out = out[~(mobile | landline)]
            continue

        if field == "__ElectionFilters" and isinstance(rule, dict):
            years = rule.get("years") or []
            types = rule.get("types") or []
            methods = set(rule.get("methods") or [])
            cols = [c for c in selected_election_columns(years, types) if c in out.columns]
            if not cols:
                out = out.iloc[0:0]
                continue
            mask = pd.Series(False, index=out.index)
            for c in cols:
                vals = out[c].map(normalize_election_method_value)
                if methods:
                    mask = mask | vals.isin(methods)
                else:
                    mask = mask | vals.astype(str).str.strip().ne("")
            out = out[mask]
            continue

        if field in out.columns and isinstance(rule, dict):
            vals = pd.to_numeric(out[field], errors="coerce")
            if "min" in rule:
                out = out[vals >= float(rule["min"])]
                vals = pd.to_numeric(out[field], errors="coerce")
            if "max" in rule:
                out = out[vals <= float(rule["max"])]
    return out

def expand_filter_values(field, vals):
    # v21c: speed tables now use clean canonical labels, so no expansion is needed.
    # Kept as a safe helper because filtering code calls it.
    return vals

def apply_filters(df: pd.DataFrame, active: dict) -> pd.DataFrame:
    out = df
    try:
        for field, vals in (active or {}).items():
            if vals and field in out.columns:
                expanded_vals = expand_filter_values(field, vals)
                out = out[out[field].astype(str).isin([str(v) for v in expanded_vals])]
        return out
    except Exception:
        return df

def options_from_geo(df: pd.DataFrame, field: str, active: dict) -> list:
    try:
        if df is None or df.empty or field not in df.columns:
            return []
        hierarchy_order = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]
        relevant = {}
        for f in hierarchy_order:
            if f == field:
                break
            if active.get(f):
                relevant[f] = active[f]
        narrowed = apply_filters(df, relevant)
        if field not in narrowed.columns:
            return []
        vals = narrowed[field].astype(str).map(clean_value)
        return sorted([v for v in vals.unique().tolist() if v], key=smart_sort_key)
    except Exception:
        return []

def options_from_filter_table(filter_options: pd.DataFrame, field: str) -> list:
    try:
        if filter_options is None or filter_options.empty:
            return []
        if "field" not in filter_options.columns or "value" not in filter_options.columns:
            return []
        vals = filter_options.loc[filter_options["field"].astype(str).eq(str(field)), "value"].astype(str).map(clean_value)
        out = sorted([v for v in vals.unique().tolist() if v], key=smart_sort_key)
        return out
    except Exception:
        return []

def clean_yes_no_all_options():
    return ["Y", "N"]

def clean_mail_options(field: str):
    fixed = {
        "MB_App": ["Applied", "Not Applied"],
        "MB_App_Status": ["Approved", "Declined"],
        "MB_Sent": ["Sent", "Not Sent"],
        "MB_Status": ["Voted", "Not Voted"],
        "MB_PERM": ["Y", "N"],
        "HasMobile": ["Yes", "No"],
        "HasLandline": ["Yes", "No"],
        "HasEmail": ["Yes", "No"],
        "HasApplicantPhone": ["Yes", "No"],
    }
    return fixed.get(field, [])

def count_cube_option_filters(field: str, active: dict) -> dict:
    """Return the filters that should narrow the dropdown for this field.

    v21f: Dropdowns are made interdependent from the rebuilt quick-count cube,
    not from detail shards. For geography, only prior geography levels are used
    so County -> Municipality -> Precinct stays predictable. For voter fields,
    all other count-safe filters are used.
    """
    active = active or {}
    if field in GEO_FIELDS:
        relevant = {}
        for f in GEO_FIELDS:
            if f == field:
                break
            if active.get(f):
                relevant[f] = active[f]
        return relevant

    relevant = count_safe_filters(active)
    relevant.pop(field, None)
    return relevant


def options_from_count_cube(field: str, active: dict) -> list:
    try:
        relevant = count_cube_option_filters(field, active)
        needed = set(relevant.keys()) | {field}
        if not field or not needed:
            return []
        cube = load_count_cube_columns(tuple(sorted(needed)))
        narrowed = apply_filters(cube, relevant)
        if field not in narrowed.columns:
            return []
        vals = narrowed[field].astype(str).map(clean_value)
        return sorted([v for v in vals.unique().tolist() if v], key=smart_sort_key)
    except Exception:
        return []


def field_options(filter_options: pd.DataFrame, field: str, active: dict | None = None):
    try:
        active = active or {}
        fixed = clean_mail_options(field)
        if fixed:
            opts = fixed
        elif field in GEO_FIELDS:
            # v21g: Use the smaller geo_hierarchy table for interdependent geography
            # when it is safe to load. Never use count_cube to draw dropdowns; that
            # was the source of the Create Universe crash.
            geo_df = load_geo_hierarchy_safe()
            opts = options_from_geo(geo_df, field, active) if geo_df is not None and not geo_df.empty else []
            if not opts:
                opts = options_from_filter_table(filter_options, field)
        else:
            # v21g: Sidebar dropdowns must stay light. Voter/contact/mail fields
            # come from filter_options; quick-count cube is used only after Update Counts.
            opts = options_from_filter_table(filter_options, field)

        # Streamlit can throw if a previously selected value disappears from options.
        # Keep current selections visible until the user clears them.
        current = [str(v) for v in (active or {}).get(field, []) if str(v).strip()]
        merged = list(opts)
        for v in current:
            if v not in merged:
                merged.append(v)
        return merged
    except Exception:
        return []

def is_cube_safe(active: dict) -> bool:
    # Geography + Party/Gender/Age_Range usually live in the count cube.
    # Anything else uses exact shard scan through the same Update Counts button.
    cube_safe = set(GEO_FIELDS + ["Party", "Gender", "Age_Range"])
    return all(k in cube_safe for k in active.keys())

def update_counts(active: dict):
    try:
        special = active_special_filters()
        if requires_remote_index_count(active or {}, special or {}):
            # Tags and specific-election filters are row-level filters. Count them
            # remotely with DuckDB over R2 index shards so Streamlit does not
            # download shards or loop through them in Python.
            summary = duckdb_index_summary(
                json.dumps(active or {}, sort_keys=True),
                json.dumps(special or {}, sort_keys=True),
            )
            return summary, "remote-index", None

        safe_active = count_safe_filters(active)
        summary = duckdb_count_cube_summary(
            json.dumps(safe_active, sort_keys=True),
            json.dumps(special or {}, sort_keys=True),
        )
        return summary, "quick", None
    except Exception as e:
        return None, "unavailable", e


def pct(n, d):
    return "0.0%" if not d else f"{(n / d) * 100:.1f}%"


def confidence_level(active: dict) -> tuple[str, str]:
    count = sum(1 for v in active.values() if v)
    voter_count = sum(1 for k, v in active.items() if k in VOTER_FIELDS and v)
    if count <= 2 and voter_count == 0:
        return "High confidence", "Quick counts are expected to match final counts for simple geography filters."
    if count <= 4 and voter_count <= 1:
        return "High confidence", "Quick counts are built from the current dataset and are suitable for exploration. Export/download files are the final source for delivery lists."
    return "Advanced filters selected", "Many filters are combined. Export/download files are the final source for delivery lists."


def find_count_col(df: pd.DataFrame) -> str | None:
    for c in ["Voters", "voters", "count", "Count", "Total", "total"]:
        if c in df.columns:
            return c
    nums = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    return nums[0] if nums else None


def summarize_from_df(df: pd.DataFrame, row_count_mode=False):
    if row_count_mode:
        total = len(df)
        if "Party" in df.columns:
            party = df["Party"].astype(str).str.upper().str.strip()
            r = int((party == "R").sum())
            d = int((party == "D").sum())
            o = int((~party.isin(["R", "D"])).sum())
        else:
            r = d = o = 0
        return {"total": total, "r": r, "d": d, "o": o}

    count_col = find_count_col(df)
    if df.empty or count_col is None:
        return {"total": 0, "r": 0, "d": 0, "o": 0}
    total = int(df[count_col].fillna(0).sum())
    r = d = o = 0
    if "Party" in df.columns:
        grouped = df.groupby("Party", dropna=False)[count_col].sum().to_dict()
        for k, v in grouped.items():
            kk = str(k).strip().upper()
            if kk == "R":
                r += int(v)
            elif kk == "D":
                d += int(v)
            else:
                o += int(v)
    return {"total": total, "r": r, "d": d, "o": o}


def render_metrics(summary, label=""):
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f'<div class="cc-metric"><div class="label">Total Voters</div><div class="value">{summary["total"]:,}</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="cc-metric"><div class="label">Republican</div><div class="value">{summary["r"]:,}</div><div class="sub">{pct(summary["r"], summary["total"])}</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="cc-metric blue"><div class="label">Democrat</div><div class="value">{summary["d"]:,}</div><div class="sub">{pct(summary["d"], summary["total"])}</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="cc-metric green"><div class="label">Other / Unaffiliated</div><div class="value">{summary["o"]:,}</div><div class="sub">{pct(summary["o"], summary["total"])}</div></div>', unsafe_allow_html=True)



def render_party_chart(summary, title="Party Breakdown"):
    """Local-style party donut. Replaces the plain Streamlit bar chart."""
    total = int(summary.get("total", 0) or 0)
    r = int(summary.get("r", 0) or 0)
    d = int(summary.get("d", 0) or 0)
    o = int(summary.get("o", 0) or 0)
    rp = round((r / total * 100), 1) if total else 0
    dp = round((d / total * 100), 1) if total else 0
    op = round((o / total * 100), 1) if total else 0
    html = f"""<div class=\"cc-home-card\"><h3>{title}</h3>
    <div class=\"cc-donut-wrap\">
      <div class=\"cc-donut\" style=\"--r:{rp};--d:{dp};--o:{op};\">
        <div class=\"cc-donut-center\"><div>{total:,}</div><div style=\"font-size:11px;color:#cbd5e1;\">Total</div></div>
      </div>
      <div style=\"min-width:260px;\">
        <div class=\"cc-legend-row\"><span class=\"cc-swatch\" style=\"background:#d51f2a\"></span><span>Republican</span><b>{r:,} ({rp:.1f}%)</b></div>
        <div class=\"cc-legend-row\"><span class=\"cc-swatch\" style=\"background:#2454d6\"></span><span>Democrat</span><b>{d:,} ({dp:.1f}%)</b></div>
        <div class=\"cc-legend-row\"><span class=\"cc-swatch\" style=\"background:#4c9a2a\"></span><span>Other / Unaffiliated</span><b>{o:,} ({op:.1f}%)</b></div>
      </div>
    </div></div>"""
    st.markdown(html, unsafe_allow_html=True)

def render_quick_exact_comparison():
    q = st.session_state.get("quick_summary")
    e = st.session_state.get("exact_summary")
    if not q or not e:
        return
    comp = pd.DataFrame([
        {"Metric": "Total", "Quick": q["total"], "Exact": e["total"], "Difference": e["total"] - q["total"]},
        {"Metric": "Republican", "Quick": q["r"], "Exact": e["r"], "Difference": e["r"] - q["r"]},
        {"Metric": "Democrat", "Quick": q["d"], "Exact": e["d"], "Difference": e["d"] - q["d"]},
        {"Metric": "Other", "Quick": q["o"], "Exact": e["o"], "Difference": e["o"] - q["o"]},
    ])
    st.markdown("### Quick vs Verified Comparison")
    st.dataframe(comp, width="stretch", hide_index=True)


def set_view(name: str):
    st.session_state["view"] = name

def render_top_nav():
    if "view" not in st.session_state:
        st.session_state["view"] = "dashboard"

    n1, n2, n3, n4 = st.columns([1, 1, 1, 1])
    with n1:
        if st.button("🏠 Dashboard", width="stretch"):
            set_view("dashboard")
            st.rerun()
    with n2:
        if st.button("🎯 Targeting", width="stretch"):
            set_view("targeting")
            st.rerun()
    with n3:
        if st.button("📊 Analysis", width="stretch"):
            set_view("analysis")
            st.rerun()
    with n4:
        if st.button("📤 Export", width="stretch"):
            set_view("export")
            st.rerun()


@st.cache_data(ttl=300, show_spinner=False)
def duckdb_count_cube_group(field: str, limit: int = 12) -> pd.DataFrame:
    """Small remote group-by for the home dashboard. Never downloads the cube."""
    field = str(field)
    if not re.fullmatch(r"[A-Za-z0-9_ /-]+", field):
        return pd.DataFrame(columns=[field, "Voters"])
    url = count_cube_url()
    query = f"""
        SELECT CAST({sql_ident(field)} AS VARCHAR) AS label, SUM(Voters) AS Voters
        FROM read_parquet({sql_lit(url)})
        WHERE CAST({sql_ident(field)} AS VARCHAR) IS NOT NULL
          AND TRIM(CAST({sql_ident(field)} AS VARCHAR)) <> ''
        GROUP BY CAST({sql_ident(field)} AS VARCHAR)
        ORDER BY Voters DESC
        LIMIT {int(limit)}
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try: con.execute("LOAD httpfs;")
            except Exception: pass
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame(columns=["label", "Voters"])
    finally:
        try: con.close()
        except Exception: pass


@st.cache_data(ttl=300, show_spinner=False)
def duckdb_count_cube_group_filtered(active_json: str, special_json: str, field: str, limit: int = 20) -> pd.DataFrame:
    """Remote quick-count group by from the count cube. Does not scan detail/index shards."""
    active = json.loads(active_json or "{}")
    special = json.loads(special_json or "{}")
    if not re.fullmatch(r"[A-Za-z0-9_ /-]+", str(field)):
        return pd.DataFrame(columns=["label", "Voters"])
    url = count_cube_url()
    where = count_cube_where_sql(active, special)
    query = f"""
        SELECT CAST({sql_ident(field)} AS VARCHAR) AS label, SUM(Voters) AS Voters
        FROM read_parquet({sql_lit(url)})
        {where}
        GROUP BY CAST({sql_ident(field)} AS VARCHAR)
        HAVING SUM(Voters) > 0
        ORDER BY Voters DESC
        LIMIT {int(limit)}
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try: con.execute("LOAD httpfs;")
            except Exception: pass
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame(columns=["label", "Voters"])
    finally:
        try: con.close()
        except Exception: pass

@st.cache_data(ttl=300, show_spinner=False)
def duckdb_county_party_table(limit: int = 67) -> pd.DataFrame:
    """County by party table for the load screen from the remote count cube."""
    url = count_cube_url()
    query = f"""
        SELECT
            CAST(County AS VARCHAR) AS County,
            SUM(Voters) AS Total,
            SUM(CASE WHEN CAST(Party AS VARCHAR)='R' THEN Voters ELSE 0 END) AS Republican,
            SUM(CASE WHEN CAST(Party AS VARCHAR)='D' THEN Voters ELSE 0 END) AS Democrat,
            SUM(CASE WHEN CAST(Party AS VARCHAR) NOT IN ('R','D') THEN Voters ELSE 0 END) AS Other
        FROM read_parquet({sql_lit(url)})
        WHERE CAST(County AS VARCHAR) IS NOT NULL AND TRIM(CAST(County AS VARCHAR)) <> ''
        GROUP BY CAST(County AS VARCHAR)
        ORDER BY County
        LIMIT {int(limit)}
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try: con.execute("LOAD httpfs;")
            except Exception: pass
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame(columns=["County","Total","Republican","Democrat","Other"])
    finally:
        try: con.close()
        except Exception: pass

def render_icon_metric(label: str, value: int, sub: str = "", icon: str = "●", klass: str = ""):
    html = f'<div class="cc-icon-metric {klass}"><div class="cc-icon-dot {klass}">{icon}</div><div><div class="cc-icon-label">{label}</div><div class="cc-icon-value">{int(value or 0):,}</div><div class="cc-icon-sub">{sub}</div></div></div>'
    st.markdown(html, unsafe_allow_html=True)

def render_home_age_card(total: int):
    age = duckdb_count_cube_group("Age_Range", 12)
    if age.empty or "Voters" not in age.columns:
        st.markdown('<div class="cc-home-card"><h3>Voters by Age Range</h3><p>Age range quick-count data is not available.</p></div>', unsafe_allow_html=True)
        return
    rows = []
    order = {"18-24":1,"25-34":2,"35-44":3,"45-54":4,"55-64":5,"65+":6,"65-74":7,"75-84":8,"85+":9}
    age["label"] = age["label"].astype(str).str.strip()
    age = age[~age["label"].str.lower().isin(["", "(blank)", "blank", "nan", "none", "null"])]
    age = age[age["Voters"].fillna(0).astype(float) > 0]
    age["sort"] = age["label"].map(lambda x: order.get(str(x), 99))
    age = age.sort_values(["sort", "label"]).head(9)
    maxv = max(int(age["Voters"].max() or 1), 1)
    for _, r in age.iterrows():
        lab = str(r.get("label", ""))
        val = int(r.get("Voters", 0) or 0)
        p = (val / total * 100) if total else 0
        w = max(2, val / maxv * 100)
        rows.append(f'<div class="cc-age-row"><b>{lab}</b><div class="cc-age-bar-bg"><div class="cc-age-bar" style="width:{w:.1f}%"></div></div><span>{p:.1f}%</span></div>')
    html = '<div class="cc-home-card"><h3>Voters by Age Range</h3>' + ''.join(rows) + '<div style="color:#94a3b8;font-size:12px;margin-top:10px;">Universe: All Voters</div></div>'
    st.markdown(html, unsafe_allow_html=True)

def render_home_geo_table(summary: dict):
    df = duckdb_county_party_table(67)
    if df.empty:
        st.markdown('<div class="cc-home-card"><h3>County Breakdown</h3><p>County quick-count data is not available.</p></div>', unsafe_allow_html=True)
        return
    show = df.copy()
    for c in ["Total","Republican","Democrat","Other"]:
        if c in show.columns:
            show[c] = show[c].fillna(0).astype(int).map(lambda x: f"{x:,}")
    st.markdown('<div class="cc-home-card"><h3>County Breakdown by Party</h3>', unsafe_allow_html=True)
    st.dataframe(show, hide_index=True, width="stretch", height=235)
    st.markdown('</div>', unsafe_allow_html=True)


def render_statewide_snapshot():
    st.markdown('<div class="cc-home-title">Voters Statewide</div>', unsafe_allow_html=True)

    summary = None
    err = None
    try:
        summary, err = quick_counts({})
    except Exception as e:
        err = e

    if not summary:
        try:
            total = int(manifest.get("total_rows", 0)) if isinstance(manifest, dict) else 0
        except Exception:
            total = 0
        summary = {"total": total, "r": 0, "d": 0, "o": 0}

    total = int(summary.get("total", 0) or 0)
    r = int(summary.get("r", 0) or 0)
    d = int(summary.get("d", 0) or 0)
    o = int(summary.get("o", 0) or 0)

    c1, c2, c3, c4 = st.columns(4)
    with c1: render_icon_metric("Total Voters", total, "100% of universe", "👥", "")
    with c2: render_icon_metric("Republican", r, pct(r, total) + " of universe", "🐘", "")
    with c3: render_icon_metric("Democrat", d, pct(d, total) + " of universe", "🫏", "blue")
    with c4: render_icon_metric("Other / Unaffiliated", o, pct(o, total) + " of universe", "●", "green")

    left, right = st.columns([1.0, 1.25])
    with left:
        render_party_chart(summary, "Voters by Party")
        gdf = duckdb_count_cube_group("Gender", 8)
        if not gdf.empty and "Voters" in gdf.columns:
            gf = {str(row.get("label", "")).upper(): int(row.get("Voters", 0) or 0) for _, row in gdf.iterrows()}
            gs = {"total": sum(gf.values()), "r": gf.get("F", 0), "d": gf.get("M", 0), "o": sum(v for k,v in gf.items() if k not in {"F","M"})}
            render_party_chart(gs, "Voters by Gender")
    with right:
        render_home_age_card(total)
        render_home_geo_table(summary)

    if err and not (r or d or o):
        st.warning("Quick-count statewide party numbers were not available, so the app showed the manifest total only.")
    st.caption("Use the sidebar to build a campaign universe, search voters, open Mail Ballot Center, or view Area Intelligence.")

def quick_counts(active: dict):
    # v21i: use DuckDB against the remote quick-count parquet so Streamlit does
    # not download the entire count_cube into memory when Gender or other voter
    # filters are selected.
    try:
        summary = duckdb_count_cube_summary(
            json.dumps(count_safe_filters(active or {}), sort_keys=True),
            json.dumps({}, sort_keys=True),
        )
        return summary, None
    except Exception as e:
        return None, e



def special_required_columns(special: dict) -> set[str]:
    cols = set()
    if not special:
        return cols
    if "__PhoneReach" in special:
        cols.update(["HasMobile", "HasLandline"])
    ef = special.get("__ElectionFilters")
    if isinstance(ef, dict):
        cols.update(selected_election_columns(ef.get("years") or [], ef.get("types") or []))
    for k in special.keys():
        if not str(k).startswith("__"):
            cols.add(k)
    return cols

def exact_counts(active: dict):
    special = active_special_filters()
    needed = set(["Party"])
    needed.update(active.keys())
    needed.update(special_required_columns(special))
    cols = tuple(sorted(needed))

    total = 0
    r_count = 0
    d_count = 0
    o_count = 0

    progress = st.progress(0)
    status = st.empty()

    shard_count = int((load_manifest().get("index", {}) or {}).get("count", DETAIL_SHARDS) or DETAIL_SHARDS)
    for i in range(shard_count):
        key = f"index/voters_index_{i:03d}.parquet"
        status.write(f"Counting index shard {i+1} of {shard_count}: {key}")
        df = load_index_columns(key, cols)

        for col, vals in active.items():
            if vals and col == "Tags" and col in df.columns:
                df = df[tag_contains_mask(df[col], vals)]
            elif vals and col in df.columns:
                expanded_vals = expand_filter_values(col, vals)
                df = df[df[col].astype(str).isin([str(v) for v in expanded_vals])]
            elif vals:
                df = df.iloc[0:0]

        df = apply_special_filters(df, special)

        total += len(df)

        if "Party" in df.columns and not df.empty:
            party = df["Party"].astype(str).str.upper().str.strip()
            r_count += int((party == "R").sum())
            d_count += int((party == "D").sum())
            o_count += int((~party.isin(["R", "D"])).sum())

        del df
        progress.progress((i + 1) / shard_count)

    status.empty()
    return {"total": total, "r": r_count, "d": d_count, "o": o_count}


def build_export(active: dict, columns: list[str]):
    special = active_special_filters()
    if not active and not special:
        raise RuntimeError("Please select at least one filter before exporting.")

    needed = set(columns)
    needed.update(active.keys())
    needed.update(special_required_columns(special))
    cols = tuple(sorted(needed))

    parts = []
    total = 0
    progress = st.progress(0)
    status = st.empty()

    for i in range(DETAIL_SHARDS):
        key = f"detail/voters_detail_{i:03d}.parquet"
        status.write(f"Building export from shard {i+1} of {DETAIL_SHARDS}: {key}")
        df = load_detail_columns(key, cols)

        for col, vals in active.items():
            if vals and col == "Tags" and col in df.columns:
                df = df[tag_contains_mask(df[col], vals)]
            elif vals and col in df.columns:
                expanded_vals = expand_filter_values(col, vals)
                df = df[df[col].astype(str).isin([str(v) for v in expanded_vals])]
            elif vals:
                df = df.iloc[0:0]

        df = apply_special_filters(df, special)

        if not df.empty:
            keep_cols = [c for c in columns if c in df.columns]
            if keep_cols:
                df = df[keep_cols]
            parts.append(df)
            total += len(df)
            if total > EXPORT_ROW_LIMIT:
                raise RuntimeError(f"Export exceeds {EXPORT_ROW_LIMIT:,} rows. Narrow filters before exporting.")

        progress.progress((i + 1) / DETAIL_SHARDS)

    status.empty()
    if not parts:
        return pd.DataFrame(columns=columns)
    return pd.concat(parts, ignore_index=True)




# -----------------------------------------------------------------------------
# Restored workspace helpers v21r (safe, remote-query-first)
# -----------------------------------------------------------------------------
def cc_text(v):
    try:
        if pd.isna(v): return ""
    except Exception:
        pass
    s = str(v).strip()
    if s.lower() in {"nan", "none", "null"}: return ""
    return re.sub(r"\s+", " ", s)


def smart_title(value, keep_upper: set[str] | None = None) -> str:
    s = cc_text(value)
    if not s:
        return ""
    keep_upper = keep_upper or {"PA","USA","US","PO","P.O.","LLC","III","IV","II","JR","SR","MDJ","SD","TWP","USC","STS","STH"}
    def one_token(tok: str) -> str:
        raw = tok
        lead = re.match(r"^([^A-Za-z0-9#]*)(.*?)([^A-Za-z0-9]*)$", raw)
        if not lead:
            return raw
        pre, core, post = lead.groups()
        if not core:
            return raw
        up = core.upper().replace('.', '')
        if up in keep_upper or re.fullmatch(r"[IVXLCM]+", up):
            return pre + up + post
        if re.fullmatch(r"\d+[A-Z]?", core):
            return pre + core.upper() + post
        if "-" in core:
            return pre + "-".join(one_token(part) for part in core.split("-")) + post
        if "'" in core:
            return pre + "'".join(one_token(part) for part in core.split("'")) + post
        return pre + core[:1].upper() + core[1:].lower() + post
    return " ".join(one_token(t) for t in re.sub(r"\s+", " ", s).split(" ")).strip()


def normalize_name_suffix(value) -> str:
    s = cc_text(value).replace('.', '')
    if not s:
        return ""
    up = s.upper()
    if up in {"JR","SR","II","III","IV","V","VI"}:
        return up
    return smart_title(s)


def normalize_phone_digits(value) -> str:
    s = cc_text(value)
    digits = re.sub(r"\D+", "", s)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits or s


def mark_downloaded(*keys):
    for k in keys:
        st.session_state.pop(k, None)


def canonical_precinct_display(value, municipality=""):
    """Normalize obvious duplicate precinct labels for display/export.
    Example: YORK 1ST WARD - 2ND PRECINCT -> York Township Precinct 540102.
    This is an app-side display/export guardrail until the same fix is moved into Step 8.
    """
    raw = cc_text(value)
    if not raw:
        return ""
    s = raw.upper().replace("–", "-").replace("—", "-")
    m = re.search(r"\bYORK\s+(\d+)(?:ST|ND|RD|TH)?\s+WARD\s*-\s*(\d+)(?:ST|ND|RD|TH)?\s+PRECINCT\b", s)
    if m:
        ward = int(m.group(1)); pct = int(m.group(2))
        return f"York Township Precinct 540{ward}0{pct}"
    # Standardize case for numeric township precincts too.
    m2 = re.search(r"\bYORK\s+TOWNSHIP\s+PRECINCT\s+(\d{6})\b", s)
    if m2:
        return f"York Township Precinct {m2.group(1)}"
    return raw


def clean_apartment_and_address2(df: pd.DataFrame) -> pd.DataFrame:
    """Keep Apartment Number to true unit values only; move other extra address text to Address Line 2."""
    if df is None or df.empty:
        return df
    df = df.copy()
    for c in ["Apartment Number", "Address Line 2"]:
        if c not in df.columns:
            df[c] = ""
    apt = df["Apartment Number"].astype(str).replace({"nan":"", "None":"", "<NA>":""}).str.strip()
    line2 = df["Address Line 2"].astype(str).replace({"nan":"", "None":"", "<NA>":""}).str.strip()
    apt_unit = re.compile(r"^(APT|APARTMENT|UNIT|#)\s*[A-Z0-9][A-Z0-9\-]*$", re.I)
    bare_apt = re.compile(r"^[A-Z0-9][A-Z0-9\-]{0,6}$", re.I)
    address2_unit = re.compile(r"^(STE|SUITE|RM|ROOM|FL|FLOOR|BLDG|BUILDING|TRLR|TRAILER|LOT|PO BOX|P\.?O\.? BOX|BOX)\b", re.I)
    street_words = re.compile(r"\b(ST|STREET|RD|ROAD|DR|DRIVE|AVE|AVENUE|LN|LANE|CT|COURT|CIR|CIRCLE|BLVD|WAY|PIKE|HWY|HIGHWAY|PKWY|PARKWAY|TER|TERRACE|PL|PLACE)\b", re.I)
    new_apt=[]; new_line2=[]
    for a,l in zip(apt, line2):
        aa=a.strip(); ll=l.strip()
        if not aa:
            new_apt.append(""); new_line2.append(ll); continue
        # Keep Apartment Number strict: only true apartment/unit identifiers.
        # Suite, PO Box, building/floor/trailer/lot and stray street text belong in Address Line 2.
        if street_words.search(aa) and not apt_unit.search(aa):
            combined = (ll + " " + aa).strip() if ll else aa
            new_apt.append(""); new_line2.append(combined); continue
        if address2_unit.search(aa):
            combined = (ll + " " + aa).strip() if ll else aa
            new_apt.append(""); new_line2.append(combined); continue
        if apt_unit.search(aa) or bare_apt.fullmatch(aa):
            new_apt.append(smart_title(aa, keep_upper={"APT","UNIT","PO","PA","JR","SR","III","IV"}))
            new_line2.append(ll)
        else:
            combined = (ll + " " + aa).strip() if ll else aa
            new_apt.append(""); new_line2.append(combined)
    df["Apartment Number"] = new_apt
    df["Address Line 2"] = [smart_title(x) for x in new_line2]
    return df

def household_display_name(group: pd.DataFrame) -> str:
    voters = group.copy()
    voters["_fn"] = voters.apply(full_name, axis=1).map(smart_title)
    voters["_last"] = voters.get("LastName", pd.Series([""]*len(voters), index=voters.index)).map(lambda x: smart_title(x).strip())
    names = [x for x in voters["_fn"].tolist() if x]
    lasts = [x for x in voters["_last"].tolist() if x]
    uniq_lasts = sorted({x for x in lasts if x})
    if len(names) == 0:
        return "Current Resident"
    if len(names) == 1:
        return names[0]
    if len(uniq_lasts) == 1:
        return f"{uniq_lasts[0]} Household"
    if len(names) <= 3:
        return " & ".join(names)
    return f"{names[0]} & Family"


def household_for_mail(df: pd.DataFrame) -> pd.DataFrame:
    """One mail row per household/address using the local app household naming logic."""
    if df is None or df.empty:
        return df
    df = normalize_download_df(df).copy()
    for c in ["County","Municipality","House Number","Street Name","Apartment Number","City","State","Zip"]:
        if c not in df.columns: df[c] = ""
    key = (df["County"].astype(str).str.upper().str.strip()+"|"+
           df["Municipality"].astype(str).str.upper().str.strip()+"|"+
           df["House Number"].astype(str).str.upper().str.strip()+"|"+
           df["Street Name"].astype(str).str.upper().str.strip()+"|"+
           df["Apartment Number"].astype(str).str.upper().str.strip()+"|"+
           df["City"].astype(str).str.upper().str.strip()+"|"+
           df["State"].astype(str).str.upper().str.strip()+"|"+
           df["Zip"].astype(str).str.upper().str.strip())
    df["_HH_KEY"] = key
    df["HouseholdCount"] = df.groupby("_HH_KEY")["_HH_KEY"].transform("size")
    hh_names = df.groupby("_HH_KEY", sort=False).apply(household_display_name).to_dict()
    out = df.sort_values(["Street Name","House Number","LastName","FirstName"], kind="stable").drop_duplicates("_HH_KEY", keep="first").copy()
    out["HouseholdName"] = out["_HH_KEY"].map(hh_names).fillna("")
    out["FullName"] = out["HouseholdName"]
    out["FirstName"] = out["HouseholdName"]
    out["MiddleName"] = ""
    out["LastName"] = ""
    out["NameSuffix"] = ""
    return out.drop(columns=["_HH_KEY"], errors="ignore")

def full_name(row):
    """Best voter display name, with strong fallbacks for lookup/household cards."""
    def usable(v):
        t = cc_text(v).strip()
        return "" if t.lower() in {"unnamed voter", "unnamed", "unknown", "none", "nan", "null"} else t

    # Prefer already-canonical display names if present. This prevents blank/partial
    # segmented fields from overriding a good FullName/Name value in the speed shards.
    for c in ["FullName", "Full Name", "Name", "VoterName", "Voter Name"]:
        val = usable(row.get(c, ""))
        if val:
            return val

    first = usable(row.get("FirstName", "")) or usable(row.get("first_name", "")) or usable(row.get("FIRST_NAME", ""))
    middle = usable(row.get("MiddleName", "")) or usable(row.get("middle_name", "")) or usable(row.get("MIDDLE_NAME", ""))
    last = usable(row.get("LastName", "")) or usable(row.get("last_name", "")) or usable(row.get("LAST_NAME", ""))
    suffix = usable(row.get("NameSuffix", "")) or usable(row.get("suffix", "")) or usable(row.get("SUFFIX", ""))
    parts = [first, middle, last, suffix]
    name = " ".join([p for p in parts if p]).strip()
    if name:
        return name

    # Last-resort fallbacks that are still better than showing repeated Unnamed Voter.
    vid = usable(row.get("voter_id", ""))
    return f"Voter {vid}" if vid else "Household Voter"

def address_line(row):
    hn = cc_text(row.get("House Number", ""))
    stn = cc_text(row.get("Street Name", ""))
    apt = cc_text(row.get("Apartment Number", ""))
    line = " ".join([x for x in [hn, stn] if x])
    return (line + (f" Apt {apt}" if apt else "")).strip() or cc_text(row.get("res_address", ""))

def format_phone_number(value):
    s = cc_text(value)
    digits = re.sub(r"\D+", "", s)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return s

def phone_entries(row):
    entries = []
    mobile = cc_text(row.get("Mobile", "")) or cc_text(row.get("MobilePhone", ""))
    land = cc_text(row.get("Landline", "")) or cc_text(row.get("Phone", ""))
    app = cc_text(row.get("Current_ApplicantPhone", "")) or cc_text(row.get("ApplicantPhone", ""))
    if mobile:
        entries.append((format_phone_number(mobile), "m"))
    if land:
        entries.append((format_phone_number(land), "l"))
    if app and app not in {mobile, land}:
        entries.append((format_phone_number(app), "u"))
    # De-dupe exact label/type pairs while preserving order.
    seen = set(); clean = []
    for num, typ in entries:
        key = (num, typ)
        if num and key not in seen:
            seen.add(key); clean.append((num, typ))
    return clean

def phone_label(row):
    return " / ".join([f"{num} ({typ})" for num, typ in phone_entries(row)])

def first_existing_col(df: pd.DataFrame, candidates: list[str]):
    if df is None or df.empty:
        return None
    wanted = [re.sub(r"[^a-z0-9]+", "", str(x).lower()) for x in candidates]
    for w in wanted:
        for c in df.columns:
            if re.sub(r"[^a-z0-9]+", "", str(c).lower()) == w:
                return c
    return None

def matching_cols(df: pd.DataFrame, candidates: list[str]) -> list[str]:
    if df is None or df.empty:
        return []
    wanted = [re.sub(r"[^a-z0-9]+", "", str(x).lower()) for x in candidates]
    hits=[]
    for w in wanted:
        for c in df.columns:
            if c in hits:
                continue
            if re.sub(r"[^a-z0-9]+", "", str(c).lower()) == w:
                hits.append(c)
    return hits

def coalesce_columns(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    out = pd.Series([""] * len(df), index=df.index, dtype="object")
    # Use every matching column, not just the first normalized hit. This fixes shards
    # that contain both a blank display column (FirstName) and a populated source
    # column (first_name / First Name).
    for col in matching_cols(df, candidates):
        vals = df[col].astype(str).replace({"nan":"", "None":"", "<NA>":""}).str.strip()
        mask = out.astype(str).str.strip().eq("") & vals.ne("")
        out.loc[mask] = vals.loc[mask]
    return out

def normalize_download_df(df: pd.DataFrame) -> pd.DataFrame:
    """Repair/standardize downloaded fields from current detail shards, with vendor-friendly casing."""
    if df is None or df.empty:
        return pd.DataFrame(columns=DEFAULT_EXPORT_COLUMNS)
    df = df.copy()
    aliases = {
        "voter_id": ["voter_id", "VoterID", "Voter ID", "IDNumber", "ID Number", "PA ID Number", "PA_ID_Number", "SURE_ID", "StateVoterID"],
        "County": ["County", "county", "CountyName"],
        "Municipality": ["Municipality", "municipality", "municipality_clean", "Municipality_Clean"],
        "Precinct": ["Precinct", "precinct", "precinct_name", "PrecinctName", "Current_PrecinctDesc"],
        "FirstName": ["FirstName", "First Name", "First_Name", "FIRSTNAME", "FIRST_NAME", "first_name", "fname", "FName", "FNAME", "first", "FIRST", "GivenName", "Given Name", "Given_Name", "NameFirst", "Name First", "NAMEFIRST", "NAME_FIRST", "name_first", "Voter First Name", "VoterFirstName", "Voter_First_Name", "Registrant First Name", "RegistrantFirstName", "Registrant_First_Name", "Given", "Given_Name", "FirstNm", "First_Nm"],
        "MiddleName": ["MiddleName", "Middle Name", "Middle_Name", "MIDDLENAME", "MIDDLE_NAME", "middle_name", "middle", "MiddleInitial", "Middle Initial", "middle_initial", "MName", "MI", "NameMiddle", "Name Middle", "NAME_MIDDLE", "name_middle", "Voter Middle Name", "VoterMiddleName", "Voter_Middle_Name", "Registrant Middle Name", "RegistrantMiddleName", "Registrant_Middle_Name", "MiddleNm", "Middle_Nm"],
        "LastName": ["LastName", "Last Name", "Last_Name", "LASTNAME", "LAST_NAME", "last_name", "surname", "lname", "LName", "LNAME", "last", "LAST", "FamilyName", "Family Name", "NameLast", "Name Last", "NAMELAST", "NAME_LAST", "name_last", "Voter Last Name", "VoterLastName", "Voter_Last_Name", "Registrant Last Name", "RegistrantLastName", "Registrant_Last_Name", "Surname", "LastNm", "Last_Nm"],
        "NameSuffix": ["NameSuffix", "Name Suffix", "Name_Suffix", "NAMESUFFIX", "suffix", "Suffix", "surnsuffix", "SurnSuffix", "SuffixName", "NameSuffixCode", "Name Suffix Code", "Suffix_Code"],
        "FullName": ["FullName", "Full Name", "Full_Name", "FULLNAME", "Name", "name", "VoterName", "Voter Name", "Voter_Name", "Voter Full Name", "VoterFullName", "Voter_Full_Name", "Registrant Name", "RegistrantName", "Registrant_Name", "DisplayName", "Display Name"],
        "Party": ["Party", "party", "party_raw", "PartyCode", "RegisteredParty"],
        "Gender": ["Gender", "gender", "Sex", "sex"],
        "DOB": ["DOB", "DateOfBirth", "Date of Birth", "Date_of_Birth", "DATEOFBIRTH", "BirthDate", "Birth Date", "birth_date", "dob"],
        "Age": ["Age", "age", "Age_Calc"],
        "Age_Range": ["Age_Range", "age_group", "Age Group"],
        "RegistrationDate": ["RegistrationDate", "Registration Date", "registration_date"],
        "House Number": ["House Number", "HouseNumber", "house_number", "res_house_number", "house_num", "street_number"],
        "House Number Suffix": ["House Number Suffix", "HouseNumberSuffix", "house_number_suffix"],
        "Street Name": ["Street Name", "StreetName", "street_name", "res_street_name", "street", "address_street"],
        "Apartment Number": ["Apartment Number", "ApartmentNumber", "Unit", "Apt", "apartment_number"],
        "Address Line 2": ["Address Line 2", "AddressLine2", "Address2", "address_line_2"],
        "City": ["City", "city", "res_city", "Mail City"],
        "State": ["State", "state", "res_state", "Mail State"],
        "Zip": ["Zip", "ZIP", "ZipCode", "zipcode", "res_zip", "Mail Zip"],
        "Email": ["Email", "EMAIL", "Current_Email", "email"],
        "Mobile": ["Mobile", "MobilePhone", "mobile_phone", "Cell", "CellPhone"],
        "Landline": ["Landline", "Phone", "phone", "HomePhone"],
        "Current_ApplicantPhone": ["Current_ApplicantPhone", "ApplicantPhone", "Applicant Phone"],
    }
    for out_col, cands in aliases.items():
        if out_col not in df.columns or df[out_col].astype(str).replace({"nan":""}).str.strip().eq("").mean() > .80:
            df[out_col] = coalesce_columns(df, cands)

    for c in ["County","Municipality","FirstName","MiddleName","LastName","Street Name","Apartment Number","Address Line 2","City","School District","School Region"]:
        if c in df.columns:
            df[c] = df[c].map(smart_title)
    if "NameSuffix" in df.columns:
        df["NameSuffix"] = df["NameSuffix"].map(normalize_name_suffix)
    if "State" in df.columns:
        df["State"] = df["State"].map(lambda x: cc_text(x).upper())
    for c in ["Mobile","Landline","Current_ApplicantPhone"]:
        if c in df.columns:
            df[c] = df[c].map(normalize_phone_digits)

    parts = []
    for c in ["FirstName", "MiddleName", "LastName", "NameSuffix"]:
        parts.append(df.get(c, pd.Series([""]*len(df), index=df.index)).astype(str).replace({"nan":""}).str.strip())
    built_full = (parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]).str.replace(r"\s+", " ", regex=True).str.strip()
    df["FullName"] = built_full.where(built_full.str.strip().ne(""), df.get("FullName", pd.Series([""]*len(df), index=df.index)).map(smart_title))

    if "Party" in df.columns:
        df["Party"] = df["Party"].map(lambda x: "R" if str(x).strip().upper() in {"R","REP","REPUBLICAN"} else ("D" if str(x).strip().upper() in {"D","DEM","DEMOCRAT","DEMOCRATIC"} else ("O" if str(x).strip() else "")))
    if "Gender" in df.columns:
        df["Gender"] = df["Gender"].map(lambda x: "M" if str(x).strip().upper() in {"M","MALE"} else ("F" if str(x).strip().upper() in {"F","FEMALE"} else ("U" if str(x).strip() else "")))

    for c in DEFAULT_EXPORT_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    if "Precinct" in df.columns:
        muni_series = df["Municipality"] if "Municipality" in df.columns else pd.Series([""]*len(df), index=df.index)
        df["Precinct"] = [canonical_precinct_display(p, m) for p, m in zip(df["Precinct"], muni_series)]
    df = clean_apartment_and_address2(df)
    election_cols = [c for c in df.columns if re.match(r"^[GPS]\d{2}(?:_\d+)?$", str(c)) or re.match(r"^[GPS]\d{2}(?:_\d+)?_method$", str(c))]
    ordered = DEFAULT_EXPORT_COLUMNS + [c for c in election_cols if c not in DEFAULT_EXPORT_COLUMNS]
    return df[ordered]

def drop_all_blank_optional_columns(df: pd.DataFrame, required: list[str] | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    required = required or ["voter_id"]
    keep = []
    for c in df.columns:
        nonblank = df[c].astype(str).replace({"nan":"", "None":""}).str.strip().ne("").any()
        if nonblank or c in required:
            keep.append(c)
    return df[keep]



def source_alias_candidates():
    return {
        "voter_id": ["voter_id", "VoterID", "Voter ID", "IDNumber", "ID Number", "PA ID Number", "PA_ID_Number", "SURE_ID", "StateVoterID"],
        "County": ["County", "county", "CountyName"],
        "Municipality": ["Municipality", "municipality", "municipality_clean", "Municipality_Clean"],
        "Precinct": ["Precinct", "precinct", "precinct_name", "PrecinctName", "Current_PrecinctDesc"],
        "FirstName": ["FirstName", "First Name", "First_Name", "FIRSTNAME", "FIRST_NAME", "first_name", "fname", "FName", "FNAME", "first", "FIRST", "GivenName", "Given Name", "Given_Name", "NameFirst", "Name First", "NAMEFIRST", "NAME_FIRST", "name_first", "Voter First Name", "VoterFirstName", "Voter_First_Name", "Registrant First Name", "RegistrantFirstName", "Registrant_First_Name", "Given", "Given_Name", "FirstNm", "First_Nm"],
        "MiddleName": ["MiddleName", "Middle Name", "Middle_Name", "MIDDLENAME", "MIDDLE_NAME", "middle_name", "middle", "MiddleInitial", "Middle Initial", "middle_initial", "MName", "MI", "NameMiddle", "Name Middle", "NAME_MIDDLE", "name_middle", "Voter Middle Name", "VoterMiddleName", "Voter_Middle_Name", "Registrant Middle Name", "RegistrantMiddleName", "Registrant_Middle_Name", "MiddleNm", "Middle_Nm"],
        "LastName": ["LastName", "Last Name", "Last_Name", "LASTNAME", "LAST_NAME", "last_name", "surname", "lname", "LName", "LNAME", "last", "LAST", "FamilyName", "Family Name", "NameLast", "Name Last", "NAMELAST", "NAME_LAST", "name_last", "Voter Last Name", "VoterLastName", "Voter_Last_Name", "Registrant Last Name", "RegistrantLastName", "Registrant_Last_Name", "Surname", "LastNm", "Last_Nm"],
        "NameSuffix": ["NameSuffix", "Name Suffix", "Name_Suffix", "NAMESUFFIX", "suffix", "Suffix", "surnsuffix", "SurnSuffix", "SuffixName", "NameSuffixCode", "Name Suffix Code", "Suffix_Code"],
        "FullName": ["FullName", "Full Name", "Full_Name", "FULLNAME", "Name", "name", "VoterName", "Voter Name", "Voter_Name", "Voter Full Name", "VoterFullName", "Voter_Full_Name", "Registrant Name", "RegistrantName", "Registrant_Name", "DisplayName", "Display Name"],
        "Party": ["Party", "party", "party_raw", "PartyCode", "RegisteredParty"],
        "Gender": ["Gender", "gender", "Sex", "sex"],
        "DOB": ["DOB", "DateOfBirth", "Date of Birth", "Date_of_Birth", "DATEOFBIRTH", "BirthDate", "Birth Date", "birth_date", "dob"],
        "Age": ["Age", "age", "Age_Calc"],
        "Age_Range": ["Age_Range", "age_group", "Age Group"],
        "RegistrationDate": ["RegistrationDate", "Registration Date", "registration_date"],
        "House Number": ["House Number", "HouseNumber", "house_number", "res_house_number", "house_num", "street_number"],
        "House Number Suffix": ["House Number Suffix", "HouseNumberSuffix", "house_number_suffix"],
        "Street Name": ["Street Name", "StreetName", "street_name", "res_street_name", "street", "address_street"],
        "Apartment Number": ["Apartment Number", "ApartmentNumber", "Unit", "Apt", "apartment_number"],
        "Address Line 2": ["Address Line 2", "AddressLine2", "Address2", "address_line_2"],
        "City": ["City", "city", "res_city", "Mail City"],
        "State": ["State", "state", "res_state", "Mail State"],
        "Zip": ["Zip", "ZIP", "ZipCode", "zipcode", "res_zip", "Mail Zip"],
        "Email": ["Email", "EMAIL", "Current_Email", "email"],
        "Mobile": ["Mobile", "MobilePhone", "mobile_phone", "Cell", "CellPhone"],
        "Landline": ["Landline", "Phone", "phone", "HomePhone"],
        "Current_ApplicantPhone": ["Current_ApplicantPhone", "ApplicantPhone", "Applicant Phone"],
        "MB_PERM": ["MB_PERM", "MB Perm", "MBPerm", "PermanentMB", "MB_Perm"],
        "MB_App": ["MB_App", "MB App"],
        "MB_App_Status": ["MB_App_Status", "MB App Status"],
        "MB_Sent": ["MB_Sent", "MB Sent"],
        "MB_Status": ["MB_Status", "MB Status"],
        "Tags": ["Tags", "tags", "Tag", "tag"],
    }

@st.cache_data(ttl=900, show_spinner=False)
def remote_parquet_columns(urls) -> list[str]:
    # Fast schema check: shard schemas are consistent, so inspect only the first URL.
    # The old version inspected the full URL list, which made voter lookup feel stuck.
    one_url = urls[0] if isinstance(urls, (list, tuple)) and urls else urls
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        df0 = con.execute(f"SELECT * FROM read_parquet({one_url!r}, union_by_name=true) LIMIT 0").df()
        return list(df0.columns)
    finally:
        con.close()

def first_existing_column(existing_cols, candidates):
    existing_map = {str(c).lower(): c for c in existing_cols}
    for cand in candidates:
        if str(cand).lower() in existing_map:
            return existing_map[str(cand).lower()]
    return None

def safe_remote_select_exprs(existing_cols, out_cols):
    aliases = source_alias_candidates()
    exprs = []
    for out_col in out_cols:
        src = first_existing_column(existing_cols, aliases.get(out_col, [out_col]))
        if src:
            exprs.append(f"CAST({sql_ident(src)} AS VARCHAR) AS {sql_ident(out_col)}")
        else:
            exprs.append(f"CAST(NULL AS VARCHAR) AS {sql_ident(out_col)}")
    return ", ".join(exprs)

def safe_search_blob_expr(existing_cols):
    aliases = source_alias_candidates()
    search_out = ["FullName", "FirstName", "MiddleName", "LastName", "NameSuffix", "County", "Municipality", "Precinct", "voter_id", "Mobile", "Landline", "Email", "Street Name", "City", "Zip"]
    srcs = []
    seen = set()
    for out in search_out:
        src = first_existing_column(existing_cols, aliases.get(out, [out]))
        if src and src.lower() not in seen:
            srcs.append(src)
            seen.add(src.lower())
    if not srcs:
        return "''"
    return "CONCAT_WS(' ', " + ", ".join([f"CAST({sql_ident(c)} AS VARCHAR)" for c in srcs]) + ")"

def report_columns():
    return list(DEFAULT_EXPORT_COLUMNS)

@st.cache_data(ttl=600, show_spinner=False)
def remote_search_voters(term, max_rows=25):
    """Fast lookup against lightweight index shards using structured predicates.

    This avoids the slow all-column CONCAT/LIKE scan that made Voter Lookup feel stuck.
    For searches like "Elmer Bowman York", York is treated as a county filter,
    and first/last name predicates are applied directly when those columns exist.
    """
    raw_term = str(term or "").strip()
    urls = voter_search_urls_for_term(raw_term)
    base_lookup_cols = [
        "voter_id", "FullName", "FirstName", "MiddleName", "LastName", "NameSuffix",
        "House Number", "House Number Suffix", "Street Name", "Apartment Number", "Address Line 2",
        "City", "State", "Zip", "Precinct", "Municipality", "County",
        "Party", "Gender", "DOB", "RegistrationDate", "Age",
        "USC", "STS", "STH", "School District", "School Region",
        "Mobile", "Landline", "Current_ApplicantPhone", "Email",
        "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "Tags", "HH_LOOKUP_KEY"
    ]
    # Keep the search row intentionally small and fast. Vote history is loaded on demand.
    lookup_cols = base_lookup_cols
    if not raw_term:
        return pd.DataFrame(columns=lookup_cols)

    county_token, search_tokens = _lookup_county_token_and_search_tokens(raw_term)

    existing_cols = remote_parquet_columns(urls)
    aliases = source_alias_candidates()
    select_cols = safe_remote_select_exprs(existing_cols, lookup_cols)

    def src(out_name):
        return first_existing_column(existing_cols, aliases.get(out_name, [out_name]))

    c_voter = src("voter_id")
    c_full = src("FullName")
    c_first = src("FirstName")
    c_middle = src("MiddleName")
    c_last = src("LastName")
    c_county = src("County")
    c_house = src("House Number")
    c_street = src("Street Name")
    c_city = src("City")
    c_zip = src("Zip")
    c_mobile = src("Mobile")
    c_land = src("Landline")
    c_email = src("Email")

    where_parts = []
    if county_token and c_county:
        where_parts.append(f"LOWER(CAST({sql_ident(c_county)} AS VARCHAR)) = {sql_lit(county_token)}")

    digits = re.sub(r"\D+", "", raw_term)
    if len(digits) >= 6 and c_voter:
        where_parts.append(f"CAST({sql_ident(c_voter)} AS VARCHAR) LIKE {sql_lit('%' + digits + '%')}")
    elif "@" in raw_term and c_email:
        email_term = raw_term.lower().replace("'", "''")
        where_parts.append(f"LOWER(CAST({sql_ident(c_email)} AS VARCHAR)) LIKE {sql_lit('%' + email_term + '%')}")
    elif len(digits) >= 7 and (c_mobile or c_land):
        phone_parts = []
        for pc in [c_mobile, c_land]:
            if pc:
                phone_parts.append(f"REGEXP_REPLACE(CAST({sql_ident(pc)} AS VARCHAR), '[^0-9]', '', 'g') LIKE {sql_lit('%' + digits + '%')}")
        if phone_parts:
            where_parts.append("(" + " OR ".join(phone_parts) + ")")
    elif search_tokens:
        # Name-first search. Two tokens usually means first + last.
        if len(search_tokens) >= 2 and c_first and c_last:
            first_t = search_tokens[0]
            last_t = search_tokens[-1]
            where_parts.append(f"LOWER(CAST({sql_ident(c_first)} AS VARCHAR)) LIKE {sql_lit(first_t + '%')}")
            where_parts.append(f"LOWER(CAST({sql_ident(c_last)} AS VARCHAR)) LIKE {sql_lit(last_t + '%')}")
        else:
            searchable = [c_full, c_first, c_middle, c_last, c_house, c_street, c_city, c_zip]
            searchable = [c for c in searchable if c]
            if searchable:
                blob = "CONCAT_WS(' ', " + ", ".join([f"CAST({sql_ident(c)} AS VARCHAR)" for c in searchable]) + ")"
                for t in search_tokens:
                    where_parts.append(f"LOWER({blob}) LIKE {sql_lit('%' + t + '%')}")

    if not where_parts:
        return pd.DataFrame(columns=lookup_cols)

    order_parts = [c for c in [c_last, c_first, c_house, c_street] if c]
    order_sql = (" ORDER BY " + ", ".join(sql_ident(c) for c in order_parts)) if order_parts else ""
    where = " AND ".join(where_parts)

    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try:
                con.execute('LOAD httpfs;')
            except Exception:
                pass
        query = f"SELECT {select_cols} FROM read_parquet({urls!r}, union_by_name=true) WHERE {where}{order_sql} LIMIT {int(max_rows)}"
        df = con.execute(query).df()
        if df.empty:
            return pd.DataFrame(columns=lookup_cols)
        # Light normalization only; do not run export cleanup here.
        for c in df.columns:
            if c in {"FirstName","MiddleName","LastName","NameSuffix","FullName","Street Name","City","Municipality","County","Precinct"}:
                df[c] = df[c].map(smart_title)
        # Rebuild missing/placeholder names from segmented name fields so result cards
        # never fall back to Unnamed Voter when SURE has the pieces available.
        rebuilt = df.apply(full_name, axis=1).map(smart_title)
        if "FullName" not in df.columns:
            df["FullName"] = rebuilt
        else:
            bad = df["FullName"].astype(str).str.strip().eq("") | df["FullName"].astype(str).str.lower().isin(["unnamed voter", "nan", "none", "null"])
            df.loc[bad, "FullName"] = rebuilt.loc[bad]
        return df
    except Exception as e:
        st.error(f"Lookup query failed quickly instead of hanging: {e}")
        return pd.DataFrame(columns=lookup_cols)
    finally:
        con.close()

@st.cache_data(ttl=600, show_spinner=False)
def remote_voter_detail(voter_id: str) -> pd.Series:
    """Fetch one full voter record from detail shards by voter_id, then normalize display fields."""
    vid = cc_text(voter_id)
    if not vid:
        return pd.Series(dtype="object")
    urls = detail_urls_from_manifest()
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        df = con.execute(
            f"SELECT * FROM read_parquet({urls!r}, union_by_name=true) "
            f"WHERE CAST({sql_ident('voter_id')} AS VARCHAR) = {sql_lit(vid)} LIMIT 1"
        ).df()
        if df.empty:
            return pd.Series(dtype="object")
        df = normalize_download_df(df)
        return df.iloc[0]
    finally:
        con.close()


@st.cache_data(ttl=600, show_spinner=False)
def remote_voter_lookup_detail(voter_id: str) -> pd.Series:
    """Fetch the selected voter's display/detail row by exact voter_id.

    This is intentionally narrower than the full export cleanup path so the lookup
    page can show DOB, districts, household keys, and vote history without pulling
    the entire export schema.
    """
    vid = cc_text(voter_id)
    if not vid:
        return pd.Series(dtype="object")
    urls = voter_detail_lookup_urls_for_id(vid)
    existing_cols = remote_parquet_columns(urls)
    base_cols = [
        "voter_id", "FullName", "FirstName", "MiddleName", "LastName", "NameSuffix",
        "House Number", "House Number Suffix", "Street Name", "Apartment Number", "Address Line 2",
        "City", "State", "Zip", "Precinct", "Municipality", "County",
        "Party", "Gender", "DOB", "RegistrationDate", "Age",
        "USC", "STS", "STH", "School District", "School Region",
        "HH_ID", "Household_ID", "Household_Party", "HouseholdCount", "HH_LOOKUP_KEY",
        "Mobile", "Landline", "Current_ApplicantPhone", "Email",
        "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "Tags"
    ]
    lookup_cols = base_cols + [c for c in election_columns_from_manifest() if c not in base_cols]
    select_cols = safe_remote_select_exprs(existing_cols, lookup_cols)
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        id_col = first_existing_column(existing_cols, source_alias_candidates().get("voter_id", ["voter_id"])) or "voter_id"
        df = con.execute(
            f"SELECT {select_cols} FROM read_parquet({urls!r}, union_by_name=true) "
            f"WHERE CAST({sql_ident(id_col)} AS VARCHAR) = {sql_lit(vid)} LIMIT 1"
        ).df()
        if df.empty:
            return pd.Series(dtype="object")
        # Light display normalization.
        for c in ["FirstName","MiddleName","LastName","FullName","Street Name","City","Municipality","County","Precinct","School District","School Region"]:
            if c in df.columns:
                df[c] = df[c].map(smart_title)
        if "NameSuffix" in df.columns:
            df["NameSuffix"] = df["NameSuffix"].map(normalize_name_suffix)
        if "State" in df.columns:
            df["State"] = df["State"].map(lambda x: cc_text(x).upper())
        parts = []
        for c in ["FirstName", "MiddleName", "LastName", "NameSuffix"]:
            parts.append(df.get(c, pd.Series([""]*len(df), index=df.index)).astype(str).replace({"nan":""}).str.strip())
        built_full = (parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]).str.replace(r"\s+", " ", regex=True).str.strip()
        if "FullName" not in df.columns:
            df["FullName"] = built_full
        else:
            bad_full = df["FullName"].astype(str).str.strip().eq("") | df["FullName"].astype(str).str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null", "household voter"]) | df["FullName"].astype(str).str.lower().str.startswith("voter ")
            df.loc[bad_full, "FullName"] = built_full.loc[bad_full]
        if "Precinct" in df.columns:
            muni = df["Municipality"] if "Municipality" in df.columns else pd.Series([""]*len(df), index=df.index)
            df["Precinct"] = [canonical_precinct_display(p, m) for p, m in zip(df["Precinct"], muni)]
        return df.iloc[0]
    finally:
        con.close()



@st.cache_data(ttl=600, show_spinner=False)
def remote_voter_search_exact_by_id(voter_id: str) -> pd.Series:
    """Fetch a thin search-card row by voter_id from the fast last-name shards.

    This is used as a name fallback for household cards because the household
    shard is intentionally thin and older speed builds may have blank FullName
    for non-selected household members.
    """
    vid = cc_text(voter_id)
    if not vid:
        return pd.Series(dtype="object")
    urls = voter_search_all_urls() or voter_lookup_or_detail_urls()
    if not urls:
        return pd.Series(dtype="object")
    existing_cols = remote_parquet_columns(urls)
    aliases = source_alias_candidates()
    id_col = first_existing_column(existing_cols, aliases.get("voter_id", ["voter_id"])) or "voter_id"
    cols = [
        "voter_id", "FullName", "Name", "FirstName", "MiddleName", "LastName", "NameSuffix",
        "Party", "Gender", "Age", "DOB", "House Number", "Street Name", "City", "State", "Zip",
        "County", "Municipality", "Precinct", "HH_LOOKUP_KEY"
    ]
    select_cols = safe_remote_select_exprs(existing_cols, cols)
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        df = con.execute(
            f"SELECT {select_cols} FROM read_parquet({urls!r}, union_by_name=true) "
            f"WHERE CAST({sql_ident(id_col)} AS VARCHAR) = {sql_lit(vid)} LIMIT 1"
        ).df()
        if df.empty:
            return pd.Series(dtype="object")
        for c in ["FirstName", "MiddleName", "LastName", "NameSuffix", "FullName", "Name", "Street Name", "City", "Municipality", "County", "Precinct"]:
            if c in df.columns:
                df[c] = df[c].map(smart_title)
        if "NameSuffix" in df.columns:
            df["NameSuffix"] = df["NameSuffix"].map(normalize_name_suffix)
        rebuilt = df.apply(full_name, axis=1).map(smart_title)
        if "FullName" not in df.columns:
            df["FullName"] = rebuilt
        else:
            bad = df["FullName"].astype(str).str.strip().eq("") | df["FullName"].astype(str).str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null"])
            df.loc[bad, "FullName"] = rebuilt.loc[bad]
        return df.iloc[0]
    finally:
        con.close()


def _draw_branded_header(c, title: str, subtitle: str = ""):
    """Simple branded PDF header. Keeps PDF generation from depending on app-only helpers."""
    w, h = landscape(letter)
    c.setFillColorRGB(0.50, 0.05, 0.12)
    c.roundRect(28, h - 54, w - 56, 32, 6, stroke=0, fill=1)
    c.setFillColorRGB(1, 1, 1)
    c.setFont("Helvetica-Bold", 15)
    c.drawString(42, h - 43, title)
    if subtitle:
        c.setFont("Helvetica", 8)
        c.drawRightString(w - 42, h - 42, str(subtitle)[:60])
    c.setFillColorRGB(0,0,0)
    return h - 70


def _history_payload(row: pd.Series, limit: int | None = None):
    """Return de-duplicated election history tuples for PDF: (column, short label, party, method)."""
    row = row if isinstance(row, pd.Series) else pd.Series(row or {})
    try:
        cols = election_columns_from_manifest()
    except Exception:
        cols = []
    if not cols:
        cols = [c for c in row.index if re.match(r"^[GPS]\d{2}(?:_\d+)?$", str(c))]

    # De-duplicate columns by display election code. Some builds expose both G25 and G25_* aliases,
    # which was doubling every election in the app/PDF.
    def dedupe_group(prefix: str):
        raw_cols = [c for c in cols if str(c).upper().startswith(prefix)]
        raw_cols = sorted(raw_cols, key=lambda c: str(c).upper(), reverse=True)
        chosen = {}
        order = []
        for c in raw_cols:
            short = str(c).split("_")[0].upper()
            if short not in chosen:
                chosen[short] = c
                order.append(short)
            else:
                # Prefer the duplicate column that actually has data for this voter.
                prev = chosen[short]
                if _blank_vote_value(row.get(prev, "")) and not _blank_vote_value(row.get(c, "")):
                    chosen[short] = c
        return [chosen[k] for k in order]

    general_cols = dedupe_group("G")
    primary_cols = dedupe_group("P")

    def method_for(col):
        # Election columns can store either an explicit vote method (MAIL/POLL/PROV)
        # or a party code (R/D/O) meaning the voter participated in that election.
        # If a voter participated and no explicit method is present, treat it as At Poll.
        # If there is no party/vote record for that election, keep Method blank.
        raw = row.get(col, "")
        explicit = normalize_vote_method(raw)
        if explicit:
            return explicit
        return "At Poll" if party_for(col) else ""

    def party_for(col):
        raw = cc_text(row.get(col, "")).upper()
        if raw in {"R","D"}:
            return raw
        if raw in {"REP","REPUBLICAN"}:
            return "R"
        if raw in {"DEM","DEMOCRAT","DEMOCRATIC"}:
            return "D"
        if not _blank_vote_value(raw) and normalize_vote_method(raw):
            pty = cc_text(row.get("Party", "")).upper()
            if pty in {"R", "D"}:
                return pty
            if pty in {"REP", "REPUBLICAN"}:
                return "R"
            if pty in {"DEM", "DEMOCRAT", "DEMOCRATIC"}:
                return "D"
        return ""

    def build(group_cols):
        out = []
        if limit is not None:
            group_cols = group_cols[:int(limit)]
        for col in group_cols:
            short = str(col).split("_")[0].upper()
            out.append((str(col), short, party_for(col), method_for(col)))
        return out

    return build(general_cols), build(primary_cols)

def make_voter_lookup_pdf(row: pd.Series, household: pd.DataFrame | None = None) -> bytes:
    """Branded voter lookup report with full available vote history."""
    if canvas is None:
        return b"PDF support unavailable."
    bio = io.BytesIO()
    c = canvas.Canvas(bio, pagesize=landscape(letter))
    w, h = landscape(letter)
    y = _draw_branded_header(c, "Voter Lookup Report", datetime.now().strftime("%m/%d/%Y")) - 18

    name = smart_title(full_name(row)) or "Selected Voter"
    c.setFillColorRGB(0.50, 0.05, 0.12)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(36, y, name[:60]); y -= 16
    c.setFillColorRGB(0.05,0.05,0.05)
    c.setFont("Helvetica", 9)
    c.drawString(36, y, smart_title(address_line(row))[:95]); y -= 22

    c.setFont("Helvetica-Bold", 8)
    metrics = [("Party", row.get("Party","")), ("Gender", row.get("Gender","")), ("Age", row.get("Age","")), ("PA ID", row.get("voter_id",""))]
    x=36
    for lab,val in metrics:
        c.setFillColorRGB(0.35,0.35,0.35); c.drawString(x,y,lab)
        c.setFillColorRGB(0,0,0); c.setFont("Helvetica-Bold", 11); c.drawString(x,y-13,cc_text(val) or "—")
        c.setFont("Helvetica-Bold", 8); x += 130
    y -= 36

    def table_box(title, rows, x, y, width, row_h=12):
        c.setFillColorRGB(0.56,0.06,0.13); c.roundRect(x, y-12, width, 15, 3, fill=1, stroke=0)
        c.setFillColorRGB(1,1,1); c.setFont("Helvetica-Bold", 8); c.drawString(x+5, y-8, title)
        y -= 17
        c.setFont("Helvetica", 6.8); c.setFillColorRGB(0,0,0)
        for i,(a,b) in enumerate(rows):
            if y < 55: break
            if i % 2 == 0:
                c.setFillColorRGB(0.96,0.90,0.91); c.rect(x, y-row_h+3, width, row_h, stroke=0, fill=1)
            c.setFillColorRGB(0,0,0)
            c.setFont("Helvetica-Bold", 6.5); c.drawString(x+4, y-7, str(a)[:25])
            c.setFont("Helvetica", 6.5); c.drawString(x+108, y-7, str(b)[:52])
            y -= row_h
        return y

    details = [
        ("Date of Birth", row.get("DOB", "")), ("Registration Date", row.get("RegistrationDate", "")),
        ("Registered Party", row.get("Party", "")), ("County", row.get("County", "")),
        ("Municipality", row.get("Municipality", "")), ("Precinct", row.get("Precinct", "")),
        ("Congressional", row.get("USC", "")), ("State Senate", row.get("STS", "")),
        ("State House", row.get("STH", "")), ("School District", row.get("School District", "")),
        ("School Region", row.get("School Region", "")),
    ]
    contact = [
        ("Mobile", format_phone_number(row.get("Mobile", ""))), ("Landline", format_phone_number(row.get("Landline", ""))),
        ("Applicant Phone", format_phone_number(row.get("Current_ApplicantPhone", ""))), ("Email", row.get("Email", "")),
        ("Mail Ballot Applied", row.get("MB_App", "")), ("Application Status", row.get("MB_App_Status", "")),
        ("Ballot Sent", row.get("MB_Sent", "")), ("Ballot Status", row.get("MB_Status", "")),
        ("Permanent MB", row.get("MB_PERM", "")), ("Tags", row.get("Tags", "")),
    ]
    y_left = table_box("Voter Details", details, 36, y, 340)
    y_right = table_box("Contact + Mail Ballot", contact, 410, y, 340)
    y = min(y_left, y_right) - 12

    if household is not None and not household.empty and y > 105:
        c.setFillColorRGB(0.56,0.06,0.13); c.roundRect(36, y-12, w-72, 15, 3, fill=1, stroke=0)
        c.setFillColorRGB(1,1,1); c.setFont("Helvetica-Bold", 8); c.drawString(41, y-8, "Household Members")
        y -= 18
        c.setFillColorRGB(0,0,0); c.setFont("Helvetica-Bold", 6.5)
        c.drawString(40, y, "Name"); c.drawString(250, y, "Party"); c.drawString(295, y, "Gender"); c.drawString(345, y, "Age"); y -= 10
        for _, rr in household.head(6).iterrows():
            nm = smart_title(full_name(rr))
            if nm.lower() == "unnamed voter":
                nm = smart_title(cc_text(rr.get("FullName", ""))) or "Unnamed Voter"
            mark = "✓ " if cc_text(rr.get("voter_id", "")) == cc_text(row.get("voter_id", "")) else ""
            c.setFont("Helvetica", 6.5)
            c.drawString(40, y, (mark + nm)[:45])
            c.drawString(250, y, cc_text(rr.get("Party", ""))[:5])
            c.drawString(295, y, cc_text(rr.get("Gender", ""))[:5])
            c.drawString(345, y, cc_text(rr.get("Age", ""))[:5])
            y -= 9
        y -= 8

    def ensure_space(needed=95):
        nonlocal y
        if y < needed:
            c.showPage()
            y = _draw_branded_header(c, "Voter Lookup Report", "Election History") - 18

    general, primary = _history_payload(row, limit=None)
    ensure_space(115)
    c.setFillColorRGB(0.56,0.06,0.13); c.roundRect(36, y-12, w-72, 15, 3, fill=1, stroke=0)
    c.setFillColorRGB(1,1,1); c.setFont("Helvetica-Bold", 8); c.drawString(41, y-8, "Full Election History")
    y -= 22

    def draw_hist(title, items, x, y, max_cols=10):
        c.setFillColorRGB(0,0,0); c.setFont("Helvetica-Bold", 7.2); c.drawString(x,y,title); y-=10
        if not items:
            c.setFont("Helvetica", 6.5); c.drawString(x, y, "No history found."); return y-10
        for start in range(0, len(items), max_cols):
            chunk = items[start:start+max_cols]
            if y < 48:
                c.showPage(); y = _draw_branded_header(c, "Voter Lookup Report", "Election History") - 18
            x0=x+42; step=20
            c.setFont("Helvetica-Bold", 6.2)
            for i,it in enumerate(chunk): c.drawCentredString(x0+i*step, y, it[1])
            y-=9
            c.drawString(x,y,"Party")
            for i,it in enumerate(chunk): c.drawCentredString(x0+i*step, y, it[2])
            y-=9
            c.drawString(x,y,"Method")
            for i,it in enumerate(chunk): c.drawCentredString(x0+i*step, y, vote_method_pdf_label(it[3]))
            y-=13
        return y

    def draw_hist_wide(title, items, y, max_cols=20):
        """Compact, boxed election history grid for PDF readability."""
        if not items:
            c.setFillColorRGB(0,0,0); c.setFont("Helvetica-Bold", 8); c.drawString(36, y, title)
            c.setFont("Helvetica", 6.5); c.drawString(92, y, "No history found.")
            return y - 14

        usable_w = w - 72
        left_label_w = 44
        row_h = 12

        for start in range(0, len(items), max_cols):
            chunk = items[start:start+max_cols]
            if y < 58:
                c.showPage(); y = _draw_branded_header(c, "Voter Lookup Report", "Election History") - 18

            cols = max(1, len(chunk))
            cell_w = (usable_w - left_label_w) / cols

            c.setFillColorRGB(0.56,0.06,0.13)
            c.roundRect(36, y-12, usable_w, 15, 3, fill=1, stroke=0)
            c.setFillColorRGB(1,1,1); c.setFont("Helvetica-Bold", 7.5)
            c.drawString(42, y-8, title)
            y -= 18

            x = 36
            table_w = left_label_w + cell_w * len(chunk)
            table_h = row_h * 3

            c.setFillColorRGB(0.88,0.90,0.94); c.rect(x, y-row_h, table_w, row_h, stroke=0, fill=1)
            c.setFillColorRGB(0.97,0.97,0.98); c.rect(x, y-row_h*2, table_w, row_h, stroke=0, fill=1)
            c.setFillColorRGB(1,1,1); c.rect(x, y-row_h*3, table_w, row_h, stroke=0, fill=1)

            c.setStrokeColorRGB(0.72,0.72,0.76); c.setLineWidth(0.35)
            for rline in range(4):
                yy = y - row_h*rline
                c.line(x, yy, x+table_w, yy)
            c.line(x, y, x, y-table_h)
            c.line(x+left_label_w, y, x+left_label_w, y-table_h)
            for i in range(len(chunk)+1):
                xx = x + left_label_w + cell_w*i
                c.line(xx, y, xx, y-table_h)

            c.setFillColorRGB(0.08,0.08,0.08)
            c.setFont("Helvetica-Bold", 5.8)
            c.drawCentredString(x + left_label_w/2, y-row_h+3.2, "Election")
            c.drawCentredString(x + left_label_w/2, y-row_h*2+3.2, "Party")
            c.drawCentredString(x + left_label_w/2, y-row_h*3+3.2, "Method")
            for i,it in enumerate(chunk):
                cx = x + left_label_w + cell_w*i + cell_w/2
                c.setFont("Helvetica-Bold", 5.8)
                c.drawCentredString(cx, y-row_h+3.2, cc_text(it[1])[:4])
                c.setFont("Helvetica-Bold", 5.8)
                c.drawCentredString(cx, y-row_h*2+3.2, cc_text(it[2])[:2])
                c.setFont("Helvetica", 5.8)
                c.drawCentredString(cx, y-row_h*3+3.2, vote_method_pdf_label(it[3])[:2])

            y -= table_h + 10
        return y

    y = draw_hist_wide("General Elections", general, y, max_cols=20) - 4
    y = draw_hist_wide("Primary Elections", primary, y, max_cols=20) - 6
    if y < 36:
        c.showPage(); y = 70
    c.setFont("Helvetica", 6.3); c.setFillColorRGB(0.25,0.25,0.25)
    c.drawString(36, max(28, y), "Legend: MB = Mail Ballot   AP = At Poll   PV = Provisional   blank = Did Not Vote / no record")
    c.save(); bio.seek(0); return bio.getvalue()

def remote_household_members(row: pd.Series, max_rows: int = 25) -> pd.DataFrame:
    """Find household members from one household-hash shard.

    Step 8 v23 writes speed/voter_household_hash_00..63.parquet with HH_LOOKUP_KEY.
    That avoids scanning every last-name shard just to find people at the same address.
    """
    hh_key_value = cc_text(row.get("HH_LOOKUP_KEY", "")) or household_lookup_key(row)
    cols = [
        "voter_id", "FullName", "Name", "FirstName", "MiddleName", "LastName", "NameSuffix",
        "first_name", "middle_name", "last_name", "suffix",
        "Party", "Gender", "Age", "DOB", "House Number", "Street Name", "Apartment Number",
        "City", "State", "Zip", "County", "Municipality", "Precinct", "HH_LOOKUP_KEY"
    ]
    if not hh_key_value or hh_key_value.count("|") < 4:
        return pd.DataFrame(columns=cols)

    hh_url = voter_household_lookup_url(row)
    if hh_url:
        urls = [hh_url]
        existing_cols = remote_parquet_columns(urls)
        key_col = first_existing_column(existing_cols, ["HH_LOOKUP_KEY"])
        if key_col:
            where = f"CAST({sql_ident(key_col)} AS VARCHAR) = {sql_lit(hh_key_value)}"
        else:
            where = "FALSE"
    else:
        # Fallback for an older shard build: slower, but still works.
        urls = voter_search_all_urls() or voter_lookup_or_detail_urls()
        existing_cols = remote_parquet_columns(urls)
        aliases = source_alias_candidates()
        conditions = []
        for field in ["County", "House Number", "Street Name", "Zip"]:
            val = cc_text(row.get(field, ""))
            src = first_existing_column(existing_cols, aliases.get(field, [field]))
            if val and src:
                conditions.append(f"LOWER(CAST({sql_ident(src)} AS VARCHAR)) = {sql_lit(val.lower())}")
        apt_val = cc_text(row.get("Apartment Number", ""))
        apt_src = first_existing_column(existing_cols, aliases.get("Apartment Number", ["Apartment Number"]))
        if apt_val and apt_src:
            conditions.append(f"LOWER(CAST({sql_ident(apt_src)} AS VARCHAR)) = {sql_lit(apt_val.lower())}")
        if len(conditions) < 3:
            return pd.DataFrame(columns=cols)
        where = " AND ".join(conditions)

    select_cols = safe_remote_select_exprs(existing_cols, cols)
    aliases = source_alias_candidates()
    order_parts = []
    for out in ["LastName", "FirstName"]:
        src = first_existing_column(existing_cols, aliases.get(out, [out]))
        if src:
            order_parts.append(sql_ident(src))
    order_sql = (" ORDER BY " + ", ".join(order_parts)) if order_parts else ""
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        df = con.execute(
            f"SELECT {select_cols} FROM read_parquet({urls!r}, union_by_name=true) "
            f"WHERE {where}{order_sql} LIMIT {int(max_rows)}"
        ).df()
        if df.empty:
            return df
        for c in ["FirstName", "MiddleName", "LastName", "NameSuffix", "FullName", "Name", "Street Name", "City", "Municipality", "County", "Precinct"]:
            if c in df.columns:
                df[c] = df[c].map(smart_title)
        if "NameSuffix" in df.columns:
            df["NameSuffix"] = df["NameSuffix"].map(normalize_name_suffix)
        rebuilt = df.apply(full_name, axis=1).map(smart_title)
        existing_full = df.get("FullName", pd.Series([""]*len(df), index=df.index)).astype(str).str.strip()
        df["FullName"] = existing_full
        mask = df["FullName"].eq("") | df["FullName"].str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null"])
        df.loc[mask, "FullName"] = rebuilt.loc[mask]
        # If the name still is not available, show a useful address/person label instead of repeating Unnamed Voter.
        missing = df["FullName"].astype(str).str.strip().eq("") | df["FullName"].astype(str).str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null"])
        df.loc[missing, "FullName"] = df.loc[missing].apply(lambda rr: f"Voter {cc_text(rr.get('voter_id',''))}" if cc_text(rr.get('voter_id','')) else "Household Voter", axis=1)
        return df
    finally:
        con.close()


def correction_store() -> dict:
    """Saved voter corrections with layered persistence.

    DEV durability order:
      1) R2 app_state/voter_record_corrections.json uploaded by Pipeline Manager
      2) local JSON state when running locally
      3) browser URL query parameter for refresh/reboot survival of recent edits
    """
    if "voter_corrections" not in st.session_state or not isinstance(st.session_state.get("voter_corrections"), dict):
        state_corr = _json_safe_corrections(_load_dev_state().get("voter_corrections") or {})
        try:
            url_corr = decode_corrections(st.query_params.get(CORRECTIONS_PARAM, ""))
        except Exception:
            url_corr = {}
        state_corr.update(url_corr or {})
        st.session_state["voter_corrections"] = state_corr
    return st.session_state["voter_corrections"]


def persist_corrections():
    clean = _json_safe_corrections(st.session_state.get("voter_corrections", {}) or {})
    st.session_state["voter_corrections"] = clean
    try:
        _persist_dev_section("voter_corrections", clean)
    except Exception:
        pass
    # Also store in the browser URL so refresh/reboot keeps recent edits until they are committed to app_state/R2.
    try:
        encoded = encode_corrections(clean)
        if encoded and len(encoded) < 7000:
            st.query_params[CORRECTIONS_PARAM] = encoded
        elif CORRECTIONS_PARAM in st.query_params:
            del st.query_params[CORRECTIONS_PARAM]
    except Exception:
        pass


def correction_rows_df() -> pd.DataFrame:
    rows = []
    for vid, payload in correction_store().items():
        base = {"voter_id": vid, "updated_at": payload.get("updated_at", ""), "notes": payload.get("notes", "")}
        base.update(payload.get("fields", {}))
        rows.append(base)
    return pd.DataFrame(rows)


def apply_local_correction(row: pd.Series) -> pd.Series:
    vid = cc_text(row.get("voter_id", ""))
    payload = correction_store().get(vid)
    if not payload:
        return row
    out = row.copy()
    for k, v in (payload.get("fields", {}) or {}).items():
        out[k] = v
    return out




def _blank_vote_value(val) -> bool:
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except Exception:
        pass
    s = str(val).strip()
    return s == "" or s.upper() in {"0", "N", "NO", "NONE", "NULL", "NAN", "FALSE", "DID NOT VOTE", "DNV"}


def normalize_vote_method(val):
    """Normalize SURE/election vote-method values for voter lookup election history."""
    if _blank_vote_value(val):
        return ""
    v = str(val).strip()
    vu = v.upper()
    if vu in {"M", "MAIL", "MB", "MAIL BALLOT"} or "MAIL" in vu:
        return "Mail Ballot"
    if vu in {"A", "AP", "AT POLL", "POLL", "IN PERSON", "IP", "VOTED"} or "POLL" in vu or "PERSON" in vu:
        return "At Poll"
    if vu in {"PROV", "PROVISIONAL"} or "PROV" in vu:
        return "Provisional"
    # A bare party value is not a method. The history payload may still use it to
    # infer At Poll only when it is a real party code, not blank/zero.
    if vu in {"R", "D", "O", "I", "NP", "REP", "DEM", "REPUBLICAN", "DEMOCRAT", "DEMOCRATIC"}:
        return ""
    return v.title()


def vote_method_icon(method: str) -> str:
    m = normalize_vote_method(method)
    if m == "Mail Ballot":
        return "✉️"
    if m == "At Poll":
        return "🗳️"
    if m == "Provisional":
        return "🟨"
    return ""


def vote_method_pdf_label(method_or_icon: str) -> str:
    """ReportLab base fonts do not reliably render emoji, so the PDF uses short labels."""
    v = cc_text(method_or_icon)
    if v in {"✉", "✉️"}:
        return "MB"
    if v in {"🗳", "🗳️"}:
        return "AP"
    if v in {"🟨"}:
        return "PV"
    m = normalize_vote_method(v)
    if m == "Mail Ballot":
        return "MB"
    if m == "At Poll":
        return "AP"
    if m == "Provisional":
        return "PV"
    return ""



def _dedup_history_cols_for_row(cols, row: pd.Series, prefix: str):
    """Keep one column per displayed election code, preferring the column with data for this voter."""
    raw_cols = [c for c in (cols or []) if str(c).upper().startswith(prefix)]
    raw_cols = sorted(raw_cols, key=lambda c: str(c).upper(), reverse=True)
    chosen = {}
    order = []
    for c in raw_cols:
        short = str(c).split("_")[0].upper()
        if short not in chosen:
            chosen[short] = c
            order.append(short)
        else:
            prev = chosen[short]
            if _blank_vote_value(row.get(prev, "")) and not _blank_vote_value(row.get(c, "")):
                chosen[short] = c
    return [chosen[k] for k in order]


def render_election_history_table(row: pd.Series):
    """Draw visible blank/no-vote years and method icons for the selected voter."""
    row = row if isinstance(row, pd.Series) else pd.Series(row or {})
    cols = election_columns_from_manifest()
    if not cols:
        cols = [c for c in row.index if re.match(r"^[GPS]\d{2}(?:_\d+)?$", str(c))]
    general = _dedup_history_cols_for_row(cols, row, "G")
    primary = _dedup_history_cols_for_row(cols, row, "P")

    def method_for(col):
        # Election columns can store either an explicit vote method (MAIL/POLL/PROV)
        # or a party code (R/D/O) meaning the voter participated in that election.
        # If a voter participated and no explicit method is present, treat it as At Poll.
        # If there is no party/vote record for that election, keep Method blank.
        raw = row.get(col, "")
        explicit = normalize_vote_method(raw)
        if explicit:
            return explicit
        return "At Poll" if party_for(col) else ""

    def party_for(col):
        raw = cc_text(row.get(col, "")).upper()
        if raw in {"R","D"}:
            return raw
        if raw in {"REP","REPUBLICAN"}:
            return "R"
        if raw in {"DEM","DEMOCRAT","DEMOCRATIC"}:
            return "D"
        if not _blank_vote_value(raw) and normalize_vote_method(raw):
            p = cc_text(row.get("Party", "")).upper()
            if p in {"R", "D"}:
                return p
            if p in {"REP", "REPUBLICAN"}:
                return "R"
            if p in {"DEM", "DEMOCRAT", "DEMOCRATIC"}:
                return "D"
        return ""

    def draw(label, group_cols):
        st.markdown(f"**{label} Elections**")
        if not group_cols:
            st.caption("No election history columns available in this shard build.")
            return
        party_row = {"Row": "Party"}
        method_row = {"Row": "Method"}
        for c in group_cols:
            short = str(c).split("_")[0].upper()
            party_row[short] = party_for(c)
            m = method_for(c)
            method_row[short] = vote_method_icon(m) or vote_method_pdf_label(m)
        hist_df = pd.DataFrame([party_row, method_row])

        # Custom HTML table fixes the clipped emoji row and forces center alignment.
        def _cell(v):
            return cc_text(v).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        headers = list(hist_df.columns)
        html = [
            '<div class="cc-history-scroll"><table class="cc-history-table">',
            '<thead><tr>' + ''.join(f'<th>{_cell(h)}</th>' for h in headers) + '</tr></thead>',
            '<tbody>'
        ]
        for _, rr in hist_df.iterrows():
            html.append('<tr>' + ''.join(f'<td>{_cell(rr.get(h, ""))}</td>' for h in headers) + '</tr>')
        html.append('</tbody></table></div>')
        css = """
<style>
.cc-history-scroll { max-width: 100%; overflow-x: auto; margin: 6px 0 14px 0; }
.cc-history-table { border-collapse: collapse; min-width: 760px; background: #0b0f19; color: #f8fafc; font-size: 12px; }
.cc-history-table th, .cc-history-table td { border: 1px solid rgba(148,163,184,.28); text-align: center !important; vertical-align: middle !important; padding: 8px 10px; line-height: 1.45; min-width: 38px; height: 34px; }
.cc-history-table th:first-child, .cc-history-table td:first-child { position: sticky; left: 0; z-index: 2; background: #111827; min-width: 58px; font-weight: 800; }
.cc-history-table th { position: sticky; top: 0; z-index: 3; background: #1f2430; font-weight: 800; }
.cc-history-table tr:nth-child(even) td { background: #0f1724; }
.cc-history-table tr:nth-child(odd) td { background: #090d16; }
.cc-history-table tr:nth-child(even) td:first-child, .cc-history-table tr:nth-child(odd) td:first-child { background: #111827; }
</style>
"""
        st.markdown(css + ''.join(html), unsafe_allow_html=True)

    draw("General", general)
    draw("Primary", primary)
    st.caption("Legend: ✉️ = Mail Ballot · 🗳️ = At Poll · 🟨 = Provisional · blank = Did Not Vote / no record")

def render_voter_lookup_workspace():
    st.markdown("## Voter Lookup")
    q = st.session_state.get(special_key("lookup_query"), "")
    maxn = st.session_state.get(special_key("lookup_max"), 25)
    if not q:
        st.info("Enter a voter name, address, PA ID, phone, or email in the left pane.")
        return

    with st.spinner("Searching voters..."):
        df = remote_search_voters(q, maxn)
    st.caption(f"{len(df)} result(s) found for: {q}")
    if df.empty:
        st.warning("No voters found.")
        return

    if "lookup_selected_id" not in st.session_state or st.session_state.get("lookup_selected_id") not in set(df.get("voter_id", pd.Series([], dtype=str)).astype(str)):
        st.session_state["lookup_selected_id"] = cc_text(df.iloc[0].get("voter_id", ""))

    left, right = st.columns([0.85, 1.8])
    with left:
        st.markdown("### Search Results")
        st.markdown("""
        <style>
        div[data-testid="stButton"] > button[kind="primary"],
        div[data-testid="stButton"] > button[kind="secondary"] {
            white-space: pre-line !important;
            height: auto !important;
            min-height: 58px !important;
            text-align: left !important;
            justify-content: flex-start !important;
            line-height: 1.15 !important;
            padding: 8px 10px !important;
        }
        div[data-testid="stButton"] button p {
            white-space: pre-line !important;
            text-align: left !important;
            line-height: 1.2 !important;
        }
        </style>
        """, unsafe_allow_html=True)
        for i, r0 in df.iterrows():
            vid = cc_text(r0.get("voter_id", ""))
            nm = smart_title(full_name(r0))
            age = cc_text(r0.get("Age", ""))
            first_line = f"{nm}, {age}" if age else nm
            addr = smart_title(address_line(r0))
            county = cc_text(r0.get('County',''))
            label = f"{first_line}\n{addr}\n{county} County"
            btn_type = "primary" if vid == st.session_state.get("lookup_selected_id") else "secondary"
            if st.button(label, key=f"lookup_pick_{vid or i}", width="stretch", type=btn_type):
                st.session_state["lookup_selected_id"] = vid
                # Clicking a card loads the full selected voter detail immediately.
                try:
                    full_detail = remote_voter_lookup_detail(vid) if vid else r0
                    st.session_state[f"lookup_detail_row_{vid}"] = pd.DataFrame([full_detail if len(full_detail) else r0]).iloc[0].to_dict()
                except Exception:
                    st.session_state[f"lookup_detail_row_{vid}"] = pd.DataFrame([r0]).iloc[0].to_dict()
                # Clear stale per-voter display sections for the previous selection.
                for k in list(st.session_state.keys()):
                    if str(k).startswith(("hh_df_", "vote_history_row_", "voter_pdf_bytes_", "voter_pdf_name_")):
                        st.session_state.pop(k, None)
                st.rerun()

    with right:
        selected_id = cc_text(st.session_state.get("lookup_selected_id", ""))
        match = df[df["voter_id"].astype(str) == selected_id] if "voter_id" in df.columns else pd.DataFrame()
        index_row = match.iloc[0] if not match.empty else df.iloc[0]
        # Keep the selected voter view fast: use the lightweight lookup row first.
        # Full detail/election history/PDF are loaded only when the user asks for them.
        detail_key = f"lookup_detail_row_{selected_id}"
        if detail_key in st.session_state and isinstance(st.session_state.get(detail_key), dict):
            detail = pd.Series(st.session_state[detail_key])
        else:
            # First visible row also gets full detail so DOB/history fields are restored without another click.
            try:
                detail = remote_voter_lookup_detail(selected_id) if selected_id else index_row
                if detail is None or len(detail) == 0:
                    detail = index_row
                st.session_state[detail_key] = pd.DataFrame([detail]).iloc[0].to_dict()
            except Exception:
                detail = index_row
        r = apply_local_correction(pd.DataFrame([detail]).iloc[0])

        st.markdown(f"## {smart_title(full_name(r))}")
        st.write(smart_title(address_line(r)))
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Party", cc_text(r.get("Party", "")) or "—")
        m2.metric("Gender", cc_text(r.get("Gender", "")) or "—")
        m3.metric("Age", cc_text(r.get("Age", "")) or "—")
        m4.metric("DOB", cc_text(r.get("DOB", "")) or "—")
        m5.metric("PA ID", selected_id or "—")
        pdf_key = f"voter_pdf_bytes_{selected_id}"
        pdf_name_key = f"voter_pdf_name_{selected_id}"
        pc1, pc2 = st.columns([0.35, 1.65])
        with pc1:
            if st.button("Prepare PDF Report", key=f"prepare_voter_pdf_{selected_id}"):
                with st.spinner("Building voter PDF..."):
                    full_r = remote_voter_lookup_detail(selected_id) if selected_id else r
                    if full_r is None or len(full_r) == 0:
                        full_r = r
                    full_r = apply_local_correction(pd.DataFrame([full_r]).iloc[0])
                    try:
                        pdf_hh = remote_household_members(full_r)
                    except Exception:
                        pdf_hh = None
                    st.session_state[pdf_key] = make_voter_lookup_pdf(full_r, pdf_hh)
                    st.session_state[pdf_name_key] = f"candidate_connect_voter_report_{selected_id or 'voter'}.pdf"
                    st.rerun()
        if st.session_state.get(pdf_key):
            with pc2:
                st.download_button(
                    "Download PDF Report",
                    st.session_state[pdf_key],
                    file_name=st.session_state.get(pdf_name_key, f"candidate_connect_voter_report_{selected_id or 'voter'}.pdf"),
                    mime="application/pdf",
                    key=f"download_voter_pdf_{selected_id}",
                )

        d1, d2 = st.columns(2)
        with d1:
            st.markdown("### Voter Details")
            voter_rows = [
                ["Date of Birth", r.get("DOB", "")],
                ["Registration Date", r.get("RegistrationDate", "")],
                ["Registered Party", r.get("Party", "")],
                ["County", r.get("County", "")],
                ["Municipality", r.get("Municipality", "")],
                ["Precinct", r.get("Precinct", "")],
                ["Congressional", r.get("USC", "")],
                ["State Senate", r.get("STS", "")],
                ["State House", r.get("STH", "")],
                ["School District", r.get("School District", "")],
                ["School Region", r.get("School Region", "")],
            ]
            st.dataframe(pd.DataFrame(voter_rows, columns=["Field", "Value"]), hide_index=True, width="stretch")
        with d2:
            st.markdown("### Contact + Mail Ballot")
            contact_rows = [
                ["Mobile", format_phone_number(r.get("Mobile", ""))],
                ["Landline", format_phone_number(r.get("Landline", ""))],
                ["Applicant Phone", format_phone_number(r.get("Current_ApplicantPhone", ""))],
                ["Email", r.get("Email", "")],
                ["Mail Ballot Applied", r.get("MB_App", "")],
                ["Application Status", r.get("MB_App_Status", "")],
                ["Ballot Sent", r.get("MB_Sent", "")],
                ["Ballot Status", r.get("MB_Status", "")],
                ["Permanent MB", r.get("MB_PERM", "")],
                ["Tags", r.get("Tags", "")],
            ]
            st.dataframe(pd.DataFrame(contact_rows, columns=["Field", "Value"]), hide_index=True, width="stretch")

        with st.expander("Edit / Correct This Voter Record", expanded=False):
            if selected_id in correction_store():
                st.info("This voter currently has a saved correction in this browser session. Download the correction CSV and place it in the pipeline correction folder before the next pipeline run.")
            else:
                st.caption("Corrections are stored in this browser session and can be downloaded as a CSV for the pipeline correction workflow.")

            fields = [
                "FirstName", "MiddleName", "LastName", "NameSuffix",
                "Gender", "Party", "DOB", "RegistrationDate",
                "House Number", "House Number Suffix", "Street Name", "Apartment Number", "Address Line 2", "City", "State", "Zip",
                "County", "Municipality", "Precinct", "School District", "School Region", "USC", "STS", "STH",
                "Mobile", "Landline", "Current_ApplicantPhone", "Email",
                "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "Tags",
            ]
            existing_payload = correction_store().get(selected_id, {})
            existing_fields = existing_payload.get("fields", {}) or {}
            edits = {}
            group_specs = [
                ("Name", fields[0:4]),
                ("Voter Details", fields[4:8]),
                ("Address", fields[8:16]),
                ("Geography", fields[16:24]),
                ("Contact + Mail Ballot", fields[24:]),
            ]
            for title, group_fields in group_specs:
                st.markdown(f"**{title}**")
                cols = st.columns(4)
                for j, field in enumerate(group_fields):
                    val = cc_text(existing_fields.get(field, r.get(field, "")))
                    with cols[j % 4]:
                        edits[field] = st.text_input(field, value=val, key=f"edit_{selected_id}_{field}")
            notes = st.text_area("Correction Notes", value=existing_payload.get("notes", ""), key=f"edit_{selected_id}_notes")
            ca, cb, cc = st.columns([1, 1, 1])
            with ca:
                if st.button("Save Voter Correction", type="primary", key=f"save_corr_{selected_id}"):
                    correction_store()[selected_id] = {"updated_at": datetime.now().isoformat(timespec="seconds"), "fields": edits, "notes": notes}
                    persist_corrections()
                    current = dict(st.session_state.get(detail_key, {}) or {})
                    current.update(edits)
                    st.session_state[detail_key] = current
                    st.success("Correction saved in DEV state and is available for correction CSV download.")
                    st.rerun()
            with cb:
                if st.button("Remove Saved Correction", key=f"remove_corr_{selected_id}"):
                    correction_store().pop(selected_id, None)
                    persist_corrections()
                    st.success("Saved correction removed.")
                    st.rerun()
            with cc:
                one_payload = {"voter_id": selected_id, "updated_at": datetime.now().isoformat(timespec="seconds"), "notes": notes, **edits}
                st.download_button("Download This Correction", pd.DataFrame([one_payload]).to_csv(index=False).encode(), file_name=f"voter_correction_{selected_id or 'unknown'}.csv", mime="text/csv")

            all_corr = correction_rows_df()
            if not all_corr.empty:
                st.download_button("Download All Saved Corrections CSV", all_corr.to_csv(index=False).encode(), file_name="candidate_connect_voter_corrections.csv", mime="text/csv", width="stretch")

        st.markdown("### Household Members")
        hh_key = f"hh_df_{selected_id}"
        if hh_key not in st.session_state:
            with st.spinner("Loading household members..."):
                st.session_state[hh_key] = remote_household_members(r).to_dict("records")
        if st.button("Refresh Household Members", key=f"load_hh_{selected_id}"):
            with st.spinner("Loading household members..."):
                st.session_state[hh_key] = remote_household_members(r).to_dict("records")
        if st.session_state.get(hh_key):
            hh = pd.DataFrame(st.session_state.get(hh_key) or [])
            if not hh.empty:
                view = hh[[c for c in ["voter_id", "FullName", "Name", "FirstName", "MiddleName", "LastName", "NameSuffix", "first_name", "middle_name", "last_name", "suffix", "Party", "Gender", "Age", "County", "City", "State"] if c in hh.columns]].copy()
                # Do not show a table here. Build household member cards and make each
                # non-current card directly clickable to load that voter.
                view["DisplayName"] = hh.apply(full_name, axis=1).map(smart_title)
                view["DisplayName"] = view["DisplayName"].replace({"Unnamed Voter": "", "Unnamed voter": ""})
                missing_names = view["DisplayName"].astype(str).str.strip().eq("") | view["DisplayName"].astype(str).str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null"])
                # If the fast household shard has IDs/party/age but not name pieces,
                # fill names from the exact voter detail hash before falling back to an ID label.
                for idx in view.index[missing_names]:
                    hvid_for_name = cc_text(view.at[idx, "voter_id"]) if "voter_id" in view.columns else ""
                    if not hvid_for_name:
                        continue
                    nm = ""
                    try:
                        # Fastest name fallback first: search-card shard by exact voter_id.
                        # Then try the detail hash, and finally the older full detail shards.
                        for fetcher in (remote_voter_search_exact_by_id, remote_voter_lookup_detail, remote_voter_detail):
                            detail_name_row = fetcher(hvid_for_name)
                            nm = smart_title(full_name(detail_name_row))
                            if nm and nm.lower() not in {"unnamed voter", "unnamed", "nan", "none", "null"} and not nm.lower().startswith("voter "):
                                break
                    except Exception:
                        nm = ""
                    if nm and nm.lower() not in {"unnamed voter", "unnamed", "nan", "none", "null"}:
                        view.at[idx, "DisplayName"] = nm
                missing_names = view["DisplayName"].astype(str).str.strip().eq("") | view["DisplayName"].astype(str).str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null"])
                view.loc[missing_names, "DisplayName"] = view.loc[missing_names].apply(lambda rr: f"Voter {cc_text(rr.get('voter_id',''))}" if cc_text(rr.get('voter_id','')) else "Household Voter", axis=1)

                st.markdown("""
                <style>
                div[data-testid="stButton"] button p { white-space: pre-line !important; line-height: 1.2 !important; text-align: center !important; }
                div[data-testid="stButton"] > button { white-space: pre-line !important; height: auto !important; min-height: 46px !important; text-align: center !important; justify-content: center !important; padding: 8px 12px !important; }
                </style>
                """, unsafe_allow_html=True)

                st.caption("Household members — click a card to open that voter.")
                for j, hhrow in view.iterrows():
                    hvid = cc_text(hhrow.get("voter_id", ""))
                    hname = smart_title(cc_text(hhrow.get("DisplayName", ""))) or hvid or "Household Voter"
                    is_current = hvid == selected_id
                    sub_bits = [x for x in [cc_text(hhrow.get("Party", "")), cc_text(hhrow.get("Gender", "")), ("Age " + cc_text(hhrow.get("Age", "")) if cc_text(hhrow.get("Age", "")) else "")] if x]
                    sub = " · ".join(sub_bits)
                    label = ("✓ " if is_current else "") + hname + (f"\n{sub}" if sub else "")
                    _card_col, _spacer_r = st.columns([0.42, 0.58])
                    with _card_col:
                        if is_current:
                            st.button(label, key=f"hh_current_{selected_id}_{j}_{hvid}", width="stretch", disabled=True)
                        else:
                            if st.button(label, key=f"open_household_card_{selected_id}_{j}_{hvid}", width="stretch"):
                                st.session_state["lookup_selected_id"] = hvid
                                try:
                                    hd = remote_voter_lookup_detail(hvid)
                                    st.session_state[f"lookup_detail_row_{hvid}"] = pd.DataFrame([hd]).iloc[0].to_dict()
                                except Exception:
                                    pass
                                for k in list(st.session_state.keys()):
                                    if str(k).startswith(("hh_df_", "vote_history_row_", "voter_pdf_bytes_", "voter_pdf_name_")):
                                        st.session_state.pop(k, None)
                                st.rerun()
        else:
            st.caption("No household members found from the selected address.")

        st.markdown("### Election History")
        vh_key = f"vote_history_row_{selected_id}"
        if vh_key not in st.session_state:
            st.session_state[vh_key] = pd.DataFrame([r]).iloc[0].to_dict()
        if st.button("Refresh Vote History", key=f"load_vote_history_{selected_id}"):
            with st.spinner("Loading vote history..."):
                full_r = remote_voter_lookup_detail(selected_id) if selected_id else r
                if full_r is None or len(full_r) == 0:
                    full_r = r
                st.session_state[vh_key] = pd.DataFrame([full_r]).iloc[0].to_dict()
                st.session_state[detail_key] = st.session_state[vh_key]
                st.rerun()
        render_election_history_table(pd.Series(st.session_state[vh_key]))


def safe_filtered_df(active: dict | None, max_rows: int = EXPORT_ROW_LIMIT) -> pd.DataFrame:
    active = active or {}
    special = active_special_filters() if "active_special_filters" in globals() else {}
    try:
        df = duckdb_detail_filtered_df(active, special, int(max_rows))
    except Exception as exc:
        st.warning(f"Could not prepare filtered voter file: {exc}")
        return pd.DataFrame()
    try:
        return normalize_download_df(df)
    except Exception:
        return df


def _mb_total_from_summary(summary: dict | None) -> int:
    if not summary:
        return 0
    for k in ["total", "Total", "TOTAL", "voters", "Voters"]:
        try:
            if k in summary:
                return int(float(summary.get(k) or 0))
        except Exception:
            pass
    try:
        return int(float(summary.get("R", 0) or 0) + float(summary.get("D", 0) or 0) + float(summary.get("O", 0) or 0))
    except Exception:
        return 0


def _mb_special_filters() -> dict:
    """Mail Ballot Center-only special filters.

    Do not use the Create Universe/global special filters here, because this
    workspace should be controlled by the Mail Ballot widgets shown on this page.
    """
    special = {}
    score = st.session_state.get(special_key("mb_score_center"), (0, 4))
    try:
        lo, hi = int(score[0]), int(score[1])
        if lo > 0 or hi < 4:
            special["MB_Prob_Score"] = {"min": lo, "max": hi}
    except Exception:
        pass
    return special


@st.cache_data(ttl=300, show_spinner=False)
def _mb_index_group_cached(active_json: str, special_json: str, field: str, limit: int = 12) -> pd.DataFrame:
    active = json.loads(active_json or "{}")
    special = json.loads(special_json or "{}")
    urls = index_urls_from_manifest()
    url_list = "[" + ",".join(sql_lit(u) for u in urls) + "]"
    where = index_where_sql(active or {}, special or {})
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try:
                con.execute("LOAD httpfs;")
            except Exception:
                pass
        q = f"""
            SELECT CAST({sql_ident(field)} AS VARCHAR) AS Category, COUNT(*) AS Voters
            FROM read_parquet({url_list}, union_by_name=true)
            {where}
            GROUP BY CAST({sql_ident(field)} AS VARCHAR)
            ORDER BY Voters DESC
            LIMIT {int(limit)}
        """
        return con.execute(q).df()
    finally:
        try:
            con.close()
        except Exception:
            pass


def _mb_summary(active: dict) -> tuple[dict | None, str, Exception | None]:
    """Use the prebuilt count cube for Mail Ballot Center counts.

    This keeps the MB workspace fast. The cube can answer filter/count questions
    instantly as long as the filter fields are in the cube. Full voter files still
    use detail shards only after the user clicks a prepare-download button.
    """
    try:
        summary = duckdb_count_cube_summary(
            json.dumps(active or {}, sort_keys=True),
            json.dumps(_mb_special_filters(), sort_keys=True),
        )
        return summary, "mail-ballot-cube", None
    except Exception as e:
        # Fallback: index shards are slower, but better than returning nothing.
        try:
            summary = duckdb_index_summary(
                json.dumps(active or {}, sort_keys=True),
                json.dumps(_mb_special_filters(), sort_keys=True),
            )
            return summary, "mail-ballot-index-fallback", None
        except Exception:
            return None, "unavailable", e


def _mb_count(active: dict, extra: dict | None = None) -> int:
    a = dict(active or {})
    for k, v in (extra or {}).items():
        a[k] = v if isinstance(v, list) else [v]
    try:
        summary, _mode, _err = _mb_summary(a)
        return _mb_total_from_summary(summary)
    except Exception:
        return 0


def _mb_group_df(active: dict, field: str, limit: int = 12) -> pd.DataFrame:
    try:
        df = duckdb_count_cube_group_filtered(
            json.dumps(active or {}, sort_keys=True),
            json.dumps(_mb_special_filters(), sort_keys=True),
            field,
            limit,
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=["Category", "Voters", "%"])
        df = df.rename(columns={"label": "Category"}).copy()
        df["Category"] = df["Category"].astype(str).replace({"": "(blank)", "nan": "(blank)", "None": "(blank)"})
        df["Voters"] = pd.to_numeric(df["Voters"], errors="coerce").fillna(0).astype(int)
        total = max(1, int(df["Voters"].sum()))
        df["%"] = (df["Voters"] / total * 100).map(lambda x: f"{x:.1f}%")
        return df[["Category", "Voters", "%"]]
    except Exception:
        # Fallback only if the cube is missing a field.
        try:
            df = _mb_index_group_cached(
                json.dumps(active or {}, sort_keys=True),
                json.dumps(_mb_special_filters(), sort_keys=True),
                field,
                limit,
            )
            if df is None or df.empty:
                return pd.DataFrame(columns=["Category", "Voters", "%"])
            df = df.copy()
            df["Category"] = df["Category"].astype(str).replace({"": "(blank)", "nan": "(blank)", "None": "(blank)"})
            df["Voters"] = pd.to_numeric(df["Voters"], errors="coerce").fillna(0).astype(int)
            total = max(1, int(df["Voters"].sum()))
            df["%"] = (df["Voters"] / total * 100).map(lambda x: f"{x:.1f}%")
            return df[["Category", "Voters", "%"]]
        except Exception:
            return pd.DataFrame(columns=["Category", "Voters", "%"])


def cc_table(df: pd.DataFrame, height: int | None = None, key: str | None = None):
    """Readable sortable Streamlit table with comma-formatted numbers.

    Streamlit's dataframe grid already gives sortable headers and sticky headers
    when a height is provided. This wrapper adds comma formatting and consistent
    centered styling for the tables we show across workspaces.
    """
    if df is None:
        df = pd.DataFrame()
    show = df.copy()
    # Format common numeric columns with commas while preserving sort where possible
    fmt_cols = {}
    for col in show.columns:
        if col in {"Voters", "Total", "Count", "Rows", "Households"} or str(col).lower().endswith(" voters"):
            show[col] = pd.to_numeric(show[col], errors="coerce")
            fmt_cols[col] = "{:,.0f}"
        elif pd.api.types.is_integer_dtype(show[col]) or pd.api.types.is_float_dtype(show[col]):
            # Keep percentage-like columns alone; format other large numeric columns.
            if str(col).strip() not in {"%", "Percent", "Pct"}:
                fmt_cols[col] = "{:,.0f}"
    try:
        styler = show.style.format(fmt_cols, na_rep="").set_properties(**{"text-align": "center"}).set_table_styles([
            {"selector": "th", "props": [("text-align", "center"), ("font-weight", "900")]},
            {"selector": "td", "props": [("text-align", "center")]},
        ])
        return st.dataframe(styler, hide_index=True, width="stretch", height=height, key=key)
    except Exception:
        # Safe fallback: pre-format values as strings.
        for col, spec in fmt_cols.items():
            show[col] = show[col].map(lambda x: "" if pd.isna(x) else format(float(x), ",.0f"))
        return st.dataframe(show, hide_index=True, width="stretch", height=height, key=key)

def _mb_render_metric(label: str, value: int, note: str = "", color_class: str = ""):
    st.markdown(
        f"""
        <div class="cc-icon-metric {color_class}">
          <div>
            <div class="cc-icon-label">{label}</div>
            <div class="cc-icon-value">{int(value or 0):,}</div>
            <div class="cc-icon-sub">{note}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _mb_prepare_download(active: dict, label: str, file_prefix: str, max_rows: int = 50000):
    key = special_key("mb_export_" + re.sub(r"[^a-z0-9]+", "_", file_prefix.lower()))
    if st.button(f"Prepare {label}", key=key + "_btn", width="stretch"):
        with st.spinner(f"Preparing {label}..."):
            df = duckdb_detail_filtered_df(active, _mb_special_filters(), int(max_rows))
            keep = [c for c in [
                "voter_id", "FirstName", "MiddleName", "LastName", "NameSuffix", "FullName",
                "Party", "Gender", "Age", "Age_Range", "County", "Municipality", "Precinct",
                "House Number", "House Number Suffix", "Street Name", "Apartment Number", "Address Line 2", "City", "State", "Zip",
                "Email", "Mobile", "Landline", "Current_ApplicantPhone",
                "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "MB_Prob_Score",
                "Current_App_Return_Date", "Current_Ballot_Sent_Date", "Current_Ballot_Returned_Date", "Tags"
            ] if c in df.columns]
            if keep:
                df = df[keep].copy()
            st.session_state[key + "_csv"] = df.to_csv(index=False).encode()
            st.session_state[key + "_rows"] = len(df)
    if key + "_csv" in st.session_state:
        st.download_button(
            f"Download {label} ({st.session_state.get(key + '_rows', 0):,} rows)",
            st.session_state[key + "_csv"],
            f"{file_prefix}.csv",
            "text/csv",
            width="stretch",
        )


def render_mail_ballot_workspace():
    st.markdown("## Mail Ballot Center")
    st.caption("Strategic mail ballot operations: cultivate applications, message applicants, chase outstanding ballots, and build targeted files.")

    # Sidebar owns this checkbox. Mail Ballot Center reads the last applied Create Universe,
    # not whatever happens to be visible in the Create Universe widgets.
    start_from_current = bool(st.session_state.get(special_key("mb_start_current"), False))
    saved_universe = get_current_universe_filters()
    base = dict(saved_universe) if (start_from_current and saved_universe) else {}
    if start_from_current and saved_universe:
        st.info(f"Starting from current universe: {st.session_state.get('current_universe_label', 'Selected universe')}")
    elif start_from_current and not saved_universe:
        st.warning("No current universe has been applied yet. Showing statewide mail-ballot data.")

    preset = st.selectbox(
        "Mail ballot mission",
        [
            "Snapshot / Custom",
            "Cultivate new mail ballot applications",
            "Message ballot applicants",
            "Chase sent ballots not returned",
            "Cure / problem ballot follow-up",
            "Permanent mail ballot growth",
        ],
        key=special_key("mb_mission"),
        help="This changes only the Mail Ballot Center filters. It does not send you back to Create Universe.",
    )

    mission_defaults = {}
    if preset == "Cultivate new mail ballot applications":
        mission_defaults = {"MB_App": ["No", "N", "DNA", "Not Applied"]}
    elif preset == "Message ballot applicants":
        mission_defaults = {"MB_App": ["Yes", "Y", "Applied"], "MB_Sent": ["No", "N", "Not Sent"]}
    elif preset == "Chase sent ballots not returned":
        mission_defaults = {"MB_Sent": ["Yes", "Y", "Sent"], "MB_Status": ["Not Voted", "Not Returned", "No", "N"]}
    elif preset == "Cure / problem ballot follow-up":
        mission_defaults = {"MB_Status": ["Cancelled", "Pending", "Rejected", "Challenged", "Cure", "Problem"]}
    elif preset == "Permanent mail ballot growth":
        mission_defaults = {"MB_PERM": ["No", "N", "0", "False"]}

    c1, c2, c3, c4 = st.columns(4)
    party = c1.multiselect("Party", field_options(filter_options, "Party", base), default=base.get("Party", []), key=special_key("mb_party"))
    gender = c2.multiselect("Gender", field_options(filter_options, "Gender", base), default=base.get("Gender", []), key=special_key("mb_gender"))
    age = c3.multiselect("Age Range", field_options(filter_options, "Age_Range", base), default=base.get("Age_Range", []), key=special_key("mb_age"))
    score = c4.slider("MB Probability Score", 0, 4, st.session_state.get(special_key("mb_score_center"), (0, 4)), key=special_key("mb_score_center"))

    # Separate "did they apply?" from "what status is the application?"
    # This matters for cultivation work: users need to target DNA / Not Applied voters
    # without accidentally selecting Approved or Declined application statuses.
    def _default_mb_vals(field, candidates):
        valid = list(field_options(filter_options, field, base))
        return [v for v in (candidates or []) if v in valid]

    c5, c6, c7, c8, c9 = st.columns(5)
    app_filed = c5.multiselect(
        "Mail Ballot Application",
        field_options(filter_options, "MB_App", base),
        default=_default_mb_vals("MB_App", mission_defaults.get("MB_App", [])),
        key=special_key("mb_app_filed"),
        help="Use this for application cultivation. Choose DNA / No / Not Applied to exclude voters who already applied.",
    )
    app = c6.multiselect(
        "Application Status",
        field_options(filter_options, "MB_App_Status", base),
        default=_default_mb_vals("MB_App_Status", mission_defaults.get("MB_App_Status", [])),
        key=special_key("mb_app_status"),
        help="Use this after an application exists, for example Approved or Declined.",
    )
    sent = c7.multiselect("Ballot Sent", field_options(filter_options, "MB_Sent", base), key=special_key("mb_sent"))
    ret = c8.multiselect("Ballot Status", field_options(filter_options, "MB_Status", base), key=special_key("mb_status"))
    perm = c9.multiselect("Permanent MB", field_options(filter_options, "MB_PERM", base), key=special_key("mb_perm"))

    c10, c11, c12 = st.columns(3)
    v4a = c10.multiselect("Vote History - All", field_options(filter_options, "V4A", base), key=special_key("mb_v4a"))
    v4g = c11.multiselect("Vote History - General", field_options(filter_options, "V4G", base), key=special_key("mb_v4g"))
    v4p = c12.multiselect("Vote History - Primary", field_options(filter_options, "V4P", base), key=special_key("mb_v4p"))

    mb_active = dict(base)
    for fld, vals in {"Party": party, "Gender": gender, "Age_Range": age, "MB_App": app_filed, "MB_App_Status": app, "MB_Sent": sent, "MB_Status": ret, "MB_PERM": perm, "V4A": v4a, "V4G": v4g, "V4P": v4p}.items():
        if vals:
            mb_active[fld] = vals
    for fld, vals in mission_defaults.items():
        if fld not in mb_active or not mb_active.get(fld):
            valid = set(field_options(filter_options, fld, base))
            matched = [v for v in vals if v in valid]
            if matched:
                mb_active[fld] = matched

    st.session_state[special_key("mb_prob_score_range")] = score
    if st.button("Apply Mail Ballot Center Filters", width="stretch", type="primary"):
        st.session_state[special_key("mb_last_active")] = mb_active
        st.success("Mail Ballot Center filters applied here. Create Universe filters were not changed.")

    summary, mode, err = _mb_summary(mb_active)
    if mode == "mail-ballot-cube":
        st.caption("Counts are using the fast prebuilt count cube. Full voter files are prepared only when you click a download button.")
    total = _mb_total_from_summary(summary)
    applied = _mb_count(mb_active, {"MB_App": ["Yes", "Y", "Applied"]}) or _mb_count(mb_active, {"MB_App_Status": ["Applied", "Approved", "Pending"]})
    not_applied = _mb_count(mb_active, {"MB_App": ["No", "N", "DNA", "Not Applied"]}) or max(0, total - applied)
    sent_count = _mb_count(mb_active, {"MB_Sent": ["Yes", "Y", "Sent"]})
    returned = _mb_count(mb_active, {"MB_Status": ["Voted", "Returned", "Ballot Returned"]})
    chase = max(0, sent_count - returned) if sent_count else _mb_count(mb_active, {"MB_Status": ["Not Voted", "Not Returned"]})

    st.markdown("### Mail Ballot Snapshot")
    m1, m2, m3, m4, m5 = st.columns(5)
    with m1: _mb_render_metric("Current Universe", total, "After MB Center filters", "")
    with m2: _mb_render_metric("Likely App Targets", not_applied, "No/DNA/not applied", "green")
    with m3: _mb_render_metric("Applicants", applied, "Applied/approved/pending", "blue")
    with m4: _mb_render_metric("Ballots Sent", sent_count, "Sent to voters", "gold")
    with m5: _mb_render_metric("Chase Universe", chase, "Sent minus returned", "")

    tabs = st.tabs(["Plan", "Analyze", "Build Files", "Notes"])

    with tabs[0]:
        st.markdown("### Recommended workflow")
        st.markdown("""
**1. Cultivate applications:** start with high MB probability voters who have not applied. Prioritize reliable general-election voters and voters with phones/email.  
**2. Message applicants:** voters who applied but have not yet been sent a ballot need status updates and reminders.  
**3. Chase ballots:** voters with ballots sent but not returned are the highest-priority follow-up universe.  
**4. Cure/problem follow-up:** isolate rejected, pending, challenged, or cure-status ballots and handle separately.  
**5. Permanent MB growth:** after the election cycle, identify strong MB users who are not permanent.
""")
        st.info("This section stays inside Mail Ballot Center. It does not overwrite the main Create Universe filters unless we intentionally add a Send to Universe button later.")

    with tabs[1]:
        left, right = st.columns(2)
        with left:
            st.markdown("#### Party")
            cc_table(_mb_group_df(mb_active, "Party", 10), height=220, key=special_key("mb_tbl_party"))
            st.markdown("#### Gender")
            cc_table(_mb_group_df(mb_active, "Gender", 10), height=220, key=special_key("mb_tbl_gender"))
            st.markdown("#### Application Status")
            cc_table(_mb_group_df(mb_active, "MB_App_Status", 12), height=240, key=special_key("mb_tbl_app_status"))
        with right:
            st.markdown("#### Age Range")
            cc_table(_mb_group_df(mb_active, "Age_Range", 12), height=240, key=special_key("mb_tbl_age"))
            st.markdown("#### Vote History - General")
            cc_table(_mb_group_df(mb_active, "V4G", 8), height=220, key=special_key("mb_tbl_v4g"))
            st.markdown("#### Ballot Status")
            cc_table(_mb_group_df(mb_active, "MB_Status", 12), height=240, key=special_key("mb_tbl_mb_status"))

    with tabs[2]:
        st.markdown("### Build Mail Ballot Files")
        st.caption("Files are prepared only when you click a button, so the page stays fast. Each file respects the Mail Ballot Center filters currently shown above.")
        st.info("For application cultivation, use the Mail Ballot Application filter above and choose DNA / No / Not Applied. Application Status is for voters who already have an application record, such as Approved or Declined.")
        f1, f2, f3 = st.columns(3)
        with f1:
            st.markdown("**Application Cultivation File**")
            st.caption("Voters who look like good mail-ballot prospects but have not applied. Use for application mail, calls, texts, or digital follow-up.")
            cultivate = dict(mb_active)
            if "MB_App" not in cultivate and "MB_App_Status" not in cultivate:
                cultivate["MB_App"] = [v for v in ["No", "N", "DNA", "Not Applied"] if v in field_options(filter_options, "MB_App", base)] or cultivate.get("MB_App", [])
            _mb_prepare_download(cultivate, "Application Cultivation File", "mail_ballot_cultivate_apps", 50000)
        with f2:
            st.markdown("**Applicant Messaging File**")
            st.caption("Voters with an application/applicant status. Use for education, reminders, and ballot-arrival messaging.")
            applicants = dict(mb_active)
            if "MB_App" not in applicants and "MB_App_Status" not in applicants:
                applicants["MB_App"] = [v for v in ["Yes", "Y", "Applied"] if v in field_options(filter_options, "MB_App", base)] or applicants.get("MB_App", [])
            _mb_prepare_download(applicants, "Applicant Messaging File", "mail_ballot_applicant_message", 50000)
        with f3:
            st.markdown("**Ballot Chase File**")
            st.caption("Voters with ballots sent but not yet marked returned/voted. Use for chase calls, texts, and door follow-up.")
            chase_active = dict(mb_active)
            if "MB_Sent" not in chase_active:
                chase_active["MB_Sent"] = [v for v in ["Yes", "Y", "Sent"] if v in field_options(filter_options, "MB_Sent", base)] or chase_active.get("MB_Sent", [])
            if "MB_Status" not in chase_active:
                chase_active["MB_Status"] = [v for v in ["Not Voted", "Not Returned", "No", "N"] if v in field_options(filter_options, "MB_Status", base)] or chase_active.get("MB_Status", [])
            _mb_prepare_download(chase_active, "Ballot Chase File", "mail_ballot_chase", 50000)
        st.divider()
        st.markdown("**Current Mail Ballot Center Universe**")
        st.caption("A general-purpose export of exactly the current Mail Ballot Center universe after your mission and quick filters.")
        _mb_prepare_download(mb_active, "Current Mail Ballot Center Universe", "mail_ballot_center_current_universe", 100000)

    with tabs[3]:
        st.text_area("Mail ballot notes / plan", key=special_key("mb_notes"), height=180)



def _area_clean_label(value) -> str:
    s = str(value or "").strip()
    if not s or s.lower() in {"nan", "none", "null", "(blank)", "blank"}:
        return "(Blank)"
    return s


def _area_pct(n, d) -> str:
    try:
        n = float(n or 0); d = float(d or 0)
        return "0.0%" if d <= 0 else f"{(n/d)*100:.1f}%"
    except Exception:
        return "0.0%"


def _area_group_df(active: dict, field: str, limit: int = 20) -> pd.DataFrame:
    """Fast cube-backed group table for Area Intelligence."""
    try:
        special = {k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}
        df = duckdb_count_cube_group_filtered(
            json.dumps(count_safe_filters(active or {}), sort_keys=True),
            json.dumps(special or {}, sort_keys=True),
            field,
            int(limit),
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=["Category", "Voters", "%"])
        df = df.rename(columns={"label": "Category"}).copy()
        df["Category"] = df["Category"].map(_area_clean_label)
        df["Voters"] = pd.to_numeric(df["Voters"], errors="coerce").fillna(0).astype(int)
        total = max(1, int(df["Voters"].sum()))
        df["%"] = df["Voters"].map(lambda x: _area_pct(x, total))
        return df[["Category", "Voters", "%"]]
    except Exception:
        return pd.DataFrame(columns=["Category", "Voters", "%"])


@st.cache_data(ttl=300, show_spinner=False)
def _area_breakdown_cube(active_json: str, special_json: str, breakdown: str, limit: int = 250) -> pd.DataFrame:
    """One-pass geography/jurisdiction profile table from count_cube."""
    active = json.loads(active_json or "{}")
    special = json.loads(special_json or "{}")
    if not re.fullmatch(r"[A-Za-z0-9_ /-]+", str(breakdown)):
        return pd.DataFrame()
    url = count_cube_url()
    where = count_cube_where_sql(active, special)
    b = sql_ident(breakdown)
    q = f"""
        SELECT
            CAST({b} AS VARCHAR) AS Area,
            SUM(Voters) AS Total,
            SUM(CASE WHEN CAST(Party AS VARCHAR) = 'R' THEN Voters ELSE 0 END) AS R,
            SUM(CASE WHEN CAST(Party AS VARCHAR) = 'D' THEN Voters ELSE 0 END) AS D,
            SUM(CASE WHEN CAST(Party AS VARCHAR) NOT IN ('R','D') THEN Voters ELSE 0 END) AS O,
            SUM(CASE WHEN CAST(Gender AS VARCHAR) = 'F' THEN Voters ELSE 0 END) AS Female,
            SUM(CASE WHEN CAST(Gender AS VARCHAR) = 'M' THEN Voters ELSE 0 END) AS Male,
            SUM(CASE WHEN CAST(Age_Range AS VARCHAR) IN ('65+', '65 Plus', '65 and over') THEN Voters ELSE 0 END) AS Age65Plus,
            SUM(CASE WHEN TRY_CAST(V4G AS DOUBLE) >= 3 THEN Voters ELSE 0 END) AS StrongGeneral,
            SUM(CASE WHEN TRY_CAST(V4A AS DOUBLE) >= 3 THEN Voters ELSE 0 END) AS StrongAll,
            SUM(CASE WHEN TRY_CAST(MB_Prob_Score AS DOUBLE) >= 3 THEN Voters ELSE 0 END) AS MBProspects,
            SUM(CASE WHEN UPPER(CAST(MB_App AS VARCHAR)) IN ('Y','YES','APPLIED','TRUE','1') OR UPPER(CAST(MB_App_Status AS VARCHAR)) IN ('APPROVED','PENDING') THEN Voters ELSE 0 END) AS MBApplicants,
            SUM(CASE WHEN UPPER(CAST(MB_Sent AS VARCHAR)) IN ('Y','YES','SENT','TRUE','1') THEN Voters ELSE 0 END) AS MBSent,
            SUM(CASE WHEN UPPER(CAST(MB_Status AS VARCHAR)) IN ('VOTED','RETURNED','BALLOT RETURNED') THEN Voters ELSE 0 END) AS MBReturned
        FROM read_parquet({sql_lit(url)})
        {where}
        GROUP BY CAST({b} AS VARCHAR)
        HAVING SUM(Voters) > 0
        ORDER BY Total DESC
        LIMIT {int(limit)}
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try: con.execute("LOAD httpfs;")
            except Exception: pass
        df = con.execute(q).df()
        if df is None or df.empty:
            return pd.DataFrame()
        df["Area"] = df["Area"].map(_area_clean_label)
        for c in [x for x in df.columns if x != "Area"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        df["R %"] = df.apply(lambda r: _area_pct(r["R"], r["Total"]), axis=1)
        df["D %"] = df.apply(lambda r: _area_pct(r["D"], r["Total"]), axis=1)
        df["O %"] = df.apply(lambda r: _area_pct(r["O"], r["Total"]), axis=1)
        df["65+ %"] = df.apply(lambda r: _area_pct(r["Age65Plus"], r["Total"]), axis=1)
        df["Strong Gen %"] = df.apply(lambda r: _area_pct(r["StrongGeneral"], r["Total"]), axis=1)
        df["MB Prospect %"] = df.apply(lambda r: _area_pct(r["MBProspects"], r["Total"]), axis=1)
        df["MB Return %"] = df.apply(lambda r: _area_pct(r["MBReturned"], r["MBSent"]), axis=1)
        return df[["Area", "Total", "R", "D", "O", "R %", "D %", "O %", "Female", "Male", "Age65Plus", "65+ %", "StrongGeneral", "Strong Gen %", "MBProspects", "MB Prospect %", "MBApplicants", "MBSent", "MBReturned", "MB Return %"]]
    except Exception:
        return pd.DataFrame()
    finally:
        try: con.close()
        except Exception: pass


def _area_default_breakdown(active: dict) -> str:
    active = active or {}
    # Exact user rule first.
    if len(active.get("Municipality") or []) == 1:
        return "Precinct"
    if len(active.get("County") or []) == 1:
        return "Municipality"
    # If district filter likely collapses to one county, try to detect it quickly.
    try:
        county_df = _area_group_df(active, "County", 5)
        nonblank = county_df[county_df["Category"].astype(str).str.strip().ne("(Blank)")]
        if len(nonblank) == 1:
            return "Municipality"
    except Exception:
        pass
    return "County"


def _area_universe_label(active: dict) -> str:
    if not active:
        return "Pennsylvania Statewide"
    try:
        return universe_label_from_filters(active)
    except Exception:
        parts = []
        for k, vals in active.items():
            if vals:
                v = vals[0] if len(vals) == 1 else f"{len(vals)} selected"
                parts.append(f"{DISPLAY_LABELS.get(k,k)}: {v}")
        return " · ".join(parts) if parts else "Selected Universe"


def _area_insights(summary: dict, party_df: pd.DataFrame, age_df: pd.DataFrame, mb_df: pd.DataFrame) -> list[str]:
    total = int(summary.get("total", 0) or 0)
    r = int(summary.get("r", 0) or 0); d = int(summary.get("d", 0) or 0); o = int(summary.get("o", 0) or 0)
    insights = []
    if total:
        if abs(r-d) / total >= 0.05:
            leader = "Republican" if r > d else "Democratic"
            margin = abs(r-d)
            insights.append(f"The universe has a {leader} registration advantage of {margin:,} voters ({_area_pct(margin, total)} of the universe).")
        else:
            insights.append("The partisan registration balance is relatively close, so turnout quality and voter-contact targeting may matter more than raw party advantage.")
        if o / total >= 0.15:
            insights.append(f"Other/unaffiliated voters are a meaningful bloc at {_area_pct(o, total)}, making persuasion and issue-based outreach important.")
    try:
        age65 = age_df[age_df["Category"].astype(str).str.contains("65", regex=False)]["Voters"].sum()
        if total and age65 / total >= 0.20:
            insights.append(f"Older voters are a major part of the universe ({_area_pct(age65, total)} age 65+), supporting mail, phone, and repeated direct-contact programs.")
    except Exception:
        pass
    try:
        applied = int(mb_df[mb_df["Category"].astype(str).str.upper().isin(["Y","YES","APPLIED"])] ["Voters"].sum())
        if total and applied / total < 0.25:
            insights.append("Mail-ballot application usage appears limited enough that cultivation can still grow the reachable vote universe.")
    except Exception:
        pass
    if not insights:
        insights.append("This universe is ready for a basic field strategy review using party, age, vote-history, and mail-ballot behavior below.")
    return insights[:5]


def _area_bar_html(df: pd.DataFrame, title: str, max_rows: int = 8) -> str:
    if df is None or df.empty:
        return f'<div class="cc-home-card"><h3>{title}</h3><div class="cc-sub">No data available.</div></div>'
    show = df.head(max_rows).copy()
    maxv = max(1, int(pd.to_numeric(show["Voters"], errors="coerce").fillna(0).max()))
    rows = []
    for _, r in show.iterrows():
        lab = str(r["Category"])
        val = int(r["Voters"] or 0)
        w = max(2, val / maxv * 100)
        pct_s = str(r.get("%", ""))
        rows.append(f'<div class="cc-age-row"><b>{lab}</b><div class="cc-age-bar-bg"><div class="cc-age-bar" style="width:{w:.1f}%"></div></div><span>{val:,} ({pct_s})</span></div>')
    return f'<div class="cc-home-card"><h3>{title}</h3>' + ''.join(rows) + '</div>'


def _area_pdf_bytes(title: str, active: dict, summary: dict, insights: list[str], tables: dict[str, pd.DataFrame], breakdown_field: str) -> bytes:
    """Professional report PDF for Area Intelligence. Built as a real report, not a screenshot."""
    bio = io.BytesIO()
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter, landscape
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    except Exception:
        return b""

    doc = SimpleDocTemplate(bio, pagesize=landscape(letter), rightMargin=0.35*inch, leftMargin=0.35*inch, topMargin=0.35*inch, bottomMargin=0.35*inch)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="CC_Title", parent=styles["Title"], fontSize=26, leading=30, textColor=colors.HexColor("#111827"), spaceAfter=12))
    styles.add(ParagraphStyle(name="CC_H", parent=styles["Heading2"], fontSize=14, leading=16, textColor=colors.HexColor("#9f151c"), spaceBefore=8, spaceAfter=6))
    styles.add(ParagraphStyle(name="CC_Body", parent=styles["BodyText"], fontSize=9, leading=12))
    story = []
    story.append(Paragraph("Candidate Connect Area Intelligence Report", styles["CC_Title"]))
    story.append(Paragraph(f"<b>Universe:</b> {title}", styles["CC_Body"]))
    story.append(Paragraph(f"<b>Report date:</b> {datetime.now().strftime('%m/%d/%Y')}", styles["CC_Body"]))
    story.append(Spacer(1, 0.15*inch))
    cards = [["Total Voters", "Republican", "Democrat", "Other / Unaffiliated"], [f"{summary.get('total',0):,}", f"{summary.get('r',0):,}", f"{summary.get('d',0):,}", f"{summary.get('o',0):,}"]]
    t = Table(cards, colWidths=[2.35*inch]*4)
    t.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#9f151c')),('TEXTCOLOR',(0,0),(-1,0),colors.white),('FONTNAME',(0,0),(-1,-1),'Helvetica-Bold'),('ALIGN',(0,0),(-1,-1),'CENTER'),('GRID',(0,0),(-1,-1),0.4,colors.HexColor('#d1d5db')),('FONTSIZE',(0,0),(-1,-1),11),('BOTTOMPADDING',(0,0),(-1,-1),8),('TOPPADDING',(0,0),(-1,-1),8)
    ]))
    story.append(t)
    story.append(Spacer(1, 0.15*inch))
    story.append(Paragraph("Executive Strategy Summary", styles["CC_H"]))
    for ins in insights:
        story.append(Paragraph(f"• {ins}", styles["CC_Body"]))
    story.append(Spacer(1, 0.1*inch))

    def add_df(title_s, df, max_rows=16):
        if df is None or df.empty:
            return
        story.append(Paragraph(title_s, styles["CC_H"]))
        show = df.head(max_rows).copy()
        # keep readable width
        if len(show.columns) > 10:
            keep = list(show.columns[:10])
            show = show[keep]
        data = [list(show.columns)]
        for _, row in show.iterrows():
            vals=[]
            for c in show.columns:
                v = row[c]
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    vals.append(f"{v:,.0f}")
                else:
                    vals.append(str(v))
            data.append(vals)
        colw = [max(0.75*inch, min(1.65*inch, 9.7*inch/len(data[0])))] * len(data[0])
        tbl = Table(data, repeatRows=1, colWidths=colw)
        tbl.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#111827')),('TEXTCOLOR',(0,0),(-1,0),colors.white),('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('ALIGN',(0,0),(-1,-1),'CENTER'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('GRID',(0,0),(-1,-1),0.25,colors.HexColor('#cbd5e1')),('FONTSIZE',(0,0),(-1,-1),7),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white, colors.HexColor('#f3f4f6')]),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4)
        ]))
        story.append(tbl)
        story.append(Spacer(1, 0.1*inch))

    add_df("Core Profile: Party", tables.get("Party"), 8)
    add_df("Core Profile: Age", tables.get("Age"), 10)
    add_df("Core Profile: Vote History", tables.get("VoteHistory"), 8)
    add_df("Core Profile: Mail Ballot", tables.get("MailBallot"), 8)
    story.append(PageBreak())
    add_df(f"Strategic Breakdown by {breakdown_field}", tables.get("Breakdown"), 30)
    doc.build(story)
    bio.seek(0)
    return bio.getvalue()


def render_area_intelligence_workspace():
    st.markdown("## Area Intelligence")
    st.caption("Professional geography and jurisdiction profile for campaign strategy, targeting, and client-ready reports.")

    saved = get_current_universe_filters()
    default_use = bool(saved)
    use_current = st.checkbox(
        f"Use current universe: {st.session_state.get('current_universe_label', 'None')}",
        value=default_use,
        disabled=not bool(saved),
        key=special_key("area_use_current_universe"),
        help="Use the universe last applied in Create Universe. If unchecked, Area Intelligence starts statewide.",
    )
    active = dict(saved) if (use_current and saved) else {}
    if active:
        st.info(f"Analyzing current universe: {_area_universe_label(active)}")
    else:
        st.info("Analyzing statewide universe. Build/apply a Create Universe first to profile a district, county, municipality, or custom target universe.")

    # Optional quick geography focus inside Area Intel without changing Create Universe.
    with st.expander("Optional: focus this Area Intelligence report without changing Create Universe", expanded=False):
        f1, f2, f3, f4 = st.columns(4)
        area_filters = {}
        for col, fld in [(f1,"County"), (f2,"Municipality"), (f3,"STH"), (f4,"STS")]:
            vals = col.multiselect(DISPLAY_LABELS.get(fld, fld), field_options(filter_options, fld, active), key=special_key("area_focus_" + fld))
            if vals:
                area_filters[fld] = vals
        f5, f6, f7, f8 = st.columns(4)
        for col, fld in [(f5,"USC"), (f6,"School District"), (f7,"School Region"), (f8,"Precinct")]:
            vals = col.multiselect(DISPLAY_LABELS.get(fld, fld), field_options(filter_options, fld, {**active, **area_filters}), key=special_key("area_focus_" + re.sub(r'[^A-Za-z0-9]+','_',fld)))
            if vals:
                area_filters[fld] = vals
        if area_filters:
            active = {**active, **area_filters}

    summary, mode, err = update_counts(active)
    if not summary:
        st.error(f"Area Intelligence counts are unavailable: {err}")
        return

    render_metrics(summary)

    party_df = _area_group_df(active, "Party", 8)
    gender_df = _area_group_df(active, "Gender", 8)
    age_df = _area_group_df(active, "Age_Range", 12)
    v4g_df = _area_group_df(active, "V4G", 8)
    mb_app_df = _area_group_df(active, "MB_App", 8)
    mb_status_df = _area_group_df(active, "MB_Status", 8)
    insights = _area_insights(summary, party_df, age_df, mb_app_df)

    st.markdown("### Executive Strategy Readout")
    for ins in insights:
        st.markdown(f"<div class='cc-note'>• {ins}</div>", unsafe_allow_html=True)

    c1, c2 = st.columns([1, 1])
    with c1:
        render_party_chart(summary, "Party Registration")
    with c2:
        st.markdown(_area_bar_html(age_df, "Age Range"), unsafe_allow_html=True)

    c3, c4 = st.columns([1, 1])
    with c3:
        st.markdown(_area_bar_html(v4g_df, "Vote History - General"), unsafe_allow_html=True)
    with c4:
        st.markdown(_area_bar_html(mb_status_df, "Mail Ballot Status"), unsafe_allow_html=True)

    default_breakdown = _area_default_breakdown(active)
    breakdown_options = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]
    default_idx = breakdown_options.index(default_breakdown) if default_breakdown in breakdown_options else 0
    breakdown = st.selectbox(
        "Break report down by",
        breakdown_options,
        index=default_idx,
        key=special_key("area_breakdown_by"),
        help="Default follows the next-area-down rule: statewide/multi-county → County; one county → Municipality; one municipality → Precinct. You can override it here.",
    )

    breakdown_df = _area_breakdown_cube(
        json.dumps(count_safe_filters(active or {}), sort_keys=True),
        json.dumps({k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}, sort_keys=True),
        breakdown,
        300,
    )

    st.markdown(f"### Strategic Breakdown by {DISPLAY_LABELS.get(breakdown, breakdown)}")
    if breakdown_df.empty:
        st.warning("No breakdown data available for this selection.")
    else:
        cc_table(breakdown_df, height=520, key=special_key("area_breakdown_table"))

    tabs = st.tabs(["Profile Tables", "Report", "Notes"])
    with tabs[0]:
        left, right = st.columns(2)
        with left:
            st.markdown("#### Party")
            cc_table(party_df, height=220, key=special_key("area_tbl_party"))
            st.markdown("#### Gender")
            cc_table(gender_df, height=220, key=special_key("area_tbl_gender"))
            st.markdown("#### Mail Ballot Application")
            cc_table(mb_app_df, height=220, key=special_key("area_tbl_mb_app"))
        with right:
            st.markdown("#### Age Range")
            cc_table(age_df, height=260, key=special_key("area_tbl_age"))
            st.markdown("#### Vote History - General")
            cc_table(v4g_df, height=220, key=special_key("area_tbl_v4g"))
            st.markdown("#### Ballot Status")
            cc_table(mb_status_df, height=220, key=special_key("area_tbl_mb_status"))
    with tabs[1]:
        report_title = _area_universe_label(active)
        st.markdown("### Client-ready Area Intelligence Report")
        st.caption("This is built as a report, not a screenshot: cover/summary, strategy notes, profile tables, and the selected geography breakdown.")
        if st.button("Prepare Area Intelligence PDF", key=special_key("area_pdf_btn"), type="primary"):
            with st.spinner("Preparing Area Intelligence report..."):
                pdf = _area_pdf_bytes(
                    report_title,
                    active,
                    summary,
                    insights,
                    {
                        "Party": party_df,
                        "Age": age_df,
                        "VoteHistory": v4g_df,
                        "MailBallot": mb_status_df,
                        "Breakdown": breakdown_df,
                    },
                    DISPLAY_LABELS.get(breakdown, breakdown),
                )
                st.session_state[special_key("area_pdf_bytes")] = pdf
        if st.session_state.get(special_key("area_pdf_bytes")):
            st.download_button(
                "Download Area Intelligence PDF",
                st.session_state[special_key("area_pdf_bytes")],
                "candidate_connect_area_intelligence_report.pdf",
                "application/pdf",
                width="stretch",
            )
    with tabs[2]:
        st.text_area("Area Intelligence notes / strategy", key=special_key("area_notes"), height=180)

def filtered_export_columns(df: pd.DataFrame) -> list[str]:
    base = ["voter_id","County","Municipality","Precinct","USC","STS","STH","School District","School Region",
            "FirstName","MiddleName","LastName","NameSuffix","FullName","Party","CalculatedParty","Gender","DOB","Age","Age_Range","RegistrationDate",
            "House Number","House Number Suffix","Street Name","Apartment Number","Address Line 2","City","State","Zip",
            "Email","Mobile","Landline","Current_ApplicantPhone","MB_App","MB_App_Status","MB_Sent","MB_Status","MB_PERM","MB_Prob_Score","Tags"]
    return [c for c in base if c in df.columns]



def safe_filtered_df(active: dict | None, max_rows: int = EXPORT_ROW_LIMIT) -> pd.DataFrame:
    """Live-safe detail export helper used by exports and Mail Ballot Center.

    Keeps heavy detail scans behind explicit download/prepare actions and applies
    the current special filters, including MB probability score and election filters.
    """
    active = active or {}
    special = active_special_filters() if "active_special_filters" in globals() else {}
    try:
        df = duckdb_detail_filtered_df(active, special, int(max_rows))
    except Exception as exc:
        st.warning(f"Could not prepare filtered voter file: {exc}")
        return pd.DataFrame()
    try:
        return normalize_download_df(df)
    except Exception:
        return df

def texting_export_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["voter_id","FirstName","MiddleName","LastName","NameSuffix","FullName","Precinct","Mobile"])
    df = normalize_download_df(df)
    df = df[df.get("Mobile", pd.Series([""]*len(df), index=df.index)).astype(str).str.strip().ne("")]
    cols = ["voter_id","FirstName","MiddleName","LastName","NameSuffix","FullName","Precinct","Mobile"]
    return df[[c for c in cols if c in df.columns]].copy()


def mail_export_df(df: pd.DataFrame, mailing_mode: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = normalize_download_df(df)
    if mailing_mode == "Householded":
        df = household_for_mail(df)
    cols = ["voter_id","HouseholdName","FirstName","MiddleName","LastName","NameSuffix","FullName","HouseholdCount",
            "House Number","House Number Suffix","Street Name","Apartment Number","Address Line 2","City","State","Zip",
            "County","Municipality","Precinct","Party","Gender","Age"]
    return df[[c for c in cols if c in df.columns]].copy()


def zip_bytes(files: dict[str, bytes]) -> bytes:
    bio = io.BytesIO()
    import zipfile
    with zipfile.ZipFile(bio, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for name, data in files.items():
            z.writestr(name, data)
    bio.seek(0)
    return bio.getvalue()


def auto_area_level_for_export(active: dict | None) -> str:
    """Pick the first Excel summary level automatically to remove a cluttering UI dropdown."""
    active = active or {}
    county = active.get("County") or []
    muni = active.get("Municipality") or []
    # If more than one county/municipality is in play, summarize by municipality.
    # If exactly one municipality is selected, summarize by precinct.
    if muni and len(muni) == 1:
        return "Precinct"
    if county or muni:
        return "Municipality"
    return "County"


def prepared_key_for(kind: str, ftype: str) -> str:
    safe = re.sub(r"[^a-z0-9]+", "_", f"{kind}_{ftype}".lower()).strip("_")
    return f"prepared_one_export_{safe}"


def build_single_export(active, export_kind: str, file_type: str, mailing_mode: str) -> tuple[str, bytes, str, int]:
    """Build one export/report at a time from a simple dropdown workflow."""
    area_level = auto_area_level_for_export(active)
    kind = export_kind.lower()
    ftype = file_type.lower()
    if export_kind in {"Full File", "Texting File", "Mail File"}:
        base_df = safe_filtered_df(active, EXPORT_ROW_LIMIT)
        if export_kind == "Texting File":
            out = texting_export_df(base_df)
            stem = "candidate_connect_texting"
        elif export_kind == "Mail File":
            out = mail_export_df(base_df, mailing_mode)
            stem = "candidate_connect_mail"
        else:
            out = base_df[[c for c in filtered_export_columns(base_df) if c in base_df.columns]].copy()
            out = drop_all_blank_optional_columns(out, required=["voter_id","FirstName","LastName","House Number","Street Name","City","State","Zip"])
            stem = "candidate_connect_filtered"
        if ftype == "csv":
            return f"{stem}.csv", out.to_csv(index=False).encode(), "text/csv", len(out)
        return f"{stem}.xlsx", dataframe_to_excel_bytes(out, area_level), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", len(out)
    if export_kind == "Street List PDF":
        return "candidate_connect_street_list.pdf", street_list_pdf(active), "application/pdf", 0
    if export_kind == "Call List PDF":
        return "candidate_connect_call_list.pdf", call_list_pdf(active), "application/pdf", 0
    if export_kind == "Mailing Labels PDF":
        return "candidate_connect_labels_avery5160.pdf", labels_pdf(active), "application/pdf", 0
    raise ValueError(f"Unsupported export type: {export_kind}")


def contact_tracking_template(kind: str) -> bytes:
    if kind == "Street Results":
        cols = ["voter_id","FullName","Street Name","House Number","Apartment Number","Phone","F","A","U","NH","Yard Sign","Notes"]
    else:
        cols = ["voter_id","FullName","Phone","Contacted","Result","Support Level","Follow-Up","Notes"]
    return pd.DataFrame(columns=cols).to_csv(index=False).encode()

def render_output_buttons(active):
    tabs = st.tabs(["Overview", "Exports", "Reports"])
    with tabs[0]:
        summary, mode, err = update_counts(active)
        if summary:
            render_metrics(summary)
            c1, c2 = st.columns([1, 1])
            with c1:
                render_party_chart(summary, "Party Breakdown")
            with c2:
                st.markdown("### Counts by Area")
                area_level_ov = st.selectbox("Area table", ["County", "Municipality", "Precinct", "School District", "School Region"], key=special_key("output_overview_area"))
                area_df_ov = duckdb_count_cube_group_filtered(json.dumps(count_safe_filters(active or {}), sort_keys=True), json.dumps({k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}, sort_keys=True), area_level_ov, 200)
                if not area_df_ov.empty:
                    area_df_ov = area_df_ov.rename(columns={"label": area_level_ov})
                    area_df_ov["Voters"] = area_df_ov["Voters"].fillna(0).astype(int)
                    if area_level_ov == "Precinct":
                        area_df_ov[area_level_ov] = area_df_ov[area_level_ov].map(canonical_precinct_display)
                        area_df_ov = area_df_ov.groupby(area_level_ov, as_index=False)["Voters"].sum()
                    area_df_ov = area_df_ov.sort_values(area_level_ov, kind="stable")
                    cc_table(area_df_ov, height=260, key=special_key("output_overview_area_table_display"))
        elif err:
            st.warning(err)

    with tabs[1]:
        st.markdown("### Export Center")
        st.caption("Pick one output type and one file type, prepare it, then download. Excel summaries are chosen automatically: county/multi-municipality universes summarize by municipality; one municipality summarizes by precinct.")
        e1, e2, e3 = st.columns([1.2, .8, 1.0])
        with e1:
            export_kind = st.selectbox("Download type", ["Full File", "Texting File", "Mail File", "Street List PDF", "Call List PDF", "Mailing Labels PDF"], key=special_key("export_kind"))
        with e2:
            allowed_types = ["PDF"] if export_kind.endswith("PDF") else ["CSV", "Excel"]
            file_type = st.selectbox("File type", allowed_types, key=special_key("export_file_type"))
        with e3:
            mailing_mode = st.radio("Mailing mode", ["Not Householded", "Householded"], horizontal=True, key=special_key("mailing_mode"), disabled=(export_kind != "Mail File"))

        key = prepared_key_for(export_kind, file_type)
        pcol, dcol = st.columns([1, 1])
        with pcol:
            if st.button("Prepare Download", width="stretch"):
                with st.spinner(f"Preparing {export_kind}..."):
                    filename, data, mime, row_count = build_single_export(active, export_kind, file_type, mailing_mode)
                    st.session_state[key] = {"filename": filename, "data": data, "mime": mime, "rows": row_count}
        with dcol:
            if key in st.session_state:
                item = st.session_state[key]
                label = f"Download {item['filename']}" + (f" ({item['rows']:,} rows)" if item.get("rows") else "")
                st.download_button(label, item["data"], item["filename"], item["mime"], width="stretch", on_click=mark_downloaded, args=(key,))
            else:
                st.button("Download", disabled=True, width="stretch")

        with st.expander("Batch ZIP export", expanded=False):
            selected_types = st.multiselect("Files to include", ["Full CSV", "Text CSV", "Mail CSV", "Full Excel", "Text Excel", "Mail Excel", "Street List PDF", "Call List PDF", "Mailing Labels PDF"], default=[], key=special_key("bulk_export_types"))
            zip_key = "prepared_export_zip"
            if st.button("Prepare Selected ZIP", width="stretch"):
                with st.spinner("Building selected ZIP..."):
                    base_df = safe_filtered_df(active, EXPORT_ROW_LIMIT)
                    files = {}
                    area_level = auto_area_level_for_export(active)
                    if "Full CSV" in selected_types:
                        fdf = base_df[[c for c in filtered_export_columns(base_df) if c in base_df.columns]]
                        files["candidate_connect_filtered.csv"] = fdf.to_csv(index=False).encode()
                    if "Text CSV" in selected_types:
                        files["candidate_connect_texting.csv"] = texting_export_df(base_df).to_csv(index=False).encode()
                    if "Mail CSV" in selected_types:
                        files["candidate_connect_mail.csv"] = mail_export_df(base_df, mailing_mode).to_csv(index=False).encode()
                    if "Full Excel" in selected_types:
                        fdf = base_df[[c for c in filtered_export_columns(base_df) if c in base_df.columns]]
                        files["candidate_connect_filtered.xlsx"] = dataframe_to_excel_bytes(fdf, area_level)
                    if "Text Excel" in selected_types:
                        files["candidate_connect_texting.xlsx"] = dataframe_to_excel_bytes(texting_export_df(base_df), area_level)
                    if "Mail Excel" in selected_types:
                        files["candidate_connect_mail.xlsx"] = dataframe_to_excel_bytes(mail_export_df(base_df, mailing_mode), area_level)
                    if "Street List PDF" in selected_types:
                        files["candidate_connect_street_list.pdf"] = street_list_pdf(active)
                    if "Call List PDF" in selected_types:
                        files["candidate_connect_call_list.pdf"] = call_list_pdf(active)
                    if "Mailing Labels PDF" in selected_types:
                        files["candidate_connect_labels_avery5160.pdf"] = labels_pdf(active)
                    st.session_state[zip_key] = zip_bytes(files) if files else b""
            if st.session_state.get(zip_key):
                st.download_button("Download Selected ZIP", st.session_state[zip_key], "candidate_connect_exports.zip", "application/zip", width="stretch", on_click=mark_downloaded, args=(zip_key,))

    with tabs[2]:
        st.markdown("### Reports + Tracking")
        st.caption("Prepare one PDF/report at a time. Street and call lists are sorted like the local list and include mobile/landline phone labels.")

        # Clean report workflow: no stale download button and no wide, confusing buttons.
        r1, r2, spacer = st.columns([1.25, .7, 2.2])
        with r1:
            report_kind = st.selectbox(
                "Report type",
                ["Street List PDF", "Call List PDF", "Mailing Labels PDF"],
                key=special_key("report_kind_clean"),
            )
        with r2:
            file_type = st.selectbox("File type", ["PDF"], key=special_key("report_file_type_clean"))

        report_key = prepared_key_for(report_kind, "PDF")
        # If the user changes the report type, do not show an older report download as if it were ready.
        current_ready_key = st.session_state.get("prepared_report_ready_key")
        report_is_ready = current_ready_key == report_key and report_key in st.session_state

        b1, b2, b3 = st.columns([.9, 1.25, 3.0])
        with b1:
            prepare_clicked = st.button("Prepare Report", key=special_key("prepare_report_button"))
        if prepare_clicked:
            # Clear old report artifacts first so no stale download appears.
            for k in list(st.session_state.keys()):
                if str(k).startswith("prepared_one_export_street_list_pdf") or str(k).startswith("prepared_one_export_call_list_pdf") or str(k).startswith("prepared_one_export_mailing_labels_pdf"):
                    _ = st.session_state.pop(k, None)
            _ = st.session_state.pop("prepared_report_ready_key", None)
            with st.spinner(f"Building {report_kind}..."):
                filename, data, mime, row_count = build_single_export(
                    active,
                    report_kind,
                    "PDF",
                    st.session_state.get(special_key("mailing_mode"), "Not Householded"),
                )
                st.session_state[report_key] = {"filename": filename, "data": data, "mime": mime, "rows": row_count}
                st.session_state["prepared_report_ready_key"] = report_key
                report_is_ready = True
            st.success(f"Prepared {filename}")

        with b2:
            if report_is_ready:
                item = st.session_state[report_key]
                st.download_button(
                    f"Download {item['filename']}",
                    item["data"],
                    item["filename"],
                    item["mime"],
                    key=special_key("download_prepared_report_button"),
                    on_click=mark_downloaded,
                    args=(report_key,),
                )

        st.markdown("---")
        st.markdown("#### Contact Tracking")
        t1, t2, t3 = st.columns([1.1, 1.1, 1.8])
        with t1:
            st.download_button("Street Results CSV Template", contact_tracking_template("Street Results"), "street_results_template.csv", "text/csv")
        with t2:
            st.download_button("Walk/Call Tracking CSV Template", contact_tracking_template("Walk Call"), "walk_call_tracking_template.csv", "text/csv")
        with t3:
            uploaded = st.file_uploader("Upload completed contact results", type=["csv", "xlsx"], key=special_key("contact_results_upload_clean"))
            if uploaded is not None:
                st.success(f"Loaded {uploaded.name}. Contact update import will be applied in the pipeline pass.")

st.markdown('<div class="cc-header">', unsafe_allow_html=True)
h_logo, h_mid, h_power = st.columns([1.1, 2.8, 1.2])
with h_logo:
    if file_exists(LOGO_CANDIDATE_CONNECT): st.image(LOGO_CANDIDATE_CONNECT, width="stretch")
    else: st.markdown('<div class="cc-title">Candidate Connect</div>', unsafe_allow_html=True)
with h_mid:
    st.markdown('<div class="cc-title">Candidate Connect DEV</div>', unsafe_allow_html=True)
    st.markdown('<div class="cc-sub">Voter Data & Engagement Platform • Stable DEV cloud build v21zs</div>', unsafe_allow_html=True)
with h_power:
    if file_exists(LOGO_TPTC): st.image(LOGO_TPTC, width="stretch")
    else: st.markdown('<div class="cc-powered">Powered by<br><b>The Political Technology Company</b></div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

try:
    with st.spinner("Loading filters from R2..."):
        manifest, filter_options, geo_hierarchy = load_filter_layer()
except Exception as e:
    st.error("Could not load the filter layer."); st.exception(e); st.stop()

if "filter_reset_token" not in st.session_state: st.session_state["filter_reset_token"] = 0
if "left_section" not in st.session_state: st.session_state["left_section"] = None
_filter_suffix = st.session_state["filter_reset_token"]

with st.sidebar:
    st.markdown("## Candidate Connect")
    st.caption("DEV final hybrid v21zs — current-universe handoff + mail ballot cleanup")
    if st.button("🎯 Create Universe", width="stretch"):
        st.session_state["left_section"]="create_universe"; st.session_state["view"]="targeting"; st.rerun()
    if st.button("🔎 Voter Lookup", width="stretch"):
        st.session_state["left_section"]="voter_lookup"; st.session_state["view"]="dashboard"; st.rerun()
    if st.button("📬 Mail Ballot Center", width="stretch"):
        st.session_state["left_section"]="mail_ballot_center"; st.session_state["view"]="dashboard"; st.rerun()
    if st.button("⌂ Area Intelligence", width="stretch"):
        st.session_state["left_section"]="area_intelligence"; st.session_state["view"]="dashboard"; st.rerun()
    st.divider()

    if st.session_state.get("left_section") == "create_universe":
        st.markdown("### Create Universe")
        with st.expander("Geography", expanded=False):
            for field in GEO_FIELDS:
                st.multiselect(DISPLAY_LABELS.get(field, field), options=field_options(filter_options, field, active_filters()), key=filter_key(field))
        with st.expander("Voter Details", expanded=False):
            for field in ["Party", "Gender", "Age_Range", "CalculatedParty", "HH-Party"]:
                opts = field_options(filter_options, field, active_filters())
                if opts: st.multiselect(DISPLAY_LABELS.get(field, field), options=opts, key=filter_key(field))
            st.slider("Newly Registered Within Last N Months",0,24,0,1,key=special_key("new_reg_months"))
        with st.expander("Vote History", expanded=False):
            st.selectbox("Vote History Type", ["All Elections","General Elections","Primary Elections"], key=special_key("vote_score_type"))
            st.slider("Vote History Score Range",0,4,(0,4),1,key=special_key("vote_history_score_range"))
            years, etypes, methods = election_options()
            st.multiselect("Election Year", years, key=special_key("election_years"))
            st.multiselect("Election Type", etypes, key=special_key("election_types"))
            st.multiselect("Vote Method", methods, key=special_key("election_methods"))
        with st.expander("Mail Ballot", expanded=False):
            for field in ["MB_App", "MB_App_Status", "MB_Sent", "MB_Status"]:
                st.multiselect(DISPLAY_LABELS.get(field, field), options=field_options(filter_options, field, active_filters()), key=filter_key(field))
            st.slider("Mail Ballot Probability Score",0,4,(0,4),1,key=special_key("mb_prob_score_range"))
        with st.expander("Contact Filters", expanded=False):
            st.selectbox("Mobile / Landline Reach", ["No phone filter","Mobile only","Landline only","Mobile OR landline","Mobile AND landline","No mobile or landline"], key=special_key("phone_reach_mode"))
            for field in ["HasEmail","HasApplicantPhone"]:
                st.multiselect(DISPLAY_LABELS.get(field, field), options=field_options(filter_options, field, active_filters()), key=filter_key(field))
        tag_opts=field_options(filter_options,"Tags",active_filters())
        if tag_opts:
            with st.expander("Tags", expanded=False): st.multiselect("Tags", tag_opts, key=filter_key("Tags"))
        with st.expander("Saved Universes", expanded=False):
            saved=load_persistent_saved_universes(); name=st.text_input("Save current filters as", key=special_key("save_universe_name"))
            if st.button("Save Universe", key=special_key("save_universe_button"), width="stretch"):
                if str(name).strip():
                    saved[str(name).strip()]={"filters":active_filters(),"special":active_special_filters()}; persist_saved_universes(saved); st.success("Saved.")
                else: st.warning("Enter a universe name first.")
            if saved:
                choice=st.selectbox("Load saved universe", [""]+sorted(saved.keys()), key=special_key("load_universe_choice"))
                ca,cb=st.columns(2)
                with ca:
                    if st.button("Load", key=special_key("load_universe_button"), width="stretch") and choice: load_saved_universe_into_widgets(saved.get(choice,{}))
                with cb:
                    if st.button("Delete", key=special_key("delete_universe_button"), width="stretch") and choice:
                        saved.pop(choice,None); persist_saved_universes(saved); st.rerun()
            else: st.caption("No saved universes saved yet.")
    elif st.session_state.get("left_section") == "voter_lookup":
        st.markdown("### Voter Lookup")
        st.caption("Search the full statewide active voter file by name, address, PA ID, phone, or email.")
        st.text_input("Search voters", key=special_key("lookup_query"), placeholder="Name, county, address, PA ID, phone, email")
        st.selectbox("Max Results", [10,25,50,100], index=1, key=special_key("lookup_max"))
        ca, cb = st.columns(2)
        with ca:
            if st.button("Search", key=special_key("lookup_search_btn"), width="stretch"):
                st.rerun()
        with cb:
            if st.button("Clear Lookup", key=special_key("lookup_clear_btn"), width="stretch"):
                for k in [special_key("lookup_query"), "lookup_selected_id"]:
                    st.session_state.pop(k, None)
                st.rerun()
    elif st.session_state.get("left_section") == "mail_ballot_center":
        st.markdown("### Mail Ballot Center")
        _has_universe = has_current_universe()
        _label = st.session_state.get("current_universe_label", "None")
        st.checkbox(
            f"Use current universe: {_label}",
            value=_has_universe,
            disabled=not _has_universe,
            key=special_key("mb_start_current"),
        )
        if _has_universe:
            st.caption(f"Last applied from Create Universe: {st.session_state.get('current_universe_updated', '')}")
        else:
            st.caption("Build a universe in Create Universe, click Update Counts, then return here to use it as your Mail Ballot base.")
    elif st.session_state.get("left_section") == "area_intelligence":
        st.markdown("### Area Intelligence")
        st.caption("Select the area on the right.")

active = active_filters()
section = st.session_state.get("left_section")

def render_enhanced_home():
    render_statewide_snapshot()

if section == "voter_lookup": render_voter_lookup_workspace(); st.stop()
if section == "mail_ballot_center": render_mail_ballot_workspace(); st.stop()
if section == "area_intelligence": render_area_intelligence_workspace(); st.stop()
if section != "create_universe": render_enhanced_home(); st.stop()

st.session_state["view"]="targeting"
st.markdown("## Create Universe")
st.markdown("### Current Universe")
special_active = active_special_filters()
if active or special_active:
    chips=[]
    for k,vals in active.items(): chips.append(f"**{DISPLAY_LABELS.get(k,k)}:** {', '.join(map(str, vals[:6]))}{'…' if len(vals)>6 else ''}")
    if "RegistrationMonthsAgo" in special_active: chips.append(f"**Newly Registered:** last {special_active['RegistrationMonthsAgo']['max']} months")
    if "__PhoneReach" in special_active: chips.append(f"**Phone Reach:** {special_active['__PhoneReach']}")
    if "__ElectionFilters" in special_active:
        ef=special_active["__ElectionFilters"]; bits=[]
        if ef.get("years"): bits.append("Years "+", ".join(map(str,ef.get("years",[]))))
        if ef.get("types"): bits.append("Types "+", ".join(map(str,ef.get("types",[]))))
        if ef.get("methods"): bits.append("Methods "+", ".join(map(str,ef.get("methods",[]))))
        chips.append("**Specific Elections:** "+"; ".join(bits))
    for sf in ["V4A","V4G","V4P","MB_Prob_Score"]:
        if sf in special_active: chips.append(f"**{DISPLAY_LABELS.get(sf,sf)}:** {special_active[sf]['min']}–{special_active[sf]['max']}")
    st.markdown(" &nbsp; | &nbsp; ".join(chips), unsafe_allow_html=True)
else: st.info("No filters selected. Choose filters in the left pane.")


a1,a2,sp = st.columns([.85,.85,4.3])
with a1:
    if st.button("Update Counts", width="stretch"):
        with st.spinner("Updating counts..."):
            summary, mode, err = update_counts(active)
        if err:
            st.warning("Counts are unavailable for this filter combination.")
            st.caption(str(err)[:500])
        else:
            st.session_state["quick_summary"] = summary
            st.session_state["count_mode"] = mode
            save_current_universe(active, summary, source="Create Universe")
            st.success(f"Current universe saved: {st.session_state.get('current_universe_label', 'Selected universe')}")
with a2: st.button("Clear Filters", width="stretch", on_click=clear_filter_state)
if st.session_state.get("quick_summary"):
    st.caption("Counts updated. Use the Output Center tabs below for the overview, exports, and reports.")

st.markdown("## Output Center")
render_output_buttons(active)
st.caption(f"Rendered at {datetime.now().isoformat(timespec='seconds')}")

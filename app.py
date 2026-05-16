# Candidate Connect DEV — Final Hybrid Cloud App v21s HOME_DESIGN_RESTORE
# Full safe filters + guarded export.
# v21p: keeps v21o phone fix and makes saved universes survive app reload/reboot via URL persistence.

import io
import json
import base64
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import duckdb
import requests
import streamlit as st
try:
    from reportlab.lib.pagesizes import letter
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


def count_cube_where_sql(active: dict, special: dict | None = None) -> str:
    clauses = []
    for field, vals in (active or {}).items():
        if not vals:
            continue
        if field == "Tags":
            continue
        cleaned = [str(v) for v in vals if str(v).strip()]
        if not cleaned:
            continue
        clauses.append(f"CAST({sql_ident(field)} AS VARCHAR) IN (" + ",".join(sql_lit(v) for v in cleaned) + ")")

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


def load_persistent_saved_universes():
    """Initialize session saved universes from URL query params after an app reboot."""
    if "saved_universes" not in st.session_state:
        raw = None
        try:
            raw = st.query_params.get(SAVED_UNIVERSES_PARAM, "")
        except Exception:
            raw = ""
        st.session_state["saved_universes"] = decode_saved_universes(raw)
    return st.session_state.setdefault("saved_universes", {})


def persist_saved_universes(saved):
    """Persist saved universes into the browser URL so refresh/reboot keeps them."""
    try:
        encoded = encode_saved_universes(saved)
        if encoded:
            st.query_params[SAVED_UNIVERSES_PARAM] = encoded
        elif SAVED_UNIVERSES_PARAM in st.query_params:
            del st.query_params[SAVED_UNIVERSES_PARAM]
    except Exception:
        # Saving still works for the current browser session if URL persistence fails.
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
    parts = [cc_text(row.get(c, "")) for c in ["FirstName", "MiddleName", "LastName", "NameSuffix"]]
    name = " ".join([p for p in parts if p])
    return name or cc_text(row.get("FullName", "")) or cc_text(row.get("Name", "")) or "Unnamed voter"

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
        "FirstName": ["FirstName", "First Name", "first_name", "FIRST_NAME", "fname", "FName", "first", "FIRST"],
        "MiddleName": ["MiddleName", "Middle Name", "middle_name", "middle", "MiddleInitial", "middle_initial", "MName"],
        "LastName": ["LastName", "Last Name", "last_name", "surname", "lname", "LName", "last", "LAST"],
        "NameSuffix": ["NameSuffix", "Name Suffix", "suffix", "Suffix", "surnsuffix", "SurnSuffix"],
        "FullName": ["FullName", "Full Name", "Name", "name"],
        "Party": ["Party", "party", "party_raw", "PartyCode", "RegisteredParty"],
        "Gender": ["Gender", "gender", "Sex", "sex"],
        "DOB": ["DOB", "DateOfBirth", "Date of Birth", "dob"],
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


def report_columns():
    return list(DEFAULT_EXPORT_COLUMNS)

def remote_search_voters(term, max_rows=25):
    urls = index_urls_from_manifest()
    term = str(term or "").strip().replace("'", "''")
    if not term: return pd.DataFrame(columns=report_columns())
    like = f"%{term.lower()}%"
    cols = report_columns()
    select_cols = ", ".join([f"CAST({sql_ident(c)} AS VARCHAR) AS {sql_ident(c)}" for c in cols])
    searchable = ["FullName","Name","FirstName","LastName","County","Municipality","Precinct","voter_id","Mobile","Landline","Email","Street Name","City","Zip"]
    where = " OR ".join([f"LOWER(CAST({sql_ident(c)} AS VARCHAR)) LIKE {sql_lit(like)}" for c in searchable])
    con = duckdb.connect(database=':memory:')
    try:
        try: con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        return con.execute(f"SELECT {select_cols} FROM read_parquet({urls!r}, union_by_name=true) WHERE {where} LIMIT {int(max_rows)}").df()
    finally:
        con.close()

def safe_filtered_df(active, max_rows=5000):
    try:
        df = duckdb_detail_filtered_df(active or {}, active_special_filters(), max_rows)
        if df is None or df.empty:
            return pd.DataFrame(columns=DEFAULT_EXPORT_COLUMNS)
        df = normalize_download_df(df).head(max_rows)
        return drop_all_blank_optional_columns(df, required=["voter_id", "County", "Municipality", "Precinct", "FirstName", "LastName", "House Number", "Street Name", "City", "State", "Zip"])
    except Exception as e:
        st.warning(f"Export query returned no rows or failed: {str(e)[:250]}")
        return pd.DataFrame(columns=DEFAULT_EXPORT_COLUMNS)

def make_simple_pdf(title, rows, headers):
    if canvas is None:
        return b"PDF support unavailable."
    bio = io.BytesIO(); c = canvas.Canvas(bio, pagesize=letter); w,h = letter
    c.setFont("Helvetica-Bold", 15); c.drawString(36, h-42, title)
    c.setFont("Helvetica", 8); y = h-66
    widths = [90, 70, 150, 120, 40, 40][:len(headers)]
    def line(vals, bold=False):
        nonlocal y
        if y < 45:
            c.showPage(); y = h-42
        c.setFont("Helvetica-Bold" if bold else "Helvetica", 7.5)
        x=36
        for i,v in enumerate(vals):
            c.drawString(x, y, str(v)[:32])
            x += widths[i] if i < len(widths) else 80
        y -= 12
    line(headers, True)
    for r in rows:
        line(r)
    c.save(); bio.seek(0); return bio.getvalue()


def _pdf_logo_path(name):
    try:
        if Path(name).exists():
            return str(Path(name))
    except Exception:
        pass
    return None

class _NumberedCanvas(canvas.Canvas if canvas else object):
    def __init__(self, *args, **kwargs):
        if canvas is None:
            return
        super().__init__(*args, **kwargs)
        self._saved_page_states = []
    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()
    def save(self):
        if canvas is None:
            return
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.setFont("Helvetica-Bold", 7)
            self.setFillColorRGB(0.12, 0.12, 0.12)
            self.drawCentredString(letter[0] / 2.0, 18, f"{self._pageNumber} of {num_pages}")
            self.drawRightString(letter[0] - 28, 18, f"Updated: {datetime.now().strftime('%m/%d/%Y')}")
            super().showPage()
        super().save()

def _draw_branded_header(c, title, subtitle=""):
    w, h = letter
    c.setFillColorRGB(1, 1, 1)
    # logos
    left_logo = _pdf_logo_path(LOGO_CANDIDATE_CONNECT)
    right_logo = _pdf_logo_path(LOGO_TPTC)
    if left_logo:
        try: c.drawImage(left_logo, 28, h-58, width=92, height=40, preserveAspectRatio=True, mask='auto')
        except Exception: pass
    if right_logo:
        try: c.drawImage(right_logo, w-122, h-58, width=94, height=40, preserveAspectRatio=True, mask='auto')
        except Exception: pass
    c.setFillColorRGB(0.50, 0.05, 0.12)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(132, h-42, title[:56])
    if subtitle:
        c.setFont("Helvetica", 7.5)
        c.setFillColorRGB(0.30, 0.30, 0.30)
        c.drawString(132, h-54, subtitle[:96])
    c.setStrokeColorRGB(0.78, 0.78, 0.78)
    c.line(28, h-66, w-28, h-66)
    return h-78

def _draw_section_bar(c, text, y, x=28, width=None):
    w, h = letter
    width = width or (w - 56)
    c.setFillColorRGB(0.56, 0.06, 0.13)
    c.roundRect(x, y-11, width, 13, 3, fill=1, stroke=0)
    c.setFillColorRGB(1, 1, 1)
    c.setFont("Helvetica-Bold", 7.4)
    c.drawString(x+5, y-7, str(text)[:72])
    return y-15

def _selected_filter_lines(active):
    lines=[]
    try:
        for k,v in (active or {}).items():
            if v in (None, [], ""):
                continue
            label = DISPLAY_LABELS.get(k, k)
            if isinstance(v, (list, tuple, set)):
                val = ", ".join(map(str, list(v)[:4]))
                if len(v) > 4: val += "..."
            else:
                val = str(v)
            lines.append(f"{label}: {val}")
    except Exception:
        pass
    try:
        sf = active_special_filters()
        for k,v in sf.items():
            if v in (None, [], "", (0,0)):
                continue
            if isinstance(v, (list, tuple, set)):
                val = ", ".join(map(str, list(v)[:4]))
            else:
                val = str(v)
            lines.append(f"{k}: {val}")
    except Exception:
        pass
    return lines[:10]

def _street_sort_key_df(df):
    df = df.copy()
    df["_precinct_sort"] = df.get("Precinct", "").astype(str)
    df["_street_sort"] = df.get("Street Name", "").astype(str).str.upper().str.replace(r"[^A-Z0-9 ]+", " ", regex=True).str.strip()
    if "House Number" in df.columns:
        df["_house_sort"] = pd.to_numeric(df["House Number"].astype(str).str.extract(r"(\d+)")[0], errors="coerce").fillna(0)
    else:
        df["_house_sort"] = 0
    df["_last_sort"] = df.get("LastName", "").astype(str).str.upper()
    df["_first_sort"] = df.get("FirstName", "").astype(str).str.upper()
    return df.sort_values(["_precinct_sort","_street_sort","_house_sort","_last_sort","_first_sort"], kind="stable")

def _contact_tracking_cols():
    # MB Perm is printed as Y/blank, not as a tracking checkbox.
    return ["F", "A", "U", "NH", "Yard Sign"]

def _build_street_pdf(active, call_mode=False):
    """Build branded street/call list PDF matching the local working street-list format.

    Layout goals:
      - cover page with selected voter summary
      - precinct counts summary
      - precinct-section bookmarks/outlines
      - precinct-separated detail pages
      - street bars, house grouping, alternating shaded voter rows
      - both mobile and landline shown with (m)/(l), applicant/unknown as (u)
      - F/A/U/NH/Yard Sign/MB Perm tracking columns for street lists
    """
    if canvas is None:
        return make_simple_pdf("PDF support unavailable", [], ["Message"])

    df = safe_filtered_df(active, 25000)
    df = normalize_download_df(df)
    if df.empty:
        title = "Candidate Connect Call List" if call_mode else "Candidate Connect Street List"
        return make_simple_pdf(title, [], ["Full Name", "Phone", "Party", "Age"])

    # Normalize display fields used by the PDF.
    df["_name"] = df.apply(full_name, axis=1).map(smart_title)
    df["_phone"] = df.apply(phone_label, axis=1)
    df["_precinct"] = df.get("Precinct", "").astype(str).map(canonical_precinct_display).replace("", "Unassigned")
    df["_street"] = df.get("Street Name", "").astype(str).map(smart_title).replace("", "Unknown Street")
    if call_mode:
        df = df[df["_phone"].astype(str).str.strip().ne("")].copy()
    if df.empty:
        return make_simple_pdf("Candidate Connect Call List", [["No voters with phone numbers found"]], ["Message"])

    # Sort like the local list: precinct -> street -> house number -> last -> first.
    df = df.copy()
    df["_precinct_sort"] = df["_precinct"].astype(str).str.upper()
    df["_street_sort"] = df["_street"].astype(str).str.upper().str.replace(r"[^A-Z0-9 ]+", " ", regex=True).str.strip()
    df["_house_sort"] = pd.to_numeric(df.get("House Number", "").astype(str).str.extract(r"(\d+)")[0], errors="coerce").fillna(0)
    df["_last_sort"] = df.get("LastName", "").astype(str).str.upper()
    df["_first_sort"] = df.get("FirstName", "").astype(str).str.upper()
    df = df.sort_values(["_precinct_sort", "_street_sort", "_house_sort", "_last_sort", "_first_sort"], kind="stable")

    bio = io.BytesIO()
    c = _NumberedCanvas(bio, pagesize=letter)
    w, h = letter
    mar_l, mar_r = 28, 28
    title = "Voter Call List" if call_mode else "Voter Contact List"
    subtitle = ""
    tracks = _contact_tracking_cols()

    def safe_bookmark(name, title_text, level=0):
        try:
            c.bookmarkPage(name)
            c.addOutlineEntry(title_text[:80], name, level=level, closed=False)
        except Exception:
            pass

    # Cover page
    safe_bookmark("cover", title, 0)
    y = _draw_branded_header(c, title, subtitle)
    c.setFont("Helvetica-Bold", 20)
    c.setFillColorRGB(0.50, 0.05, 0.12)
    c.drawString(40, y-8, title)
    c.setFont("Helvetica-Bold", 11)
    c.setFillColorRGB(0.15,0.15,0.15)
    c.drawString(40, y-32, datetime.now().strftime("%m/%d/%Y"))
    hh_key = (df.get("County", "").astype(str).str.upper()+"|"+df.get("Municipality", "").astype(str).str.upper()+"|"+df.get("House Number", "").astype(str).str.upper()+"|"+df.get("Street Name", "").astype(str).str.upper()+"|"+df.get("Apartment Number", "").astype(str).str.upper())
    households = int(hh_key.nunique()) if len(df) else 0
    c.setFont("Helvetica-Bold", 13)
    c.drawString(40, y-62, f"Individuals: {len(df):,}   Households: {households:,}")
    lines = _selected_filter_lines(active)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(40, y-98, "Selected Voters")
    c.setFont("Helvetica", 9)
    yy = y-116
    if lines:
        for line in lines:
            c.drawString(54, yy, u"• " + line[:88])
            yy -= 15
    else:
        c.drawString(54, yy, u"• All active selected voters")
        yy -= 15
    yy -= 16
    c.setFillColorRGB(0.50, 0.05, 0.12)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(40, yy, "Legend")
    yy -= 15
    c.setFillColorRGB(0.15, 0.15, 0.15)
    c.setFont("Helvetica", 8.5)
    legend_lines = [
        "Phones: (m) mobile, (l) landline, (u) applicant/unknown",
        "Contact boxes: F = Favorable, A = Against, U = Undecided, NH = Not Home",
        "Yard Sign is a tracking checkbox. MB Perm prints Y when the voter is a permanent mail ballot voter.",
    ]
    for line in legend_lines:
        c.drawString(54, yy, u"• " + line)
        yy -= 13
    c.showPage()

    # Precinct summary pages
    safe_bookmark("precinct_summary", "Precinct Counts Summary", 0)
    summary = df.groupby("_precinct", dropna=False).size().reset_index(name="Individuals")
    hh_sum = pd.DataFrame({"_precinct": df["_precinct"], "HH": hh_key}).groupby("_precinct")["HH"].nunique().reset_index(name="Households")
    summary = summary.merge(hh_sum, on="_precinct", how="left").sort_values("_precinct", kind="stable")
    y = _draw_branded_header(c, "Precinct Counts Summary", subtitle)
    _draw_section_bar(c, "Precinct Counts Summary", y); y -= 24
    c.setFont("Helvetica-Bold", 8.5); c.setFillColorRGB(0.05,0.05,0.05)
    c.drawString(42,y,"Precinct"); c.drawRightString(420,y,"Individuals"); c.drawRightString(510,y,"Households"); y-=13
    c.setFont("Helvetica", 8.2)
    for _,r in summary.iterrows():
        if y < 44:
            c.showPage()
            y = _draw_branded_header(c, "Precinct Counts Summary", subtitle)
            _draw_section_bar(c, "Precinct Counts Summary", y); y -= 24
            c.setFont("Helvetica-Bold", 8.5); c.drawString(42,y,"Precinct"); c.drawRightString(420,y,"Individuals"); c.drawRightString(510,y,"Households"); y-=13
            c.setFont("Helvetica", 8.2)
        c.drawString(42,y, smart_title(r.get("_precinct", ""))[:56])
        c.drawRightString(420,y, f"{int(r.get('Individuals',0)):,}")
        c.drawRightString(510,y, f"{int(r.get('Households',0) or 0):,}")
        y-=12
    c.setFont("Helvetica-Bold",8.5)
    c.drawString(42,y,"TOTAL"); c.drawRightString(420,y,f"{len(df):,}"); c.drawRightString(510,y,f"{households:,}")
    c.showPage()

    def new_detail_page(precinct, cont=False, first_for_precinct=False):
        if first_for_precinct:
            safe_bookmark("pct_" + re.sub(r"[^A-Za-z0-9]+", "_", precinct)[:60], smart_title(precinct), 1)
        header_title = f"{smart_title(precinct)[:58]}{' (cont)' if cont else ''}"
        yy = _draw_branded_header(c, header_title, subtitle)
        c.setFillColorRGB(0.56,0.06,0.13)
        c.roundRect(mar_l, yy-12, w-mar_l-mar_r, 14, 3, fill=1, stroke=0)
        c.setFillColorRGB(1,1,1)
        c.setFont("Helvetica-Bold", 6.8)
        if call_mode:
            headers=[("Full Name",34), ("Phone",210), ("Party",375), ("Sex",405), ("Age",433), ("Precinct",458)]
            for txt,x in headers: c.drawString(x, yy-8, txt)
        else:
            headers=[("House",42), ("Full Name",78), ("Phone",250), ("Party",395), ("Sex",418), ("Age",440)]
            for txt,x in headers: c.drawString(x, yy-8, txt)
            x=462
            for t in tracks:
                label = "YS" if t == "Yard Sign" else t
                c.drawCentredString(x, yy-8, label)
                x += 22
            c.setFont("Helvetica-Bold", 6.0)
            c.drawCentredString(586, yy-8, "MB")
        return yy-22

    current_precinct = None
    seen_precincts = set()
    current_street = None
    y = None
    row_count = 0
    for _,r in df.iterrows():
        precinct = cc_text(r.get("_precinct", "")) or "Unassigned"
        street = smart_title(r.get("_street", "")) or "Unknown Street"
        house_raw = cc_text(r.get("House Number", ""))
        m_house = re.search(r"\d+", house_raw)
        house = m_house.group(0) if m_house else house_raw[:8]
        apt_raw = cc_text(r.get("Apartment Number", ""))
        apt = ""
        if re.fullmatch(r"(?i)(?:apt|unit|ste|suite|#)?\s*[A-Z0-9-]{1,8}", apt_raw or "") and not re.search(r"(?i)\b(?:dr|rd|st|ave|ln|ct|cir|blvd|way|road|street|drive|lane)\b", apt_raw or ""):
            apt = re.sub(r"(?i)^(apt|unit|ste|suite)\s+", "", apt_raw).strip()
        house_text = (house + (f" Apt {apt}" if apt else "")).strip()
        need_new_precinct = precinct != current_precinct
        if y is None or need_new_precinct or y < 44:
            if y is not None:
                c.showPage()
            first_for_precinct = precinct not in seen_precincts
            y = new_detail_page(precinct, cont=(not need_new_precinct), first_for_precinct=first_for_precinct)
            seen_precincts.add(precinct)
            current_precinct = precinct
            current_street = None
        if not call_mode and street != current_street:
            if y < 62:
                c.showPage(); y = new_detail_page(precinct, cont=True)
            y = _draw_section_bar(c, street, y, x=mar_l, width=w-mar_l-mar_r)
            y -= 6
            current_street = street
        # alternating voter row shading
        c.setFillColorRGB(0.965, 0.86, 0.88) if row_count % 2 == 0 else c.setFillColorRGB(1,1,1)
        row_h = 22 if (not call_mode and len(phone_entries(r)) > 1) else 15
        c.rect(mar_l+6, y-6, w-mar_l-mar_r-12, row_h, fill=1, stroke=0)
        c.setFillColorRGB(0.08,0.08,0.08)
        if call_mode:
            c.setFont("Helvetica", 6.7)
            c.drawString(34, y, smart_title(r.get("_name", ""))[:39])
            c.drawString(210, y, cc_text(r.get("_phone", ""))[:37])
            c.drawString(378, y, cc_text(r.get("Party", ""))[:1])
            c.drawString(408, y, cc_text(r.get("Gender", ""))[:1])
            c.drawRightString(450, y, cc_text(r.get("Age", ""))[:3])
            c.drawString(458, y, smart_title(precinct)[:23])
        else:
            # Taller row so mobile/landline can be stacked instead of running into party/age columns.
            phone_lines = [f"{num} ({typ})" for num, typ in phone_entries(r)]
            c.setFont("Helvetica-Bold", 6.7)
            c.drawString(42, y, house_text[:10])
            c.setFont("Helvetica", 6.3)
            c.drawString(78, y, smart_title(r.get("_name", ""))[:34])
            py = y + 4 if len(phone_lines) > 1 else y
            for ph in phone_lines[:2]:
                c.drawString(250, py, ph[:31])
                py -= 8
            c.drawString(397, y, cc_text(r.get("Party", ""))[:1])
            c.drawString(421, y, cc_text(r.get("Gender", ""))[:1])
            c.drawRightString(454, y, cc_text(r.get("Age", ""))[:3])
            x=462
            for t in tracks:
                c.rect(x-3, y-2, 6, 6, fill=0, stroke=1)
                x += 22
            if str(r.get("MB_PERM", "") or r.get("MB_Perm", "") or r.get("Permanent MB", "")).strip().upper() in {"Y", "YES", "1", "TRUE"}:
                c.setFont("Helvetica-Bold", 6.8)
                c.drawCentredString(586, y, "Y")
        y -= (25 if (not call_mode and len(phone_entries(r)) > 1) else 17)
        row_count += 1
    c.save(); bio.seek(0); return bio.getvalue()

def street_list_pdf(active):
    return _build_street_pdf(active, call_mode=False)

def call_list_pdf(active):
    return _build_street_pdf(active, call_mode=True)

def summary_pdf(active):
    summary, mode, err = update_counts(active)
    rows = [["Total", summary.get("total",0)], ["Republican", summary.get("r",0)], ["Democrat", summary.get("d",0)], ["Other", summary.get("o",0)]] if summary else [["Unavailable", err or ""]]
    return make_simple_pdf("Candidate Connect Summary Report", rows, ["Metric", "Value"])

def labels_pdf(active):
    df = safe_filtered_df(active, 3000)
    if canvas is None: return b"PDF support unavailable."
    bio = io.BytesIO(); c = canvas.Canvas(bio, pagesize=letter); w,h = letter
    c.setFont("Helvetica-Bold", 8); c.drawString(36, h-24, "Avery 5160 / 8160 compatible — 30 labels per sheet. This note is in the top margin and should not overlap labels.")
    left, top = 0.1875*inch, h-0.5*inch; label_w, label_h = 2.625*inch, 1.0*inch; gap_x=0.125*inch
    xs=[left,left+label_w+gap_x,left+2*(label_w+gap_x)]; idx=0
    c.setFont("Helvetica", 8)
    for _,r in df.iterrows():
        name=full_name(r); addr=address_line(r); city=cc_text(r.get("City","")) or cc_text(r.get("res_city","")); state=cc_text(r.get("State","")) or cc_text(r.get("res_state","")) or "PA"; z=cc_text(r.get("Zip","")) or cc_text(r.get("res_zip",""))
        if not name or not addr: continue
        pos=idx%30
        if idx and pos==0: c.showPage(); c.setFont("Helvetica",8)
        col=pos%3; row=pos//3; x=xs[col]+0.10*inch; y=top-row*label_h-0.28*inch
        c.drawString(x,y,name[:34]); c.drawString(x,y-11,addr[:34]); c.drawString(x,y-22,f"{city}, {state} {z}"[:34]); idx+=1
    c.save(); bio.seek(0); return bio.getvalue()

def render_enhanced_home():
    render_statewide_snapshot()

def render_voter_lookup_workspace():
    st.markdown("## Voter Lookup")
    q = st.session_state.get(special_key("lookup_query"), "")
    maxn = st.session_state.get(special_key("lookup_max"), 25)
    if not q:
        st.info("Enter a search in the left pane."); return
    with st.spinner("Searching voters..."):
        df = remote_search_voters(q, maxn)
    st.caption(f"{len(df)} result(s) found for: {q}")
    if df.empty: st.warning("No voters found."); return
    if "lookup_selected_idx" not in st.session_state: st.session_state["lookup_selected_idx"] = 0
    left,right = st.columns([.9,1.6])
    with left:
        st.markdown("### Search Results")
        for i,r in df.iterrows():
            if st.button(f"{full_name(r)}\n{address_line(r)}\n{cc_text(r.get('County',''))} County", key=f"lookup_pick_{i}", width="stretch"):
                st.session_state["lookup_selected_idx"] = int(i); st.rerun()
    with right:
        r = df.iloc[min(int(st.session_state.get("lookup_selected_idx",0)), len(df)-1)]
        st.markdown(f"## {full_name(r)}")
        st.write(address_line(r))
        a,b,c1,d = st.columns(4)
        a.metric("Party", cc_text(r.get("Party","")) or "—"); b.metric("Gender", cc_text(r.get("Gender","")) or "—"); c1.metric("Age", cc_text(r.get("Age","")) or "—"); d.metric("PA ID", cc_text(r.get("voter_id","")) or "—")
        st.dataframe(pd.DataFrame([["County",r.get("County","")],["Municipality",r.get("Municipality","")],["Precinct",r.get("Precinct","")],["School District",r.get("School District","")],["Phone",phone_label(r)],["Email",r.get("Email","")],["Mail Ballot",r.get("MB_App","") or r.get("MIB_Applied","")],["Tags",r.get("Tags","")]], columns=["Field","Value"]), hide_index=True, width="stretch")
        with st.expander("Edit / Correct This Voter Record", expanded=False):
            st.info("Download the correction JSON and place it in the pipeline correction workflow if needed.")
            edits={}
            cols=st.columns(4)
            for j,field in enumerate(["FirstName","MiddleName","LastName","NameSuffix","Gender","Party","DOB","RegistrationDate","House Number","Street Name","Apartment Number","City","State","Zip","Municipality","Precinct","School District","School Region","Mobile","Landline","Email","MB_App","MB_Status","Tags"]):
                with cols[j%4]: edits[field]=st.text_input(field, value=cc_text(r.get(field,"")), key=f"edit_{field}_{r.get('voter_id','')}")
            payload={"voter_id": cc_text(r.get("voter_id","")), "updated_at": datetime.now().isoformat(timespec="seconds"), "fields": edits, "notes": st.text_area("Correction Notes")}
            st.download_button("Download Correction JSON", json.dumps(payload, indent=2).encode(), file_name=f"voter_correction_{payload['voter_id'] or 'unknown'}.json", mime="application/json")

def render_mail_ballot_workspace():
    st.markdown("## Mail Ballot Center")
    st.caption("Strategic mail ballot operations, targeting, and follow-up workspace.")
    base = active_filters() if st.session_state.get(special_key("mb_start_current"), True) else {}
    c1,c2,c3,c4 = st.columns(4)
    app = c1.multiselect("Application Status", field_options(filter_options,"MB_App_Status",base), key=special_key("mb_app_status"))
    sent = c2.multiselect("Ballot Sent", field_options(filter_options,"MB_Sent",base), key=special_key("mb_sent"))
    ret = c3.multiselect("Ballot Status", field_options(filter_options,"MB_Status",base), key=special_key("mb_status"))
    score = c4.slider("MB Probability Score",0,4,(0,4),key=special_key("mb_score_center"))
    mb_active = dict(base)
    if app: mb_active["MB_App_Status"] = app
    if sent: mb_active["MB_Sent"] = sent
    if ret: mb_active["MB_Status"] = ret
    st.session_state[special_key("mb_prob_score_range")] = score
    if st.button("Apply Mail Ballot Filters", width="stretch"):
        st.session_state.update({filter_key(k):v for k,v in mb_active.items()})
        st.session_state["left_section"]="create_universe"; st.session_state["view"]="targeting"; st.rerun()
    summary, mode, err = update_counts(mb_active)
    if summary: render_metrics(summary)
    tabs=st.tabs(["Operations","Exports","Analysis","Notes"])
    with tabs[0]: st.info("Use presets here to build chase, cure, and growth universes. Counts use the same remote quick-count/index engine.")
    with tabs[1]: st.download_button("Download MB CSV", safe_filtered_df(mb_active,50000).to_csv(index=False).encode(), "mail_ballot_universe.csv", "text/csv")
    with tabs[2]: st.write("Mail ballot analysis workspace restored for DEV testing.")
    with tabs[3]: st.text_area("Notes")

def render_area_intelligence_workspace():
    st.markdown("## Area Intelligence")
    level = st.selectbox("Report Level", ["County","Municipality","Precinct","School District","School Region","USC","STS","STH"], key=special_key("area_level"))
    opts = field_options(filter_options, level, {})
    val = st.selectbox(level, opts, key=special_key("area_value")) if opts else ""
    area_active = {level:[val]} if val else {}
    st.markdown(f"### {level} Profile")
    summary, mode, err = update_counts(area_active)
    if summary: render_metrics(summary)
    st.info("Area Intelligence profile restored for DEV testing. More profile details/charts can be layered back after live-safe export/report testing.")


def filtered_export_columns(df: pd.DataFrame) -> list[str]:
    base = ["voter_id","County","Municipality","Precinct","USC","STS","STH","School District","School Region",
            "FirstName","MiddleName","LastName","NameSuffix","FullName","Party","CalculatedParty","Gender","DOB","Age","Age_Range","RegistrationDate",
            "House Number","House Number Suffix","Street Name","Apartment Number","Address Line 2","City","State","Zip",
            "Email","Mobile","Landline","Current_ApplicantPhone","MB_App","MB_App_Status","MB_Sent","MB_Status","MB_PERM","MB_Prob_Score","Tags"]
    return [c for c in base if c in df.columns]


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
                    st.dataframe(area_df_ov, hide_index=True, width="stretch", height=260)
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
    st.markdown('<div class="cc-sub">Voter Data & Engagement Platform • Stable DEV cloud build v21zd</div>', unsafe_allow_html=True)
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
    st.caption("DEV final hybrid v21zd — street list padding + magic none fix")
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
        st.text_input("Search voters", key=special_key("lookup_query"), placeholder="Name, county, address, PA ID, phone, email")
        st.selectbox("Max Results", [10,25,50,100], index=1, key=special_key("lookup_max"))
    elif st.session_state.get("left_section") == "mail_ballot_center":
        st.markdown("### Mail Ballot Center")
        st.checkbox("Start from current main universe", value=True, key=special_key("mb_start_current"))
    elif st.session_state.get("left_section") == "area_intelligence":
        st.markdown("### Area Intelligence")
        st.caption("Select the area on the right.")

active = active_filters()
section = st.session_state.get("left_section")
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
        if err: st.warning("Counts are unavailable for this filter combination."); st.caption(str(err)[:500])
        else: st.session_state["quick_summary"]=summary; st.session_state["count_mode"]=mode
with a2: st.button("Clear Filters", width="stretch", on_click=clear_filter_state)
if st.session_state.get("quick_summary"):
    st.markdown("### Current Counts")
    render_metrics(st.session_state["quick_summary"])
    render_party_chart(st.session_state["quick_summary"], "Party Breakdown")
    cgender, cage = st.columns(2)
    with cgender:
        render_group_bar(active, "Gender", "Gender Breakdown", ["F", "M", "U"])
    with cage:
        render_group_bar(active, "Age_Range", "Age Range Breakdown", ["18-24", "25-34", "35-44", "45-54", "55-64", "65+", "65-74", "75-84", "85+"])
    st.markdown("### Counts by Area")
    area_choice = st.selectbox("Area table level", ["County", "Municipality", "Precinct", "School District", "School Region", "USC", "STS", "STH"], key=special_key("counts_area_table_level"))
    area_df = duckdb_count_cube_group_filtered(json.dumps(count_safe_filters(active or {}), sort_keys=True), json.dumps({k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}, sort_keys=True), area_choice, 200)
    if not area_df.empty:
        area_df = area_df.rename(columns={"label": area_choice})
        area_df["Voters"] = area_df["Voters"].fillna(0).astype(int)
        if area_choice == "Precinct":
            area_df[area_choice] = area_df[area_choice].map(canonical_precinct_display)
            area_df = area_df.groupby(area_choice, as_index=False)["Voters"].sum()
        area_df = area_df.sort_values(area_choice, kind="stable")
        st.dataframe(area_df, hide_index=True, width="stretch", height=280)

st.markdown("## Output Center")
render_output_buttons(active)
st.caption(f"Rendered at {datetime.now().isoformat(timespec='seconds')}")

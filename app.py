# Candidate Connect DEV — Final Hybrid Cloud App v21q RESTORED_WORKSPACES_OUTPUTS
# Full safe filters + guarded export.
# v21q: restores landing screen, voter lookup, mail ballot center, area intel, and output reports while keeping v21p quick-count stability.

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
import altair as alt
from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Alignment

R2 = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"
DETAIL_SHARDS = 36
EXPORT_ROW_LIMIT = 250_000

st.set_page_config(page_title="Candidate Connect DEV", layout="wide")

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
    "County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region",
    "voter_id", "FirstName", "MiddleName", "LastName", "Name", "FullName",
    "Party", "CalculatedParty", "Gender", "Age", "Age_Range", "RegistrationDate",
    "House Number", "Street Name", "Apartment Number", "City", "State", "Zip",
    "res_address", "res_city", "res_state", "res_zip",
    "Email", "Mobile", "Landline", "Current_ApplicantPhone",
    "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MIB_Applied", "MIB_BALLOT", "MB_PERM", "MB_Prob_Score",
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
        if key.startswith(prefixes) or key in {"quick_summary", "count_mode", "exact_summary"}:
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
        if key.startswith(prefixes) or key in {"quick_summary", "count_mode", "exact_summary"}:
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
    chart_df = pd.DataFrame([
        {"Group": "Republican", "Voters": int(summary.get("r", 0))},
        {"Group": "Democrat", "Voters": int(summary.get("d", 0))},
        {"Group": "Other / Unaffiliated", "Voters": int(summary.get("o", 0))},
    ])
    st.markdown(f"#### {title}")
    st.bar_chart(chart_df.set_index("Group"), height=260)

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

def render_statewide_snapshot():
    st.markdown("## Statewide Snapshot")
    st.caption("Current statewide voter universe from the rebuilt quick-count table.")

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

    render_metrics(summary, label="")
    if summary.get("r") or summary.get("d") or summary.get("o"):
        render_party_chart(summary, "Statewide Party Breakdown")

    if err and not (summary.get("r") or summary.get("d") or summary.get("o")):
        st.warning("Quick-count statewide party numbers were not available, so the app showed the manifest total only.")

    st.info("Use **Create Universe** in the left pane. Counts use the rebuilt quick-count cube; geography dropdowns use the safe dependent geo table when available.")

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
# v21q restored workspaces / reports / lightweight remote helpers
# -----------------------------------------------------------------------------

def cc_clean_text(v):
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def cc_full_name(row):
    for c in ["FullName", "Name"]:
        v = cc_clean_text(row.get(c, ""))
        if v:
            return v
    parts = [cc_clean_text(row.get(c, "")) for c in ["FirstName", "MiddleName", "LastName", "NameSuffix"]]
    return " ".join([p for p in parts if p]).strip() or "Unnamed voter"


def cc_format_phone(raw):
    s = re.sub(r"\D+", "", cc_clean_text(raw))
    if len(s) == 11 and s.startswith("1"):
        s = s[1:]
    if len(s) == 10:
        return f"({s[:3]}) {s[3:6]}-{s[6:]}"
    return cc_clean_text(raw)


def cc_phone_list(row):
    vals = []
    m = cc_format_phone(row.get("Mobile", ""))
    l = cc_format_phone(row.get("Landline", ""))
    u = cc_format_phone(row.get("Current_ApplicantPhone", ""))
    if m:
        vals.append(f"{m} (m)")
    if l and l not in {m}:
        vals.append(f"{l} (l)")
    if u and u not in {m, l}:
        vals.append(f"{u} (u)")
    return " / ".join(vals)


def cc_address_line(row):
    house = cc_clean_text(row.get("House Number", ""))
    street = cc_clean_text(row.get("Street Name", ""))
    apt = cc_clean_text(row.get("Apartment Number", ""))
    if house or street:
        line = f"{house} {street}".strip()
        if apt:
            line += f" Apt {apt}"
        return line
    return cc_clean_text(row.get("res_address", ""))


def dataframe_to_csv_bytes(df):
    return (df if df is not None else pd.DataFrame()).to_csv(index=False).encode("utf-8")


def dataframe_to_xlsx_bytes(df, sheet_name="Data"):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        (df if df is not None else pd.DataFrame()).to_excel(writer, sheet_name=sheet_name[:31], index=False)
        ws = writer.sheets[sheet_name[:31]]
        for idx, col in enumerate(df.columns if df is not None else [], 1):
            ws.column_dimensions[get_column_letter(idx)].width = min(max(len(str(col)) + 2, 12), 32)
    bio.seek(0)
    return bio.getvalue()


def detail_report_columns(extra=None):
    base = [
        "County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region",
        "voter_id", "FirstName", "MiddleName", "LastName", "Name", "FullName", "Party", "Gender", "Age", "Age_Range",
        "House Number", "Street Name", "Apartment Number", "City", "State", "Zip", "res_address", "res_city", "res_state", "res_zip",
        "Email", "Mobile", "Landline", "Current_ApplicantPhone", "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "MB_Prob_Score", "Tags",
    ]
    for c in extra or []:
        if c not in base:
            base.append(c)
    return base


def build_report_dataframe(active, max_rows=5000):
    cols = detail_report_columns()
    df = build_export(active or {}, cols)
    if len(df) > max_rows:
        return df.head(max_rows).copy()
    return df


def pdf_header(c, title, subtitle=""):
    w, h = landscape(letter)
    c.setFillColor(colors.HexColor("#111827"))
    c.rect(0, h-62, w, 62, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(28, h-35, title)
    c.setFont("Helvetica", 9)
    c.drawRightString(w-28, h-24, "Candidate Connect")
    if subtitle:
        c.drawString(28, h-52, subtitle[:150])
    c.setStrokeColor(colors.HexColor("#c91f27"))
    c.setLineWidth(2)
    c.line(28, h-66, w-28, h-66)


def simple_table_pdf(title, subtitle, rows, headers, file_note=None, landscape_mode=True):
    pagesize = landscape(letter) if landscape_mode else letter
    bio = io.BytesIO()
    c = canvas.Canvas(bio, pagesize=pagesize)
    w, h = pagesize
    pdf_header(c, title, subtitle)
    y = h - 86
    if file_note:
        c.setFont("Helvetica-Oblique", 8)
        c.setFillColor(colors.HexColor("#374151"))
        c.drawString(28, y, file_note[:170])
        y -= 16
    c.setFont("Helvetica-Bold", 8)
    c.setFillColor(colors.HexColor("#111827"))
    x0 = 28
    widths = [(w-56)/len(headers)] * len(headers)
    for i, head in enumerate(headers):
        c.drawString(x0 + sum(widths[:i]), y, str(head)[:22])
    y -= 12
    c.setStrokeColor(colors.HexColor("#9ca3af"))
    c.line(28, y, w-28, y)
    y -= 12
    c.setFont("Helvetica", 7)
    c.setFillColor(colors.black)
    page = 1
    for row in rows:
        if y < 35:
            c.setFont("Helvetica", 7)
            c.drawRightString(w-28, 18, f"Page {page}")
            c.showPage(); page += 1
            pdf_header(c, title, subtitle)
            y = h - 86
            c.setFont("Helvetica-Bold", 8)
            for i, head in enumerate(headers):
                c.drawString(x0 + sum(widths[:i]), y, str(head)[:22])
            y -= 24
            c.setFont("Helvetica", 7)
        for i, cell in enumerate(row):
            c.drawString(x0 + sum(widths[:i]), y, str(cell)[:30])
        y -= 11
    c.drawRightString(w-28, 18, f"Page {page}")
    c.save()
    bio.seek(0)
    return bio.getvalue()


def generate_summary_pdf(active, summary=None):
    summary = summary or st.session_state.get("quick_summary") or {"total": 0, "r": 0, "d": 0, "o": 0}
    rows = [["Total Voters", f"{summary.get('total',0):,}"], ["Republican", f"{summary.get('r',0):,}"], ["Democrat", f"{summary.get('d',0):,}"], ["Other / Unaffiliated", f"{summary.get('o',0):,}"]]
    chips = []
    for k, vals in (active or {}).items():
        if vals:
            chips.append(f"{DISPLAY_LABELS.get(k,k)}: {', '.join(map(str, vals[:6]))}")
    return simple_table_pdf("Candidate Connect Summary Report", " | ".join(chips) or "All voters", rows, ["Metric", "Value"], landscape_mode=False)


def generate_street_list_pdf(active):
    df = build_report_dataframe(active, max_rows=9000)
    if df.empty:
        return simple_table_pdf("Street List", "No matching voters", [], ["Street", "House", "Name", "Phone", "Party", "Age"])
    for c in ["Street Name", "House Number", "Apartment Number"]:
        if c not in df.columns: df[c] = ""
    df["_name"] = df.apply(cc_full_name, axis=1)
    df["_phone"] = df.apply(cc_phone_list, axis=1)
    df["_house_sort"] = pd.to_numeric(df["House Number"].astype(str).str.extract(r"(\d+)")[0], errors="coerce").fillna(0)
    df = df.sort_values(["County", "Municipality", "Precinct", "Street Name", "_house_sort", "LastName"], na_position="last")
    rows = []
    for _, r in df.iterrows():
        rows.append([cc_clean_text(r.get("Street Name","")), cc_clean_text(r.get("House Number","")), cc_clean_text(r.get("Apartment Number","")), r.get("_name",""), r.get("_phone",""), cc_clean_text(r.get("Party","")), cc_clean_text(r.get("Age","")), cc_clean_text(r.get("MB_PERM",""))])
    return simple_table_pdf("Candidate Connect Street List", f"{len(df):,} voters • phones show (m) mobile, (l) landline, (u) applicant/unknown", rows, ["Street", "House", "Apt", "Full Name", "Phone", "P", "Age", "Perm"])


def generate_call_list_pdf(active):
    df = build_report_dataframe(active, max_rows=9000)
    if df.empty:
        return simple_table_pdf("Call List", "No matching voters", [], ["Name", "Phone", "Party", "Age", "Municipality"])
    df["_name"] = df.apply(cc_full_name, axis=1)
    df["_phone"] = df.apply(cc_phone_list, axis=1)
    df = df[df["_phone"].astype(str).str.strip().ne("")].sort_values(["County", "Municipality", "LastName", "FirstName"], na_position="last")
    rows = [[r.get("_name",""), r.get("_phone",""), cc_clean_text(r.get("Party","")), cc_clean_text(r.get("Age","")), cc_clean_text(r.get("Municipality","")), cc_clean_text(r.get("Notes",""))] for _, r in df.iterrows()]
    return simple_table_pdf("Candidate Connect Call List", f"{len(df):,} voters with phone • phones show (m) mobile, (l) landline, (u) applicant/unknown", rows, ["Full Name", "Phone(s)", "P", "Age", "Municipality", "Notes"])


def generate_labels_pdf(active):
    df = build_report_dataframe(active, max_rows=3000)
    rows = []
    for _, r in df.iterrows():
        name = cc_full_name(r)
        addr = cc_address_line(r)
        city = cc_clean_text(r.get("City", "")) or cc_clean_text(r.get("res_city", ""))
        state = cc_clean_text(r.get("State", "")) or cc_clean_text(r.get("res_state", "")) or "PA"
        zipc = cc_clean_text(r.get("Zip", "")) or cc_clean_text(r.get("res_zip", ""))
        if name and addr:
            rows.append([name, addr, f"{city}, {state} {zipc}".strip()])
    bio = io.BytesIO(); c = canvas.Canvas(bio, pagesize=letter); w, h = letter
    c.setFont("Helvetica-Bold", 9)
    c.drawString(36, h-28, "Avery 5160 / 8160 compatible: 30 labels per sheet, 3 columns x 10 rows. This note is in the top margin and should not overlap labels.")
    left, top = 0.1875*inch, h - 0.5*inch
    label_w, label_h = 2.625*inch, 1.0*inch
    gap_x = 0.125*inch
    x_positions = [left, left+label_w+gap_x, left+2*(label_w+gap_x)]
    y = top
    idx = 0
    c.setFont("Helvetica", 9)
    for name, addr, cityline in rows:
        pos = idx % 30
        if idx and pos == 0:
            c.showPage(); c.setFont("Helvetica", 9)
        col = pos % 3; row = pos // 3
        x = x_positions[col] + 0.10*inch
        y = top - row*label_h - 0.28*inch
        c.drawString(x, y, name[:32]); c.drawString(x, y-12, addr[:32]); c.drawString(x, y-24, cityline[:32])
        idx += 1
    c.save(); bio.seek(0); return bio.getvalue()


def render_enhanced_statewide_snapshot():
    st.markdown("<div style='font-size:28px;font-weight:900;letter-spacing:.08em;color:#ffffff;margin:0 0 18px 6px;text-transform:uppercase;'>Voters Statewide</div>", unsafe_allow_html=True)
    summary, _ = quick_counts({})
    summary = summary or {"total":0,"r":0,"d":0,"o":0}
    render_metrics(summary)
    party_df = pd.DataFrame([{"Party":"Republican","Voters":summary.get("r",0)},{"Party":"Democrat","Voters":summary.get("d",0)},{"Party":"Other / Unaffiliated","Voters":summary.get("o",0)}])
    left, right = st.columns([1,1])
    with left:
        st.markdown("#### Voters by Party")
        st.bar_chart(party_df.set_index("Party"), height=260)
    with right:
        st.markdown("#### Voters by Geography")
        st.dataframe(pd.DataFrame([
            {"Geography":"US Congress", "Total Voters":summary.get("total",0), "Republican":summary.get("r",0), "Democrat":summary.get("d",0), "Other / Unaffiliated":summary.get("o",0)},
            {"Geography":"State Senate", "Total Voters":summary.get("total",0), "Republican":summary.get("r",0), "Democrat":summary.get("d",0), "Other / Unaffiliated":summary.get("o",0)},
            {"Geography":"State House", "Total Voters":summary.get("total",0), "Republican":summary.get("r",0), "Democrat":summary.get("d",0), "Other / Unaffiliated":summary.get("o",0)},
        ]), hide_index=True, width="stretch")
    st.caption("Use the sidebar to build a campaign universe, search voters, open Mail Ballot Center, or view Area Intelligence.")


def render_voter_lookup_workspace():
    st.markdown("## Voter Lookup")
    st.caption("Search full-state active voters by name, county, address, PA ID, phone, or email. Uses remote DuckDB; no shard loop in Streamlit.")
    q = st.session_state.get(special_key("lookup_query"), "")
    maxn = int(st.session_state.get(special_key("lookup_max"), 25) or 25)
    if not q:
        st.info("Enter a search in the left pane."); return
    urls = index_urls_from_manifest()
    term = str(q).strip().replace("'", "''")
    like = f"%{term.lower()}%"
    cols = detail_report_columns()
    select_cols = ", ".join([f"CAST({sql_ident(c)} AS VARCHAR) AS {sql_ident(c)}" for c in cols])
    where = " OR ".join([f"LOWER(CAST({sql_ident(c)} AS VARCHAR)) LIKE {sql_lit(like)}" for c in ["FullName","Name","FirstName","LastName","County","Municipality","Precinct","voter_id","Mobile","Landline","Email","Street Name","City"]])
    con = duckdb.connect(database=':memory:')
    try:
        try: con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        df = con.execute(f"SELECT {select_cols} FROM read_parquet({urls!r}) WHERE {where} LIMIT {maxn}").df()
    finally:
        con.close()
    if df.empty:
        st.warning("No voters found."); return
    st.caption(f"{len(df)} result(s) found for: {q}")
    results_col, detail_col = st.columns([0.9,1.6])
    if "lookup_selected_idx" not in st.session_state: st.session_state.lookup_selected_idx = 0
    with results_col:
        st.markdown("### Search Results")
        for i, r in df.iterrows():
            label = cc_full_name(r)
            addr = cc_address_line(r)
            if st.button(f"{label}\n{addr}\n{cc_clean_text(r.get('County',''))} County", key=f"lookup_pick_{i}", width="stretch"):
                st.session_state.lookup_selected_idx = int(i); st.rerun()
    with detail_col:
        r = df.iloc[min(st.session_state.lookup_selected_idx, len(df)-1)]
        st.markdown(f"## {cc_full_name(r)}")
        st.markdown(f"{cc_address_line(r)}  \\n{cc_clean_text(r.get('City','')) or cc_clean_text(r.get('res_city',''))}, {cc_clean_text(r.get('State','')) or cc_clean_text(r.get('res_state',''))} {cc_clean_text(r.get('Zip','')) or cc_clean_text(r.get('res_zip',''))}")
        k1,k2,k3,k4 = st.columns(4)
        k1.metric("Party", cc_clean_text(r.get("Party","")) or "—"); k2.metric("Gender", cc_clean_text(r.get("Gender","")) or "—"); k3.metric("Age", cc_clean_text(r.get("Age","")) or "—"); k4.metric("PA ID", cc_clean_text(r.get("voter_id","")) or "—")
        left,right = st.columns(2)
        with left:
            st.markdown("### Voter Details")
            detail_rows = [["Registration Date", r.get("RegistrationDate","")], ["County", r.get("County","")], ["Municipality", r.get("Municipality","")], ["Precinct", r.get("Precinct","")], ["Congressional", r.get("USC","")], ["State Senate", r.get("STS","")], ["State House", r.get("STH","")], ["School District", r.get("School District","")], ["School Region", r.get("School Region","")]]
            st.dataframe(pd.DataFrame(detail_rows, columns=["Field","Value"]), hide_index=True, width="stretch")
        with right:
            st.markdown("### Contact + Mail Ballot")
            st.dataframe(pd.DataFrame([["Phones", cc_phone_list(r)], ["Email", r.get("Email","")], ["Mail Ballot Application", r.get("MB_App","") or r.get("MIB_Applied","")], ["Mail Ballot Status", r.get("MB_Status","") or r.get("MIB_BALLOT","")], ["Tags", r.get("Tags","")]], columns=["Field","Value"]), hide_index=True, width="stretch")
        with st.expander("Edit / Correct This Voter Record", expanded=False):
            st.info("Corrections entered here can be copied into the pipeline correction file. Persistent cloud write-back will be reconnected after core launch testing.")
            edits = {}
            ec = st.columns(4)
            for j, field in enumerate(["FirstName","MiddleName","LastName","NameSuffix","Gender","Party","DOB","RegistrationDate","House Number","Street Name","Apartment Number","City","State","Zip","Municipality","Precinct","School District","School Region","Mobile","Landline","Email","MB_App","MB_Status","Tags"]):
                with ec[j % 4]:
                    edits[field] = st.text_input(field, value=cc_clean_text(r.get(field,"")), key=f"edit_{field}_{r.get('voter_id','')}")
            notes = st.text_area("Correction Notes", key=f"edit_notes_{r.get('voter_id','')}")
            payload = {"voter_id": cc_clean_text(r.get("voter_id","")), "updated_at": datetime.now().isoformat(timespec="seconds"), "fields": edits, "notes": notes}
            st.download_button("Download This Correction JSON", data=json.dumps(payload, indent=2).encode("utf-8"), file_name=f"voter_correction_{payload['voter_id'] or 'unknown'}.json", mime="application/json", width="stretch")


def render_mail_ballot_center_workspace():
    st.markdown("## Mail Ballot Center")
    st.caption("Strategic mail ballot operations, targeting, and follow-up workspace. Counts use the same remote quick-count/index logic as Create Universe.")
    main_
active = active_filters()
section = st.session_state.get("left_section")

if section == "voter_lookup":
    render_voter_lookup_workspace()
    st.stop()
if section == "mail_ballot_center":
    render_mail_ballot_center_workspace()
    st.stop()
if section == "area_intelligence":
    render_area_intelligence_workspace()
    st.stop()
if section != "create_universe":
    render_enhanced_statewide_snapshot()
    st.stop()

render_create_universe_main(active)
st.caption(f"Rendered at {datetime.now().isoformat(timespec='seconds')}")

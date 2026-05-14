# Candidate Connect DEV — Final Hybrid Cloud App v10b
# Full safe filters + update counts + guarded export.
# Designed after R2/manifest/filter-layer diagnostics passed.

import io
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

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

</style>
""",
    unsafe_allow_html=True,
)


def r2_url(key: str) -> str:
    return f"{R2}/{key.lstrip('/')}"


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
    manifest = load_manifest()
    speed = manifest.get("speed", {}).get("tables", {})
    filter_options = load_parquet(speed.get("filter_options", "speed/filter_options.parquet"))
    geo_hierarchy = load_parquet(speed.get("geo_hierarchy", "speed/geo_hierarchy.parquet"))
    return manifest, filter_options, geo_hierarchy


@st.cache_data(ttl=300, show_spinner=False)
def load_count_cube_columns(cols_tuple):
    manifest = load_manifest()
    speed = manifest.get("speed", {}).get("tables", {})
    key = speed.get("count_cube", "speed/count_cube.parquet")
    try:
        return load_parquet(key, columns=list(cols_tuple))
    except Exception:
        # Fallback: full read only if column-select fails.
        return load_parquet(key)


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


def selected(field: str):
    suffix = st.session_state.get("filter_reset_token", 0)
    return st.session_state.get(f"filter_{field}_{suffix}", [])

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
    # v16: quick counts use the rebuilt speed/count_cube fields.
    safe = set(GEO_FIELDS + [
        "Party", "Gender", "Age_Range", "V4A", "V4G", "V4P",
        "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM",
        "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone",
    ])
    return {k: v for k, v in active.items() if k in safe}

def non_count_filters(active: dict) -> dict:
    safe = set(GEO_FIELDS + [
        "Party", "Gender", "Age_Range", "V4A", "V4G", "V4P",
        "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM",
        "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone",
    ])
    return {k: v for k, v in active.items() if k not in safe}

def manifest_columns() -> list[str]:
    try:
        return (
            (manifest.get("schema") or {}).get("index_columns")
            or (manifest.get("index") or {}).get("columns")
            or []
        )
    except Exception:
        return []

def election_filter_columns() -> list[str]:
    cols = []
    for c in manifest_columns():
        s = str(c)
        u = re.sub(r"[^A-Z0-9]+", "_", s.upper()).strip("_")
        if re.search(r"(^|_)(G|GENERAL|GEN|P|PRIMARY|PRI|PRIM)_?((?:20)?\d{2})(_|$)", u) or re.search(r"(^|_)((?:20)?\d{2})_?(G|GENERAL|GEN|P|PRIMARY|PRI|PRIM)(_|$)", u):
            cols.append(s)
    return cols

def election_years_from_columns() -> list[str]:
    years = set()
    for c in election_filter_columns():
        u = re.sub(r"[^A-Z0-9]+", "_", str(c).upper()).strip("_")
        for m in re.finditer(r"(20\d{2}|\d{2})", u):
            y = m.group(1)
            if len(y) == 2:
                y = "20" + y
            try:
                yi = int(y)
                if 2000 <= yi <= 2030:
                    years.add(str(yi))
            except Exception:
                pass
    return sorted(years, reverse=True)

def vote_score_field_from_selection() -> str:
    choice = st.session_state.get("vote_score_type", "All Elections")
    if choice == "General Elections":
        return "V4G"
    if choice == "Primary Elections":
        return "V4P"
    return "V4A"


def active_special_filters() -> dict:
    special = {}

    # Newly registered slider: export/report detail-shard filter.
    new_reg_months = st.session_state.get("new_reg_months", 0)
    if new_reg_months and int(new_reg_months) > 0:
        special["RegistrationDate"] = {"months": int(new_reg_months)}

    # Vote history score slider: can apply to count cube and export/detail if selected.
    vh_range = st.session_state.get("vote_history_score_range", (0, 4))
    vh_field = vote_score_field_from_selection()
    if vh_range != (0, 4):
        special[vh_field] = {"min": int(vh_range[0]), "max": int(vh_range[1])}

    # MB probability score slider.
    mb_prob = st.session_state.get("mb_prob_score_range", (0, 4))
    if mb_prob != (0, 4):
        special["MB_Prob_Score"] = {"min": int(mb_prob[0]), "max": int(mb_prob[1])}

    return special

def apply_special_filters(df: pd.DataFrame, special: dict) -> pd.DataFrame:
    out = df
    for field, rule in (special or {}).items():
        if field == "RegistrationDate" and field in out.columns:
            months = int(rule.get("months", 0))
            if months > 0:
                cutoff = pd.Timestamp.now().normalize() - pd.DateOffset(months=months)
                dates = pd.to_datetime(out[field], errors="coerce")
                out = out[dates >= cutoff]
        elif field in out.columns and isinstance(rule, dict) and "min" in rule and "max" in rule:
            vals = pd.to_numeric(out[field], errors="coerce")
            out = out[(vals >= float(rule["min"])) & (vals <= float(rule["max"]))]
    return out

def apply_filters(df: pd.DataFrame, active: dict) -> pd.DataFrame:
    out = df
    for field, vals in active.items():
        if vals and field in out.columns:
            expanded_vals = expand_filter_values(field, vals)
            out = out[out[field].astype(str).isin([str(v) for v in expanded_vals])]
    return out


def options_from_geo(df: pd.DataFrame, field: str, active: dict) -> list:
    if field not in df.columns:
        return []
    hierarchy_order = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]
    relevant = {}
    for f in hierarchy_order:
        if f == field:
            break
        if active.get(f):
            relevant[f] = active[f]
    narrowed = apply_filters(df, relevant)
    vals = narrowed[field].astype(str).map(clean_value)
    return sorted([v for v in vals.unique().tolist() if v], key=smart_sort_key)


def options_from_filter_table(filter_options: pd.DataFrame, field: str) -> list:
    if "field" not in filter_options.columns or "value" not in filter_options.columns:
        return []
    vals = filter_options.loc[filter_options["field"].astype(str).eq(field), "value"].astype(str).map(clean_value)
    return sorted([v for v in vals.unique().tolist() if v], key=smart_sort_key)

def clean_yes_no_all_options():
    return ["Y", "N"]

def clean_mail_options(field: str):
    fixed = {
        "MB_App": ["Applied", "Not Applied"],
        "MB_App_Status": ["Approved", "Declined"],
        "MB_Sent": ["Sent", "Not Sent"],
        "MB_Status": ["Voted", "Not Voted"],
        "MB_PERM": ["Y", "N"],
        "HasMobile": ["Y", "N"],
        "HasLandline": ["Y", "N"],
        "HasEmail": ["Y", "N"],
        "HasApplicantPhone": ["Y", "N"],
    }
    return fixed.get(field, [])

def field_options(filter_options: pd.DataFrame, field: str):
    fixed = clean_mail_options(field)
    if fixed:
        return fixed
    return options_from_filter_table(filter_options, field)

def is_cube_safe(active: dict) -> bool:
    # Geography + Party/Gender/Age_Range usually live in the count cube.
    # Anything else uses exact shard scan through the same Update Counts button.
    cube_safe = set(GEO_FIELDS + ["Party", "Gender", "Age_Range"])
    return all(k in cube_safe for k in active.keys())

def update_counts(active: dict):
    # v20: Update Counts uses quick-count tables plus supported range filters.
    safe_active = count_safe_filters(active) if "count_safe_filters" in globals() else active
    special = active_special_filters()
    try:
        needed_special = {k: v for k, v in special.items() if k != "RegistrationDate"}
        summary, err = quick_counts({**safe_active})
        if err is not None:
            return None, "unavailable", err

        # If range sliders are active and available, recalculate directly from count cube.
        if needed_special:
            needed = set(["Party"])
            needed.update(safe_active.keys())
            needed.update(needed_special.keys())
            possible_count_cols = ["Voters", "voters", "count", "Count", "Total", "total"]
            last_error = None
            for count_col in possible_count_cols:
                try:
                    cube = load_count_cube_columns(tuple(sorted(needed | {count_col})))
                    filtered = apply_filters(cube, safe_active)
                    filtered = apply_special_filters(filtered, needed_special)
                    return summarize_from_df(filtered, row_count_mode=False), "quick", None
                except Exception as e:
                    last_error = e
            return summary, "quick", last_error
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
    st.caption("Current statewide voter universe from the latest uploaded speed tables.")

    with st.spinner("Loading statewide snapshot..."):
        summary, err = quick_counts({})

    if err:
        st.warning("Statewide snapshot is not available yet.")
        st.caption(str(err)[:500])
        return

    render_metrics(summary, label="")

    chart_left, chart_right = st.columns([1, 1])
    with chart_left:
        render_party_chart(summary, "Statewide Party Breakdown")
    with chart_right:
        st.markdown("#### Snapshot Notes")
        st.info("Use Targeting to build a voter universe. Counts update from speed tables; exports and reports apply the selected filters to detail shards.")

def quick_counts(active: dict):
    # v14b: quick counts only use selected columns from speed/count_cube.parquet.
    # No full-cube fallback and no detail-shard fallback.
    needed = set(["Party"])
    needed.update(active.keys())

    possible_count_cols = ["Voters", "voters", "count", "Count", "Total", "total"]
    last_error = None

    for count_col in possible_count_cols:
        cols = tuple(sorted(needed | {count_col}))
        try:
            cube = load_count_cube_columns(cols)
            filtered = apply_filters(cube, active)
            return summarize_from_df(filtered, row_count_mode=False), None
        except Exception as e:
            last_error = e
            continue

    return None, last_error or RuntimeError("Quick count fields are not available for this filter combination.")


def exact_counts(active: dict):
    special = active_special_filters()
    needed = set(["Party"])
    needed.update(active.keys())
    needed.update(special.keys())
    cols = tuple(sorted(needed))

    total = 0
    r_count = 0
    d_count = 0
    o_count = 0

    progress = st.progress(0)
    status = st.empty()

    for i in range(DETAIL_SHARDS):
        key = f"detail/voters_detail_{i:03d}.parquet"
        status.write(f"Verifying shard {i+1} of {DETAIL_SHARDS}: {key}")
        df = load_detail_columns(key, cols)

        for col, vals in active.items():
            if vals and col in df.columns:
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
        progress.progress((i + 1) / DETAIL_SHARDS)

    status.empty()
    return {"total": total, "r": r_count, "d": d_count, "o": o_count}


def build_export(active: dict, columns: list[str]):
    special = active_special_filters()
    if not active and not special:
        raise RuntimeError("Please select at least one filter before exporting.")

    needed = set(columns)
    needed.update(active.keys())
    needed.update(special.keys())
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
            if vals and col in df.columns:
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


st.markdown('<div class="cc-header">', unsafe_allow_html=True)
h_logo, h_mid, h_power = st.columns([1.1, 2.8, 1.2])
with h_logo:
    if file_exists(LOGO_CANDIDATE_CONNECT):
        st.image(LOGO_CANDIDATE_CONNECT, width="stretch")
    else:
        st.markdown('<div class="cc-title">Candidate Connect</div>', unsafe_allow_html=True)
with h_mid:
    st.markdown('<div class="cc-title">Candidate Connect DEV</div>', unsafe_allow_html=True)
    st.markdown('<div class="cc-sub">Voter Data & Engagement Platform • Stable DEV cloud build v20b</div>', unsafe_allow_html=True)
with h_power:
    if file_exists(LOGO_TPTC):
        st.image(LOGO_TPTC, width="stretch")
    else:
        st.markdown('<div class="cc-powered">Powered by<br><b>The Political Technology Company</b></div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

try:
    with st.spinner("Loading filters from R2..."):
        manifest, filter_options, geo_hierarchy = load_filter_layer()
except Exception as e:
    st.error("Could not load the filter layer.")
    st.exception(e)
    st.stop()

if "filter_reset_token" not in st.session_state:
    st.session_state["filter_reset_token"] = 0
_filter_suffix = st.session_state["filter_reset_token"]

with st.sidebar:
    st.markdown("## Candidate Connect")
    st.caption("DEV final hybrid v20b")

    if "left_section" not in st.session_state:
        st.session_state["left_section"] = None

    if st.button("🎯 Create Universe", width="stretch"):
        st.session_state["left_section"] = "create_universe"
        st.session_state["view"] = "targeting"
        st.rerun()

    if st.button("📬 Mail Ballot Center", width="stretch"):
        st.session_state["left_section"] = "mail_ballot_center"
        st.session_state["view"] = "dashboard"
        st.rerun()

    if st.button("🔎 Voter Lookup", width="stretch"):
        st.session_state["left_section"] = "voter_lookup"
        st.session_state["view"] = "dashboard"
        st.rerun()

    if st.button("⌂ Area Intelligence", width="stretch"):
        st.session_state["left_section"] = "area_intelligence"
        st.session_state["view"] = "dashboard"
        st.rerun()

    st.divider()

    if st.session_state.get("left_section") == "create_universe":
        st.markdown("### Create Universe")

        with st.expander("Geography", expanded=False):
            for field in GEO_FIELDS:
                label = DISPLAY_LABELS.get(field, field)
                opts = options_from_geo(geo_hierarchy, field, active_geo_filters())
                st.multiselect(label, options=opts, key=f"filter_{field}_{_filter_suffix}")

        with st.expander("Voter Details", expanded=False):
            for field in ["Party", "Gender", "Age_Range", "CalculatedParty", "HH-Party"]:
                label = DISPLAY_LABELS.get(field, field.replace("_", " "))
                opts = field_options(filter_options, field)
                if opts:
                    st.multiselect(label, options=opts, key=f"filter_{field}_{_filter_suffix}")

            st.slider(
                "Newly Registered Within Last N Months",
                min_value=0,
                max_value=24,
                value=0,
                step=1,
                help="0 = all voters. This is applied in exports/reports and detail-based tools.",
                key="new_reg_months",
            )

        with st.expander("Vote History", expanded=False):
            st.selectbox(
                "Vote History Type",
                options=["All Elections", "General Elections", "Primary Elections"],
                key="vote_score_type",
            )
            st.slider(
                "Vote History Score Range",
                min_value=0,
                max_value=4,
                value=(0, 4),
                step=1,
                key="vote_history_score_range",
            )

            years = election_years_from_columns()
            if years:
                st.multiselect("Election Year", options=years, key=f"filter_ElectionYear_{_filter_suffix}")
                st.multiselect("Election Type", options=["General", "Primary"], key=f"filter_ElectionType_{_filter_suffix}")
                st.multiselect("Vote Method", options=["Voted", "Mail Ballot", "Polls / In Person", "Did Not Vote"], key=f"filter_ElectionMethod_{_filter_suffix}")
                st.caption("Election year/type/method controls are being restored carefully. They will be wired to detail exports/reports before being added to quick counts.")

        with st.expander("Mail Ballot", expanded=False):
            for field in ["MB_App", "MB_App_Status", "MB_Sent", "MB_Status"]:
                label = DISPLAY_LABELS.get(field, field.replace("_", " "))
                opts = field_options(filter_options, field)
                st.multiselect(label, options=opts, key=f"filter_{field}_{_filter_suffix}")

            st.slider(
                "Mail Ballot Probability Score",
                min_value=0,
                max_value=4,
                value=(0, 4),
                step=1,
                key="mb_prob_score_range",
            )

        with st.expander("Contact Filters", expanded=False):
            for field in ["HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone"]:
                label = DISPLAY_LABELS.get(field, field.replace("_", " "))
                opts = field_options(filter_options, field)
                st.multiselect(label, options=opts, key=f"filter_{field}_{_filter_suffix}")

        tag_opts = field_options(filter_options, "Tags")
        if tag_opts:
            with st.expander("Tags", expanded=False):
                st.multiselect("Tags", options=tag_opts, key=f"filter_Tags_{_filter_suffix}")

    elif st.session_state.get("left_section") == "mail_ballot_center":
        st.markdown("### Mail Ballot Center")
        st.info("Mail Ballot Center will be restored next. Use Create Universe for mail ballot filtering and exports for now.")

    elif st.session_state.get("left_section") == "voter_lookup":
        st.markdown("### Voter Lookup")
        st.info("Voter Lookup will be restored after the Create Universe workflow is stable.")

    elif st.session_state.get("left_section") == "area_intelligence":
        st.markdown("### Area Intelligence")
        st.info("Area Intelligence will be restored after the core targeting/export tools are stable.")


active = active_filters()

if st.session_state.get("left_section") == "create_universe":
    st.session_state["view"] = "targeting"

if st.session_state.get("view", "dashboard") == "dashboard":
    render_statewide_snapshot()
    st.stop()

if st.session_state.get("view") == "analysis":
    st.markdown("## Analysis")
    st.info("Analysis dashboards are next. The stable targeting and export engine is already in place.")
    st.stop()

if st.session_state.get("view") == "export":
    st.markdown("## Export")
else:
    st.markdown("## Create Universe")

st.markdown("### Current Universe")
special_active = active_special_filters()
if active or special_active:
    chips = []
    for k, vals in active.items():
        chips.append(f"**{DISPLAY_LABELS.get(k, k)}:** {', '.join(map(str, vals[:6]))}{'…' if len(vals) > 6 else ''}")
    if "RegistrationDate" in special_active:
        chips.append(f"**Newly Registered:** last {special_active['RegistrationDate']['months']} months")
    for _score_field in ["V4A", "V4G", "V4P", "MB_Prob_Score"]:
        if _score_field in special_active:
            chips.append(f"**{DISPLAY_LABELS.get(_score_field, _score_field)}:** {special_active[_score_field]['min']}–{special_active[_score_field]['max']}")
    st.markdown(" &nbsp; | &nbsp; ".join(chips), unsafe_allow_html=True)
else:
    st.info("No filters selected. Choose filters in the left pane.")

st.markdown(
    '<div class="cc-note"><b>Update Counts uses the rebuilt quick-count tables.</b> '
    'Tags are applied in exports and reports.</div>',
    unsafe_allow_html=True,
)
extra_filters = non_count_filters(active)
if extra_filters:
    st.info("Some selected tag filters are export/report filters and are not included in the quick count total yet.")

st.markdown("## Counts")

action_left, action_mid, action_spacer = st.columns([0.85, 0.85, 4.3])
with action_left:
    if st.button("Update Counts", width="stretch"):
        with st.spinner("Updating counts..."):
            summary, mode, err = update_counts(active)
        if err:
            st.warning("Counts are unavailable for this filter combination.")
            st.caption(str(err)[:500])
        else:
            st.session_state["quick_summary"] = summary
            st.session_state["count_mode"] = mode

with action_mid:
    if st.button("Clear Filters", width="stretch"):
        st.session_state["filter_reset_token"] = st.session_state.get("filter_reset_token", 0) + 1
        st.session_state.pop("quick_summary", None)
        st.session_state.pop("count_mode", None)
        st.rerun()

if st.session_state.get("quick_summary"):
    st.markdown("### Current Counts")
    st.caption("Updated using fast count tables." if st.session_state.get("count_mode") == "quick" else "Updated using exact detail scan for this filter combination.")
    render_metrics(st.session_state["quick_summary"], label="")
    left_chart, right_blank = st.columns([1, 1])
    with left_chart:
        render_party_chart(st.session_state["quick_summary"], "Party Breakdown")

st.markdown("## Output Center")
st.caption("Exports and reports apply the current filters against the detail shards. Update Counts uses rebuilt quick-count tables and does not scan detail shards.")

selected_cols = st.multiselect("Export columns", options=DEFAULT_EXPORT_COLUMNS, default=DEFAULT_EXPORT_COLUMNS)

if st.button("Build Export File", width="stretch"):
    try:
        with st.spinner("Building filtered export from detail shards..."):
            df_export = build_export(active, selected_cols)
    except Exception as e:
        st.error("Could not build export.")
        st.exception(e)
        st.stop()

    st.success(f"Export built: {len(df_export):,} rows")
    st.dataframe(df_export.head(250), width="stretch")
    st.download_button(
        "Download CSV",
        data=df_export.to_csv(index=False).encode("utf-8"),
        file_name=f"candidate_connect_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        width="stretch",
    )

st.caption(f"Rendered at {datetime.now().isoformat(timespec='seconds')}")
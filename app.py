import io
import json
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Candidate Connect DEV", layout="wide")

R2_BASE = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"

GEO_FIELDS = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]
VOTER_FIELDS = ["Party", "Gender", "Age_Range", "MIB_Applied", "MIB_BALLOT", "MB_PERM"]
ALL_FILTER_FIELDS = GEO_FIELDS + VOTER_FIELDS

DISPLAY_LABELS = {
    "USC": "Congressional District",
    "STS": "State Senate District",
    "STH": "State House District",
    "Age_Range": "Age Range",
    "MIB_Applied": "Mail Ballot Application",
    "MIB_BALLOT": "Mail Ballot Status",
    "MB_PERM": "Permanent Mail Ballot",
}

DEFAULT_EXPORT_COLUMNS = [
    "County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region",
    "voter_id", "FirstName", "MiddleName", "LastName", "Name", "FullName",
    "Party", "CalculatedParty", "Gender", "Age", "Age_Range", "RegistrationDate",
    "House Number", "Street Name", "Apartment Number", "City", "State", "Zip",
    "res_address", "res_city", "res_state", "res_zip",
    "Email", "Mobile", "Landline", "Current_ApplicantPhone",
    "MIB_Applied", "MIB_BALLOT", "MB_PERM", "MB_Prob_Score",
    "Current_App_Return_Date", "Current_Ballot_Sent_Date", "Current_Ballot_Returned_Date",
    "Tags",
]

EXPORT_ROW_LIMIT = 250_000
DETAIL_SHARD_COUNT = 36

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
.block-container { padding-top: 1.25rem; max-width: 1500px; }
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
</style>
""",
    unsafe_allow_html=True,
)

def r2_url(key: str) -> str:
    return f"{R2_BASE}/{key.lstrip('/')}"

@st.cache_data(ttl=600, show_spinner=False)
def fetch_bytes(key: str) -> bytes:
    resp = requests.get(r2_url(key), timeout=120)
    resp.raise_for_status()
    return resp.content

@st.cache_data(ttl=600, show_spinner=False)
def fetch_text(key: str) -> str:
    return fetch_bytes(key).decode("utf-8")

@st.cache_data(ttl=600, show_spinner=False)
def fetch_manifest() -> dict:
    return json.loads(fetch_text("dataset_manifest.json"))

@st.cache_data(ttl=600, show_spinner=False)
def fetch_parquet(key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(fetch_bytes(key)))

@st.cache_data(ttl=600, show_spinner=False)
def load_filter_layer():
    manifest = fetch_manifest()
    speed = manifest.get("speed", {}).get("tables", {})
    filter_options = fetch_parquet(speed.get("filter_options", "speed/filter_options.parquet"))
    geo_hierarchy = fetch_parquet(speed.get("geo_hierarchy", "speed/geo_hierarchy.parquet"))
    return manifest, filter_options, geo_hierarchy

@st.cache_data(ttl=300, show_spinner=False)
def load_count_cube():
    manifest = fetch_manifest()
    speed = manifest.get("speed", {}).get("tables", {})
    return fetch_parquet(speed.get("count_cube", "speed/count_cube.parquet"))

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
    return st.session_state.get(f"filter_{field}", [])

def active_filters() -> dict:
    out = {}
    for f in ALL_FILTER_FIELDS:
        vals = selected(f)
        if vals:
            out[f] = vals
    return out

def active_geo_filters() -> dict:
    return {k: v for k, v in active_filters().items() if k in GEO_FIELDS}

def apply_filters(df: pd.DataFrame, active: dict) -> pd.DataFrame:
    out = df
    for field, vals in active.items():
        if vals and field in out.columns:
            out = out[out[field].astype(str).isin([str(v) for v in vals])]
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

def find_count_col(df: pd.DataFrame) -> str | None:
    for c in ["Voters", "voters", "count", "Count", "Total", "total"]:
        if c in df.columns:
            return c
    nums = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    return nums[0] if nums else None

def summarize_counts(count_cube: pd.DataFrame, active: dict):
    df = apply_filters(count_cube, active)
    count_col = find_count_col(df)
    if count_col is None or df.empty:
        return {"voters": 0, "r": 0, "d": 0, "o": 0, "emails": 0, "mobiles": 0}, df

    voters = int(df[count_col].fillna(0).sum())
    party_counts = {"R": 0, "D": 0, "O": 0}
    if "Party" in df.columns:
        p = df.groupby("Party", dropna=False)[count_col].sum().to_dict()
        for key, val in p.items():
            k = str(key).strip().upper()
            if k in party_counts:
                party_counts[k] += int(val)
            elif k:
                party_counts["O"] += int(val)
    emails = int(df["Emails"].fillna(0).sum()) if "Emails" in df.columns else 0
    mobiles = int(df["Mobiles"].fillna(0).sum()) if "Mobiles" in df.columns else 0
    return {"voters": voters, "r": party_counts["R"], "d": party_counts["D"], "o": party_counts["O"], "emails": emails, "mobiles": mobiles}, df

def pct(n, d):
    return "0.0%" if not d else f"{(n / d) * 100:.1f}%"

def group_table(df: pd.DataFrame, field: str) -> pd.DataFrame:
    count_col = find_count_col(df)
    if df.empty or field not in df.columns or count_col is None:
        return pd.DataFrame(columns=[field, "Voters", "Share"])
    g = df.groupby(field, dropna=False, as_index=False)[count_col].sum()
    g = g.rename(columns={count_col: "Voters"})
    g[field] = g[field].astype(str).map(clean_value)
    total = int(g["Voters"].sum())
    g["Share"] = g["Voters"].map(lambda x: pct(int(x), total))
    return g.sort_values("Voters", ascending=False)

def get_detail_keys(manifest: dict):
    shards = manifest.get("detail", {}).get("shards", [])
    keys = []
    for item in shards:
        if isinstance(item, dict) and item.get("key"):
            keys.append(item["key"])
        elif isinstance(item, str):
            keys.append(item)
    if not keys:
        keys = [f"detail/voters_detail_{i:03d}.parquet" for i in range(DETAIL_SHARD_COUNT)]
    return keys[:DETAIL_SHARD_COUNT]

def filter_detail_df(df: pd.DataFrame, active: dict) -> pd.DataFrame:
    out = df
    for field, vals in active.items():
        if vals and field in out.columns:
            out = out[out[field].astype(str).isin([str(v) for v in vals])]
    return out

@st.cache_data(ttl=180, show_spinner=False)
def build_export(active_json: str, columns_json: str):
    active = json.loads(active_json)
    selected_cols = json.loads(columns_json)
    manifest = fetch_manifest()
    parts = []
    total = 0
    for key in get_detail_keys(manifest):
        df = fetch_parquet(key)
        df = filter_detail_df(df, active)
        if df.empty:
            continue
        cols = [c for c in selected_cols if c in df.columns]
        if cols:
            df = df[cols]
        parts.append(df)
        total += len(df)
        if total > EXPORT_ROW_LIMIT:
            raise RuntimeError(f"Export is over {EXPORT_ROW_LIMIT:,} rows. Please narrow filters.")
    if not parts:
        return pd.DataFrame(columns=selected_cols)
    return pd.concat(parts, ignore_index=True)

st.markdown(
    """
<div class="cc-header">
  <div class="cc-title">Candidate Connect DEV</div>
  <div class="cc-sub">Cloud-safe rescue build • Stable filters • On-demand counts/export</div>
</div>
""",
    unsafe_allow_html=True,
)

try:
    with st.spinner("Loading safe filter layer from R2..."):
        manifest, filter_options, geo_hierarchy = load_filter_layer()
except Exception as e:
    st.error("Filter layer failed to load.")
    st.exception(e)
    st.stop()

with st.sidebar:
    st.markdown("## Candidate Connect")
    st.caption("DEV rescue build")

    if st.button("Clear Filters", use_container_width=True):
        for key in list(st.session_state.keys()):
            if key.startswith("filter_"):
                del st.session_state[key]
        st.rerun()

    st.divider()
    st.markdown("### Geography")
    for field in GEO_FIELDS:
        label = DISPLAY_LABELS.get(field, field)
        opts = options_from_geo(geo_hierarchy, field, active_geo_filters())
        st.multiselect(label, options=opts, key=f"filter_{field}")

    st.divider()
    st.markdown("### Basic Voter Filters")
    for field in VOTER_FIELDS:
        label = DISPLAY_LABELS.get(field, field.replace("_", " "))
        opts = options_from_filter_table(filter_options, field)
        st.multiselect(label, options=opts, key=f"filter_{field}")

active = active_filters()

st.markdown("### Current Universe")
if active:
    chips = []
    for k, vals in active.items():
        chips.append(f"**{DISPLAY_LABELS.get(k, k)}:** {', '.join(map(str, vals[:6]))}{'…' if len(vals) > 6 else ''}")
    st.markdown(" &nbsp; | &nbsp; ".join(chips), unsafe_allow_html=True)
else:
    st.info("No filters selected yet. Select filters in the left pane.")

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="cc-metric"><div class="label">Dataset Rows</div><div class="value">{int(manifest.get("total_rows", 0)):,}</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="cc-metric"><div class="label">Filter Options</div><div class="value">{len(filter_options):,}</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="cc-metric"><div class="label">Geo Rows</div><div class="value">{len(geo_hierarchy):,}</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="cc-metric"><div class="label">Built</div><div class="value" style="font-size:16px;">{manifest.get("built_at", "unknown")}</div></div>', unsafe_allow_html=True)

st.markdown("## Counts")
st.caption("Counts are on-demand for cloud stability.")

if st.button("Calculate Counts", use_container_width=True):
    try:
        with st.spinner("Loading count cube and calculating selected universe..."):
            count_cube = load_count_cube()
            summary, filtered_cube = summarize_counts(count_cube, active)
    except Exception as e:
        st.error("Could not calculate counts from count cube.")
        st.exception(e)
        st.stop()

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(f'<div class="cc-metric"><div class="label">Voters</div><div class="value">{summary["voters"]:,}</div></div>', unsafe_allow_html=True)
    with k2:
        st.markdown(f'<div class="cc-metric"><div class="label">Republican</div><div class="value">{summary["r"]:,}</div><div class="sub">{pct(summary["r"], summary["voters"])}</div></div>', unsafe_allow_html=True)
    with k3:
        st.markdown(f'<div class="cc-metric blue"><div class="label">Democrat</div><div class="value">{summary["d"]:,}</div><div class="sub">{pct(summary["d"], summary["voters"])}</div></div>', unsafe_allow_html=True)
    with k4:
        st.markdown(f'<div class="cc-metric green"><div class="label">Other / Unaffiliated</div><div class="value">{summary["o"]:,}</div><div class="sub">{pct(summary["o"], summary["voters"])}</div></div>', unsafe_allow_html=True)

    left, mid, right = st.columns(3)
    with left:
        st.markdown("#### Party")
        st.dataframe(group_table(filtered_cube, "Party"), use_container_width=True, hide_index=True)
    with mid:
        st.markdown("#### Gender")
        st.dataframe(group_table(filtered_cube, "Gender"), use_container_width=True, hide_index=True)
    with right:
        st.markdown("#### Mail Ballot Application")
        st.dataframe(group_table(filtered_cube, "MIB_Applied"), use_container_width=True, hide_index=True)

st.markdown("## Output Center")
st.caption("Build export loads detail/voters_detail_000.parquet through voters_detail_035.parquet only after click.")

selected_cols = st.multiselect("Export columns", options=DEFAULT_EXPORT_COLUMNS, default=DEFAULT_EXPORT_COLUMNS)

if st.button("Build Export File", use_container_width=True):
    if not active:
        st.error("Please select at least one filter before exporting. Statewide export is blocked.")
        st.stop()

    try:
        with st.spinner("Loading filtered detail shards for export..."):
            export_df = build_export(json.dumps(active, sort_keys=True), json.dumps(selected_cols))
    except Exception as e:
        st.error("Could not build export.")
        st.exception(e)
        st.stop()

    st.success(f"Export built: {len(export_df):,} rows")
    st.dataframe(export_df.head(250), use_container_width=True)

    csv_bytes = export_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name=f"candidate_connect_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

st.caption(f"Rendered at {datetime.now().isoformat(timespec='seconds')}")

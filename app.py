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
.stButton > button {
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
def fetch_manifest():
    return json.loads(fetch_text("dataset_manifest.json"))

@st.cache_data(ttl=600, show_spinner=False)
def fetch_parquet(key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(fetch_bytes(key)))

@st.cache_data(ttl=600, show_spinner=False)
def load_app_layer():
    manifest = fetch_manifest()
    speed = manifest.get("speed", {}).get("tables", {})
    filter_options = fetch_parquet(speed.get("filter_options", "speed/filter_options.parquet"))
    geo_hierarchy = fetch_parquet(speed.get("geo_hierarchy", "speed/geo_hierarchy.parquet"))
    count_cube = fetch_parquet(speed.get("count_cube", "speed/count_cube.parquet"))
    return manifest, filter_options, geo_hierarchy, count_cube

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
    candidates = ["Voters", "voters", "count", "Count", "Total", "total"]
    for c in candidates:
        if c in df.columns:
            return c
    # fallback: first numeric column
    nums = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    return nums[0] if nums else None

def summarize_counts(count_cube: pd.DataFrame, active: dict) -> tuple[dict, pd.DataFrame]:
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

    return {
        "voters": voters,
        "r": party_counts["R"],
        "d": party_counts["D"],
        "o": party_counts["O"],
        "emails": emails,
        "mobiles": mobiles,
    }, df

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

st.markdown(
    """
<div class="cc-header">
  <div class="cc-title">Candidate Connect DEV</div>
  <div class="cc-sub">Cloud-safe rebuild • Auto filters + count cube</div>
</div>
""",
    unsafe_allow_html=True,
)

try:
    with st.spinner("Loading filters and count cube from R2..."):
        manifest, filter_options, geo_hierarchy, count_cube = load_app_layer()
except Exception as e:
    st.error("App layer failed to load.")
    st.exception(e)
    st.stop()

with st.sidebar:
    st.markdown("## Candidate Connect")
    st.caption("DEV count-layer test")

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
summary, filtered_cube = summarize_counts(count_cube, active)

st.markdown("### Active Universe")
if active:
    chips = []
    for k, vals in active.items():
        chips.append(f"**{DISPLAY_LABELS.get(k, k)}:** {', '.join(map(str, vals[:6]))}{'…' if len(vals) > 6 else ''}")
    st.markdown(" &nbsp; | &nbsp; ".join(chips), unsafe_allow_html=True)
else:
    st.info("No filters selected. Showing statewide count cube summary.")

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="cc-metric"><div class="label">Total Voters</div><div class="value">{summary["voters"]:,}</div><div class="sub">Selected universe</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="cc-metric"><div class="label">Republican</div><div class="value">{summary["r"]:,}</div><div class="sub">{pct(summary["r"], summary["voters"])}</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="cc-metric blue"><div class="label">Democrat</div><div class="value">{summary["d"]:,}</div><div class="sub">{pct(summary["d"], summary["voters"])}</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="cc-metric green"><div class="label">Other / Unaffiliated</div><div class="value">{summary["o"]:,}</div><div class="sub">{pct(summary["o"], summary["voters"])}</div></div>', unsafe_allow_html=True)

st.markdown("")

left, mid, right = st.columns(3)
with left:
    st.markdown('<div class="cc-card">', unsafe_allow_html=True)
    st.markdown("#### Party Breakdown")
    st.dataframe(group_table(filtered_cube, "Party"), use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

with mid:
    st.markdown('<div class="cc-card">', unsafe_allow_html=True)
    st.markdown("#### Gender Breakdown")
    st.dataframe(group_table(filtered_cube, "Gender"), use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown('<div class="cc-card">', unsafe_allow_html=True)
    st.markdown("#### Mail Ballot Application")
    st.dataframe(group_table(filtered_cube, "MIB_Applied"), use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("### Debug / Health")
h1, h2, h3, h4 = st.columns(4)
with h1:
    st.metric("Dataset rows", f"{int(manifest.get('total_rows', 0)):,}")
with h2:
    st.metric("Filter options", f"{len(filter_options):,}")
with h3:
    st.metric("Geo rows", f"{len(geo_hierarchy):,}")
with h4:
    st.metric("Count cube rows", f"{len(count_cube):,}")

st.caption(f"Rendered at {datetime.now().isoformat(timespec='seconds')}")

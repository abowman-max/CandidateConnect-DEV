import io
import json
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Candidate Connect DEV", layout="wide")

R2_BASE = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"

GEO_FIELDS = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]

DISPLAY_LABELS = {
    "USC": "Congressional District",
    "STS": "State Senate District",
    "STH": "State House District",
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
.block-container {
    padding-top: 1.25rem;
    max-width: 1500px;
}
.cc-header {
    border: 1px solid rgba(201,31,39,.85);
    border-radius: 18px;
    padding: 18px 22px;
    background: radial-gradient(circle at 80% 0%, rgba(201,31,39,.23), transparent 35%),
                linear-gradient(90deg, #03070c, #0b111a 55%, #190407);
    box-shadow: 0 14px 35px rgba(0,0,0,.45);
    margin-bottom: 16px;
}
.cc-title {
    font-size: 30px;
    font-weight: 950;
    color: #fff;
}
.cc-sub {
    color: #cbd5e1;
    margin-top: 4px;
    font-size: 13px;
}
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
    min-height: 90px;
}
.cc-metric .label {
    color: #94a3b8;
    font-size: 11px;
    font-weight: 900;
    letter-spacing: .05em;
    text-transform: uppercase;
}
.cc-metric .value {
    color: #fff;
    font-size: 26px;
    font-weight: 950;
    margin-top: 8px;
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
[data-baseweb="tag"] {
    background: rgba(201,31,39,.30) !important;
    color: white !important;
}
.stAlert {
    background: rgba(15,23,42,.95) !important;
    color: #f8fafc !important;
}
</style>
""",
    unsafe_allow_html=True,
)


def r2_url(key: str) -> str:
    return f"{R2_BASE}/{key.lstrip('/')}"


@st.cache_data(ttl=600, show_spinner=False)
def fetch_bytes(key: str) -> bytes:
    resp = requests.get(r2_url(key), timeout=60)
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
def load_filter_layer():
    manifest = fetch_manifest()
    speed = manifest.get("speed", {}).get("tables", {})
    filter_options = fetch_parquet(speed.get("filter_options", "speed/filter_options.parquet"))
    geo_hierarchy = fetch_parquet(speed.get("geo_hierarchy", "speed/geo_hierarchy.parquet"))
    return manifest, filter_options, geo_hierarchy


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
    for f in GEO_FIELDS:
        vals = selected(f)
        if vals:
            out[f] = vals
    return out


def apply_geo_filters(df: pd.DataFrame, active: dict) -> pd.DataFrame:
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
    narrowed = apply_geo_filters(df, relevant)
    vals = narrowed[field].astype(str).map(clean_value)
    return sorted([v for v in vals.unique().tolist() if v], key=smart_sort_key)


def options_from_filter_table(filter_options: pd.DataFrame, field: str) -> list:
    if "field" not in filter_options.columns or "value" not in filter_options.columns:
        return []
    vals = filter_options.loc[filter_options["field"].astype(str).eq(field), "value"].astype(str).map(clean_value)
    return sorted([v for v in vals.unique().tolist() if v], key=smart_sort_key)


st.markdown(
    """
<div class="cc-header">
  <div class="cc-title">Candidate Connect DEV</div>
  <div class="cc-sub">Cloud-safe rebuild • Auto-loading filter layer only</div>
</div>
""",
    unsafe_allow_html=True,
)

st.success("App rendered. Now loading the safe filter layer...")

try:
    with st.spinner("Loading filter options from R2..."):
        manifest, filter_options, geo_hierarchy = load_filter_layer()
except Exception as e:
    st.error("Filter layer failed to load.")
    st.exception(e)
    st.stop()

with st.sidebar:
    st.markdown("## Candidate Connect")
    st.caption("DEV filter-layer test")

    if st.button("Clear Filters", use_container_width=True):
        for key in list(st.session_state.keys()):
            if key.startswith("filter_"):
                del st.session_state[key]
        st.rerun()

    st.divider()
    st.markdown("### Geography")
    for field in GEO_FIELDS:
        label = DISPLAY_LABELS.get(field, field)
        opts = options_from_geo(geo_hierarchy, field, active_filters())
        st.multiselect(label, options=opts, key=f"filter_{field}")

    st.divider()
    st.markdown("### Basic Voter Filters")
    for field in ["Party", "Gender", "Age_Range", "MIB_Applied", "MIB_BALLOT", "MB_PERM"]:
        label = field.replace("_", " ")
        opts = options_from_filter_table(filter_options, field)
        st.multiselect(label, options=opts, key=f"filter_{field}")

st.markdown("### Filter Layer Loaded")

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="cc-metric"><div class="label">Dataset Rows</div><div class="value">{int(manifest.get("total_rows", 0)):,}</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="cc-metric"><div class="label">Filter Options</div><div class="value">{len(filter_options):,}</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="cc-metric"><div class="label">Geo Rows</div><div class="value">{len(geo_hierarchy):,}</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="cc-metric"><div class="label">Built</div><div class="value" style="font-size:16px;">{manifest.get("built_at", "unknown")}</div></div>', unsafe_allow_html=True)

active = active_filters()
st.markdown("### Current Selections")
if active:
    st.json(active)
else:
    st.info("No geography filters selected yet.")

st.markdown("### Preview: Geography Option Counts")
preview_rows = []
for field in GEO_FIELDS:
    preview_rows.append({"Field": DISPLAY_LABELS.get(field, field), "Options Available": len(options_from_geo(geo_hierarchy, field, active))})
st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)

st.caption(f"Rendered at {datetime.now().isoformat(timespec='seconds')}")

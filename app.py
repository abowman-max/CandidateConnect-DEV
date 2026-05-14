# Candidate Connect DEV — Final Hybrid Cloud App
# Fast counts + exact verification + guarded export.
# Designed after R2/manifest/filter-layer diagnostics passed.

import io
import json
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

R2 = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"
DETAIL_SHARDS = 36
EXPORT_ROW_LIMIT = 250_000

st.set_page_config(page_title="Candidate Connect DEV", layout="wide")

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


def pct(n, d):
    return "0.0%" if not d else f"{(n / d) * 100:.1f}%"


def confidence_level(active: dict) -> tuple[str, str]:
    count = sum(1 for v in active.values() if v)
    voter_count = sum(1 for k, v in active.items() if k in VOTER_FIELDS and v)
    if count <= 2 and voter_count == 0:
        return "High confidence", "Quick counts are expected to match final counts for simple geography filters."
    if count <= 4 and voter_count <= 1:
        return "High confidence", "Quick counts are built from the current dataset and are suitable for exploration. Verify before final export."
    return "Verify recommended", "Many filters are combined. Use Verify Exact Counts before final targeting or export."


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
        st.markdown(f'<div class="cc-metric"><div class="label">Total {label}</div><div class="value">{summary["total"]:,}</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="cc-metric"><div class="label">Republican</div><div class="value">{summary["r"]:,}</div><div class="sub">{pct(summary["r"], summary["total"])}</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="cc-metric blue"><div class="label">Democrat</div><div class="value">{summary["d"]:,}</div><div class="sub">{pct(summary["d"], summary["total"])}</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="cc-metric green"><div class="label">Other / Unaffiliated</div><div class="value">{summary["o"]:,}</div><div class="sub">{pct(summary["o"], summary["total"])}</div></div>', unsafe_allow_html=True)


def quick_counts(active: dict):
    needed = set(["Party"])
    needed.update(active.keys())
    # Include possible count column names. Parquet will error if missing, so we try in stages.
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

    # Final fallback: full count cube read. If it fails, the app shows message but does not crash.
    try:
        cube = load_count_cube_columns(tuple())
        filtered = apply_filters(cube, active)
        return summarize_from_df(filtered, row_count_mode=False), None
    except Exception as e:
        return None, e


def exact_counts(active: dict):
    needed = set(["Party"])
    needed.update(active.keys())
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
                df = df[df[col].astype(str).isin([str(v) for v in vals])]
            elif vals:
                df = df.iloc[0:0]

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
    if not active:
        raise RuntimeError("Please select at least one filter before exporting.")

    needed = set(columns)
    needed.update(active.keys())
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
                df = df[df[col].astype(str).isin([str(v) for v in vals])]
            elif vals:
                df = df.iloc[0:0]

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


st.markdown(
    """
<div class="cc-header">
  <div class="cc-title">Candidate Connect DEV</div>
  <div class="cc-sub">Final hybrid cloud build • Quick counts + exact verification + guarded export</div>
</div>
""",
    unsafe_allow_html=True,
)

try:
    with st.spinner("Loading filters from R2..."):
        manifest, filter_options, geo_hierarchy = load_filter_layer()
except Exception as e:
    st.error("Could not load the filter layer.")
    st.exception(e)
    st.stop()

with st.sidebar:
    st.markdown("## Candidate Connect")
    st.caption("DEV final hybrid")

    if st.button("Clear Filters", use_container_width=True):
        for key in list(st.session_state.keys()):
            if key.startswith("filter_"):
                del st.session_state[key]
        st.session_state.pop("quick_summary", None)
        st.session_state.pop("exact_summary", None)
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
    st.info("No filters selected. Choose filters in the left pane.")

level, note = confidence_level(active)
if level == "High confidence":
    st.markdown(f'<div class="cc-note"><b>{level} quick counts.</b> {note}</div>', unsafe_allow_html=True)
else:
    st.markdown(f'<div class="cc-verify"><b>{level}.</b> {note}</div>', unsafe_allow_html=True)

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

col_q, col_v = st.columns(2)
with col_q:
    if st.button("Quick Counts", use_container_width=True):
        with st.spinner("Loading quick count table..."):
            summary, err = quick_counts(active)
        if err:
            st.warning("Quick counts are unavailable for this build. Use Verify Exact Counts.")
            st.caption(str(err)[:500])
        else:
            st.session_state["quick_summary"] = summary
            st.success("Quick counts loaded.")

with col_v:
    if st.button("Verify Exact Counts", use_container_width=True):
        with st.spinner("Scanning all detail shards for exact counts..."):
            summary = exact_counts(active)
        st.session_state["exact_summary"] = summary
        st.success("Exact counts verified.")

if st.session_state.get("quick_summary"):
    st.markdown("### Quick Counts")
    render_metrics(st.session_state["quick_summary"], label="Quick")

if st.session_state.get("exact_summary"):
    st.markdown("### Verified Exact Counts")
    render_metrics(st.session_state["exact_summary"], label="Exact")

st.markdown("## Output Center")
st.caption("Exports scan all detail shards, apply filters, and block overly broad exports.")

selected_cols = st.multiselect("Export columns", options=DEFAULT_EXPORT_COLUMNS, default=DEFAULT_EXPORT_COLUMNS)

if st.button("Build Export File", use_container_width=True):
    try:
        with st.spinner("Building filtered export from detail shards..."):
            df_export = build_export(active, selected_cols)
    except Exception as e:
        st.error("Could not build export.")
        st.exception(e)
        st.stop()

    st.success(f"Export built: {len(df_export):,} rows")
    st.dataframe(df_export.head(250), use_container_width=True)
    st.download_button(
        "Download CSV",
        data=df_export.to_csv(index=False).encode("utf-8"),
        file_name=f"candidate_connect_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

st.caption(f"Rendered at {datetime.now().isoformat(timespec='seconds')}")

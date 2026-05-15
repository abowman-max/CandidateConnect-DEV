# Candidate Connect DEV — Stable Speed-Table Build
# Purpose: get DEV operational against the rebuilt Step 8 manifest/speed tables.
# Election year/type/method filters are intentionally disabled for this DEV rescue build.

import io
import json
from datetime import datetime
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Candidate Connect DEV", layout="wide")

R2_BASE = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"
EXPORT_ROW_LIMIT = 250_000

MEASURE_COLS = ["Voters", "Emails", "Landlines", "Mobiles"]
GEO_FIELDS = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]
TARGETING_FIELDS = [
    "Party", "CalculatedParty", "HH-Party", "Gender", "Age_Range",
    "V4A", "V4G", "V4P",
    "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "MB_Prob_Score",
    "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone",
    "RegistrationMonthsAgo",
]
# Keep Tags out of speed-count filters unless Step 8 later adds it to count_cube.
PREFERRED_FILTER_FIELDS = GEO_FIELDS + TARGETING_FIELDS

DISPLAY_LABELS = {
    "USC": "Congressional District",
    "STS": "State Senate District",
    "STH": "State House District",
    "Age_Range": "Age Range",
    "CalculatedParty": "Calculated Party",
    "HH-Party": "Household Party",
    "V4A": "Vote History - All Elections",
    "V4G": "Vote History - General Elections",
    "V4P": "Vote History - Primary Elections",
    "MB_App": "Mail Ballot Application",
    "MB_App_Status": "Application Status",
    "MB_Sent": "Ballot Sent",
    "MB_Status": "Ballot Returned / Status",
    "MB_PERM": "Permanent Mail Ballot",
    "MB_Prob_Score": "Mail Ballot Probability Score",
    "HasMobile": "Mobile Phone",
    "HasLandline": "Landline",
    "HasEmail": "Email",
    "HasApplicantPhone": "Applicant Phone",
    "RegistrationMonthsAgo": "Registration Months Ago",
}

EXPORT_COLUMNS_PREFERRED = [
    "County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region",
    "voter_id", "FirstName", "MiddleName", "LastName", "first_name", "middle_name", "last_name",
    "Party", "party", "CalculatedParty", "Gender", "gender", "Age", "age", "Age_Range", "age_group", "RegistrationDate", "registration_date",
    "House Number", "Street Name", "Apartment Number", "City", "State", "Zip",
    "res_address", "res_city", "res_state", "res_zip",
    "Email", "Landline", "Mobile", "MobilePhone", "Phone", "Current_ApplicantPhone",
    "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MIB_Applied", "MIB_BALLOT", "MB_PERM", "MB_Prob_Score",
    "Current_App_Return_Date", "Current_Ballot_Sent_Date", "Current_Ballot_Returned_Date",
    "V4A", "V4G", "V4P", "Tags",
]

st.markdown(
    """
<style>
html, body, [data-testid="stAppViewContainer"], .stApp { background: #000000 !important; color: #f8fafc !important; }
[data-testid="stSidebar"] { background: #05080d !important; border-right: 1px solid rgba(201,31,39,.45); }
.block-container { padding-top: 1.1rem; max-width: 1550px; }
.cc-header { border: 1px solid rgba(201,31,39,.85); border-radius: 18px; padding: 18px 22px; background: radial-gradient(circle at 80% 0%, rgba(201,31,39,.23), transparent 35%), linear-gradient(90deg, #03070c, #0b111a 55%, #190407); box-shadow: 0 14px 35px rgba(0,0,0,.45); margin-bottom: 16px; }
.cc-title { font-size: 30px; font-weight: 950; color: #fff; }
.cc-sub { color: #cbd5e1; margin-top: 4px; font-size: 13px; }
.cc-card { border: 1px solid rgba(148,163,184,.24); border-radius: 16px; background: linear-gradient(180deg, #07101a, #03070c); padding: 16px; margin-bottom: 16px; }
.cc-metric { border: 1px solid rgba(148,163,184,.22); border-left: 4px solid #c91f27; border-radius: 14px; background: linear-gradient(180deg, #0d1724, #07101a); padding: 16px; min-height: 96px; }
.cc-metric.blue { border-left-color:#1d4ed8; } .cc-metric.green { border-left-color:#4c9a2a; } .cc-metric.gold { border-left-color:#f2b84b; }
.cc-metric .label { color: #94a3b8; font-size: 11px; font-weight: 900; letter-spacing: .05em; text-transform: uppercase; }
.cc-metric .value { color: #fff; font-size: 28px; font-weight: 950; margin-top: 8px; }
.cc-metric .sub { color: #cbd5e1; font-size: 11px; margin-top: 4px; }
.stButton > button, div[data-testid="stDownloadButton"] > button { border-radius: 10px !important; font-weight: 850 !important; background: linear-gradient(180deg, #9f151c, #6e0f14) !important; color: white !important; border: 1px solid rgba(242,184,75,.45) !important; }
[data-baseweb="select"] > div, [data-baseweb="input"] > div, textarea, input { background-color: #0f172a !important; color: #f8fafc !important; border-color: #334155 !important; }
[data-baseweb="tag"] { background: rgba(201,31,39,.30) !important; color: white !important; padding-left: 14px !important; margin-left: 6px !important; }
.stAlert { background: rgba(15,23,42,.95) !important; color: #f8fafc !important; }
[data-testid="stSidebar"] details > summary, [data-testid="stSidebar"] details > summary * { color:#f8fafc !important; font-weight:900 !important; }
</style>
""",
    unsafe_allow_html=True,
)


def r2_url(key: str) -> str:
    return f"{R2_BASE}/{str(key).lstrip('/')}"


@st.cache_data(ttl=600, show_spinner=False)
def fetch_bytes(key: str) -> bytes:
    resp = requests.get(r2_url(key), timeout=120)
    resp.raise_for_status()
    return resp.content


@st.cache_data(ttl=600, show_spinner=False)
def fetch_manifest() -> dict:
    return json.loads(fetch_bytes("dataset_manifest.json").decode("utf-8"))


@st.cache_data(ttl=600, show_spinner=False)
def fetch_parquet(key: str, columns_tuple: Tuple[str, ...] | None = None) -> pd.DataFrame:
    columns = list(columns_tuple) if columns_tuple else None
    return pd.read_parquet(io.BytesIO(fetch_bytes(key)), columns=columns)


@st.cache_data(ttl=600, show_spinner=False)
def load_speed_metadata() -> tuple[dict, pd.DataFrame, pd.DataFrame, list[str]]:
    manifest = fetch_manifest()
    speed = (manifest.get("speed") or {}).get("tables") or {}
    filter_options = fetch_parquet(speed.get("filter_options", "speed/filter_options.parquet"))
    geo_hierarchy = fetch_parquet(speed.get("geo_hierarchy", "speed/geo_hierarchy.parquet"))
    # Read only one row to get columns is not reliable over remote bytes, so read no columns where supported.
    count_cube = fetch_parquet(speed.get("count_cube", "speed/count_cube.parquet"))
    count_columns = list(count_cube.columns)
    return manifest, filter_options, geo_hierarchy, count_columns


@st.cache_data(ttl=300, show_spinner=False)
def load_count_cube_columns(cols_tuple: Tuple[str, ...]) -> pd.DataFrame:
    manifest = fetch_manifest()
    speed = (manifest.get("speed") or {}).get("tables") or {}
    key = speed.get("count_cube", "speed/count_cube.parquet")
    return fetch_parquet(key, tuple(cols_tuple))


def clean_value(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if s.lower() in {"", "nan", "none", "null", "(blank)"}:
        return ""
    return s


def smart_sort_key(v):
    s = str(v).strip()
    try:
        return (0, int(float(s)))
    except Exception:
        return (1, s.upper())


def normalize_yes_no_options(options: list[str]) -> list[str]:
    # Preserve the speed table's actual stored values; this only sorts common booleans nicely.
    order = {"Y": 0, "YES": 0, "1": 0, "TRUE": 0, "N": 1, "NO": 1, "0": 1, "FALSE": 1}
    return sorted(options, key=lambda x: (order.get(str(x).upper(), 9), smart_sort_key(x)))


def field_label(field: str) -> str:
    return DISPLAY_LABELS.get(field, field)


def get_filter_key(field: str) -> str:
    return f"filter_{field}"


def selected(field: str) -> list:
    return st.session_state.get(get_filter_key(field), [])


def active_filters(enabled_fields: list[str]) -> Dict[str, list]:
    out = {}
    for field in enabled_fields:
        vals = selected(field)
        if vals:
            out[field] = vals
    return out



def expand_filter_values(field: str, vals: list) -> list:
    """DEV compatibility shim.

    Older app builds called this helper before applying filters.
    The rebuilt speed tables already store canonical filter values, so
    DEV should filter directly against the selected values.
    """
    if vals is None:
        return []
    if isinstance(vals, (str, int, float, bool)):
        return [vals]
    return list(vals)


def apply_filters(df: pd.DataFrame, active: Dict[str, list]) -> pd.DataFrame:
    out = df
    for field, vals in active.items():
        if not vals:
            continue
        if field not in out.columns:
            return out.iloc[0:0]
        val_set = {str(v) for v in vals}
        out = out[out[field].astype(str).isin(val_set)]
    return out


def options_from_filter_table(filter_options: pd.DataFrame, field: str) -> list[str]:
    if filter_options.empty or "field" not in filter_options.columns or "value" not in filter_options.columns:
        return []
    sub = filter_options[filter_options["field"].astype(str).eq(field)].copy()
    if sub.empty:
        return []
    if "sort_order" in sub.columns:
        sub = sub.sort_values("sort_order")
    vals = [clean_value(v) for v in sub["value"].tolist()]
    vals = [v for v in vals if v]
    if field.startswith("Has") or field in {"MB_PERM", "MB_App", "MB_Sent"}:
        return normalize_yes_no_options(list(dict.fromkeys(vals)))
    return sorted(list(dict.fromkeys(vals)), key=smart_sort_key)


def options_from_geo(geo_hierarchy: pd.DataFrame, field: str, active: Dict[str, list]) -> list[str]:
    if field not in geo_hierarchy.columns:
        return []
    relevant = {}
    for f in GEO_FIELDS:
        if f == field:
            break
        if active.get(f):
            relevant[f] = active[f]
    narrowed = apply_filters(geo_hierarchy, relevant)
    vals = narrowed[field].astype(str).map(clean_value)
    return sorted([v for v in vals.unique().tolist() if v], key=smart_sort_key)


def summarize_from_cube(active: Dict[str, list], count_columns: list[str]) -> tuple[dict, pd.DataFrame]:
    needed = set(active.keys()) | {"Party"} | {c for c in MEASURE_COLS if c in count_columns}
    needed = [c for c in needed if c in count_columns]
    if "Voters" not in needed and "Voters" in count_columns:
        needed.append("Voters")
    if not needed:
        return {"total": 0, "r": 0, "d": 0, "o": 0, "emails": 0, "mobiles": 0, "landlines": 0}, pd.DataFrame()
    cube = load_count_cube_columns(tuple(sorted(needed)))
    filtered = apply_filters(cube, active)
    count_col = "Voters" if "Voters" in filtered.columns else None
    if not count_col:
        total = len(filtered)
        party_weights = pd.Series([1] * len(filtered), index=filtered.index)
    else:
        total = int(pd.to_numeric(filtered[count_col], errors="coerce").fillna(0).sum())
        party_weights = pd.to_numeric(filtered[count_col], errors="coerce").fillna(0)
    party = filtered["Party"].astype(str).str.upper() if "Party" in filtered.columns else pd.Series([], dtype=str)
    r = int(party_weights[party.eq("R")].sum()) if len(party) else 0
    d = int(party_weights[party.eq("D")].sum()) if len(party) else 0
    o = int(total - r - d)
    summary = {
        "total": total,
        "r": r,
        "d": d,
        "o": o,
        "emails": int(pd.to_numeric(filtered.get("Emails", 0), errors="coerce").fillna(0).sum()) if "Emails" in filtered.columns else 0,
        "mobiles": int(pd.to_numeric(filtered.get("Mobiles", 0), errors="coerce").fillna(0).sum()) if "Mobiles" in filtered.columns else 0,
        "landlines": int(pd.to_numeric(filtered.get("Landlines", 0), errors="coerce").fillna(0).sum()) if "Landlines" in filtered.columns else 0,
    }
    return summary, filtered


def pct(n: int, d: int) -> str:
    return "0.0%" if not d else f"{n / d * 100:.1f}%"


def metric_card(label: str, value: int, sub: str = "", klass: str = ""):
    st.markdown(
        f'<div class="cc-metric {klass}"><div class="label">{label}</div><div class="value">{int(value):,}</div><div class="sub">{sub}</div></div>',
        unsafe_allow_html=True,
    )


def render_metrics(summary: dict):
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        metric_card("Total Voters", summary["total"], "Selected universe", "gold")
    with c2:
        metric_card("Republican", summary["r"], pct(summary["r"], summary["total"]))
    with c3:
        metric_card("Democrat", summary["d"], pct(summary["d"], summary["total"]), "blue")
    with c4:
        metric_card("Other", summary["o"], pct(summary["o"], summary["total"]), "green")


def render_party_chart(summary: dict):
    chart_df = pd.DataFrame([
        {"Party Group": "Republican", "Voters": summary["r"]},
        {"Party Group": "Democrat", "Voters": summary["d"]},
        {"Party Group": "Other / Unaffiliated", "Voters": summary["o"]},
    ])
    st.bar_chart(chart_df.set_index("Party Group"), height=280)


def group_rollup(filtered_cube: pd.DataFrame, field: str) -> pd.DataFrame:
    if filtered_cube.empty or field not in filtered_cube.columns or "Voters" not in filtered_cube.columns:
        return pd.DataFrame(columns=[field, "Voters"])
    out = filtered_cube.groupby(field, dropna=False, as_index=False)["Voters"].sum()
    out[field] = out[field].astype(str).map(clean_value)
    out = out[out[field].ne("")].sort_values("Voters", ascending=False)
    return out.head(250)


def detail_shards_from_manifest(manifest: dict) -> list[str]:
    shards = ((manifest.get("detail") or {}).get("shards") or [])
    return [s.get("key") for s in shards if s.get("key")]


def available_detail_columns(manifest: dict) -> list[str]:
    return list((manifest.get("detail") or {}).get("columns") or [])


def map_filter_to_detail_column(field: str, detail_cols: Iterable[str]) -> str | None:
    detail_cols = list(detail_cols)
    if field in detail_cols:
        return field
    aliases = {
        "Party": ["Party", "party", "Party_Group"],
        "Gender": ["Gender", "gender"],
        "Age_Range": ["Age_Range", "age_group"],
        "RegistrationMonthsAgo": ["RegistrationMonthsAgo"],
        "HasMobile": ["HasMobile"],
        "HasLandline": ["HasLandline"],
        "HasEmail": ["HasEmail"],
        "HasApplicantPhone": ["HasApplicantPhone"],
    }
    for cand in aliases.get(field, []):
        if cand in detail_cols:
            return cand
    return None


@st.cache_data(ttl=600, show_spinner=False)
def load_detail_shard(key: str, cols_tuple: Tuple[str, ...]) -> pd.DataFrame:
    return fetch_parquet(key, cols_tuple)


def export_detail_csv(manifest: dict, active: Dict[str, list], max_rows: int) -> bytes:
    detail_cols = available_detail_columns(manifest)
    wanted = [c for c in EXPORT_COLUMNS_PREFERRED if c in detail_cols]
    filter_col_map = {f: map_filter_to_detail_column(f, detail_cols) for f in active.keys()}
    filter_cols = [c for c in filter_col_map.values() if c]
    cols = list(dict.fromkeys(wanted + filter_cols))
    if not cols:
        raise RuntimeError("No compatible detail columns are available for export.")
    parts = []
    total = 0
    for key in detail_shards_from_manifest(manifest):
        df = load_detail_shard(key, tuple(cols))
        for field, vals in active.items():
            col = filter_col_map.get(field)
            if col and col in df.columns:
                df = df[df[col].astype(str).isin({str(v) for v in vals})]
            elif vals:
                df = df.iloc[0:0]
        if not df.empty:
            keep = [c for c in wanted if c in df.columns]
            parts.append(df[keep])
            total += len(df)
            if total >= max_rows:
                break
    if not parts:
        return pd.DataFrame(columns=wanted).to_csv(index=False).encode("utf-8")
    out = pd.concat(parts, ignore_index=True).head(max_rows)
    return out.to_csv(index=False).encode("utf-8")


try:
    with st.spinner("Loading Candidate Connect DEV speed tables..."):
        manifest, filter_options, geo_hierarchy, count_columns = load_speed_metadata()
except Exception as e:
    st.error("Candidate Connect could not load the rebuilt DEV speed tables from R2.")
    st.exception(e)
    st.stop()

enabled_fields = [f for f in PREFERRED_FILTER_FIELDS if f in count_columns]
disabled_expected = [f for f in PREFERRED_FILTER_FIELDS if f not in count_columns]

st.markdown(
    f"""
<div class="cc-header">
  <div class="cc-title">Candidate Connect DEV</div>
  <div class="cc-sub">
    Stable speed-table build • Dataset rows: {int(manifest.get('total_rows', 0)):,}
    • Built: {manifest.get('built_at', 'unknown')}
    • Detail shards: {(manifest.get('detail') or {}).get('count', 0)}
  </div>
</div>
""",
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("## Candidate Connect")
    st.caption("DEV stable rescue build")
    if st.button("Clear Filters", use_container_width=True):
        for k in list(st.session_state.keys()):
            if k.startswith("filter_"):
                del st.session_state[k]
        st.session_state["filters_applied"] = False
        st.rerun()

    current_active = active_filters(enabled_fields)

    with st.expander("Geography", expanded=False):
        for field in [f for f in GEO_FIELDS if f in enabled_fields]:
            opts = options_from_geo(geo_hierarchy, field, current_active) if field in geo_hierarchy.columns else options_from_filter_table(filter_options, field)
            st.multiselect(field_label(field), options=opts, key=get_filter_key(field))

    with st.expander("Voter", expanded=False):
        for field in ["Party", "CalculatedParty", "HH-Party", "Gender", "Age_Range", "RegistrationMonthsAgo"]:
            if field in enabled_fields:
                st.multiselect(field_label(field), options=options_from_filter_table(filter_options, field), key=get_filter_key(field))

    with st.expander("Vote History Scores", expanded=False):
        for field in ["V4A", "V4G", "V4P"]:
            if field in enabled_fields:
                st.multiselect(field_label(field), options=options_from_filter_table(filter_options, field), key=get_filter_key(field))
        st.caption("Election year/type/method filters are disabled in this DEV rescue build.")

    with st.expander("Mail Ballot / Contact", expanded=False):
        for field in ["MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "MB_Prob_Score", "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone"]:
            if field in enabled_fields:
                st.multiselect(field_label(field), options=options_from_filter_table(filter_options, field), key=get_filter_key(field))

    st.divider()
    if st.button("Apply / Update Counts", use_container_width=True):
        st.session_state["filters_applied"] = True
        st.rerun()

active = active_filters(enabled_fields)

try:
    summary, filtered_cube = summarize_from_cube(active, count_columns)
except Exception as e:
    st.error("Counts failed against speed/count_cube.parquet.")
    st.exception(e)
    st.stop()

left, right = st.columns([2, 1])
with left:
    st.markdown("### Current Universe")
    if not active:
        st.info("No filters selected. Showing statewide speed-table counts. Open a sidebar section, select filters, then click Apply / Update Counts.")
    else:
        st.success(f"{len(active)} active filter group(s): " + ", ".join(active.keys()))
with right:
    st.caption("Speed-table status")
    st.write(f"Enabled count filters: **{len(enabled_fields)}**")
    st.write(f"Count cube columns: **{len(count_columns)}**")

render_metrics(summary)

st.markdown("### Analysis")
chart_col, table_col = st.columns([1, 1])
with chart_col:
    st.markdown("#### Party Breakdown")
    render_party_chart(summary)
with table_col:
    st.markdown("#### Contact Counts")
    contact_df = pd.DataFrame([
        {"Metric": "Emails", "Count": summary["emails"], "Share": pct(summary["emails"], summary["total"])},
        {"Metric": "Mobiles", "Count": summary["mobiles"], "Share": pct(summary["mobiles"], summary["total"])},
        {"Metric": "Landlines", "Count": summary["landlines"], "Share": pct(summary["landlines"], summary["total"])},
    ])
    st.dataframe(contact_df, use_container_width=True, hide_index=True)

st.markdown("### Rollups")
rollup_choices = [f for f in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region", "Party", "Gender", "Age_Range", "MB_App_Status", "MB_Status"] if f in filtered_cube.columns]
if rollup_choices:
    roll_field = st.selectbox("Roll up selected universe by", options=rollup_choices, index=0)
    st.dataframe(group_rollup(filtered_cube, roll_field), use_container_width=True, hide_index=True)
else:
    st.warning("No rollup columns are available from the selected speed cube columns.")

st.markdown("### Export")
st.caption("Export scans detail shards only when you click the download button. Keep exports filtered and under the safety limit.")
if summary["total"] > EXPORT_ROW_LIMIT:
    st.warning(f"This universe is {summary['total']:,} voters. Add more filters before exporting; current export safety limit is {EXPORT_ROW_LIMIT:,} rows.")
else:
    try:
        csv_bytes = export_detail_csv(manifest, active, EXPORT_ROW_LIMIT)
        st.download_button(
            "Download selected voters CSV",
            data=csv_bytes,
            file_name=f"candidate_connect_dev_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    except Exception as e:
        st.warning("Export is not available for this filter combination yet, but counts and analysis are operational.")
        st.caption(str(e)[:500])

with st.expander("DEV diagnostics", expanded=False):
    st.json({
        "manifest_built_at": manifest.get("built_at"),
        "total_rows": manifest.get("total_rows"),
        "index_shards": (manifest.get("index") or {}).get("count"),
        "detail_shards": (manifest.get("detail") or {}).get("count"),
        "speed_tables": (manifest.get("speed") or {}).get("tables"),
        "enabled_filters": enabled_fields,
        "disabled_missing_from_count_cube": disabled_expected,
        "count_cube_columns": count_columns,
    })

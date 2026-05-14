import io
import json
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Candidate Connect DEV", layout="wide")

APP_ENV = "DEV"
R2_BASE = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"
EXPORT_ROW_LIMIT = 1_000_000
DETAIL_SHARD_BATCH_LIMIT = 36

GEO_FIELDS = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]
FILTER_FIELDS = GEO_FIELDS + [
    "Party", "Gender", "Age_Range", "V4A", "V4G", "V4P",
    "MIB_Applied", "MIB_BALLOT", "MB_PERM", "MB_Prob_Score",
    "BallotSentStatus", "BallotReturnedStatus",
    "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone",
    "Tags",
]

DISPLAY_LABELS = {
    "USC": "Congressional District",
    "STS": "State Senate District",
    "STH": "State House District",
    "Age_Range": "Age Range",
    "V4A": "Vote History - All",
    "V4G": "Vote History - General",
    "V4P": "Vote History - Primary",
    "MIB_Applied": "Mail Ballot Application Status",
    "MIB_BALLOT": "Mail Ballot Status",
    "MB_PERM": "Permanent Mail Ballot",
    "MB_Prob_Score": "Mail Ballot Probability",
    "BallotSentStatus": "Ballot Sent",
    "BallotReturnedStatus": "Ballot Returned",
    "HasMobile": "Mobile Phone",
    "HasLandline": "Landline",
    "HasEmail": "Email",
    "HasApplicantPhone": "Applicant Phone",
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
html, body, [data-testid="stAppViewContainer"], .stApp { background: #000000 !important; color: #f8fafc !important; }
[data-testid="stSidebar"] { background: #05080d !important; border-right: 1px solid rgba(201,31,39,.45); }
.block-container { padding-top: 1.1rem; max-width: 1650px; }
.cc-header { border: 1px solid rgba(201,31,39,.85); border-radius: 18px; padding: 18px 22px; background: radial-gradient(circle at 80% 0%, rgba(201,31,39,.23), transparent 35%), linear-gradient(90deg, #03070c, #0b111a 55%, #190407); box-shadow: 0 14px 35px rgba(0,0,0,.45); margin-bottom: 16px; }
.cc-title { font-size: 30px; font-weight: 950; color: #fff; margin: 0; }
.cc-sub { color: #cbd5e1; margin-top: 4px; font-size: 13px; }
.cc-card { border: 1px solid rgba(148,163,184,.24); border-radius: 16px; background: linear-gradient(180deg, #07101a, #03070c); padding: 16px; box-shadow: 0 10px 24px rgba(0,0,0,.28); }
.cc-metric { border: 1px solid rgba(148,163,184,.22); border-left: 4px solid #c91f27; border-radius: 14px; background: linear-gradient(180deg, #0d1724, #07101a); padding: 16px; min-height: 105px; }
.cc-metric .label { color: #94a3b8; font-size: 11px; font-weight: 900; letter-spacing: .05em; text-transform: uppercase; }
.cc-metric .value { color: #fff; font-size: 30px; font-weight: 950; margin-top: 8px; }
.stButton > button, div[data-testid="stDownloadButton"] > button { border-radius: 10px !important; font-weight: 850 !important; background: linear-gradient(180deg, #9f151c, #6e0f14) !important; color: white !important; border: 1px solid rgba(242,184,75,.45) !important; }
.stButton > button:hover, div[data-testid="stDownloadButton"] > button:hover { background: linear-gradient(180deg, #c91f27, #8f1118) !important; border-color: #f2b84b !important; }
[data-baseweb="select"] > div, [data-baseweb="input"] > div, textarea, input { background-color: #0f172a !important; color: #f8fafc !important; border-color: #334155 !important; }
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
def load_manifest() -> dict:
    return json.loads(fetch_text("dataset_manifest.json"))

@st.cache_data(ttl=600, show_spinner=False)
def load_parquet(key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(fetch_bytes(key)))

@st.cache_data(ttl=600, show_spinner=False)
def load_speed_tables():
    manifest = load_manifest()
    speed_tables = manifest.get("speed", {}).get("tables", {})
    filter_options = load_parquet(speed_tables.get("filter_options", "speed/filter_options.parquet"))
    geo_hierarchy = load_parquet(speed_tables.get("geo_hierarchy", "speed/geo_hierarchy.parquet"))
    count_cube = load_parquet(speed_tables.get("count_cube", "speed/count_cube.parquet"))
    mail_counts = load_parquet(speed_tables.get("mail_ballot_counts", "speed/mail_ballot_counts.parquet"))
    try:
        ranges = json.loads(fetch_text(speed_tables.get("filter_ranges", "speed/filter_ranges.json")))
    except Exception:
        ranges = {}
    return manifest, filter_options, geo_hierarchy, count_cube, mail_counts, ranges

def clean_value(value):
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

def active_filters_from_state() -> dict:
    active = {}
    for field in FILTER_FIELDS:
        vals = selected(field)
        if vals:
            active[field] = vals
    return active

def apply_filters_to_df(df: pd.DataFrame, active: dict) -> pd.DataFrame:
    out = df
    for field, vals in active.items():
        if vals and field in out.columns:
            out = out[out[field].astype(str).isin([str(v) for v in vals])]
    return out

def field_options_from_filter_table(filter_options: pd.DataFrame, field: str) -> list:
    if "field" not in filter_options.columns or "value" not in filter_options.columns:
        return []
    vals = filter_options.loc[filter_options["field"].astype(str).eq(field), "value"].astype(str).map(clean_value)
    return sorted([v for v in vals.unique().tolist() if v], key=smart_sort_key)

def dependent_options(geo_df: pd.DataFrame, field: str, active: dict) -> list:
    if field not in geo_df.columns:
        return []
    hierarchy_order = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]
    relevant = {}
    for f in hierarchy_order:
        if f == field:
            break
        if active.get(f):
            relevant[f] = active[f]
    df = apply_filters_to_df(geo_df, relevant)
    vals = df[field].astype(str).map(clean_value)
    return sorted([v for v in vals.unique().tolist() if v], key=smart_sort_key)

def summarize_from_count_cube(count_cube: pd.DataFrame, active: dict) -> dict:
    df = apply_filters_to_df(count_cube, active)
    if df.empty:
        return {
            "voters": 0, "emails": 0, "mobiles": 0, "landlines": 0,
            "party": pd.DataFrame(columns=["Party", "Voters"]),
            "gender": pd.DataFrame(columns=["Gender", "Voters"]),
            "mail": pd.DataFrame(columns=["MIB_Applied", "Voters"]),
        }
    voters = int(df["Voters"].sum()) if "Voters" in df.columns else 0
    emails = int(df["Emails"].sum()) if "Emails" in df.columns else 0
    mobiles = int(df["Mobiles"].sum()) if "Mobiles" in df.columns else 0
    landlines = int(df["Landlines"].sum()) if "Landlines" in df.columns else 0

    def group(field):
        if field not in df.columns:
            return pd.DataFrame(columns=[field, "Voters"])
        g = df.groupby(field, dropna=False, as_index=False)["Voters"].sum()
        g[field] = g[field].astype(str).replace({"(Blank)": ""})
        return g.sort_values("Voters", ascending=False)

    return {"voters": voters, "emails": emails, "mobiles": mobiles, "landlines": landlines, "party": group("Party"), "gender": group("Gender"), "mail": group("MIB_Applied")}

def pct(n, d):
    return "0.0%" if not d else f"{(n / d) * 100:.1f}%"

def get_detail_shard_keys(manifest: dict) -> list:
    detail = manifest.get("detail", {})
    shards = detail.get("shards", [])
    keys = []
    for item in shards:
        if isinstance(item, dict) and item.get("key"):
            keys.append(item["key"])
        elif isinstance(item, str):
            keys.append(item)
    if not keys:
        keys = [f"detail/voters_detail_{i:03d}.parquet" for i in range(36)]
    return keys[:DETAIL_SHARD_BATCH_LIMIT]

def row_matches_detail(df: pd.DataFrame, active: dict) -> pd.DataFrame:
    out = df
    for field, vals in active.items():
        if vals and field in out.columns:
            out = out[out[field].astype(str).isin([str(v) for v in vals])]
    return out

@st.cache_data(ttl=120, show_spinner=False)
def load_filtered_detail_for_export(active_json: str, selected_columns: list) -> pd.DataFrame:
    active = json.loads(active_json)
    manifest = load_manifest()
    keys = get_detail_shard_keys(manifest)
    parts = []
    total = 0
    for key in keys:
        df = load_parquet(key)
        df = row_matches_detail(df, active)
        if df.empty:
            continue
        use_cols = [c for c in selected_columns if c in df.columns]
        if use_cols:
            df = df[use_cols].copy()
        parts.append(df)
        total += len(df)
        if total > EXPORT_ROW_LIMIT:
            raise RuntimeError(f"Export exceeds safety limit of {EXPORT_ROW_LIMIT:,} rows. Add more filters before downloading.")
    if not parts:
        return pd.DataFrame(columns=selected_columns)
    return pd.concat(parts, ignore_index=True)

try:
    with st.spinner("Loading Candidate Connect DEV speed tables..."):
        manifest, filter_options, geo_hierarchy, count_cube, mail_counts, ranges = load_speed_tables()
except Exception as e:
    st.error("Candidate Connect could not load the DEV speed tables from R2.")
    st.exception(e)
    st.stop()

st.markdown(
    f"""
<div class="cc-header">
  <div class="cc-title">Candidate Connect DEV</div>
  <div class="cc-sub">
    Cloud-safe core build • Speed-table filters/counts • Detail shards load only for exports
    • Dataset rows: {int(manifest.get("total_rows", 0)):,}
    • Built: {manifest.get("built_at", "unknown")}
  </div>
</div>
""",
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("## Candidate Connect")
    st.caption("DEV cloud-safe core")

    if st.button("Clear Filters", use_container_width=True):
        for key in list(st.session_state.keys()):
            if key.startswith("filter_"):
                del st.session_state[key]
        st.session_state["filters_applied"] = False
        st.rerun()

    st.divider()
    st.markdown("### Geography")
    for field in GEO_FIELDS:
        label = DISPLAY_LABELS.get(field, field)
        if field in ["County", "Municipality", "Precinct", "School District", "School Region"]:
            options = dependent_options(geo_hierarchy, field, active_filters_from_state())
        else:
            options = field_options_from_filter_table(filter_options, field)
        st.multiselect(label, options=options, key=f"filter_{field}")

    st.divider()
    st.markdown("### Voter")
    for field in ["Party", "Gender", "Age_Range", "V4A", "V4G", "V4P"]:
        st.multiselect(DISPLAY_LABELS.get(field, field), options=field_options_from_filter_table(filter_options, field), key=f"filter_{field}")

    st.divider()
    st.markdown("### Mail Ballot / Contact")
    for field in ["MIB_Applied", "MIB_BALLOT", "MB_PERM", "MB_Prob_Score", "BallotSentStatus", "BallotReturnedStatus", "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone"]:
        st.multiselect(DISPLAY_LABELS.get(field, field), options=field_options_from_filter_table(filter_options, field), key=f"filter_{field}")

    tag_options = field_options_from_filter_table(filter_options, "Tags")
    if tag_options:
        st.divider()
        st.markdown("### Tags")
        st.multiselect("Tags", options=tag_options, key="filter_Tags")

    st.divider()
    if st.button("Apply Filters", use_container_width=True):
        st.session_state["filters_applied"] = True
        st.rerun()

active = active_filters_from_state()
filters_applied = st.session_state.get("filters_applied", False)

if not filters_applied:
    st.markdown("### Select filters on the left, then click **Apply Filters**.")
    st.info("This DEV rescue build is intentionally lean: fast filters, counts, and export. Advanced modules can be added back after the cloud app is stable.")
    statewide_summary = summarize_from_count_cube(count_cube, {})
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f'<div class="cc-metric"><div class="label">Statewide Voters</div><div class="value">{statewide_summary["voters"]:,}</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="cc-metric"><div class="label">Emails</div><div class="value">{statewide_summary["emails"]:,}</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="cc-metric"><div class="label">Mobiles</div><div class="value">{statewide_summary["mobiles"]:,}</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="cc-metric"><div class="label">Landlines</div><div class="value">{statewide_summary["landlines"]:,}</div></div>', unsafe_allow_html=True)
    st.stop()

summary = summarize_from_count_cube(count_cube, active)

st.markdown("### Active Universe")
if active:
    chips = []
    for k, vals in active.items():
        label = DISPLAY_LABELS.get(k, k)
        chips.append(f"**{label}:** {', '.join(map(str, vals[:5]))}{'…' if len(vals) > 5 else ''}")
    st.markdown(" &nbsp; | &nbsp; ".join(chips), unsafe_allow_html=True)
else:
    st.warning("No filters selected. This is statewide mode. Counts are shown, but exports should be filtered before downloading.")

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="cc-metric"><div class="label">Voters</div><div class="value">{summary["voters"]:,}</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="cc-metric"><div class="label">Emails</div><div class="value">{summary["emails"]:,}</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="cc-metric"><div class="label">Mobiles</div><div class="value">{summary["mobiles"]:,}</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="cc-metric"><div class="label">Landlines</div><div class="value">{summary["landlines"]:,}</div></div>', unsafe_allow_html=True)

left, mid, right = st.columns(3)
with left:
    st.markdown('<div class="cc-card">', unsafe_allow_html=True)
    st.markdown("#### Party")
    party = summary["party"].copy()
    if not party.empty:
        party["Share"] = party["Voters"].map(lambda x: pct(int(x), summary["voters"]))
        st.dataframe(party, use_container_width=True, hide_index=True)
    else:
        st.caption("No party rows.")
    st.markdown("</div>", unsafe_allow_html=True)

with mid:
    st.markdown('<div class="cc-card">', unsafe_allow_html=True)
    st.markdown("#### Gender")
    gender = summary["gender"].copy()
    if not gender.empty:
        gender["Share"] = gender["Voters"].map(lambda x: pct(int(x), summary["voters"]))
        st.dataframe(gender, use_container_width=True, hide_index=True)
    else:
        st.caption("No gender rows.")
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown('<div class="cc-card">', unsafe_allow_html=True)
    st.markdown("#### Mail Ballot Application")
    mail = summary["mail"].copy()
    if not mail.empty:
        mail["Share"] = mail["Voters"].map(lambda x: pct(int(x), summary["voters"]))
        st.dataframe(mail, use_container_width=True, hide_index=True)
    else:
        st.caption("No mail ballot rows.")
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("## Output Center")
st.caption("Exports load detail shards only after you click the export button. Add filters before exporting large districts.")

if summary["voters"] > EXPORT_ROW_LIMIT:
    st.warning(f"This universe has {summary['voters']:,} voters. Exports are limited to {EXPORT_ROW_LIMIT:,} rows in this cloud-safe build. Add more filters before downloading.")

selected_cols = st.multiselect("Export columns", options=DEFAULT_EXPORT_COLUMNS, default=DEFAULT_EXPORT_COLUMNS)
build_export = st.button("Build Export File", use_container_width=True)

if build_export:
    if not active:
        st.error("Please apply at least one filter before exporting. Statewide exports are intentionally blocked.")
        st.stop()
    if summary["voters"] > EXPORT_ROW_LIMIT:
        st.error(f"Please narrow the universe below {EXPORT_ROW_LIMIT:,} voters before exporting.")
        st.stop()

    active_json = json.dumps(active, sort_keys=True)
    with st.spinner("Loading filtered voters from detail shards..."):
        try:
            export_df = load_filtered_detail_for_export(active_json, selected_cols)
        except Exception as e:
            st.error("Could not build export.")
            st.exception(e)
            st.stop()

    st.success(f"Export built: {len(export_df):,} rows")
    st.dataframe(export_df.head(250), use_container_width=True)

    csv_bytes = export_df.to_csv(index=False).encode("utf-8")
    filename = f"candidate_connect_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    st.download_button("Download CSV", data=csv_bytes, file_name=filename, mime="text/csv", use_container_width=True)

st.markdown("---")
st.caption("Candidate Connect DEV cloud-safe core build. Advanced legacy sections are intentionally disabled for today’s deployment rescue.")

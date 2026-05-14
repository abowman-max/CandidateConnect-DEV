import json
from datetime import datetime

import requests
import streamlit as st

st.set_page_config(page_title="Candidate Connect DEV", layout="wide")

R2_BASE = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"

st.markdown(
    """
<style>
html, body, [data-testid="stAppViewContainer"], .stApp {
    background: #000000 !important;
    color: #f8fafc !important;
}
[data-testid="stSidebar"] {
    background: #05080d !important;
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
}
.stButton > button {
    border-radius: 10px !important;
    font-weight: 850 !important;
    background: linear-gradient(180deg, #9f151c, #6e0f14) !important;
    color: white !important;
    border: 1px solid rgba(242,184,75,.45) !important;
}
</style>
""",
    unsafe_allow_html=True,
)


def r2_url(key: str) -> str:
    return f"{R2_BASE}/{key.lstrip('/')}"


@st.cache_data(ttl=600, show_spinner=False)
def fetch_manifest():
    url = r2_url("dataset_manifest.json")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return url, resp.json()


st.markdown(
    """
<div class="cc-header">
  <div class="cc-title">Candidate Connect DEV</div>
  <div class="cc-sub">Minimal cloud-safe startup test • manifest only</div>
</div>
""",
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("## Candidate Connect")
    st.caption("DEV minimal startup")
    st.write("This version only loads the manifest first.")

st.success("App rendered before loading heavy data.")

try:
    url, manifest = fetch_manifest()
    st.markdown("### Manifest")
    st.write("URL:", url)
    st.json({
        "built_at": manifest.get("built_at"),
        "total_rows": manifest.get("total_rows"),
        "index_count": len(manifest.get("index", {}).get("shards", [])),
        "detail_count": len(manifest.get("detail", {}).get("shards", [])),
        "speed_tables": manifest.get("speed", {}).get("tables", {}),
    })
except Exception as e:
    st.error("Manifest load failed.")
    st.exception(e)
    st.stop()

st.markdown("### Next")
st.info("If this page stays connected, we can safely add filter_options next, then count_cube, then exports.")

if st.button("Test lightweight speed files"):
    checks = [
        "speed/speed_manifest.json",
        "speed/filter_ranges.json",
    ]
    for key in checks:
        try:
            r = requests.get(r2_url(key), timeout=30)
            st.write(f"{key}: HTTP {r.status_code}, {len(r.content):,} bytes")
        except Exception as e:
            st.error(f"Failed: {key}")
            st.exception(e)

st.caption(f"Rendered at {datetime.now().isoformat(timespec='seconds')}")

import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Candidate Connect DEV Diagnostic", layout="wide")

R2_BASE = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"

def fetch_text(key: str, timeout=30):
    url = f"{R2_BASE}/{key.lstrip('/')}"
    r = requests.get(url, timeout=timeout)
    return url, r.status_code, r.text if r.ok else r.text[:500]

def fetch_bytes(key: str, timeout=60):
    url = f"{R2_BASE}/{key.lstrip('/')}"
    r = requests.get(url, timeout=timeout)
    return url, r.status_code, r.content if r.ok else r.text[:500].encode()

st.title("Candidate Connect DEV Diagnostic")
st.caption(f"Loaded at {datetime.now().isoformat(timespec='seconds')}")

st.markdown("### 1. App boot")
st.success("Streamlit app booted successfully.")

st.markdown("### 2. Manifest check")
try:
    url, status, text = fetch_text("dataset_manifest.json")
    st.write("Manifest URL:", url)
    st.write("HTTP status:", status)
    if status != 200:
        st.error("Manifest did not load.")
        st.code(text)
        st.stop()

    manifest = json.loads(text)
    st.success("Manifest loaded and parsed.")
    st.json({
        "built_at": manifest.get("built_at"),
        "total_rows": manifest.get("total_rows"),
        "index_count": len(manifest.get("index", {}).get("shards", [])),
        "detail_count": len(manifest.get("detail", {}).get("shards", [])),
        "speed": manifest.get("speed", {}),
    })
except Exception as e:
    st.error("Manifest check failed.")
    st.exception(e)
    st.stop()

st.markdown("### 3. Speed table checks")
speed_keys = [
    "speed/speed_manifest.json",
    "speed/filter_options.parquet",
    "speed/filter_ranges.json",
    "speed/geo_hierarchy.parquet",
    "speed/count_cube.parquet",
    "speed/mail_ballot_counts.parquet",
]

for key in speed_keys:
    try:
        url, status, payload = fetch_bytes(key)
        st.write(f"{key} — HTTP {status} — {len(payload) if isinstance(payload, bytes) else 'n/a'} bytes")
        if status != 200:
            st.error(f"Missing or inaccessible: {key}")
            st.stop()
    except Exception as e:
        st.error(f"Failed while checking {key}")
        st.exception(e)
        st.stop()

st.success("All speed table files are reachable.")

st.markdown("### 4. Read small speed table sample")
try:
    import io
    _, _, payload = fetch_bytes("speed/filter_options.parquet")
    df = pd.read_parquet(io.BytesIO(payload))
    st.write("filter_options rows:", len(df))
    st.dataframe(df.head(25), use_container_width=True)
except Exception as e:
    st.error("Could not read filter_options.parquet.")
    st.exception(e)
    st.stop()

st.markdown("### 5. Result")
st.success("Diagnostic passed. If this page stays connected, R2/manifest/speed tables are OK and the crash is inside the full app code after startup.")

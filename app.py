# VERSION: CLOUD ACCURATE COUNTS v6 - COLUMN-LIMITED MEMORY SAFE

import io
import json
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

R2 = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"
DETAIL_SHARDS = 36

st.set_page_config(page_title="Candidate Connect DEV", layout="wide")

st.title("Candidate Connect DEV — Accurate Counts Mode")
st.caption("Memory-safe count test: loads only needed columns from each shard.")

@st.cache_data(ttl=600, show_spinner=False)
def get_bytes(key: str) -> bytes:
    r = requests.get(f"{R2}/{key}", timeout=120)
    r.raise_for_status()
    return r.content

@st.cache_data(ttl=600, show_spinner=False)
def load_manifest():
    return json.loads(get_bytes("dataset_manifest.json").decode("utf-8"))

@st.cache_data(ttl=600, show_spinner=False)
def load_geo():
    return pd.read_parquet(io.BytesIO(get_bytes("speed/geo_hierarchy.parquet")))

@st.cache_data(ttl=600, show_spinner=False)
def load_detail_columns(key: str, cols: tuple[str, ...]):
    # Critical: only read the columns needed for this operation.
    return pd.read_parquet(io.BytesIO(get_bytes(key)), columns=list(cols))

manifest = load_manifest()
geo = load_geo()

county_options = [""] + sorted([str(x) for x in geo["County"].dropna().unique()])
county = st.selectbox("County", county_options)

if county:
    muni_df = geo[geo["County"].astype(str).eq(str(county))]
else:
    muni_df = geo

municipality_options = [""] + sorted([str(x) for x in muni_df["Municipality"].dropna().unique()])
municipality = st.selectbox("Municipality", municipality_options)

filters = {}
if county:
    filters["County"] = county
if municipality:
    filters["Municipality"] = municipality

st.write("Active Filters:", filters)

st.info("This version calculates exact counts by scanning all 36 detail shards, but only loads filter columns + Party.")

if st.button("Calculate Counts (Accurate / Memory Safe)"):
    needed_cols = set(["Party"])
    needed_cols.update(filters.keys())
    needed_cols = tuple(sorted(needed_cols))

    total = 0
    r_count = 0
    d_count = 0
    o_count = 0
    failed = []

    progress = st.progress(0)
    status = st.empty()

    for i in range(DETAIL_SHARDS):
        key = f"detail/voters_detail_{i:03d}.parquet"
        status.write(f"Reading shard {i+1} of {DETAIL_SHARDS}: {key}")

        try:
            df = load_detail_columns(key, needed_cols)
        except Exception as e:
            failed.append((key, str(e)))
            progress.progress((i + 1) / DETAIL_SHARDS)
            continue

        for col, val in filters.items():
            if col in df.columns:
                df = df[df[col].astype(str).eq(str(val))]
            else:
                df = df.iloc[0:0]

        total += len(df)

        if "Party" in df.columns and not df.empty:
            party = df["Party"].astype(str).str.upper().str.strip()
            r_count += int((party == "R").sum())
            d_count += int((party == "D").sum())
            o_count += int((~party.isin(["R", "D"])).sum())

        # Drop reference quickly.
        del df
        progress.progress((i + 1) / DETAIL_SHARDS)

    status.empty()

    if failed:
        st.warning(f"{len(failed)} shard(s) failed. Showing first failure:")
        st.code(f"{failed[0][0]}: {failed[0][1]}")

    st.success("Counts complete.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total", f"{total:,}")
    c2.metric("R", f"{r_count:,}")
    c3.metric("D", f"{d_count:,}")
    c4.metric("O", f"{o_count:,}")

st.caption(f"Rendered at {datetime.now().isoformat(timespec='seconds')}")

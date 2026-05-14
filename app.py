# VERSION: CLOUD SHARD COUNT (FULL ACCURATE)

import streamlit as st
import pandas as pd
import requests
import io
import json

R2 = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"

st.set_page_config(layout="wide")

@st.cache_data
def load_manifest():
    return requests.get(f"{R2}/dataset_manifest.json").json()

@st.cache_data
def load_parquet(key):
    return pd.read_parquet(io.BytesIO(requests.get(f"{R2}/{key}").content))

manifest = load_manifest()

st.title("Candidate Connect DEV — Accurate Counts Mode")

# FILTER LAYER
filter_options = load_parquet("speed/filter_options.parquet")
geo = load_parquet("speed/geo_hierarchy.parquet")

county = st.selectbox("County", [""] + sorted(geo["County"].dropna().unique().tolist()))
muni = st.selectbox("Municipality", [""] + sorted(geo["Municipality"].dropna().unique().tolist()))

filters = {}
if county:
    filters["County"] = county
if muni:
    filters["Municipality"] = muni

st.write("Active Filters:", filters)

# COUNT BUTTON
if st.button("Calculate Counts (Accurate)"):
    st.warning("Loading ALL shards — accurate but slower")

    total = 0
    r = 0
    d = 0
    o = 0

    progress = st.progress(0)

    for i in range(36):
        key = f"detail/voters_detail_{i:03d}.parquet"
        df = load_parquet(key)

        for col, val in filters.items():
            df = df[df[col] == val]

        total += len(df)
        r += len(df[df["Party"] == "R"])
        d += len(df[df["Party"] == "D"])
        o += len(df[~df["Party"].isin(["R","D"])])

        progress.progress((i+1)/36)

    st.success("Counts Complete")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total", f"{total:,}")
    col2.metric("R", f"{r:,}")
    col3.metric("D", f"{d:,}")
    col4.metric("O", f"{o:,}")

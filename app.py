# Candidate Connect DEV v17 - NAV + SNAPSHOT (working version)

import streamlit as st

st.set_page_config(layout="wide")

# ---- STATE ----
if "view" not in st.session_state:
    st.session_state["view"] = "dashboard"

# ---- NAV BAR ----
nav1, nav2, nav3, nav4 = st.columns([1,1,1,1])

with nav1:
    if st.button("🏠 Dashboard", width="stretch"):
        st.session_state["view"] = "dashboard"

with nav2:
    if st.button("🎯 Targeting", width="stretch"):
        st.session_state["view"] = "targeting"

with nav3:
    st.button("📊 Analysis", disabled=True, width="stretch")

with nav4:
    st.button("📤 Export", disabled=True, width="stretch")

st.divider()

# ---- VIEWS ----
if st.session_state["view"] == "dashboard":
    st.header("Statewide Snapshot")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Voters", "Loading...")
    c2.metric("Republican", "Loading...")
    c3.metric("Democrat", "Loading...")
    c4.metric("Other", "Loading...")

    st.info("Next step: connect this to your quick count table.")

elif st.session_state["view"] == "targeting":
    st.header("Targeting")

    st.write("Your existing filters and Update Counts section will go here.")


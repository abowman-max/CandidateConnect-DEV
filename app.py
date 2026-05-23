
# --- ADD THIS TO YOUR GLOBAL CSS BLOCK ---

st.markdown("""
<style>

/* LEFT HEADER (TARGET ENGAGE WIN) */
.left-header-text {
    font-family: Impact, Haettenschweiler, 'Arial Narrow Bold', sans-serif !important;
    letter-spacing: 1px;
}

/* ensure sidebar header uses it */
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    font-family: Impact, Haettenschweiler, 'Arial Narrow Bold', sans-serif !important;
}

</style>
""", unsafe_allow_html=True)

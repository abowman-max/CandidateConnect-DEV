# Candidate Connect LIVE — Final Hybrid Cloud App v21zp VOTER_LOOKUP_PERSIST_HISTORY_PDF_FIX
# Full safe filters + guarded export.
# v21p: keeps v21o phone fix and makes saved universes survive app reload/reboot via URL persistence.

import io
import json
import mimetypes
import os
import base64
import re
import hashlib
import html
import secrets
import string
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import duckdb
import requests
import streamlit as st
try:
    import streamlit.components.v1 as components  # fallback only for older Streamlit builds
except Exception:
    components = None
try:
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import inch
except Exception:
    letter = canvas = inch = None

R2 = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"
DETAIL_SHARDS = 36
EXPORT_ROW_LIMIT = 250_000

st.set_page_config(page_title="Candidate Connect", layout="wide", initial_sidebar_state="expanded")


# Early hard CSS fixes: loaded before any st.stop() branches.
st.markdown("""
<style>
/* login/setup card */
div[data-testid="stForm"]{background:#f8f4ea!important;border:1px solid #b9ad99!important;border-radius:16px!important;box-shadow:0 12px 28px rgba(7,29,58,.12)!important;padding:22px 26px!important;}
/* all normal action buttons */
.stButton button:not(:disabled),.stFormSubmitButton button:not(:disabled),div[data-testid="stFormSubmitButton"] button:not(:disabled),div[data-testid="stDownloadButton"] button:not(:disabled),button[data-testid*="baseButton"]:not(:disabled){background:linear-gradient(180deg,#b01822,#9f151c)!important;background-color:#9f151c!important;color:#fff!important;-webkit-text-fill-color:#fff!important;border:1px solid #6f0d13!important;font-weight:900!important;opacity:1!important;text-shadow:none!important;}
.stButton button:not(:disabled) *,.stFormSubmitButton button:not(:disabled) *,div[data-testid="stFormSubmitButton"] button:not(:disabled) *,div[data-testid="stDownloadButton"] button:not(:disabled) *,button[data-testid*="baseButton"]:not(:disabled) *{color:#fff!important;-webkit-text-fill-color:#fff!important;fill:#fff!important;opacity:1!important;}
/* disabled buttons */
.stButton button:disabled,.stFormSubmitButton button:disabled,div[data-testid="stFormSubmitButton"] button:disabled,div[data-testid="stDownloadButton"] button:disabled{background:#d8cfc0!important;background-color:#d8cfc0!important;color:#222!important;-webkit-text-fill-color:#222!important;border:1px solid #b9ad99!important;opacity:.8!important;}
.stButton button:disabled *,.stFormSubmitButton button:disabled *,div[data-testid="stFormSubmitButton"] button:disabled *,div[data-testid="stDownloadButton"] button:disabled *{color:#222!important;-webkit-text-fill-color:#222!important;}
/* password eye: readable, not red */
button[aria-label*="password"],button[title*="password"],[data-testid="stTextInputRootElement"] button,[data-baseweb="input"] button{background:#fff!important;background-color:#fff!important;color:#071d3a!important;-webkit-text-fill-color:#071d3a!important;border:0!important;border-left:1px solid #d0c7b7!important;opacity:1!important;}
button[aria-label*="password"] *,button[title*="password"] *,[data-testid="stTextInputRootElement"] button *,[data-baseweb="input"] button *{color:#071d3a!important;-webkit-text-fill-color:#071d3a!important;fill:#071d3a!important;opacity:1!important;}
/* leave tab buttons alone */
div[data-testid="stTabs"] button,div[data-testid="stTabs"] button *,[role="tab"],[role="tab"] *{background:transparent!important;border:none!important;box-shadow:none!important;color:#071d3a!important;-webkit-text-fill-color:#071d3a!important;font-weight:900!important;}
</style>
""", unsafe_allow_html=True)
try:
    st.set_option("runner.magicEnabled", False)
except Exception:
    pass


def inject_clean_theme_css():
    """Single Candidate Connect theme. Avoid broad global selectors that resize unrelated widgets."""
    st.markdown("""
<style>
:root { color-scheme: light !important; }

html, body, .stApp, [data-testid="stAppViewContainer"] {
    background: #efe8d8 !important;
    color: #071d3a !important;
    font-size: 10pt !important;
}

.block-container {
    max-width: 1280px !important;
    margin-left: 0 !important;
    margin-right: auto !important;
    padding: 9.0rem 1.25rem 1.25rem 1.5rem !important;
}

h1, h2, h3, h4, h5, h6 {
    color: #071d3a !important;
    font-weight: 900 !important;
}

p, label, .stMarkdown, [data-testid="stMarkdownContainer"] {
    color: #071d3a !important;
}

/* fixed brand header */
.cc-header { display: none !important; }

.cc-global-header {
    position: fixed !important;
    inset: 0 0 auto 0 !important;
    height: 142px !important;
    z-index: 999999 !important;
    background: #efe8d8 !important;
    border-bottom: 2px solid #9f151c !important;
    box-shadow: 0 6px 18px rgba(7,29,58,.16) !important;
}

.cc-global-sidebar-fill { display: none !important; }

.cc-global-header-inner {
    width: 100vw !important;
    box-sizing: border-box !important;
    padding: 10px 28px 12px 28px !important;
}

.cc-global-redbar {
    height: 14px !important;
    border-radius: 999px !important;
    background: #940d14 !important;
    border: 1px solid #5d0b10 !important;
    margin-bottom: 6px !important;
    box-shadow: 0 6px 14px rgba(7,29,58,.22) !important;
}

.cc-global-brand-row {
    min-height: 108px !important;
    display: grid !important;
    grid-template-columns: 240px minmax(320px,1fr) 230px !important;
    align-items: center !important;
    gap: 18px !important;
}

.cc-global-tagline {
    justify-self: start !important;
    display: flex !important;
    flex-direction: column !important;
    color: #071d3a !important;
    font-family: Impact, Haettenschweiler, 'Arial Narrow Bold', sans-serif !important;
    font-size: 27px !important;
    line-height: .92 !important;
    letter-spacing: .02em !important;
    text-transform: uppercase !important;
    padding-left: 18px !important;
}

.cc-global-tagline span {
    color: #071d3a !important;
    text-shadow: 2px 2px 0 #f8f4ea, 3px 3px 5px rgba(7,29,58,.35) !important;
}

.cc-global-logo-center-wrap {
    justify-self: center !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
}

.cc-global-logo-center {
    height: 96px !important;
    max-width: 380px !important;
    object-fit: contain !important;
}

.cc-global-logo-right {
    justify-self: end !important;
    height: 76px !important;
    max-width: 230px !important;
    object-fit: contain !important;
}

/* sidebar */
[data-testid="stSidebar"] {
    background: #e6ddcc !important;
    border-right: 2px solid #9f151c !important;
    min-width: 250px !important;
    width: 250px !important;
    max-width: 250px !important;
}

[data-testid="stSidebar"] > div:first-child {
    padding-top: 8.6rem !important;
}

[data-testid="stSidebar"] p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
    color: #071d3a !important;
}

[data-testid="stSidebar"] .stButton > button {
    min-height: 38px !important;
    height: 38px !important;
    max-height: 38px !important;
    width: 100% !important;
    padding: 5px 9px !important;
    margin: 0 0 4px 0 !important;
    border-radius: 8px !important;
    font-size: 10pt !important;
    line-height: 1.1 !important;
}

/* real action buttons */
.stButton > button,
div[data-testid="stDownloadButton"] > button,
[data-testid="stFileUploader"] button {
    background: linear-gradient(180deg, #b01822, #8f1119) !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    border: 1px solid #7a0d14 !important;
    border-radius: 9px !important;
    font-weight: 850 !important;
    box-shadow: none !important;
    text-shadow: none !important;
    min-height: 34px !important;
    padding: 6px 12px !important;
    font-size: 10pt !important;
    line-height: 1.15 !important;
}

.stButton > button *,
div[data-testid="stDownloadButton"] > button *,
[data-testid="stFileUploader"] button * {
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
}

.stButton > button:disabled,
div[data-testid="stDownloadButton"] > button:disabled {
    background: #d8cfc0 !important;
    color: #7a0d14 !important;
    -webkit-text-fill-color: #7a0d14 !important;
    border: 1px solid #b9ad99 !important;
    opacity: 1 !important;
}

/* controls */
[data-baseweb="select"] > div,
[data-baseweb="input"] > div,
textarea,
input {
    background: #ffffff !important;
    color: #071d3a !important;
    border-color: #8b8171 !important;
    font-size: 10pt !important;
}

[data-baseweb="popover"],
[data-baseweb="menu"],
[role="listbox"] {
    background: #ffffff !important;
    color: #071d3a !important;
}

[role="option"],
[role="option"] * {
    background: #ffffff !important;
    color: #071d3a !important;
}

[role="option"]:hover,
[role="option"][aria-selected="true"] {
    background: #f1e7d6 !important;
    color: #071d3a !important;
}

[data-baseweb="tag"] {
    background: #9f151c !important;
    color: #ffffff !important;
}

[data-baseweb="tag"] * {
    color: #ffffff !important;
}

/* tabs: never style them like action buttons */
div[data-testid="stTabs"] button,
button[data-baseweb="tab"],
[role="tab"] {
    background: transparent !important;
    color: #071d3a !important;
    -webkit-text-fill-color: #071d3a !important;
    border: 0 !important;
    box-shadow: none !important;
    font-weight: 900 !important;
}

div[data-testid="stTabs"] button *,
button[data-baseweb="tab"] *,
[role="tab"] * {
    color: #071d3a !important;
    -webkit-text-fill-color: #071d3a !important;
}

div[data-testid="stTabs"] [data-baseweb="tab-highlight"] {
    background-color: #b0121b !important;
    height: 4px !important;
}

/* cards and dashboard elements */
.cc-card,
.cc-home-card,
.cc-metric,
.cc-icon-metric {
    background: #f8f4ea !important;
    color: #071d3a !important;
    border: 1px solid #b9ad99 !important;
    border-radius: 12px !important;
    box-shadow: 0 8px 18px rgba(7,29,58,.12) !important;
    padding: 12px !important;
    margin-bottom: 12px !important;
}

.cc-home-title {
    font-size: 21pt !important;
    font-weight: 950 !important;
    letter-spacing: .05em !important;
    text-transform: uppercase !important;
    color: #071d3a !important;
    margin: 6px 0 12px 0 !important;
}

.cc-icon-metric {
    min-height: 70px !important;
    display: flex !important;
    align-items: center !important;
    gap: 12px !important;
}

.cc-icon-dot {
    width: 42px !important;
    height: 42px !important;
    border-radius: 999px !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    background: radial-gradient(circle at 35% 20%, #ff6b6b, #9f151c 72%) !important;
    color: #ffffff !important;
}

.cc-icon-dot.blue { background: radial-gradient(circle at 35% 20%, #60a5fa, #1d4ed8 72%) !important; }
.cc-icon-dot.green { background: radial-gradient(circle at 35% 20%, #86efac, #3f8f27 72%) !important; }
.cc-icon-dot.gold { background: radial-gradient(circle at 35% 20%, #fde68a, #b7791f 72%) !important; }

.cc-icon-label,
.cc-metric .label {
    color: #5f6b7a !important;
    font-size: 9pt !important;
    font-weight: 900 !important;
    text-transform: uppercase !important;
    letter-spacing: .06em !important;
}

.cc-icon-value,
.cc-metric .value {
    color: #071d3a !important;
    font-size: 19pt !important;
    font-weight: 950 !important;
    line-height: 1.05 !important;
}

.cc-icon-sub,
.cc-metric .sub {
    color: #5f6b7a !important;
    font-size: 9pt !important;
}

/* donut / charts */
.cc-donut-wrap {
    display: flex !important;
    align-items: center !important;
    justify-content: flex-start !important;
    gap: 18px !important;
    flex-wrap: wrap !important;
    min-height: 190px !important;
}

.cc-donut {
    width: 150px !important;
    height: 150px !important;
    border-radius: 50% !important;
    position: relative !important;
    flex: 0 0 auto !important;
}

.cc-donut:after {
    content: '' !important;
    position: absolute !important;
    inset: 40px !important;
    border-radius: 50% !important;
    background: #071d3a !important;
}

.cc-donut-center,
.cc-donut-center * {
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    text-shadow: 0 1px 2px rgba(0,0,0,.65) !important;
    position: relative !important;
    z-index: 2 !important;
}

.cc-legend-row {
    display: grid !important;
    grid-template-columns: 14px minmax(120px, 1fr) auto !important;
    gap: 8px !important;
    align-items: center !important;
    margin: 8px 0 !important;
    color: #071d3a !important;
    font-size: 10pt !important;
}

.cc-swatch {
    width: 12px !important;
    height: 12px !important;
    border-radius: 999px !important;
}

.cc-age-row {
    display: grid !important;
    grid-template-columns: 64px 1fr 72px !important;
    gap: 10px !important;
    align-items: center !important;
    margin: 7px 0 !important;
    font-size: 10pt !important;
}

.cc-age-bar-bg {
    height: 14px !important;
    border-radius: 999px !important;
    background: #071d3a !important;
    overflow: hidden !important;
}

.cc-age-bar {
    height: 100% !important;
    border-radius: 999px !important;
    background: linear-gradient(90deg, #8b0d13, #ef4444) !important;
}

/* tables */
.cc-table-wrap,
.cc-scroll-table {
    overflow: auto !important;
    border: 1px solid #9f151c !important;
    border-radius: 10px !important;
    background: #ffffff !important;
    margin: 8px 0 16px 0 !important;
}

.cc-html-table,
.cc-home-table {
    width: 100% !important;
    border-collapse: separate !important;
    border-spacing: 0 !important;
    background: #ffffff !important;
    color: #000000 !important;
    font-size: 10pt !important;
}

.cc-html-table th,
.cc-home-table th {
    background: #9f151c !important;
    color: #ffffff !important;
    text-align: center !important;
    font-weight: 900 !important;
    padding: 8px 10px !important;
}

.cc-html-table td,
.cc-home-table td {
    color: #000000 !important;
    text-align: center !important;
    padding: 7px 10px !important;
    border-bottom: 1px solid #e7dfd2 !important;
    vertical-align: middle !important;
}

.cc-html-table tbody tr:nth-child(even) td,
.cc-home-table tbody tr:nth-child(even) td {
    background: #f3eadc !important;
}

.cc-empty-table,
.cc-note,
.cc-verify {
    border: 1px solid #8aa3bf !important;
    background: #d9e8f8 !important;
    color: #071d3a !important;
    border-radius: 10px !important;
    padding: 11px 13px !important;
    margin: 10px 0 14px 0 !important;
}

/* login/setup */
.cc-login-spacer { height: 2rem !important; }

.cc-login-title {
    text-align: center !important;
    color: #071d3a !important;
    font-size: 18pt !important;
    font-weight: 950 !important;
    margin-bottom: .25rem !important;
}

.cc-login-subtitle {
    text-align: center !important;
    color: #5f6b7a !important;
    font-size: 10pt !important;
    margin-bottom: 1rem !important;
}

div[data-testid="stForm"] {
    max-width: 430px;
}

[data-testid="stFileUploader"] section {
    background: #fbf7ee !important;
    border: 1px solid rgba(170,20,30,.35) !important;
    border-radius: 12px !important;
}

/* keep Streamlit menu hidden, but keep header recovery/collapse controls available */
#MainMenu,
footer,
[data-testid="stToolbar"],
[data-testid="stDecoration"],
[data-testid="stStatusWidget"] {
    visibility: hidden !important;
    height: 0 !important;
}

header[data-testid="stHeader"] {
    visibility: visible !important;
    height: auto !important;
    background: transparent !important;
}

/* Responsive header */
@media (max-width: 900px) {
    .cc-global-header { height: 118px !important; }
    .block-container { padding-top: 7.8rem !important; }
    [data-testid="stSidebar"] > div:first-child { padding-top: 7.8rem !important; }
    .cc-global-brand-row { grid-template-columns: 90px minmax(160px,1fr) 90px !important; }
    .cc-global-tagline { font-size: 17px !important; padding-left: 4px !important; }
    .cc-global-logo-center { height: 70px !important; max-width: 230px !important; }
    .cc-global-logo-right { height: 50px !important; max-width: 140px !important; }
}
</style>
""", unsafe_allow_html=True)


inject_clean_theme_css()

st.markdown("""
<style>
/* Help ? icon and tooltip readability only */
[data-testid="stTooltipHoverTarget"],
[data-testid="stTooltipHoverTarget"] *,
button[aria-label="Help"],
button[aria-label="Help"] *,
svg[aria-label="Help"],
[data-testid="stWidgetLabel"] svg {
    color: #071d3a !important;
    fill: #071d3a !important;
    stroke: #071d3a !important;
    opacity: 1 !important;
}
div[data-testid="stTooltipContent"],
div[role="tooltip"],
[data-baseweb="popover"] {
    background: #ffffff !important;
    color: #071d3a !important;
    -webkit-text-fill-color: #071d3a !important;
    border: 1px solid #b9ad99 !important;
    box-shadow: 0 8px 24px rgba(7,29,58,.18) !important;
}
div[data-testid="stTooltipContent"] *,
div[role="tooltip"] *,
[data-baseweb="popover"] * {
    color: #071d3a !important;
    -webkit-text-fill-color: #071d3a !important;
    opacity: 1 !important;
}
</style>
""", unsafe_allow_html=True)

GEO_FIELDS = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]
VOTER_FIELDS = ["Party", "Gender", "Age_Range", "V4A", "V4G", "V4P", "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone", "Tags"]
ALL_FILTER_FIELDS = GEO_FIELDS + VOTER_FIELDS

DISPLAY_LABELS = {
    "USC": "Congressional District",
    "STS": "State Senate District",
    "STH": "State House District",
    "Magisterial District": "Magisterial District",
    "Age_Range": "Age Range",
    "MB_App": "Mail Ballot Application",
    "MB_App_Status": "Application Status",
    "MB_Sent": "Ballot Sent",
    "MB_Status": "Ballot Status",
    "MB_PERM": "Permanent Mail Ballot",
    "MailBallotNewRegistrant": "Newly Registered / Current Only",
    "CalculatedParty": "Calculated Party",
    "HH-Party": "Household Party",
    "V4A": "Vote History - All Elections",
    "V4G": "Vote History - General Elections",
    "V4P": "Vote History - Primary Elections",

    "HasMobile": "Mobile Phone",
    "HasLandline": "Landline",
    "HasEmail": "Email",
    "HasApplicantPhone": "Mail Ballot Application Phone",
    "Tags": "Tags",
}


def scope_summary(scope: dict | None) -> str:
    try:
        scope = scope or {}
        parts = []
        for k, v in scope.items():
            vals = v if isinstance(v, list) else [v]
            vals = [str(x).strip() for x in vals if str(x).strip()]
            if vals:
                parts.append(f"{DISPLAY_LABELS.get(k, k)}: {', '.join(vals)}")
        return " | ".join(parts) if parts else "Statewide / unrestricted"
    except Exception:
        return "Statewide / unrestricted"


LOGO_CANDIDATE_CONNECT = "candidate_connect_logo.png"
LOGO_TPTC = "TSS_Logo_Transparent.png"

def file_exists(path: str) -> bool:
    try:
        return Path(path).exists()
    except Exception:
        return False


def img_data_uri(path: str) -> str:
    """Return a safe data URI for local branding images used in the fixed full-width header."""
    try:
        p = Path(path)
        if not p.exists():
            alt = Path(__file__).resolve().parent / path
            p = alt if alt.exists() else p
        if not p.exists():
            return ""
        ext = p.suffix.lower().lstrip(".") or "png"
        mime = "image/png" if ext in {"png", "apng"} else ("image/jpeg" if ext in {"jpg", "jpeg"} else "image/png")
        return f"data:{mime};base64," + base64.b64encode(p.read_bytes()).decode("ascii")
    except Exception:
        return ""

DEFAULT_EXPORT_COLUMNS = [
    # Keep voter_id in every CSV/Excel output so street/walk/contact results can be matched later.
    "voter_id",
    "County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region",
    "FirstName", "MiddleName", "LastName", "NameSuffix", "FullName",
    "Party", "CalculatedParty", "Gender", "DOB", "Age", "Age_Range", "RegistrationDate",
    "House Number", "House Number Suffix", "Street Name", "Apartment Number", "Address Line 2", "City", "State", "Zip",
    "res_address", "res_city", "res_state", "res_zip",
    "Email", "Mobile", "Landline", "Current_ApplicantPhone",
    "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "MB_Prob_Score",
    "Current_App_Return_Date", "Current_Ballot_Sent_Date", "Current_Ballot_Returned_Date",
    "Tags",
]

pass




# v23b: home dashboard gender labels + responsive chart/table readability.
pass



# Cross-browser LIVE readability lock for Safari/Mac dark-mode quirks.
pass



# Final production readability lock for tabs/toggles/buttons after all earlier CSS blocks.
pass



# Final sidebar recovery lock: keep the left navigation from collapsing into an unrecoverable state.
# This is intentionally placed after the main CSS stack so it wins over earlier Streamlit chrome-hiding rules.
pass


# Safe design system patch added after legacy CSS stack.
pass


# Final safe button consistency patch.
pass


# Compact UX patch: tabs restored and page length reduced.
pass


# UI cleanup patch: card spacing and compact strategy readout.
pass


# Theme readability fix for code chips, alerts, and password icons.
pass


def r2_url(key: str) -> str:
    base = current_data_base_url()
    return f"{base.rstrip('/')}/{key.lstrip('/')}"


def root_r2_url(key: str) -> str:
    """Always read durable app_state from the root R2 bucket, not a campaign mini dataset."""
    return f"{R2.rstrip('/')}/{str(key).lstrip('/')}"


def sql_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def sql_lit(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def tag_contains_mask(series: pd.Series, selected_tags) -> pd.Series:
    """Match Tags safely when a row may contain comma/semicolon/pipe separated values."""
    if series is None:
        return pd.Series([], dtype=bool)
    vals = [str(v).strip().lower() for v in (selected_tags or []) if str(v).strip()]
    if not vals:
        return pd.Series(True, index=series.index)

    def has_any(raw) -> bool:
        txt = str(raw or "").strip().lower()
        if not txt:
            return False
        parts = [p.strip() for p in re.split(r"[,;|]+", txt) if p.strip()]
        if parts:
            return any(v == p for v in vals for p in parts)
        return any(v in txt for v in vals)

    return series.map(has_any).fillna(False)


def count_cube_url() -> str:
    manifest = load_manifest()
    speed = manifest.get("speed", {}).get("tables", {})
    key = speed.get("count_cube", "speed/count_cube.parquet")
    return r2_url(key)


def _count_cube_expanded_values(field: str, vals) -> list[str]:
    """Expand user-facing MB yes/no selections to the canonical values that may exist in count_cube.

    This is especially important for MB_PERM. In some SURE-derived files permanent MB
    is stored as Y/blank, in others as Y/N or Yes/No. Selecting N should mean
    "not permanent," including blank/no/false/0 variants.
    """
    raw = [str(v).strip() for v in (vals or []) if str(v).strip()]
    if not raw:
        return []

    yes = {"Y", "YES", "TRUE", "T", "1", "APPLIED", "SENT", "VOTED", "RETURNED", "PERMANENT"}
    no = {"N", "NO", "FALSE", "F", "0", "DNA", "DID NOT APPLY", "NOT APPLIED", "NOT SENT", "NOT VOTED", "NOT RETURNED", "NOT PERMANENT", "NON PERMANENT", "NON-PERMANENT"}

    expanded = []
    for v in raw:
        u = v.upper()
        if field in {"MB_PERM", "MB_App", "MB_Sent", "MB_Status"}:
            if u in yes:
                expanded.extend([v, "Y", "Yes", "YES", "True", "TRUE", "1"])
                if field == "MB_App":
                    expanded.extend(["Applied"])
                if field == "MB_Sent":
                    expanded.extend(["Sent"])
                if field == "MB_Status":
                    expanded.extend(["Voted", "Returned"])
                if field == "MB_PERM":
                    expanded.extend(["Permanent"])
            elif u in no:
                expanded.extend([v, "", "N", "No", "NO", "False", "FALSE", "0"])
                if field == "MB_App":
                    expanded.extend(["DNA", "Not Applied", "Did Not Apply"])
                if field == "MB_Sent":
                    expanded.extend(["Not Sent"])
                if field == "MB_Status":
                    expanded.extend(["Not Voted", "Not Returned"])
                if field == "MB_PERM":
                    expanded.extend(["Not Permanent", "Non Permanent", "Non-Permanent"])
            else:
                expanded.append(v)
        elif field == "MB_App_Status":
            # Application status is not a yes/no field, but keep common capitalization variants.
            expanded.extend([v, v.title(), u])
        else:
            expanded.append(v)

    out = []
    seen = set()
    for x in expanded:
        sx = str(x)
        if sx not in seen:
            seen.add(sx)
            out.append(sx)
    return out


def count_cube_where_sql(active: dict, special: dict | None = None) -> str:
    clauses = []
    for field, vals in (active or {}).items():
        if not vals:
            continue
        if field == "Tags":
            continue
        cleaned = _count_cube_expanded_values(field, vals)
        if not cleaned:
            continue
        expr = f"COALESCE(CAST({sql_ident(field)} AS VARCHAR), '')"
        # MB_PERM is a Y/blank style field in many builds. For this filter,
        # N must mean "not permanent" — not just literal N. The safest fast
        # count-cube expression is therefore NOT IN all yes/permanent variants.
        if field == "MB_PERM":
            raw_upper = {str(v).strip().upper() for v in (vals or []) if str(v).strip()}
            yes_tokens = {"Y", "YES", "TRUE", "T", "1", "PERMANENT"}
            no_tokens = {"N", "NO", "FALSE", "F", "0", "NOT PERMANENT", "NON PERMANENT", "NON-PERMANENT", ""}
            upper_expr = f"UPPER(TRIM({expr}))"
            if raw_upper and raw_upper.issubset(no_tokens):
                clauses.append(f"({upper_expr} NOT IN ('Y','YES','TRUE','T','1','PERMANENT'))")
                continue
            if raw_upper and raw_upper.issubset(yes_tokens):
                clauses.append(f"({upper_expr} IN ('Y','YES','TRUE','T','1','PERMANENT'))")
                continue
        clauses.append(f"{expr} IN (" + ",".join(sql_lit(v) for v in cleaned) + ")")

    special = special or {}
    for field, rule in special.items():
        if field == "__PhoneReach":
            mobile = "LOWER(CAST(\"HasMobile\" AS VARCHAR)) = 'yes'"
            landline = "LOWER(CAST(\"HasLandline\" AS VARCHAR)) = 'yes'"
            mode = str(rule)
            if mode == "Mobile only":
                clauses.append(f"({mobile})")
            elif mode == "Landline only":
                clauses.append(f"({landline})")
            elif mode == "Mobile OR landline":
                clauses.append(f"(({mobile}) OR ({landline}))")
            elif mode == "Mobile AND landline":
                clauses.append(f"(({mobile}) AND ({landline}))")
            elif mode == "No mobile or landline":
                clauses.append(f"(NOT (({mobile}) OR ({landline})))")
            continue
        if str(field).startswith("__"):
            continue
        if isinstance(rule, dict):
            expr = f"TRY_CAST({sql_ident(field)} AS DOUBLE)"
            if "min" in rule:
                clauses.append(f"{expr} >= {float(rule['min'])}")
            if "max" in rule:
                clauses.append(f"{expr} <= {float(rule['max'])}")

    return " WHERE " + " AND ".join(clauses) if clauses else ""


@st.cache_data(ttl=300, show_spinner=False)
def duckdb_count_cube_summary(active_json: str, special_json: str) -> dict:
    active = enforce_security_scope(json.loads(active_json or "{}"))
    special = json.loads(special_json or "{}")
    url = count_cube_url()
    where = count_cube_where_sql(active, special)
    query = f"""
        SELECT CAST(Party AS VARCHAR) AS Party, SUM(Voters) AS Voters
        FROM read_parquet({sql_lit(url)})
        {where}
        GROUP BY CAST(Party AS VARCHAR)
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try:
                con.execute("LOAD httpfs;")
            except Exception:
                pass
        df = con.execute(query).df()
    finally:
        try:
            con.close()
        except Exception:
            pass
    return summarize_from_df(df, row_count_mode=False)


@st.cache_data(ttl=600, show_spinner=False)
def _index_urls_from_base(base_url: str) -> list[str]:
    """Remote index shard URLs for DuckDB. Keeps counting out of Streamlit memory."""
    m = _load_manifest_from_base(base_url)
    count = int(((m.get("index", {}) or {}).get("count", DETAIL_SHARDS)) or DETAIL_SHARDS)
    return [f"{base_url.rstrip('/')}/index/voters_index_{i:03d}.parquet" for i in range(count)]


def index_urls_from_manifest() -> list[str]:
    return _index_urls_from_base(current_data_base_url())


@st.cache_data(ttl=600, show_spinner=False)
def _detail_urls_from_base(base_url: str) -> list[str]:
    """Remote detail shard URLs for DuckDB exports/reports."""
    m = _load_manifest_from_base(base_url)
    count = int(((m.get("detail", {}) or {}).get("count", DETAIL_SHARDS)) or DETAIL_SHARDS)
    return [f"{base_url.rstrip('/')}/detail/voters_detail_{i:03d}.parquet" for i in range(count)]


def detail_urls_from_manifest() -> list[str]:
    return _detail_urls_from_base(current_data_base_url())


def speed_table_key(stem: str) -> str:
    try:
        m = load_manifest()
        return (((m.get("speed", {}) or {}).get("tables", {}) or {}).get(stem, ""))
    except Exception:
        return ""


def speed_table_url(stem: str) -> str:
    key = speed_table_key(stem)
    return r2_url(key) if key else ""


def voter_search_all_urls() -> list[str]:
    urls = []
    for ch in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["OTHER"]:
        u = speed_table_url(f"voter_search_lname_{ch}")
        if u:
            urls.append(u)
    return urls


def _lookup_county_token_and_search_tokens(term: str):
    county_names = {
        "adams","allegheny","armstrong","beaver","bedford","berks","blair","bradford","bucks","butler",
        "cambria","cameron","carbon","centre","chester","clarion","clearfield","clinton","columbia","crawford",
        "cumberland","dauphin","delaware","elk","erie","fayette","forest","franklin","fulton","greene",
        "huntingdon","indiana","jefferson","juniata","lackawanna","lancaster","lawrence","lebanon","lehigh",
        "luzerne","lycoming","mckean","mercer","mifflin","monroe","montgomery","montour","northampton",
        "northumberland","perry","philadelphia","pike","potter","schuylkill","snyder","somerset","sullivan",
        "susquehanna","tioga","union","venango","warren","washington","wayne","westmoreland","wyoming","york"
    }
    raw_tokens = [t.strip() for t in re.split(r"\s+", str(term or "").strip()) if t.strip()]
    tokens_lower = [t.lower().replace("'", "''") for t in raw_tokens]
    county_token = next((t for t in tokens_lower if t in county_names), "")
    search_tokens = [t for t in tokens_lower if t != county_token]
    return county_token, search_tokens


def voter_search_urls_for_term(term: str) -> list[str]:
    """Pick the smallest useful search file set.

    Normal name searches read one last-name-letter shard. Address-only searches
    fall back to all 27 thin search shards. If Step 8 has not produced these yet,
    fall back to the regular index shards.
    """
    county_token, search_tokens = _lookup_county_token_and_search_tokens(term)
    digits = re.sub(r"\D+", "", str(term or ""))
    if len(digits) >= 6 or "@" in str(term or ""):
        return voter_search_all_urls() or index_urls_from_manifest()
    if search_tokens:
        last_t = search_tokens[-1]
        ch = last_t[:1].upper()
        if "A" <= ch <= "Z":
            u = speed_table_url(f"voter_search_lname_{ch}")
            if u:
                return [u]
    return voter_search_all_urls() or index_urls_from_manifest()


def voter_detail_hash_url(voter_id: str) -> str:
    vid = cc_text(voter_id)
    if not vid:
        return ""
    bucket = int(hashlib.md5(vid.encode("utf-8")).hexdigest(), 16) % 64
    return speed_table_url(f"voter_detail_hash_{bucket:02d}")


def voter_detail_lookup_urls_for_id(voter_id: str) -> list[str]:
    u = voter_detail_hash_url(voter_id)
    return [u] if u else detail_urls_from_manifest()


def _hh_norm(value) -> str:
    s = cc_text(value).upper().strip()
    s = re.sub(r"\bSTREET\b", "ST", s)
    s = re.sub(r"\bROAD\b", "RD", s)
    s = re.sub(r"\bDRIVE\b", "DR", s)
    s = re.sub(r"\bAVENUE\b", "AVE", s)
    s = re.sub(r"\bLANE\b", "LN", s)
    s = re.sub(r"\bCOURT\b", "CT", s)
    s = re.sub(r"\bTOWNSHIP\b", "TWP", s)
    s = re.sub(r"\bBOROUGH\b", "BORO", s)
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def household_lookup_key(row) -> str:
    parts = [
        row.get("County", ""), row.get("House Number", ""), row.get("Street Name", ""),
        row.get("Apartment Number", ""), row.get("Zip", ""),
    ]
    return "|".join(_hh_norm(x) for x in parts)

def household_hash_bucket_from_key(key: str, buckets: int = 64) -> int:
    return int(hashlib.md5(cc_text(key).encode("utf-8")).hexdigest(), 16) % int(buckets)

def voter_household_lookup_url(row) -> str:
    key = cc_text(row.get("HH_LOOKUP_KEY", "")) or household_lookup_key(row)
    if not key or key.count("|") < 4:
        return ""
    bucket = household_hash_bucket_from_key(key)
    return speed_table_url(f"voter_household_hash_{bucket:02d}")


def voter_lookup_urls_from_manifest() -> list[str]:
    """Backward-compatible helper for older lookup builds."""
    key = speed_table_key("voter_lookup")
    return [r2_url(key)] if key else []


def voter_lookup_or_detail_urls() -> list[str]:
    return voter_lookup_urls_from_manifest() or detail_urls_from_manifest()


def normalize_compare_value(value) -> str:
    s = clean_value(value).upper()
    s = s.replace("&", " AND ")
    s = re.sub(r"\bTOWNSHIP\b", "TWP", s)
    s = re.sub(r"\bTWP\.\b", "TWP", s)
    s = re.sub(r"\bBOROUGH\b", "BORO", s)
    s = re.sub(r"\bBORO\.\b", "BORO", s)
    s = re.sub(r"\bPRECINCT\b", "PRECINCT", s)
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def detail_filter_where_sql(active: dict, special: dict | None = None) -> str:
    clauses = []
    for field, vals in (active or {}).items():
        cleaned = [str(v).strip() for v in (vals or []) if str(v).strip()]
        if not cleaned:
            continue
        if field == "Tags":
            expr = f"LOWER(CAST({sql_ident(field)} AS VARCHAR))"
            clauses.append("(" + " OR ".join([f"{expr} LIKE {sql_lit('%' + v.lower().replace(chr(39), chr(39)+chr(39)) + '%')}" for v in cleaned]) + ")")
        else:
            norm_vals = [normalize_compare_value(v) for v in cleaned]
            expr = (
                "REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("
                f"UPPER(CAST({sql_ident(field)} AS VARCHAR)), "
                "'\\bTOWNSHIP\\b','TWP','g'), "
                "'\\bTWP\\.\\b','TWP','g'), "
                "'\\bBOROUGH\\b','BORO','g'), "
                "'[^A-Z0-9]+',' ','g')"
            )
            clauses.append(f"TRIM({expr}) IN (" + ",".join(sql_lit(v) for v in norm_vals) + ")")
    special = special or {}
    for field, rule in special.items():
        if field == "__ElectionFilters":
            continue
        if field == "__PhoneReach":
            phone_clause = index_phone_reach_sql(str(rule))
            if phone_clause:
                clauses.append(phone_clause)
            continue
        if str(field).startswith("__"):
            continue
        if isinstance(rule, dict):
            expr = f"TRY_CAST({sql_ident(field)} AS DOUBLE)"
            if "min" in rule:
                clauses.append(f"{expr} >= {float(rule['min'])}")
            if "max" in rule:
                clauses.append(f"{expr} <= {float(rule['max'])}")
    ef = special.get("__ElectionFilters")
    if isinstance(ef, dict):
        cols = selected_election_columns(ef.get("years") or [], ef.get("types") or [])
        if cols:
            clauses.append(election_method_sql(cols, ef.get("methods") or []))
        elif ef.get("years") or ef.get("types") or ef.get("methods"):
            clauses.append("(FALSE)")
    return " WHERE " + " AND ".join(clauses) if clauses else ""


def duckdb_detail_filtered_df(active: dict, special: dict | None, max_rows: int) -> pd.DataFrame:
    urls = detail_urls_from_manifest()
    url_list = "[" + ",".join(sql_lit(u) for u in urls) + "]"
    where = detail_filter_where_sql(active or {}, special or {})
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        q = f"SELECT * FROM read_parquet({url_list}, union_by_name=true) {where} LIMIT {int(max_rows)}"
        return con.execute(q).df()
    finally:
        try: con.close()
        except Exception: pass


def duckdb_detail_group(active: dict, special: dict | None, field: str, limit: int = 20) -> pd.DataFrame:
    urls = detail_urls_from_manifest()
    url_list = "[" + ",".join(sql_lit(u) for u in urls) + "]"
    where = detail_filter_where_sql(active or {}, special or {})
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        q = f"""
            SELECT CAST({sql_ident(field)} AS VARCHAR) AS label, COUNT(*) AS Voters
            FROM read_parquet({url_list}, union_by_name=true)
            {where}
            GROUP BY CAST({sql_ident(field)} AS VARCHAR)
            ORDER BY Voters DESC
            LIMIT {int(limit)}
        """
        return con.execute(q).df()
    except Exception:
        return pd.DataFrame(columns=["label", "Voters"])
    finally:
        try: con.close()
        except Exception: pass


def dataframe_to_excel_bytes(df: pd.DataFrame, area_level: str = "Municipality") -> bytes:
    bio = io.BytesIO()
    if df is None:
        df = pd.DataFrame()
    area_col = area_level if area_level in df.columns else ("Precinct" if "Precinct" in df.columns else ("Municipality" if "Municipality" in df.columns else None))
    if area_col and not df.empty:
        counts = df.groupby(area_col, dropna=False).size().reset_index(name="Voters").sort_values(area_col, ascending=True)
    else:
        counts = pd.DataFrame(columns=[area_level, "Voters"])
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        counts.to_excel(writer, sheet_name="Area Counts", index=False)
        df.to_excel(writer, sheet_name="Data", index=False)
    bio.seek(0)
    return bio.getvalue()


def render_group_bar(active: dict, field: str, title: str, order: list[str] | None = None):
    """Native Streamlit grouped Party/Gender bars. No iframe/component, so no clipping."""
    special = {k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}
    df = duckdb_count_cube_group_filtered(
        json.dumps(count_safe_filters(with_campaign_boundary(active or {})), sort_keys=True),
        json.dumps(special or {}, sort_keys=True),
        field,
        20,
    )
    if df.empty or "Voters" not in df.columns:
        return
    df["label"] = df["label"].astype(str).str.strip()
    df = df[~df["label"].str.lower().isin(["", "(blank)", "blank", "nan", "none", "null"])]
    df = df[df["Voters"].fillna(0).astype(float) > 0]
    if df.empty:
        return
    if order:
        sortmap = {str(v): i for i, v in enumerate(order)}
        df["_sort"] = df["label"].map(lambda x: sortmap.get(str(x), 999))
        df = df.sort_values(["_sort", "label"])
    total = float(df["Voters"].sum() or 1)

    def _bar_color(label: str) -> str:
        l = str(label).strip().upper()
        if field == "Party" or "Party" in title:
            if l in ("R", "REPUBLICAN"):
                return "#d51f2a"
            if l in ("D", "DEMOCRAT"):
                return "#2454d6"
            return "#4c9a2a"
        if field == "Gender" or "Gender" in title:
            if l in ("F", "FEMALE"):
                return "#d51f2a"
            if l in ("M", "MALE"):
                return "#2454d6"
            return "#4c9a2a"
        return "#d51f2a"

    def _display_label(label: str) -> str:
        l = str(label).strip()
        u = l.upper()
        if field == "Party":
            return {"R": "Republican", "D": "Democrat", "O": "Other / Unaffiliated"}.get(u, l)
        if field == "Gender":
            return {"F": "Female", "M": "Male", "U": "Unknown / Other", "UNKNOWN": "Unknown / Other"}.get(u, l)
        return l

    rows = []
    for _, r in df.iterrows():
        lab = str(r["label"])
        label = _display_label(lab)
        val = int(r["Voters"] or 0)
        color = _bar_color(lab)
        rows.append((label, val, color))
    _cc_bar_component(title, rows, int(total))


def election_method_sql(selected_cols: list[str], methods: list[str]) -> str:
    if not selected_cols:
        return "(FALSE)"
    method_vals = [str(m).strip().upper() for m in (methods or []) if str(m).strip()]
    col_checks = []
    for c in selected_cols:
        expr = f"UPPER(CAST({sql_ident(c)} AS VARCHAR))"
        if not method_vals:
            col_checks.append(f"({expr} NOT IN ('', 'NAN', 'NONE', 'NULL', '0', 'N', 'NO'))")
            continue
        tests = []
        for m in method_vals:
            if m == "VOTED":
                tests.append(f"({expr} NOT IN ('', 'NAN', 'NONE', 'NULL', '0', 'N', 'NO'))")
            elif m == "MAIL":
                tests.append(f"({expr} LIKE '%MAIL%' OR {expr} IN ('M','MB'))")
            elif m == "ABSENTEE":
                tests.append(f"({expr} LIKE '%ABS%' OR {expr} = 'A')")
            elif m == "POLLS":
                tests.append(f"({expr} LIKE '%POLL%' OR {expr} LIKE '%PERSON%' OR {expr} = 'P')")
            elif m == "PROVISIONAL":
                tests.append(f"({expr} LIKE '%PROV%')")
            else:
                tests.append(f"({expr} = {sql_lit(m)})")
        col_checks.append("(" + " OR ".join(tests) + ")")
    return "(" + " OR ".join(col_checks) + ")"


def index_contact_flag_sql(field: str, vals: list[str]) -> str | None:
    """Translate count-cube contact flags to real columns in lightweight index shards.
    Step 8 index shards include Mobile/Landline/Email/Current_ApplicantPhone,
    not HasMobile/HasLandline/HasEmail/HasApplicantPhone.
    """
    col_map = {
        "HasMobile": "Mobile",
        "HasLandline": "Landline",
        "HasEmail": "Email",
        "HasApplicantPhone": "Current_ApplicantPhone",
    }
    col = col_map.get(field)
    if not col:
        return None
    has_expr = f"NULLIF(TRIM(CAST({sql_ident(col)} AS VARCHAR)), '') IS NOT NULL"
    wanted = {str(v).strip().lower() for v in (vals or []) if str(v).strip()}
    parts = []
    if wanted & {"yes", "y", "true", "1"}:
        parts.append(f"({has_expr})")
    if wanted & {"no", "n", "false", "0"}:
        parts.append(f"(NOT ({has_expr}))")
    return "(" + " OR ".join(parts) + ")" if parts else None


def index_phone_reach_sql(mode: str) -> str | None:
    mobile = "NULLIF(TRIM(CAST(\"Mobile\" AS VARCHAR)), '') IS NOT NULL"
    landline = "NULLIF(TRIM(CAST(\"Landline\" AS VARCHAR)), '') IS NOT NULL"
    mode = str(mode or "").strip()
    if mode == "Mobile only":
        return f"(({mobile}) AND NOT ({landline}))"
    if mode == "Landline only":
        return f"(({landline}) AND NOT ({mobile}))"
    if mode == "Mobile OR landline":
        return f"(({mobile}) OR ({landline}))"
    if mode == "Mobile AND landline":
        return f"(({mobile}) AND ({landline}))"
    if mode == "No mobile or landline":
        return f"(NOT (({mobile}) OR ({landline})))"
    return None


def index_where_sql(active: dict, special: dict | None = None) -> str:
    clauses = []
    for field, vals in (active or {}).items():
        if not vals:
            continue
        cleaned = [str(v) for v in vals if str(v).strip()]
        if not cleaned:
            continue
        if field == "Tags":
            tag_expr = f"LOWER(CAST({sql_ident(field)} AS VARCHAR))"
            tag_clauses = [f"{tag_expr} LIKE {sql_lit('%' + v.lower().replace(chr(39), chr(39)+chr(39)) + '%')}" for v in cleaned]
            clauses.append("(" + " OR ".join(tag_clauses) + ")")
        else:
            contact_clause = index_contact_flag_sql(field, cleaned)
            if contact_clause:
                clauses.append(contact_clause)
            else:
                clauses.append(f"CAST({sql_ident(field)} AS VARCHAR) IN (" + ",".join(sql_lit(v) for v in cleaned) + ")")

    special = special or {}
    for field, rule in special.items():
        if field == "__ElectionFilters":
            continue
        if field == "__PhoneReach":
            phone_clause = index_phone_reach_sql(str(rule))
            if phone_clause:
                clauses.append(phone_clause)
            continue
        if str(field).startswith("__"):
            continue
        if isinstance(rule, dict):
            expr = f"TRY_CAST({sql_ident(field)} AS DOUBLE)"
            if "min" in rule:
                clauses.append(f"{expr} >= {float(rule['min'])}")
            if "max" in rule:
                clauses.append(f"{expr} <= {float(rule['max'])}")

    ef = special.get("__ElectionFilters")
    if isinstance(ef, dict):
        cols = selected_election_columns(ef.get("years") or [], ef.get("types") or [])
        if cols:
            clauses.append(election_method_sql(cols, ef.get("methods") or []))
        elif ef.get("years") or ef.get("types") or ef.get("methods"):
            clauses.append("(FALSE)")

    return " WHERE " + " AND ".join(clauses) if clauses else ""


@st.cache_data(ttl=300, show_spinner=False)
def duckdb_index_summary(active_json: str, special_json: str) -> dict:
    active = enforce_security_scope(json.loads(active_json or "{}"))
    special = json.loads(special_json or "{}")
    urls = index_urls_from_manifest()
    where = index_where_sql(active, special)
    url_list = "[" + ",".join(sql_lit(u) for u in urls) + "]"
    query = f"""
        SELECT CAST(Party AS VARCHAR) AS Party, COUNT(*) AS Voters
        FROM read_parquet({url_list})
        {where}
        GROUP BY CAST(Party AS VARCHAR)
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try:
                con.execute("LOAD httpfs;")
            except Exception:
                pass
        df = con.execute(query).df()
    finally:
        try:
            con.close()
        except Exception:
            pass
    return summarize_from_df(df, row_count_mode=False)


def requires_remote_index_count(active: dict, special: dict) -> bool:
    if active.get("Tags"):
        return True
    if (special or {}).get("__ElectionFilters"):
        return True
    return False


@st.cache_data(ttl=600, show_spinner=False)
def _get_bytes_from_base(base_url: str, key: str) -> bytes:
    r = requests.get(f"{str(base_url).rstrip('/')}/{key.lstrip('/')}", timeout=120)
    r.raise_for_status()
    return r.content


def get_bytes(key: str) -> bytes:
    # Cache must include the active data base URL. Otherwise an Admin statewide
    # cache entry can be reused for a campaign mini dataset, causing slow/spinning
    # campaign logins.
    return _get_bytes_from_base(current_data_base_url(), key)


@st.cache_data(ttl=600, show_spinner=False)
def _load_manifest_from_base(base_url: str):
    return json.loads(_get_bytes_from_base(base_url, "dataset_manifest.json").decode("utf-8"))


def load_manifest():
    return _load_manifest_from_base(current_data_base_url())


@st.cache_data(ttl=600, show_spinner=False)
def _load_parquet_from_base(base_url: str, key: str, columns_tuple=None) -> pd.DataFrame:
    columns = list(columns_tuple) if columns_tuple else None
    return pd.read_parquet(io.BytesIO(_get_bytes_from_base(base_url, key)), columns=columns)


def load_parquet(key: str, columns=None) -> pd.DataFrame:
    columns_tuple = tuple(columns) if columns is not None else None
    return _load_parquet_from_base(current_data_base_url(), key, columns_tuple)


@st.cache_data(ttl=600, show_spinner=False)
def _load_filter_layer_from_base(base_url: str):
    """Load only the small filter layer needed to draw the UI for one dataset base."""
    manifest = _load_manifest_from_base(base_url)
    speed = manifest.get("speed", {}).get("tables", {})
    filter_options_key = speed.get("filter_options", "speed/filter_options.parquet")
    try:
        filter_options = _load_parquet_from_base(base_url, filter_options_key, None)
    except Exception:
        # Campaign mini datasets may not include filter_options yet. Fall back to
        # the statewide filter layer for dropdown labels; hard security scope still
        # prevents access outside the campaign universe.
        filter_options = _load_parquet_from_base(R2, "speed/filter_options.parquet", None)
    geo_hierarchy = pd.DataFrame()
    return manifest, filter_options, geo_hierarchy


def load_filter_layer():
    return _load_filter_layer_from_base(current_data_base_url())


def r2_content_length(key: str) -> int:
    try:
        r = requests.head(r2_url(key), timeout=20)
        r.raise_for_status()
        return int(r.headers.get("Content-Length", "0") or 0)
    except Exception:
        return 0


@st.cache_data(ttl=600, show_spinner=False)
def load_geo_hierarchy_safe(max_bytes: int = 90_000_000) -> pd.DataFrame:
    """Optional dependent geo table.

    This keeps Create Universe from crashing: if the R2 geo_hierarchy file is too
    large or unavailable, the app falls back to flat filter_options instead of
    killing the Streamlit process.
    """
    try:
        manifest = load_manifest()
        speed = manifest.get("speed", {}).get("tables", {})
        key = speed.get("geo_hierarchy", "speed/geo_hierarchy.parquet")
        size = r2_content_length(key)
        if size and size > max_bytes:
            return pd.DataFrame()
        return load_parquet(key)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _load_count_cube_columns_from_base(base_url: str, cols_tuple):
    """Read only requested columns from the quick-count cube for one dataset base."""
    manifest = _load_manifest_from_base(base_url)
    speed = manifest.get("speed", {}).get("tables", {})
    key = speed.get("count_cube", "speed/count_cube.parquet")
    return _load_parquet_from_base(base_url, key, tuple(cols_tuple))


def load_count_cube_columns(cols_tuple):
    return _load_count_cube_columns_from_base(current_data_base_url(), tuple(cols_tuple))


@st.cache_data(ttl=600, show_spinner=False)
def _load_index_columns_from_base(base_url: str, key: str, cols_tuple):
    return _load_parquet_from_base(base_url, key, tuple(cols_tuple))


def load_index_columns(key: str, cols_tuple):
    return _load_index_columns_from_base(current_data_base_url(), key, tuple(cols_tuple))


@st.cache_data(ttl=600, show_spinner=False)
def _load_detail_columns_from_base(base_url: str, key: str, cols_tuple):
    return _load_parquet_from_base(base_url, key, tuple(cols_tuple))


def load_detail_columns(key: str, cols_tuple):
    return _load_detail_columns_from_base(current_data_base_url(), key, tuple(cols_tuple))


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


def current_filter_suffix() -> int:
    return int(st.session_state.get("filter_reset_token", 0))


def filter_key(field: str) -> str:
    return f"filter_{field}_{current_filter_suffix()}"


def special_key(name: str) -> str:
    return f"{name}_{current_filter_suffix()}"


SAVED_UNIVERSES_PARAM = "cc_saved_universes"

def _json_safe_saved_universes(saved):
    """Return saved universes as plain JSON-safe dict/list/scalar values."""
    if not isinstance(saved, dict):
        return {}
    clean = {}
    for name, data in saved.items():
        if not str(name).strip() or not isinstance(data, dict):
            continue
        clean[str(name)] = {
            "filters": data.get("filters") or {},
            "special": data.get("special") or {},
        }
    return clean


def encode_saved_universes(saved) -> str:
    try:
        payload = json.dumps(_json_safe_saved_universes(saved), separators=(",", ":"), ensure_ascii=False)
        return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")
    except Exception:
        return ""


def decode_saved_universes(raw):
    try:
        if isinstance(raw, list):
            raw = raw[0] if raw else ""
        raw = str(raw or "").strip()
        if not raw:
            return {}
        payload = base64.urlsafe_b64decode(raw.encode("ascii") + b"=" * (-len(raw) % 4)).decode("utf-8")
        data = json.loads(payload)
        return _json_safe_saved_universes(data)
    except Exception:
        return {}



CORRECTIONS_PARAM = "cc_voter_corrections"

def _json_safe_corrections(corrections):
    """Return voter corrections as JSON-safe durable data."""
    if not isinstance(corrections, dict):
        return {}
    clean = {}
    for vid, payload in corrections.items():
        vid_s = str(vid or "").strip()
        if not vid_s or not isinstance(payload, dict):
            continue
        fields = payload.get("fields") or {}
        if not isinstance(fields, dict):
            fields = {}
        clean[vid_s] = {
            "updated_at": str(payload.get("updated_at", "")),
            "fields": {str(k): cc_text(v) for k, v in fields.items()},
            "notes": cc_text(payload.get("notes", "")),
        }
    return clean

def encode_corrections(corrections) -> str:
    try:
        payload = json.dumps(_json_safe_corrections(corrections), separators=(",", ":"), ensure_ascii=False)
        return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")
    except Exception:
        return ""

def decode_corrections(raw):
    try:
        if isinstance(raw, list):
            raw = raw[0] if raw else ""
        raw = str(raw or "").strip()
        if not raw:
            return {}
        payload = base64.urlsafe_b64decode(raw.encode("ascii") + b"=" * (-len(raw) % 4)).decode("utf-8")
        return _json_safe_corrections(json.loads(payload))
    except Exception:
        return {}

@st.cache_data(ttl=30, show_spinner=False)
def _load_remote_app_state() -> dict:
    """Read durable app_state uploaded to R2 by Pipeline Manager, when available."""
    state = {}
    try:
        r = requests.get(root_r2_url("app_state/saved_universes.json"), timeout=10)
        if r.ok:
            state["saved_universes"] = _json_safe_saved_universes(r.json())
    except Exception:
        pass
    try:
        r = requests.get(root_r2_url("app_state/voter_record_corrections.json"), timeout=10)
        if r.ok:
            raw = r.json()
            # Accept either direct correction-store JSON or a rows/list export.
            if isinstance(raw, dict):
                state["voter_corrections"] = _json_safe_corrections(raw)
    except Exception:
        pass
    try:
        # Security/account store. Try the newer clearer name first, then the earlier export name.
        for _security_key in ("app_state/security_store.json", "app_state/security_users.json"):
            r = requests.get(root_r2_url(_security_key), timeout=10)
            if r.ok:
                raw = r.json()
                if isinstance(raw, dict):
                    state["security"] = raw
                    break
    except Exception:
        pass
    return state

def _state_file_candidates():
    """Local persistence candidates for saved universes/corrections.

    Keep both the clean production filename and the earlier DEV filename so
    saved universes/voter edits made before the LIVE cleanup are not orphaned
    after redeploy/reboot.
    """
    out = []
    names = [".candidate_connect_state.json", ".candidate_connect_dev_state.json"]
    for name in names:
        try:
            out.append(Path.cwd() / name)
        except Exception:
            pass
        try:
            out.append(Path.home() / name)
        except Exception:
            pass
        try:
            out.append(Path("/tmp") / name)
        except Exception:
            pass
    # de-duplicate while preserving order
    deduped = []
    seen = set()
    for path in out:
        s = str(path)
        if s not in seen:
            deduped.append(path)
            seen.add(s)
    return deduped


def _security_store_prefer_newer(remote_security, local_security):
    """Choose the safest security store when both R2 and local temp state exist.

    v17 rule: R2 is the durable source of truth for account/campaign security.
    The queue worker updates app_state/security_store.json after a campaign build.
    Streamlit Cloud local/temp files can have timestamps from a different clock/timezone
    and can otherwise mask that worker update, leaving campaigns stuck as pending_build.
    So when a valid remote store exists, prefer it. Local is only a fallback when R2
    security_store.json is missing or empty.
    """
    if not isinstance(remote_security, dict):
        remote_security = {}
    if not isinstance(local_security, dict):
        local_security = {}
    remote_users = remote_security.get("users") if isinstance(remote_security.get("users"), dict) else {}
    remote_campaigns = remote_security.get("campaigns") if isinstance(remote_security.get("campaigns"), dict) else {}
    local_users = local_security.get("users") if isinstance(local_security.get("users"), dict) else {}
    # Prefer R2 whenever it contains a real security store. R2 is updated by the app
    # on admin edits and by queue_worker.py when campaign builds finish.
    if remote_users or remote_campaigns:
        return remote_security
    if local_users:
        return local_security
    return remote_security or local_security or {}


def _load_state() -> dict:
    # Remote app_state is the durable baseline after rebuild/deploy.
    # Local/browser state can override saved universes/corrections, but security
    # must prefer the newest valid store so stale temp files do not trigger setup.
    state = _load_remote_app_state()
    remote_security = state.get("security") if isinstance(state, dict) else {}
    for path in _state_file_candidates():
        try:
            if path.exists():
                local_state = json.loads(path.read_text(encoding="utf-8")) or {}
                if isinstance(local_state, dict):
                    local_security = local_state.get("security")
                    state.update(local_state)
                    chosen_security = _security_store_prefer_newer(remote_security, local_security)
                    if chosen_security:
                        state["security"] = chosen_security
                    return state
        except Exception:
            continue
    return state


def _save_state(state: dict):
    for path in _state_file_candidates():
        try:
            path.write_text(json.dumps(state or {}, ensure_ascii=False, indent=2), encoding="utf-8")
            return True
        except Exception:
            continue
    return False


def _persist_state_section(section: str, data):
    state = _load_state()
    state[section] = data
    _save_state(state)
    try:
        load_security_store.clear()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Candidate Connect Phase 1 Security
# ---------------------------------------------------------------------------
# Phase 1 uses local app_state plus an optional R2 baseline:
#   app_state/security_users.json
# The structure is intentionally simple so it can later move behind a real
# database/API when you start selling accounts commercially.
SECURITY_ROLES = ["Super Admin", "Campaign Admin", "Manager", "Field User", "Viewer"]

ROLE_PERMISSIONS = {
    "Super Admin": {
        "create_universe": True,
        "voter_lookup": True,
        "mail_ballot_center": True,
        "area_intelligence": True,
        "exports_reports": True,
        "account_admin": True,
        "manage_all_accounts": True,
    },
    "Campaign Admin": {
        "create_universe": True,
        "voter_lookup": True,
        "mail_ballot_center": True,
        "area_intelligence": True,
        "exports_reports": True,
        "account_admin": True,
        "manage_all_accounts": False,
    },
    "Manager": {
        "create_universe": True,
        "voter_lookup": True,
        "mail_ballot_center": True,
        "area_intelligence": True,
        "exports_reports": True,
        "account_admin": False,
        "manage_all_accounts": False,
    },
    "Field User": {
        "create_universe": False,
        "voter_lookup": True,
        "mail_ballot_center": False,
        "area_intelligence": False,
        "exports_reports": False,
        "account_admin": False,
        "manage_all_accounts": False,
    },
    "Viewer": {
        "create_universe": True,
        "voter_lookup": True,
        "mail_ballot_center": True,
        "area_intelligence": True,
        "exports_reports": False,
        "account_admin": False,
        "manage_all_accounts": False,
    },
}

def _security_slug(value: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    return s or "default"

def _password_hash(username: str, password: str) -> str:
    """Simple salted hash for Phase 1. Replace with managed auth before commercial sale."""
    salt = f"candidate-connect-v1::{str(username).strip().lower()}::"
    return hashlib.sha256((salt + str(password or "")).encode("utf-8")).hexdigest()

def _generate_temp_password(length: int = 14) -> str:
    """Generate a readable one-time temporary password for admin resets."""
    alphabet = string.ascii_letters + string.digits + "!@#$%"
    while True:
        pw = "".join(secrets.choice(alphabet) for _ in range(length))
        if (any(c.islower() for c in pw) and any(c.isupper() for c in pw)
                and any(c.isdigit() for c in pw) and any(c in "!@#$%" for c in pw)):
            return pw

def _password_validation_error(password: str, confirm: str = None, old_password: str = None) -> str:
    pw = str(password or "")
    if confirm is not None and pw != str(confirm or ""):
        return "Passwords do not match."
    if len(pw) < 8:
        return "Password must be at least 8 characters."
    if old_password is not None and pw == str(old_password or ""):
        return "New password must be different from the current password."
    return ""

def _set_user_password(store: dict, username: str, password: str, *, force_change: bool = False, reset_by: str = "") -> bool:
    users = (store or {}).setdefault("users", {})
    uname = str(username or "").strip().lower()
    if not uname or uname not in users:
        return False
    users[uname]["password_hash"] = _password_hash(uname, password)
    users[uname]["force_password_change"] = bool(force_change)
    users[uname]["password_updated_at"] = datetime.now().isoformat(timespec="seconds")
    if reset_by:
        users[uname]["password_reset_by"] = str(reset_by)
        users[uname]["password_reset_at"] = datetime.now().isoformat(timespec="seconds")
    return True

def _refresh_current_session_user(store: dict, username: str = None) -> None:
    uname = str(username or current_username() or "").strip().lower()
    user = ((store or {}).get("users") or {}).get(uname)
    if user:
        st.session_state["auth_user"] = user

def _user_must_change_password() -> bool:
    return bool((current_user() or {}).get("force_password_change"))


# ---------------------------------------------------------------------------
# Temporary Gmail SMTP Forgot Password workflow (v19)
# ---------------------------------------------------------------------------
def _smtp_setting(*names: str) -> str:
    for name in names:
        val = _get_secret_value(name)
        if val:
            return str(val).strip()
    return ""


def _password_reset_code_hash(username: str, code: str) -> str:
    uname = str(username or "").strip().lower()
    clean_code = re.sub(r"\D+", "", str(code or ""))
    return hashlib.sha256(f"candidate-connect-reset-v1::{uname}::{clean_code}".encode("utf-8")).hexdigest()


def _find_password_reset_user(store: dict, identifier: str) -> tuple[str, dict]:
    ident = str(identifier or "").strip().lower()
    if not ident:
        return "", {}
    users = (store or {}).get("users") or {}
    if ident in users:
        return ident, users.get(ident) or {}
    for uname, user in users.items():
        email = str((user or {}).get("email") or "").strip().lower()
        if email and email == ident:
            return str(uname).strip().lower(), user or {}
    return "", {}


def _send_smtp_email(to_email: str, subject: str, body: str) -> tuple[bool, str]:
    """Send a plain text email through SMTP. Gmail testing uses an App Password."""
    to_email = str(to_email or "").strip()
    if not to_email or "@" not in to_email:
        return False, "No valid email is saved for this user."
    host = _smtp_setting("SMTP_HOST", "GMAIL_SMTP_HOST") or "smtp.gmail.com"
    port_raw = _smtp_setting("SMTP_PORT", "GMAIL_SMTP_PORT") or "465"
    try:
        port = int(port_raw)
    except Exception:
        port = 465
    smtp_user = _smtp_setting("SMTP_USER", "GMAIL_SMTP_USER", "EMAIL_USER")
    smtp_password = _smtp_setting("SMTP_PASSWORD", "GMAIL_SMTP_APP_PASSWORD", "EMAIL_PASSWORD")
    from_email = _smtp_setting("SMTP_FROM", "GMAIL_SMTP_FROM") or smtp_user
    from_name = _smtp_setting("SMTP_FROM_NAME") or "Candidate Connect"
    if not smtp_user or not smtp_password or not from_email:
        return False, "SMTP is not configured. Add SMTP_USER and SMTP_PASSWORD/GMAIL_SMTP_APP_PASSWORD."
    try:
        import smtplib
        import ssl
        from email.message import EmailMessage
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = to_email
        msg.set_content(body)
        if port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, context=context, timeout=30) as server:
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=30) as server:
                server.starttls(context=ssl.create_default_context())
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
        return True, "sent"
    except Exception as exc:
        return False, str(exc)


def _request_password_reset(store: dict, identifier: str) -> tuple[bool, str]:
    """Create and email a one-time code. Returns generic success for account privacy."""
    uname, user = _find_password_reset_user(store, identifier)
    if not uname or not user or user.get("disabled") or user.get("pending_approval"):
        # Do not reveal whether the account exists.
        return True, "If that account exists and has an email, a reset code was sent."
    to_email = str(user.get("email") or "").strip()
    if not to_email:
        return False, "That account does not have an email address saved. Ask an admin to reset the password."
    code = f"{secrets.randbelow(1000000):06d}"
    expires = datetime.now() + timedelta(minutes=30)
    resets = store.setdefault("password_resets", {})
    resets[uname] = {
        "code_hash": _password_reset_code_hash(uname, code),
        "expires_at": expires.isoformat(timespec="seconds"),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "requested_for": uname,
    }
    save_security_store(store)
    body = (
        "Candidate Connect password reset\n\n"
        f"Your one-time reset code is: {code}\n\n"
        "This code expires in 30 minutes. If you did not request this, you can ignore this email.\n"
    )
    ok, msg = _send_smtp_email(to_email, "Candidate Connect password reset code", body)
    if ok:
        return True, "Reset code sent. Check your email."
    # Remove unusable code if email failed.
    try:
        store.get("password_resets", {}).pop(uname, None)
        save_security_store(store)
    except Exception:
        pass
    return False, f"Could not send email: {msg}"


def _complete_password_reset(store: dict, identifier: str, code: str, new_password: str, confirm_password: str) -> tuple[bool, str]:
    uname, user = _find_password_reset_user(store, identifier)
    if not uname or not user:
        return False, "Invalid reset code or account."
    err = _password_validation_error(new_password, confirm_password)
    if err:
        return False, err
    reset = ((store or {}).get("password_resets") or {}).get(uname) or {}
    if not reset:
        return False, "Invalid or expired reset code."
    try:
        expires_at = datetime.fromisoformat(str(reset.get("expires_at") or ""))
    except Exception:
        expires_at = datetime.min
    if datetime.now() > expires_at:
        try:
            store.get("password_resets", {}).pop(uname, None)
            save_security_store(store)
        except Exception:
            pass
        return False, "Reset code expired. Request a new one."
    if str(reset.get("code_hash") or "") != _password_reset_code_hash(uname, code):
        return False, "Invalid reset code."
    if not _set_user_password(store, uname, new_password, force_change=False, reset_by="self_service_email_reset"):
        return False, "Could not reset password."
    try:
        store.get("password_resets", {}).pop(uname, None)
    except Exception:
        pass
    save_security_store(store)
    return True, "Password changed. You can log in now."


def render_forgot_password_panel(store: dict):
    with st.expander("Forgot password?"):
        st.caption("Temporary Gmail SMTP reset for testing. A one-time code will be emailed to the address saved on the account.")
        tab_request, tab_reset = st.tabs(["Send code", "Use code"])
        with tab_request:
            with st.form("forgot_password_request_form"):
                ident = st.text_input("Username or email", key="forgot_password_identifier")
                send = st.form_submit_button("Send reset code", type="primary")
            if send:
                ok, msg = _request_password_reset(store, ident)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
        with tab_reset:
            with st.form("forgot_password_complete_form"):
                ident2 = st.text_input("Username or email", key="forgot_password_identifier_2")
                code = st.text_input("Reset code", key="forgot_password_code")
                pw1 = st.text_input("New password", type="password", key="forgot_password_pw1")
                pw2 = st.text_input("Confirm new password", type="password", key="forgot_password_pw2")
                reset = st.form_submit_button("Change password", type="primary")
            if reset:
                ok, msg = _complete_password_reset(store, ident2, code, pw1, pw2)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)

def _empty_security_store() -> dict:
    return {"users": {}, "campaigns": {}, "version": 1, "updated_at": datetime.now().isoformat(timespec="seconds")}

@st.cache_data(ttl=10, show_spinner=False)
def load_security_store() -> dict:
    raw = (_load_state().get("security") or {})
    if not isinstance(raw, dict):
        raw = {}
    raw.setdefault("users", {})
    raw.setdefault("campaigns", {})
    raw.setdefault("version", 1)
    return raw

def _get_secret_value(name: str) -> str | None:
    try:
        val = st.secrets.get(name)  # type: ignore[attr-defined]
        if val:
            return str(val)
    except Exception:
        pass
    return os.environ.get(name)


def _write_security_store_to_r2(store: dict) -> tuple[bool, str]:
    """Best-effort durable sync for Account Admin changes.

    Approval/activation should survive app reboot without manually downloading and
    re-uploading security_store.json. If write credentials are not configured,
    the local/export backup still works and the UI shows the manual fallback.
    """
    try:
        import boto3  # type: ignore
    except Exception as exc:
        return False, f"boto3 not available: {exc}"

    endpoint_url = _get_secret_value("R2_ENDPOINT_URL") or _get_secret_value("CLOUDFLARE_R2_ENDPOINT_URL")
    account_id = _get_secret_value("R2_ACCOUNT_ID") or _get_secret_value("CLOUDFLARE_ACCOUNT_ID")
    if not endpoint_url and account_id:
        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
    access_key = _get_secret_value("R2_ACCESS_KEY_ID") or _get_secret_value("AWS_ACCESS_KEY_ID")
    secret_key = _get_secret_value("R2_SECRET_ACCESS_KEY") or _get_secret_value("AWS_SECRET_ACCESS_KEY")
    try:
        _target_name, bucket, _public = _r2_bucket_for_current_app()
    except Exception:
        bucket = _get_secret_value("R2_BUCKET_NAME") or _get_secret_value("CANDIDATE_CONNECT_R2_BUCKET") or _get_secret_value("CANDIDATE_CONNECT_DEV_BUCKET") or "candidate-connect-data-dev"
    if not endpoint_url or not access_key or not secret_key or not bucket:
        return False, "R2 write credentials/bucket not configured"
    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )
        body = json.dumps(store or {}, ensure_ascii=False, indent=2).encode("utf-8")
        client.put_object(Bucket=bucket, Key="app_state/security_store.json", Body=body, ContentType="application/json")
        return True, f"Synced security_store.json to R2 bucket {bucket}"
    except Exception as exc:
        return False, str(exc)



def _r2_bucket_for_current_app() -> tuple[str, str, str]:
    """Return (target_name, bucket, public_url) for this app instance.

    DEV and LIVE apps should keep separate app_state/security_store.json and
    app_state/build_queue/*.json files. We infer from the public R2 base URL,
    with secrets/env overrides available for deployments.
    """
    public = str(R2 or "").rstrip("/")
    dev_public = "https://pub-376c4497d59b4a7988a8af29700531e0.r2.dev"
    live_public = "https://pub-a9e33b718082407cbd85e7b86b0fcb5c.r2.dev"
    env_name = (_get_secret_value("CANDIDATE_CONNECT_ENV") or _get_secret_value("APP_ENV") or "").strip().upper()
    if env_name == "LIVE" or public == live_public:
        return "LIVE", (_get_secret_value("R2_LIVE_BUCKET_NAME") or _get_secret_value("CANDIDATE_CONNECT_LIVE_BUCKET") or "candidate-connect-data"), live_public
    return "DEV", (_get_secret_value("R2_DEV_BUCKET_NAME") or _get_secret_value("CANDIDATE_CONNECT_DEV_BUCKET") or _get_secret_value("R2_BUCKET_NAME") or "candidate-connect-data-dev"), dev_public


def _put_json_to_r2_key(key: str, payload: dict) -> tuple[bool, str]:
    """Best-effort JSON upload to the current app's R2 bucket."""
    try:
        import boto3  # type: ignore
    except Exception as exc:
        return False, f"boto3 not available: {exc}"
    endpoint_url = _get_secret_value("R2_ENDPOINT_URL") or _get_secret_value("CLOUDFLARE_R2_ENDPOINT_URL")
    account_id = _get_secret_value("R2_ACCOUNT_ID") or _get_secret_value("CLOUDFLARE_ACCOUNT_ID")
    if not endpoint_url and account_id:
        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
    access_key = _get_secret_value("R2_ACCESS_KEY_ID") or _get_secret_value("AWS_ACCESS_KEY_ID")
    secret_key = _get_secret_value("R2_SECRET_ACCESS_KEY") or _get_secret_value("AWS_SECRET_ACCESS_KEY")
    target_name, bucket, _public = _r2_bucket_for_current_app()
    if not endpoint_url or not access_key or not secret_key or not bucket:
        return False, "R2 write credentials/bucket not configured"
    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )
        body = json.dumps(payload or {}, ensure_ascii=False, indent=2).encode("utf-8")
        client.put_object(Bucket=bucket, Key=str(key).lstrip("/"), Body=body, ContentType="application/json")
        return True, f"Synced {key} to {target_name} R2 bucket {bucket}"
    except Exception as exc:
        return False, str(exc)


def enqueue_campaign_build(campaign_id: str, reason: str = "campaign_approved") -> tuple[bool, str]:
    """Create a tiny R2 build request so the queue worker can build the campaign dataset.

    R2 is storage only; this does not build the dataset by itself. The local or
    hosted queue worker processes app_state/build_queue/*.json and marks the
    campaign active/uploaded after the build succeeds.
    """
    cid = _campaign_slug(campaign_id or "")
    if not cid:
        return False, "Missing campaign_id"
    target_name, bucket, public = _r2_bucket_for_current_app()
    payload = {
        "job_type": "build_campaign_dataset",
        "campaign_id": cid,
        "target": target_name,
        "bucket": bucket,
        "public_url": public,
        "reason": reason,
        "status": "queued",
        "requested_at": datetime.now().isoformat(timespec="seconds"),
        "requested_by": current_username() if 'current_username' in globals() else "",
    }
    return _put_json_to_r2_key(f"app_state/build_queue/{cid}.json", payload)

def _r2_client_for_current_app():
    """Return (client, target_name, bucket) for current app R2 writes/deletes."""
    try:
        import boto3  # type: ignore
    except Exception as exc:
        return None, "", "", f"boto3 not available: {exc}"
    endpoint_url = _get_secret_value("R2_ENDPOINT_URL") or _get_secret_value("CLOUDFLARE_R2_ENDPOINT_URL")
    account_id = _get_secret_value("R2_ACCOUNT_ID") or _get_secret_value("CLOUDFLARE_ACCOUNT_ID")
    if not endpoint_url and account_id:
        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
    access_key = _get_secret_value("R2_ACCESS_KEY_ID") or _get_secret_value("AWS_ACCESS_KEY_ID")
    secret_key = _get_secret_value("R2_SECRET_ACCESS_KEY") or _get_secret_value("AWS_SECRET_ACCESS_KEY")
    target_name, bucket, _public = _r2_bucket_for_current_app()
    if not endpoint_url or not access_key or not secret_key or not bucket:
        return None, target_name, bucket, "R2 write credentials/bucket not configured"
    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )
        return client, target_name, bucket, ""
    except Exception as exc:
        return None, target_name, bucket, str(exc)


def _delete_r2_prefix(prefix: str) -> tuple[bool, str]:
    """Delete every R2 object under prefix in the current app bucket."""
    client, target_name, bucket, err = _r2_client_for_current_app()
    if client is None:
        return False, err
    prefix = str(prefix or "").lstrip("/")
    if not prefix:
        return False, "Refusing to delete empty R2 prefix"
    try:
        deleted = 0
        token = None
        while True:
            kwargs = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = client.list_objects_v2(**kwargs)
            objs = [{"Key": o["Key"]} for o in (resp.get("Contents") or []) if o.get("Key")]
            if objs:
                client.delete_objects(Bucket=bucket, Delete={"Objects": objs, "Quiet": True})
                deleted += len(objs)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return True, f"Deleted {deleted} object(s) from {target_name} R2 under {prefix}"
    except Exception as exc:
        return False, str(exc)


def _delete_r2_key(key: str) -> tuple[bool, str]:
    client, target_name, bucket, err = _r2_client_for_current_app()
    if client is None:
        return False, err
    key = str(key or "").lstrip("/")
    if not key:
        return False, "Refusing to delete empty R2 key"
    try:
        client.delete_object(Bucket=bucket, Key=key)
        return True, f"Deleted {key} from {target_name} R2"
    except Exception as exc:
        return False, str(exc)


def refresh_security_admin_view():
    """Clear cached app/security state and rerun without logging the user out."""
    for fn_name in ("load_security_store", "_load_remote_app_state", "_load_state"):
        try:
            fn = globals().get(fn_name)
            if fn and hasattr(fn, "clear"):
                fn.clear()
        except Exception:
            pass
    st.toast("Refreshed account/campaign state from R2 app_state.")
    st.rerun()

def save_security_store(store: dict) -> bool:
    if not isinstance(store, dict):
        store = _empty_security_store()
    store.setdefault("users", {})
    store.setdefault("campaigns", {})
    store["updated_at"] = datetime.now().isoformat(timespec="seconds")
    _persist_state_section("security", store)

    # Also write a plain backup file wherever the app has write access.
    # In Streamlit Cloud this is temporary, but it gives Super Admins a clean file
    # to download/export and manually place in R2 app_state/security_store.json.
    try:
        Path("security_store.json").write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    try:
        Path("/tmp/security_store.json").write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

    ok_r2, msg_r2 = _write_security_store_to_r2(store)
    try:
        load_security_store.clear()
    except Exception:
        pass
    try:
        _load_remote_app_state.clear()
    except Exception:
        pass
    try:
        st.session_state["security_store_r2_sync"] = {"ok": ok_r2, "message": msg_r2, "at": datetime.now().isoformat(timespec="seconds")}
    except Exception:
        pass
    return True


def campaign_boundary_filters() -> dict:
    """Return the logged-in user's immutable campaign boundary.

    Campaign users may only see/filter/select values inside this boundary.
    Super Admin remains unrestricted.
    """
    try:
        user = current_user() or {}
        if is_super_admin():
            return {}
        boundary = user.get("scope_filters") or {}
        if not boundary:
            cid = str(user.get("campaign_id") or "").strip()
            campaign_name = str(user.get("campaign") or "").strip()
            store = load_security_store()
            campaigns = store.get("campaigns") or {}
            campaign = campaigns.get(cid) if cid else None
            if not campaign and campaign_name:
                for rec in campaigns.values():
                    if str((rec or {}).get("campaign_name") or "").strip() == campaign_name:
                        campaign = rec
                        break
            boundary = (campaign or {}).get("scope_filters") or {}
        if not isinstance(boundary, dict):
            return {}
        return {k: v for k, v in boundary.items() if v not in (None, "", [], {})}
    except Exception:
        return {}


def option_filters_for_field(current_filters: dict | None, field: str) -> dict:
    """Filters to use when populating one dropdown's options.

    Drop the field being populated so users can change it, then re-apply the
    campaign boundary so downstream geo lists never escape the campaign.
    """
    f = dict(current_filters or {})
    f.pop(field, None)
    return with_campaign_boundary(f)




def campaign_scoped_option_values(field: str, current_filters: dict | None = None, limit: int = 5000) -> list[str]:
    """Return dropdown options scoped to the campaign hard boundary."""
    try:
        f = option_filters_for_field(current_filters or {}, field)
        df = duckdb_count_cube_group_filtered(
            json.dumps(count_safe_filters(f), sort_keys=True),
            json.dumps({}, sort_keys=True),
            field,
            int(limit or 5000),
        )
        if df is None or df.empty or "label" not in df.columns:
            return []
        vals = []
        for x in df["label"].tolist():
            s = str(x).strip()
            if s and s.lower() not in ("nan", "none", "null", "(blank)", "blank"):
                vals.append(s)
        return vals
    except Exception:
        return []



def hard_scope_options(field: str, options: list, current_filters: dict | None = None) -> list:
    """Final safety gate for dropdown values.

    Even if an older option-loader returns statewide values, this intersects it
    with the values available inside the logged-in campaign's hard boundary.
    """
    try:
        if is_super_admin():
            return options
        scoped = campaign_scoped_option_values(field, current_filters or {})
        if not scoped:
            return []
        scoped_set = {str(x).strip() for x in scoped}
        return [x for x in options if str(x).strip() in scoped_set]
    except Exception:
        return options


def option_filters_for_field(current_filters: dict | None, field: str) -> dict:
    """Filters to use when populating one dropdown's options.

    The field being populated is removed so users can change that selection,
    but the campaign boundary is immediately re-applied afterward. This prevents
    values outside the campaign scope from appearing in the left pane.
    """
    f = dict(current_filters or {})
    f.pop(field, None)
    return with_campaign_boundary(f)


def with_campaign_boundary(filters: dict | None = None) -> dict:
    """Merge user-selected filters with the immutable campaign boundary."""
    merged = dict(filters or {})
    for k, v in (campaign_boundary_filters() or {}).items():
        merged[k] = v
    return merged


def enforce_campaign_boundary_on_session_filters():
    """Keep visible Create Universe filters inside the campaign boundary."""
    try:
        boundary = campaign_boundary_filters()
        if not boundary:
            return
        for k, v in boundary.items():
            vals = v if isinstance(v, list) else [v]
            vals = [x for x in vals if str(x).strip()]
            if not vals:
                continue

            # Common session-state containers used by this app over time.
            for container_key in ("active_filters", "filters", "current_filters", "universe_filters"):
                obj = st.session_state.get(container_key)
                if isinstance(obj, dict):
                    obj[k] = vals
                    st.session_state[container_key] = obj

            # Direct widget/session keys.
            for key in (k, f"filter_{k}", f"{k}_filter", f"cu_{k}", f"create_{k}", f"geo_{k}"):
                if key in st.session_state:
                    st.session_state[key] = vals
    except Exception:
        return


def current_user() -> dict:
    return dict(st.session_state.get("auth_user") or {})

def current_username() -> str:
    return str(st.session_state.get("auth_username") or "").strip().lower()

def current_role() -> str:
    return current_user().get("role", "Viewer")

def user_can(permission: str) -> bool:
    role = current_role()
    return bool(ROLE_PERMISSIONS.get(role, {}).get(permission, False))

def is_super_admin() -> bool:
    return current_role() == "Super Admin"

def is_campaign_scoped() -> bool:
    return bool(current_user()) and not is_super_admin()

def security_scope_filters() -> dict:
    """Hard boundary for non-super-admin users.

    Campaign Admin / Manager / Field User / Viewer accounts cannot query, save,
    export, or report outside these filters. Typical examples:
      {"USC": ["10"]} or {"STS": ["28"]} or {"County": ["York"], "Municipality": ["York Township"]}
    """
    if is_super_admin():
        return {}
    user = current_user()
    scope = user.get("scope_filters") or {}
    if not isinstance(scope, dict):
        return {}
    clean = {}
    for k, v in scope.items():
        vals = v if isinstance(v, list) else [v]
        vals = [str(x).strip() for x in vals if str(x).strip()]
        if vals:
            clean[str(k)] = vals
    return clean

def apply_security_scope_to_filters(filters: dict) -> dict:
    merged = {str(k): list(v or []) for k, v in (filters or {}).items() if v}
    scope = security_scope_filters()
    for field, allowed in scope.items():
        selected_vals = [str(x) for x in (merged.get(field) or [])]
        if selected_vals:
            allowed_set = set(map(str, allowed))
            narrowed = [x for x in selected_vals if x in allowed_set]
            merged[field] = narrowed or list(allowed)
        else:
            merged[field] = list(allowed)
    return merged


def enforce_security_scope(active: dict | None) -> dict:
    """Apply the logged-in user's campaign scope as a hard query boundary.

    This is intentionally used by every workspace, not just Create Universe, so
    Mail Ballot Center, Area Intelligence, exports, reports, and lookup cannot
    drift back to statewide for campaign-scoped accounts.
    """
    return apply_security_scope_to_filters(active or {})


def security_scope_label() -> str:
    scope = security_scope_filters()
    if not scope:
        return "Statewide / unrestricted"
    pieces = []
    for field in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]:
        vals = scope.get(field) or []
        if vals:
            shown = ", ".join(map(str, vals[:3]))
            if len(vals) > 3:
                shown += f" +{len(vals)-3} more"
            pieces.append(f"{DISPLAY_LABELS.get(field, field)}: {shown}")
    return " | ".join(pieces) if pieces else "Campaign scoped"

def saved_universe_state_section() -> str:
    if is_super_admin():
        return "saved_universes"
    campaign = current_user().get("campaign") or current_username() or "campaign"
    return f"saved_universes_{_security_slug(campaign)}"

def security_export_json_bytes() -> bytes:
    return json.dumps(load_security_store(), ensure_ascii=False, indent=2).encode("utf-8")


def _campaign_slug(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "-", str(name or "").strip().lower()).strip("-")
    return s or "campaign"


def campaign_dataset_base_url(campaign_id: str) -> str:
    cid = _campaign_slug(campaign_id)
    return f"{R2}/app_state/campaigns/{cid}/dataset"


def current_campaign_record() -> dict:
    try:
        user = current_user() or {}
        store = load_security_store()
        campaigns = store.get("campaigns") or {}
        cid = user.get("campaign_id") or _campaign_slug(user.get("campaign") or "")
        if cid and cid in campaigns:
            return campaigns.get(cid) or {}
    except Exception:
        pass
    return {}


def current_data_base_url() -> str:
    try:
        if not st.session_state.get("auth_user"):
            return R2
        if current_role() == "Super Admin":
            return R2
        rec = current_campaign_record()
        if str(rec.get("dataset_status") or "").lower() in {"active", "uploaded"}:
            # Always derive the mini-dataset URL from the current app R2 base.
            # This prevents a DEV app from spinning against an old LIVE dataset_base_url.
            cid = rec.get("campaign_id") or _campaign_slug(rec.get("campaign_name") or current_user().get("campaign") or "")
            if cid:
                return campaign_dataset_base_url(cid).rstrip("/")
            if rec.get("dataset_base_url"):
                return str(rec.get("dataset_base_url")).rstrip("/")
    except Exception:
        pass
    return R2


def campaign_dataset_status_label() -> str:
    try:
        rec = current_campaign_record()
        status = rec.get("dataset_status") or "statewide-filtered"
        if current_data_base_url().rstrip("/") != R2.rstrip("/"):
            return f"Campaign mini dataset active · {status}"
        return f"Using statewide dataset with enforced campaign boundary · {status}"
    except Exception:
        return "Using statewide dataset"


def upsert_campaign_record(store: dict, campaign_id: str, data: dict) -> dict:
    store.setdefault("campaigns", {})
    cid = _campaign_slug(campaign_id or data.get("campaign_name") or "")
    rec = store["campaigns"].get(cid, {})
    rec.update(data or {})
    rec["campaign_id"] = cid
    rec.setdefault("dataset_status", "not_built")
    rec.setdefault("account_status", "pending_approval")
    rec.setdefault("created_at", datetime.now().isoformat(timespec="seconds"))
    rec["updated_at"] = datetime.now().isoformat(timespec="seconds")
    rec.setdefault("dataset_base_url", campaign_dataset_base_url(cid))
    store["campaigns"][cid] = rec
    return rec




def _parse_scope_text_lines(raw: str) -> dict:
    """Parse public signup scope text into Candidate Connect scope_filters.

    Accepted examples:
      County: Allegheny
      Municipality: Dormont
      Precinct: Dormont 00 05, Dormont 00 03
    """
    scope = {}
    aliases = {
        "county": "County",
        "counties": "County",
        "municipality": "Municipality",
        "municipalities": "Municipality",
        "precinct": "Precinct",
        "precincts": "Precinct",
        "congressional": "USC",
        "congressional district": "USC",
        "state senate": "STS",
        "state senate district": "STS",
        "state house": "STH",
        "state house district": "STH",
        "school district": "School District",
        "school region": "School Region",
    }
    for line in str(raw or "").splitlines():
        if ":" not in line:
            continue
        key, val = line.split(":", 1)
        field = aliases.get(key.strip().lower())
        if not field:
            continue
        vals = [v.strip() for v in re.split(r"[,;]", val) if v.strip()]
        if vals:
            scope[field] = vals
    return scope


def _signup_options(field: str) -> list[str]:
    """Small helper for public signup dropdowns.

    Uses the statewide filter_options table when available. If the public request
    screen cannot load it, the form still works with blank/manual options.
    """
    try:
        _, filter_options, _ = load_filter_layer()
        vals = options_from_filter_table(filter_options, field)
        return [str(v).strip() for v in vals if str(v).strip()]
    except Exception:
        return []


def _signup_scope_for_campaign_type(campaign_type: str, values: dict) -> tuple[dict, bool, str]:
    """Return (scope_filters, manual_boundary_required, manual_note)."""
    t = str(campaign_type or '').strip()
    scope: dict[str, list[str]] = {}
    manual_required = False
    manual_note = ''

    def add(field: str, value):
        vals = value if isinstance(value, list) else [value]
        vals = [str(v).strip() for v in vals if str(v).strip()]
        if vals:
            scope[field] = vals

    if t == 'Municipal':
        add('County', values.get('county'))
        add('Municipality', values.get('municipality'))
    elif t == 'Countywide':
        add('County', values.get('county'))
    elif t == 'School District':
        # Do not use School Region here. SURE is too inconsistent for that.
        add('School District', values.get('school_district'))
    elif t == 'State House':
        add('STH', values.get('state_house'))
    elif t == 'State Senate':
        add('STS', values.get('state_senate'))
    elif t == 'Congressional':
        add('USC', values.get('congressional'))
    elif t == 'Statewide':
        # A true statewide account intentionally has no geography boundary.
        scope = {}
    elif t == 'Magisterial District':
        # Placeholder for later. Do not pretend the boundary is enforceable until
        # we normalize/add MDJ fields to shards/cubes.
        manual_required = True
        manual_note = 'Magisterial District requested: ' + str(values.get('magisterial') or '').strip()
    elif t == 'Custom / Other':
        scope = _parse_scope_text_lines(values.get('custom_scope') or '')
        manual_note = str(values.get('custom_scope') or '').strip()
        manual_required = not bool(scope)
    return scope, manual_required, manual_note


def render_public_campaign_signup_request(store: dict):
    """Public campaign request form with structured address + structured campaign scope."""
    with st.expander("Request Campaign Account", expanded=False):
        st.caption("Submit a campaign account request. A Super Admin must approve it before login is enabled.")

        st.markdown("#### Account")
        c1, c2 = st.columns(2)
        with c1:
            username = st.text_input("Requested username", key="signup_username")
            pw1 = st.text_input("Password", type="password", key="signup_pw1")
        with c2:
            email = st.text_input("Email", key="signup_email")
            pw2 = st.text_input("Confirm password", type="password", key="signup_pw2")

        st.markdown("#### Campaign Information")
        c1, c2 = st.columns(2)
        with c1:
            campaign_name = st.text_input("Campaign name", key="signup_campaign_name")
            office = st.text_input("Office sought", key="signup_office")
            contact_name = st.text_input("Contact name", key="signup_contact_name")
        with c2:
            phone = st.text_input("Phone", key="signup_phone")
            campaign_type = st.selectbox(
                "Campaign type",
                [
                    "Municipal",
                    "School District",
                    "Countywide",
                    "State House",
                    "State Senate",
                    "Congressional",
                    "Magisterial District",
                    "Statewide",
                    "Custom / Other",
                ],
                key="signup_campaign_type",
                help="This determines the voter universe Candidate Connect will build for the campaign.",
            )

        st.markdown("#### Campaign / Billing Address")
        a1, a2, a3, a4 = st.columns([2.3, 1.2, .6, .8])
        with a1:
            street = st.text_input("Street address", key="signup_street")
        with a2:
            city = st.text_input("City", key="signup_city")
        with a3:
            state = st.text_input("State", value="PA", key="signup_state")
        with a4:
            zip_code = st.text_input("ZIP", key="signup_zip")

        st.markdown("#### Campaign Universe / Boundary")
        st.caption("Select the geographic scope of the campaign. This determines the voter dataset we build after approval.")

        vals = {}
        if campaign_type == "Municipal":
            c1, c2 = st.columns(2)
            counties = _signup_options("County")
            munis = _signup_options("Municipality")
            with c1:
                vals["county"] = st.selectbox("County", hard_scope_options("County", [""] + counties, active if "active" in locals() else st.session_state.get("active_filters", {})), key="signup_scope_county")
            with c2:
                vals["municipality"] = st.selectbox("Municipality", hard_scope_options("Municipality", [""] + munis, active if "active" in locals() else st.session_state.get("active_filters", {})), key="signup_scope_municipality")
        elif campaign_type == "Countywide":
            counties = _signup_options("County")
            vals["county"] = st.selectbox("County", hard_scope_options("County", [""] + counties, active if "active" in locals() else st.session_state.get("active_filters", {})), key="signup_scope_countywide")
        elif campaign_type == "School District":
            sds = _signup_options("School District")
            vals["school_district"] = st.selectbox("School District", hard_scope_options("School District", [""] + sds, active if "active" in locals() else st.session_state.get("active_filters", {})), key="signup_scope_school_district")
            st.caption("School Region is intentionally not used because the SURE data is inconsistent.")
        elif campaign_type == "State House":
            vals["state_house"] = st.selectbox("State House District", [""] + _signup_options("STH"), key="signup_scope_sth")
        elif campaign_type == "State Senate":
            vals["state_senate"] = st.selectbox("State Senate District", [""] + _signup_options("STS"), key="signup_scope_sts")
        elif campaign_type == "Congressional":
            vals["congressional"] = st.selectbox("Congressional District", [""] + _signup_options("USC"), key="signup_scope_usc")
        elif campaign_type == "Magisterial District":
            vals["magisterial"] = st.text_input("Magisterial District", key="signup_scope_magisterial", help="Placeholder for MDJ district support. Super Admin review required before activation.")
            st.warning("Magisterial District is a placeholder until MDJ fields are normalized in the data pipeline. This request will need manual Super Admin review before approval.")
        elif campaign_type == "Statewide":
            st.info("Statewide campaigns have no geographic boundary. Approval should be limited to authorized statewide clients only.")
        else:
            vals["custom_scope"] = st.text_area(
                "Custom boundary request",
                value="County: \nMunicipality: ",
                help="Use one field per line. Example: County: Allegheny    School District: Keystone Oaks SD",
                height=95,
                key="signup_scope_custom",
            )

        submitted = st.button("Submit Campaign Request", key="signup_submit_request", type="primary")

        if submitted:
            username_clean = str(username or "").strip().lower()
            cid = _campaign_slug(campaign_name or username_clean)
            scope, manual_boundary_required, manual_note = _signup_scope_for_campaign_type(campaign_type, vals)
            users = store.setdefault("users", {})
            campaigns = store.setdefault("campaigns", {})
            address = ", ".join([x for x in [str(street or '').strip(), str(city or '').strip(), str(state or '').strip() + (" " + str(zip_code or '').strip() if str(zip_code or '').strip() else "")] if x.strip()])

            if not username_clean:
                st.error("Enter a requested username.")
            elif username_clean in users:
                st.error("That username already exists. Choose another username.")
            elif not pw1 or pw1 != pw2:
                st.error("Enter matching passwords.")
            elif not campaign_name:
                st.error("Campaign name is required.")
            elif not email:
                st.error("Email is required.")
            elif campaign_type != "Statewide" and not scope and not manual_boundary_required:
                st.error("Campaign boundary is required. Select the district/geography for this campaign.")
            else:
                campaigns[cid] = {
                    **(campaigns.get(cid, {}) or {}),
                    "campaign_id": cid,
                    "campaign_name": campaign_name,
                    "campaign_type": campaign_type,
                    "office": office,
                    "contact_name": contact_name,
                    "email": email,
                    "phone": phone,
                    "address": address,
                    "billing_address": {
                        "street": street,
                        "city": city,
                        "state": state,
                        "zip": zip_code,
                    },
                    "scope_filters": scope,
                    "manual_boundary_required": bool(manual_boundary_required),
                    "manual_boundary_note": manual_note,
                    "account_status": "pending_approval",
                    "dataset_status": "not_built",
                    "dataset_base_url": campaign_dataset_base_url(cid),
                    "signup_username": username_clean,
                    "created_at": (campaigns.get(cid, {}) or {}).get("created_at") or datetime.now().isoformat(timespec="seconds"),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
                users[username_clean] = {
                    "display_name": contact_name or username_clean,
                    "password_hash": _password_hash(username_clean, pw1),
                    "force_password_change": False,
                    "role": "Campaign Admin",
                    "campaign": campaign_name,
                    "campaign_id": cid,
                    "campaign_type": campaign_type,
                    "email": email,
                    "phone": phone,
                    "address": address,
                    "billing_address": {
                        "street": street,
                        "city": city,
                        "state": state,
                        "zip": zip_code,
                    },
                    "scope_filters": scope,
                    "manual_boundary_required": bool(manual_boundary_required),
                    "manual_boundary_note": manual_note,
                    "disabled": True,
                    "pending_approval": True,
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                }
                save_security_store(store)
                st.success("Request submitted. A Super Admin must approve this campaign before login is enabled.")



def _campaign_name_from_user(username: str, user: dict) -> str:
    return str(
        user.get("campaign")
        or user.get("campaign_name")
        or user.get("display_name")
        or username
        or ""
    ).strip()


def _ensure_campaign_record_for_user(store: dict, username: str) -> tuple[str, bool]:
    """Ensure every non-super-admin campaign user has a matching campaigns[campaign_id].

    This is the auto-create bridge:
      signup/manual account -> campaign record -> Step 9 buildable mini dataset.
    """
    users = store.setdefault("users", {})
    campaigns = store.setdefault("campaigns", {})
    user = users.get(username) or {}
    if not user or str(user.get("role") or "") == "Super Admin":
        return "", False

    campaign_name = _campaign_name_from_user(username, user)
    scope = user.get("scope_filters") or {}
    cid = str(user.get("campaign_id") or "").strip()
    if not cid:
        cid = _campaign_slug(campaign_name or username)
        user["campaign_id"] = cid

    existing = campaigns.get(cid) or {}
    changed = False

    account_status = str(existing.get("account_status") or "").strip()
    if not account_status:
        account_status = "pending_approval" if bool(user.get("pending_approval")) or bool(user.get("disabled")) else "active"

    dataset_status = str(existing.get("dataset_status") or "").strip()
    if account_status == "active" and dataset_status in ("", "not_built", "pending_approval"):
        dataset_status = "pending_build"
    elif not dataset_status:
        dataset_status = "not_built"

    record = dict(existing)
    defaults = {
        "campaign_id": cid,
        "campaign_name": campaign_name,
        "campaign_type": user.get("campaign_type") or existing.get("campaign_type") or "Custom / Other",
        "office": user.get("office") or existing.get("office") or "",
        "contact_name": user.get("display_name") or existing.get("contact_name") or "",
        "email": user.get("email") or existing.get("email") or "",
        "phone": user.get("phone") or existing.get("phone") or "",
        "address": user.get("address") or existing.get("address") or "",
        "billing_address": user.get("billing_address") or existing.get("billing_address") or {},
        "scope_filters": scope or existing.get("scope_filters") or {},
        "manual_boundary_required": bool(existing.get("manual_boundary_required") or user.get("manual_boundary_required", False)),
        "manual_boundary_note": existing.get("manual_boundary_note") or user.get("manual_boundary_note") or "",
        "account_status": account_status,
        "dataset_status": dataset_status,
        "dataset_base_url": existing.get("dataset_base_url") or campaign_dataset_base_url(cid),
        "signup_username": existing.get("signup_username") or username,
        "created_at": existing.get("created_at") or user.get("created_at") or datetime.now().isoformat(timespec="seconds"),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    for k, v in defaults.items():
        if record.get(k) != v:
            record[k] = v
            changed = True

    if campaigns.get(cid) != record:
        campaigns[cid] = record
        changed = True
    if users.get(username) != user:
        users[username] = user
        changed = True

    return cid, changed


def reconcile_security_campaign_records(store: dict) -> bool:
    """Backfill missing campaign records from existing users and normalize build statuses."""
    changed = False
    users = store.setdefault("users", {})
    campaigns = store.setdefault("campaigns", {})

    for uname, user in list(users.items()):
        role = str(user.get("role") or "")
        if role and role != "Super Admin":
            _, did = _ensure_campaign_record_for_user(store, uname)
            changed = changed or did

    for cid, campaign in list(campaigns.items()):
        rec = dict(campaign or {})
        rec["campaign_id"] = rec.get("campaign_id") or cid
        rec["dataset_base_url"] = rec.get("dataset_base_url") or campaign_dataset_base_url(cid)
        status = str(rec.get("account_status") or "").strip().lower()
        dstatus = str(rec.get("dataset_status") or "").strip().lower()
        if status == "active" and dstatus in ("", "not_built", "pending_approval"):
            rec["dataset_status"] = "pending_build"
        if campaigns.get(cid) != rec:
            campaigns[cid] = rec
            changed = True

    return changed



def approve_campaign_request(store: dict, campaign_id: str):
    campaigns = store.setdefault("campaigns", {})
    users = store.setdefault("users", {})
    campaign_id = _campaign_slug(campaign_id)

    # Auto-create missing campaign record if an older/manual account has the campaign_id
    # but no campaigns[campaign_id] row yet.
    campaign = campaigns.get(campaign_id) or {}
    if not campaign:
        for uname, user in list(users.items()):
            if str(user.get("campaign_id") or "").strip() == campaign_id:
                _ensure_campaign_record_for_user(store, uname)
                campaign = campaigns.get(campaign_id) or {}
                break

    if not campaign:
        return False

    if campaign.get("manual_boundary_required"):
        campaign["account_status"] = "pending_manual_boundary"
        campaign["updated_at"] = datetime.now().isoformat(timespec="seconds")
        campaigns[campaign_id] = campaign
        save_security_store(store)
        return False

    campaign["campaign_id"] = campaign.get("campaign_id") or campaign_id
    campaign["account_status"] = "active"
    campaign["dataset_status"] = "pending_build" if str(campaign.get("dataset_status") or "").strip() not in ("uploaded", "active") else campaign.get("dataset_status")
    campaign["dataset_base_url"] = campaign.get("dataset_base_url") or campaign_dataset_base_url(campaign_id)
    campaign["approved_at"] = datetime.now().isoformat(timespec="seconds")
    campaign["updated_at"] = datetime.now().isoformat(timespec="seconds")
    campaigns[campaign_id] = campaign

    linked_any = False
    for uname, user in list(users.items()):
        if str(user.get("campaign_id") or "").strip() == str(campaign_id) or str(user.get("campaign") or "").strip() == str(campaign.get("campaign_name") or "").strip():
            user["campaign_id"] = campaign_id
            user["disabled"] = False
            user["pending_approval"] = False
            user["scope_filters"] = campaign.get("scope_filters") or user.get("scope_filters") or {}
            user["campaign"] = campaign.get("campaign_name") or user.get("campaign") or ""
            user["campaign_type"] = campaign.get("campaign_type") or user.get("campaign_type") or ""
            user["updated_at"] = datetime.now().isoformat(timespec="seconds")
            users[uname] = user
            linked_any = True

    # If this was a campaign signup with username saved on campaign but no user linked, link it.
    signup_username = str(campaign.get("signup_username") or "").strip().lower()
    if signup_username and signup_username in users and not linked_any:
        user = users[signup_username]
        user["campaign_id"] = campaign_id
        user["disabled"] = False
        user["pending_approval"] = False
        user["scope_filters"] = campaign.get("scope_filters") or user.get("scope_filters") or {}
        user["campaign"] = campaign.get("campaign_name") or user.get("campaign") or ""
        user["campaign_type"] = campaign.get("campaign_type") or user.get("campaign_type") or ""
        user["updated_at"] = datetime.now().isoformat(timespec="seconds")
        users[signup_username] = user

    reconcile_security_campaign_records(store)
    save_security_store(store)
    try:
        ok_q, msg_q = enqueue_campaign_build(campaign_id, reason="campaign_approved")
        st.session_state["campaign_build_queue_sync"] = {"ok": ok_q, "message": msg_q, "campaign_id": campaign_id, "at": datetime.now().isoformat(timespec="seconds")}
    except Exception as exc:
        st.session_state["campaign_build_queue_sync"] = {"ok": False, "message": str(exc), "campaign_id": campaign_id, "at": datetime.now().isoformat(timespec="seconds")}
    return True


def disable_campaign_request(store: dict, campaign_id: str):
    campaigns = store.setdefault("campaigns", {})
    users = store.setdefault("users", {})
    campaign = campaigns.get(campaign_id) or {}
    if not campaign:
        return False
    campaign["account_status"] = "disabled"
    campaign["dataset_status"] = "disabled"
    campaign["updated_at"] = datetime.now().isoformat(timespec="seconds")
    campaigns[campaign_id] = campaign
    for uname, user in users.items():
        if str(user.get("campaign_id") or "") == str(campaign_id):
            user["disabled"] = True
            user["updated_at"] = datetime.now().isoformat(timespec="seconds")
            users[uname] = user
    save_security_store(store)
    return True


def render_security_gate():
    """Block the app until a user is logged in, or create the first Super Admin."""
    store = load_security_store()
    users = store.get("users") or {}

    # Login/setup screens use a compact centered card instead of full-width inputs.
    pass


    if not users:
        st.markdown('<div class="cc-login-spacer"></div>', unsafe_allow_html=True)
        st.markdown('<div class="cc-login-title">Candidate Connect Setup</div>', unsafe_allow_html=True)
        st.markdown('<div class="cc-login-subtitle">Create the first Super Admin account. After this, the app will require login.</div>', unsafe_allow_html=True)
        _left, _center, _right = st.columns([1, 1.15, 1])
        with _center:
            with st.form("first_admin_setup"):
                username = st.text_input("Super Admin username")
                pw1 = st.text_input("Password", type="password")
                pw2 = st.text_input("Confirm password", type="password")
                submitted = st.form_submit_button("Create Super Admin", type="primary")
        if submitted:
            username_clean = str(username or "").strip().lower()
            if not username_clean or not pw1:
                st.error("Enter a username and password.")
            elif pw1 != pw2:
                st.error("Passwords do not match.")
            else:
                store = _empty_security_store()
                store["users"][username_clean] = {
                    "display_name": username_clean,
                    "password_hash": _password_hash(username_clean, pw1),
                    "role": "Super Admin",
                    "campaign": "",
                    "scope_filters": {},
                    "disabled": False,
                    "force_password_change": False,
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                }
                save_security_store(store)
                st.success("Super Admin created. Log in now.")
                st.rerun()
        st.stop()

    if st.session_state.get("auth_user"):
        return

    st.markdown('<div class="cc-login-spacer"></div>', unsafe_allow_html=True)
    st.markdown('<div class="cc-login-title">Candidate Connect Login</div>', unsafe_allow_html=True)
    st.markdown('<div class="cc-login-subtitle">Sign in to access your campaign universe, reports, and tools.</div>', unsafe_allow_html=True)
    _left, _center, _right = st.columns([1, 1.15, 1])
    with _center:
        with st.form("candidate_connect_login"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Log In", type="primary")
    if submitted:
        username_clean = str(username or "").strip().lower()
        user = (users or {}).get(username_clean)
        if not user:
            st.error("Invalid username or password.")
        elif user.get("disabled") or user.get("pending_approval"):
            st.error("This account is not active yet. Contact your Candidate Connect administrator.")
        elif user.get("campaign_id") and ((store.get("campaigns") or {}).get(user.get("campaign_id"), {}) or {}).get("account_status") not in ("active", "", None):
            st.error("This campaign account is not active yet.")
        elif user.get("password_hash") != _password_hash(username_clean, password):
            st.error("Invalid username or password.")
        else:
            st.session_state["auth_username"] = username_clean
            st.session_state["auth_user"] = user
            st.success("Logged in.")
            st.rerun()

    _left_pw, _center_pw, _right_pw = st.columns([1, 1.15, 1])
    with _center_pw:
        render_forgot_password_panel(store)

    _left2, _center2, _right2 = st.columns([1, 1.15, 1])
    with _center2:
        render_public_campaign_signup_request(store)
    st.stop()


def render_password_change_panel(*, forced: bool = False):
    """Logged-in password change screen. Forced mode stops all other app access."""
    store = load_security_store()
    users = store.setdefault("users", {})
    uname = current_username()
    user = users.get(uname, {})
    title = "Change Password Required" if forced else "My Account"
    st.markdown(f"## {title}")
    if forced:
        st.warning("Your administrator reset your password. Choose a new password before continuing.")
    else:
        st.caption("Change your Candidate Connect password.")

    with st.form("logged_in_change_password_form"):
        current_pw = st.text_input("Current password", type="password")
        new_pw = st.text_input("New password", type="password")
        new_pw2 = st.text_input("Confirm new password", type="password")
        submitted = st.form_submit_button("Update Password", type="primary")

    if submitted:
        if not user:
            st.error("Could not find your user record. Please log out and sign in again.")
            return
        if user.get("password_hash") != _password_hash(uname, current_pw):
            st.error("Current password is incorrect.")
            return
        err = _password_validation_error(new_pw, new_pw2, current_pw)
        if err:
            st.error(err)
            return
        _set_user_password(store, uname, new_pw, force_change=False)
        save_security_store(store)
        _refresh_current_session_user(store, uname)
        st.success("Password updated.")
        if forced:
            st.info("You can continue using Candidate Connect now.")
        st.rerun()

    if forced:
        if st.button("Log Out", key="forced_password_logout"):
            for _k in ["auth_user", "auth_username"]:
                st.session_state.pop(_k, None)
            st.rerun()

def render_my_account_workspace():
    render_password_change_panel(forced=False)


def _scope_multiselect_options(filter_options: pd.DataFrame, field: str, current_scope: dict | None = None) -> list[str]:
    """Return clean dropdown options for campaign scope assignment."""
    try:
        # Scope assignment should use real dataset values. Use the same filter sources as the app.
        active = current_scope or {}
        if field in GEO_FIELDS:
            geo_df = load_geo_hierarchy_safe()
            opts = options_from_geo(geo_df, field, active) if geo_df is not None and not geo_df.empty else []
            if not opts:
                opts = options_from_filter_table(filter_options, field)
        else:
            opts = options_from_filter_table(filter_options, field)
        current = [str(v) for v in (active or {}).get(field, []) if str(v).strip()]
        merged = list(opts or [])
        for v in current:
            if v not in merged:
                merged.append(v)
        return sorted([v for v in merged if not is_unusable_label(v)], key=smart_sort_key)
    except Exception:
        return []


def build_scope_from_admin_widgets(prefix: str, filter_options: pd.DataFrame, base_scope: dict | None = None, disabled: bool = False) -> dict:
    """Dropdown-based campaign scope builder.

    This replaces manual JSON entry. Super Admins can define a campaign boundary
    using any combination of geography and district fields. Campaign Admins can
    only pass through their own inherited boundary when creating users.
    """
    base_scope = base_scope or {}
    st.markdown("#### Campaign Universe / Hard Boundary")
    st.caption("Use these dropdowns to define what this account is allowed to see. Leave all fields blank only for Super Admin/internal statewide access.")

    scope_fields = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]
    labels = {
        "County": "County",
        "Municipality": "Municipality",
        "Precinct": "Precinct",
        "USC": "Congressional District",
        "STS": "State Senate District",
        "STH": "State House District",
    "Magisterial District": "Magisterial District",
        "School District": "School District",
        "School Region": "School Region",
    }
    out = {}
    # Two-column layout keeps this usable without a long JSON box.
    for i in range(0, len(scope_fields), 2):
        cols = st.columns(2)
        for col, field in zip(cols, scope_fields[i:i+2]):
            with col:
                opts = _scope_multiselect_options(filter_options, field, out or base_scope)
                default = [v for v in (base_scope.get(field, []) or []) if v in opts]
                # If the old saved value is not in the option list for any reason, keep it visible.
                for v in (base_scope.get(field, []) or []):
                    if v not in opts:
                        opts.append(v)
                        default.append(v)
                vals = st.multiselect(labels.get(field, field), options=opts, default=default, key=f"{prefix}_scope_{field}", disabled=disabled)
                vals = [str(v).strip() for v in vals if str(v).strip()]
                if vals:
                    out[field] = vals
    if out:
        st.info("This account will be limited to: " + json.dumps(out, ensure_ascii=False))
    else:
        st.warning("No scope selected. Only use blank scope for Super Admin/internal statewide access.")
    return out



def visible_campaign_records_for_current_user(campaigns: dict) -> dict:
    """Return only campaign records visible to the current user.

    Super Admin sees all campaigns. Campaign Admin sees only their assigned campaign.
    This is a hard UI/data guard for Account Admin tables and controls.
    """
    try:
        if is_super_admin():
            return campaigns or {}
        me = current_user() or {}
        my_cid = str(me.get("campaign_id") or "").strip()
        my_campaign = str(me.get("campaign") or "").strip()
        out = {}
        for cid, c in (campaigns or {}).items():
            c = c or {}
            if my_cid and str(cid).strip() == my_cid:
                out[cid] = c
                continue
            if my_cid and str(c.get("campaign_id") or "").strip() == my_cid:
                out[cid] = c
                continue
            if my_campaign and str(c.get("campaign_name") or "").strip() == my_campaign:
                out[cid] = c
                continue
        return out
    except Exception:
        return {}


def visible_user_records_for_current_user(users: dict) -> dict:
    """Return only account records visible to current user."""
    try:
        if is_super_admin():
            return users or {}
        me = current_user() or {}
        my_username = str(me.get("username") or "").strip()
        my_cid = str(me.get("campaign_id") or "").strip()
        my_campaign = str(me.get("campaign") or "").strip()
        out = {}
        for uname, u in (users or {}).items():
            u = u or {}
            if str(uname).strip() == my_username:
                out[uname] = u
                continue
            if my_cid and str(u.get("campaign_id") or "").strip() == my_cid:
                out[uname] = u
                continue
            if my_campaign and str(u.get("campaign") or "").strip() == my_campaign:
                out[uname] = u
                continue
        return out
    except Exception:
        return {}


def render_account_admin_workspace(filter_options=None):
    st.markdown("## Account Admin")
    st.caption("Manage Candidate Connect accounts and campaign scopes. Campaign-scoped users cannot see, search, export, or report outside their assigned universe.")

    store = load_security_store()
    if reconcile_security_campaign_records(store):
        save_security_store(store)
    users = store.setdefault("users", {})
    me = current_user()
    my_campaign = str(me.get("campaign") or "").strip()

    acct_hdr, acct_refresh_col = st.columns([5, 1])
    with acct_hdr:
        st.markdown("### Current Accounts")
    with acct_refresh_col:
        if st.button("Refresh Accounts", key="refresh_accounts_section", width="stretch"):
            refresh_security_admin_view()
    rows = []
    visible_users = locals().get("visible_users", visible_user_records_for_current_user(users))
    visible_campaigns = locals().get("visible_campaigns", visible_campaign_records_for_current_user(campaigns if "campaigns" in locals() else {}))
    for uname, u in sorted(visible_users.items()):
        if not is_super_admin() and str(u.get("campaign") or "") != my_campaign:
            continue
        rows.append({
            "Username": uname,
            "Name": u.get("display_name", ""),
            "Role": u.get("role", ""),
            "Campaign": u.get("campaign", ""),
            "Scope": json.dumps(u.get("scope_filters") or {}, ensure_ascii=False),
            "Disabled": bool(u.get("disabled", False)),
        })
    if rows:
        cc_table(pd.DataFrame(rows), height=260, key="security_accounts_table")
    else:
        st.info("No accounts visible.")

    existing_names = [""] + sorted([u for u in users.keys() if is_super_admin() or str(users.get(u, {}).get("campaign") or "") == my_campaign])
    edit_user = st.selectbox("Optional: load existing account to edit", existing_names, key="security_edit_user")
    existing = users.get(edit_user, {}) if edit_user else {}

    if edit_user:
        with st.expander("Delete User", expanded=False):
            st.warning("This permanently removes the selected user account from this app_state security store. It does not delete campaign datasets or saved universes.")
            confirm_delete_user = st.checkbox(f"I understand — delete user {edit_user}", key=f"confirm_delete_user_{edit_user}")
            can_delete_user = True
            if edit_user == current_username():
                can_delete_user = False
                st.error("You cannot delete the account you are currently using.")
            if users.get(edit_user, {}).get("role") == "Super Admin":
                super_admin_count = sum(1 for _u in users.values() if (_u or {}).get("role") == "Super Admin" and not (_u or {}).get("disabled"))
                if super_admin_count <= 1:
                    can_delete_user = False
                    st.error("You cannot delete the last active Super Admin.")
            if not is_super_admin():
                target = users.get(edit_user, {}) or {}
                if str(target.get("campaign_id") or "") != str((current_user() or {}).get("campaign_id") or ""):
                    can_delete_user = False
                    st.error("Campaign Admins can only delete users inside their own campaign.")
            if st.button("Delete Selected User", key=f"delete_user_btn_{edit_user}", type="primary", disabled=(not confirm_delete_user or not can_delete_user)):
                users.pop(edit_user, None)
                save_security_store(store)
                st.success(f"Deleted user: {edit_user}")
                st.rerun()

        with st.expander("Admin Reset Password", expanded=False):
            st.warning("Generates a temporary password and forces this user to change it on next login.")
            can_reset_user = True
            if edit_user == current_username():
                can_reset_user = False
                st.info("Use My Account to change your own password.")
            if not is_super_admin():
                target = users.get(edit_user, {}) or {}
                if str(target.get("campaign_id") or "") != str((current_user() or {}).get("campaign_id") or ""):
                    can_reset_user = False
                    st.error("Campaign Admins can only reset users inside their own campaign.")
                if (target.get("role") or "") in ("Super Admin", "Campaign Admin"):
                    can_reset_user = False
                    st.error("Campaign Admins can reset Manager, Field User, and Viewer accounts only.")
            confirm_reset = st.checkbox(f"I understand — reset password for {edit_user}", key=f"confirm_reset_password_{edit_user}")
            if st.button("Generate Temporary Password", key=f"admin_reset_password_btn_{edit_user}", type="primary", disabled=(not can_reset_user or not confirm_reset)):
                temp_pw = _generate_temp_password()
                if _set_user_password(store, edit_user, temp_pw, force_change=True, reset_by=current_username()):
                    save_security_store(store)
                    st.session_state[f"temp_password_for_{edit_user}"] = temp_pw
                    st.success(f"Temporary password created for {edit_user}. Give this to the user securely. They must change it on next login.")
                else:
                    st.error("Could not reset that user password.")
            temp_display = st.session_state.get(f"temp_password_for_{edit_user}")
            if temp_display:
                st.code(temp_display, language="text")
                st.caption("This is shown only in your current browser session. Copy it now.")

    camp_hdr, camp_refresh_col = st.columns([5, 1])
    with camp_hdr:
        st.markdown("### Campaigns / Mini Datasets")
    with camp_refresh_col:
        if st.button("Refresh Campaigns", key="refresh_campaigns_section", width="stretch"):
            refresh_security_admin_view()
    st.caption("Phase 2A: define campaign records now. Once its mini dataset is built/uploaded, campaign users can read the smaller dataset instead of statewide.")
    if is_super_admin():
        if st.button("Repair / Sync Campaign Records", key="repair_campaign_records_btn"):
            if reconcile_security_campaign_records(store):
                save_security_store(store)
                st.success("Campaign records synced from users. Download/upload security_store.json, then run Step 9.")
                st.rerun()
            else:
                st.info("Campaign records already look synced.")

    campaigns = store.setdefault("campaigns", {})
    visible_campaigns = visible_campaign_records_for_current_user(campaigns)
    visible_users = visible_user_records_for_current_user(users)
    camp_rows = []
    visible_campaigns = locals().get("visible_campaigns", visible_campaign_records_for_current_user(campaigns if "campaigns" in locals() else {}))
    for cid, c in sorted(visible_campaigns.items()):
        camp_rows.append({
            "Campaign ID": cid,
            "Campaign": c.get("campaign_name", ""),
            "Office": c.get("office", ""),
            "Contact": c.get("contact_name", ""),
            "Email": c.get("email", ""),
            "Account Status": c.get("account_status", ""),
            "Dataset Status": c.get("dataset_status", ""),
            "Scope": json.dumps(c.get("scope_filters") or {}, ensure_ascii=False),
        })
    if camp_rows:
        cc_table(pd.DataFrame(camp_rows), height=220, key="campaigns_admin_table")
    else:
        st.info("No campaign records yet.")

    if is_super_admin() and visible_campaigns:
        with st.expander("Delete Campaign", expanded=False):
            st.warning("This is a hard-delete cleanup tool for test/bad campaigns. It removes the campaign record, build queue item, and R2 campaign folder for the current DEV/LIVE app. Linked users are deleted only if you check that option.")
            delete_cid = st.selectbox("Campaign to delete", [""] + sorted(visible_campaigns.keys()), key="delete_campaign_select")
            delete_linked_users = st.checkbox("Also delete users linked to this campaign", value=True, key="delete_campaign_linked_users")
            confirm_delete_campaign = st.checkbox("I understand this permanently deletes the selected campaign data", key="confirm_delete_campaign")
            if delete_cid:
                linked_usernames = sorted([uname for uname, u in users.items() if str((u or {}).get("campaign_id") or "") == str(delete_cid)])
                if linked_usernames:
                    st.caption("Linked users: " + ", ".join(linked_usernames))
                st.code(f"app_state/campaigns/{delete_cid}/", language="text")
            if st.button("Delete Selected Campaign", key="delete_campaign_btn", type="primary", disabled=(not delete_cid or not confirm_delete_campaign)):
                cid = str(delete_cid)
                campaigns.pop(cid, None)
                if delete_linked_users:
                    for uname in list(users.keys()):
                        if str((users.get(uname) or {}).get("campaign_id") or "") == cid:
                            users.pop(uname, None)
                else:
                    for uname, u in list(users.items()):
                        if str((u or {}).get("campaign_id") or "") == cid:
                            u["disabled"] = True
                            u["campaign_id"] = ""
                            u["updated_at"] = datetime.now().isoformat(timespec="seconds")
                save_security_store(store)
                ok1, msg1 = _delete_r2_prefix(f"app_state/campaigns/{cid}/")
                ok2, msg2 = _delete_r2_key(f"app_state/build_queue/{cid}.json")
                local_dir = Path("07_outputs") / "campaign_datasets" / cid
                try:
                    import shutil
                    if local_dir.exists():
                        shutil.rmtree(local_dir)
                except Exception:
                    pass
                st.success(f"Deleted campaign {cid}. {msg1}. {msg2}.")
                st.rerun()

    if is_super_admin():
        pending_campaigns = {cid: c for cid, c in campaigns.items() if str(c.get("account_status") or "") in ("pending_approval", "pending_manual_boundary", "pending_payment", "payment_failed")}
        if pending_campaigns:
            st.markdown("### Pending Campaign Requests")
            pending_rows = []
            for cid, c in sorted(pending_campaigns.items()):
                pending_rows.append({
                    "Campaign ID": cid,
                    "Campaign": c.get("campaign_name", ""),
                    "Contact": c.get("contact_name", ""),
                    "Email": c.get("email", ""),
                    "Scope": scope_summary(c.get("scope_filters") or {}),
                    "Status": c.get("account_status", ""),
                    "Manual Review": "Yes" if c.get("manual_boundary_required") else "",
                })
            cc_table(pd.DataFrame(pending_rows), height=220, key="pending_campaign_requests_table")
            selected_pending = st.selectbox("Select pending campaign", [""] + sorted(pending_campaigns.keys()), key="pending_campaign_select")
            c1, c2, c3 = st.columns([1, 1, 4])
            with c1:
                if st.button("Approve Campaign", key="approve_pending_campaign", type="primary", disabled=not bool(selected_pending)):
                    if approve_campaign_request(store, selected_pending):
                        st.success("Campaign approved and queued for automatic build. Use Refresh Campaigns in a minute to check whether the worker marked it active.")
                        st.rerun()
                    else:
                        st.error("This request needs manual boundary review before approval. Edit the campaign scope first, then approve it.")
            with c2:
                if st.button("Disable Campaign", key="disable_pending_campaign", disabled=not bool(selected_pending)):
                    if disable_campaign_request(store, selected_pending):
                        st.success("Campaign disabled.")
                        st.rerun()

        active_campaigns = {cid: c for cid, c in campaigns.items() if str(c.get("account_status") or "") == "active" and str(c.get("dataset_status") or "") != "active"}
        if active_campaigns:
            st.markdown("### Dataset Activation")
            st.caption("After Step 9 finishes and the campaign dataset is uploaded to R2, mark the dataset active here.")
            selected_active = st.selectbox("Campaign dataset to activate", [""] + sorted(active_campaigns.keys()), key="dataset_activation_select")
            if st.button("Mark Dataset Active", key="mark_dataset_active", type="primary", disabled=not bool(selected_active)):
                store.setdefault("campaigns", {})[selected_active]["dataset_status"] = "active"
                store["campaigns"][selected_active]["updated_at"] = datetime.now().isoformat(timespec="seconds")
                save_security_store(store)
                st.success("Dataset marked active and security_store sync attempted automatically.")
                st.rerun()

    st.markdown("#### Dataset Routing Rules")
    st.markdown("""
- **Super Admin** uses the statewide dataset.
- **Campaign users** use their assigned campaign mini dataset only when that campaign's `Dataset Status` is `active`.
- Until the mini dataset is active, campaign users remain protected by the hard campaign boundary but still read from statewide data.
- Account requests are stored in `security_store.json`; after approvals/status changes, download/upload it to R2 `app_state/security_store.json` so changes survive app rebuilds.
""")

    if is_super_admin():
        with st.expander("Add / Update Campaign", expanded=False):
            campaign_ids_existing = [""] + sorted(campaigns.keys())
            edit_campaign_id = st.selectbox("Optional: load existing campaign", campaign_ids_existing, key="campaign_edit_id")
            existing_campaign = campaigns.get(edit_campaign_id, {}) if edit_campaign_id else {}
            with st.form("campaign_admin_form"):
                campaign_name = st.text_input("Campaign name", value=existing_campaign.get("campaign_name", ""))
                campaign_type_options = ["Municipal", "School District", "Countywide", "State House", "State Senate", "Congressional", "Magisterial District", "Statewide", "Custom / Other"]
                campaign_type_val = existing_campaign.get("campaign_type", "Municipal")
                campaign_type = st.selectbox("Campaign type", campaign_type_options, index=campaign_type_options.index(campaign_type_val) if campaign_type_val in campaign_type_options else 0)
                office = st.text_input("Office sought", value=existing_campaign.get("office", ""))
                contact_name = st.text_input("Contact name", value=existing_campaign.get("contact_name", ""))
                address = st.text_input("Address", value=existing_campaign.get("address", ""))
                phone = st.text_input("Phone", value=existing_campaign.get("phone", ""))
                email = st.text_input("Email", value=existing_campaign.get("email", ""))
                account_status_options = ["pending_approval", "active", "disabled", "pending_payment", "cancelled", "payment_failed"]
                dataset_status_options = ["not_built", "pending_build", "uploaded", "active", "rebuild_needed", "disabled"]
                account_status_val = existing_campaign.get("account_status", "pending_approval")
                dataset_status_val = existing_campaign.get("dataset_status", "not_built")
                account_status = st.selectbox("Account status", account_status_options, index=account_status_options.index(account_status_val) if account_status_val in account_status_options else 0)
                dataset_status = st.selectbox("Dataset status", dataset_status_options, index=dataset_status_options.index(dataset_status_val) if dataset_status_val in dataset_status_options else 0)
                campaign_id_preview = _campaign_slug(edit_campaign_id or campaign_name)
                dataset_base_url = st.text_input("Dataset base URL", value=existing_campaign.get("dataset_base_url") or campaign_dataset_base_url(campaign_id_preview))
                st.caption("Build/upload target:")
                st.code(f"app_state/campaigns/{campaign_id_preview}/dataset/", language="text")
                campaign_scope = build_scope_from_admin_widgets("campaign", filter_options if filter_options is not None else pd.DataFrame(), base_scope=existing_campaign.get("scope_filters", {}), disabled=False)
                campaign_submitted = st.form_submit_button("Save Campaign", type="primary")

            if campaign_submitted:
                if not campaign_name:
                    st.error("Campaign name is required.")
                elif not campaign_scope:
                    st.error("Campaign needs a scope.")
                else:
                    cid = _campaign_slug(edit_campaign_id or campaign_name)
                    rec = upsert_campaign_record(store, cid, {
                        "campaign_name": campaign_name,
                        "campaign_type": campaign_type,
                        "office": office,
                        "contact_name": contact_name,
                        "address": address,
                        "phone": phone,
                        "email": email,
                        "account_status": account_status,
                        "dataset_status": dataset_status,
                        "dataset_base_url": str(dataset_base_url or campaign_dataset_base_url(cid)).rstrip("/"),
                        "scope_filters": campaign_scope,
                        "manual_boundary_required": False,
                        "manual_boundary_note": "",
                    })
                    save_security_store(store)
                    if dataset_status == "active" and not str(dataset_base_url or "").strip():
                        st.warning("Campaign saved, but active datasets need a Dataset base URL.")
                    st.success(f"Campaign saved: {rec['campaign_id']}")
                    st.rerun()


    st.markdown("### Add / Update Account")
    allowed_roles = SECURITY_ROLES if is_super_admin() else ["Manager", "Field User", "Viewer"]

    username_default = edit_user or ""
    display_default = existing.get("display_name", "") if existing else ""
    role_default = existing.get("role", "Campaign Admin") if existing else ("Campaign Admin" if is_super_admin() else "Manager")
    role_index = allowed_roles.index(role_default) if role_default in allowed_roles else 0
    campaign_default = existing.get("campaign", "") if existing else ("" if is_super_admin() else my_campaign)
    scope_default = existing.get("scope_filters", {}) if existing else ({} if is_super_admin() else security_scope_filters())

    with st.form("account_admin_form"):
        username = st.text_input("Username", value=username_default).strip().lower()
        display_name = st.text_input("Display name", value=display_default)
        password = st.text_input("New password / reset password", type="password")
        force_pw_change = st.checkbox("Force password change on next login", value=bool(password and existing and edit_user != current_username()), key="force_pw_change_on_save")
        role = st.selectbox("Role", allowed_roles, index=role_index)
        campaign = st.text_input("Campaign / client name", value=campaign_default, disabled=not is_super_admin())
        campaign_ids = [""] + sorted((store.get("campaigns") or {}).keys())
        existing_campaign_id = existing.get("campaign_id", "")
        campaign_id_index = campaign_ids.index(existing_campaign_id) if existing_campaign_id in campaign_ids else 0
        campaign_id = st.selectbox("Link to campaign mini dataset", campaign_ids, index=campaign_id_index, disabled=not is_super_admin())
        if campaign_id:
            linked_campaign = (store.get("campaigns") or {}).get(campaign_id, {})
            st.caption(f"Linked dataset: {linked_campaign.get('dataset_status', 'not_built')} · {linked_campaign.get('dataset_base_url', '')}")
        disabled = st.checkbox("Disable this account", value=bool(existing.get("disabled", False)) if existing else False)

        if is_super_admin():
            scope = build_scope_from_admin_widgets("acct", filter_options if filter_options is not None else pd.DataFrame(), base_scope=scope_default, disabled=(role == "Super Admin"))
            if role == "Super Admin":
                scope = {}
        else:
            st.info("Users you create inherit your campaign boundary automatically.")
            scope = security_scope_filters()
        st.markdown("**Campaign boundary:** " + scope_summary(scope if "scope" in locals() else {}))
        submitted = st.form_submit_button("Save Account", type="primary")

    if submitted:
        if not username:
            st.error("Username is required.")
            return
        if not is_super_admin() and username in users and str(users[username].get("campaign") or "") != my_campaign:
            st.error("You can only edit accounts in your campaign.")
            return
        if not password and username not in users:
            st.error("Password is required for a new account.")
            return
        if role != "Super Admin" and campaign_id:
            linked_scope = ((store.get("campaigns") or {}).get(campaign_id, {}) or {}).get("scope_filters") or {}
            if linked_scope:
                scope = linked_scope
        if role != "Super Admin" and not scope:
            st.error("Campaign-scoped accounts need a scope. Select at least one county, municipality, precinct, or district.")
            return

        linked_campaign_record = ((store.get("campaigns") or {}).get(campaign_id, {}) or {}) if campaign_id else {}
        campaign_name_final = str(campaign or linked_campaign_record.get("campaign_name") or "").strip()

        # Auto-create/link a campaign mini-dataset record for campaign-scoped users
        # so Step 9 always has something buildable.
        final_campaign_id = str(campaign_id or "").strip()
        if role != "Super Admin" and not final_campaign_id:
            final_campaign_id = _campaign_slug(campaign_name_final or username)
            store.setdefault("campaigns", {}).setdefault(final_campaign_id, {
                "campaign_id": final_campaign_id,
                "campaign_name": campaign_name_final or username,
                "campaign_type": "Custom / Other",
                "office": "",
                "contact_name": display_name or username,
                "email": "",
                "phone": "",
                "address": "",
                "billing_address": {},
                "scope_filters": scope,
                "manual_boundary_required": False,
                "manual_boundary_note": "",
                "account_status": "active" if not disabled else "disabled",
                "dataset_status": "pending_build" if not disabled else "disabled",
                "dataset_base_url": campaign_dataset_base_url(final_campaign_id),
                "signup_username": username,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            })

        prior = users.get(username, {})
        record = dict(prior)
        record.update({
            "display_name": display_name or username,
            "role": role,
            "campaign": campaign_name_final,
            "campaign_id": final_campaign_id,
            "scope_filters": scope,
            "disabled": bool(disabled),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        })
        if password:
            err = _password_validation_error(password)
            if err:
                st.error(err)
                return
            record["password_hash"] = _password_hash(username, password)
            if username != current_username():
                record["force_password_change"] = bool(force_pw_change or username in users)
                record["password_reset_by"] = current_username()
                record["password_reset_at"] = datetime.now().isoformat(timespec="seconds")
            else:
                record["force_password_change"] = False
            record["password_updated_at"] = datetime.now().isoformat(timespec="seconds")
        if "created_at" not in record:
            record["created_at"] = datetime.now().isoformat(timespec="seconds")
        record.setdefault("force_password_change", False)
        users[username] = record
        reconcile_security_campaign_records(store)
        save_security_store(store)
        st.success("Account saved. The scope is now a hard boundary and a campaign mini-dataset record exists for Step 9.")
        st.rerun()

    st.markdown("### Backup / Restore")
    st.info("Important: download this backup after account changes and upload it to R2 at app_state/security_store.json so accounts survive app rebuilds/reboots.")
    st.download_button(
        "Download security_store.json backup",
        data=security_export_json_bytes(),
        file_name="security_store.json",
        mime="application/json",
        width="stretch",
    )
    uploaded = st.file_uploader("Restore security_store.json / security_users.json", type=["json"], key="security_restore_upload")
    if uploaded is not None:
        try:
            restored = json.loads(uploaded.read().decode("utf-8"))
            if isinstance(restored, dict) and isinstance(restored.get("users"), dict):
                if st.button("Apply restored security file", width="stretch"):
                    save_security_store(restored)
                    st.success("Security file restored.")
                    st.rerun()
            else:
                st.error("That file does not look like a Candidate Connect security file.")
        except Exception as e:
            st.error(f"Could not restore security file: {e}")

def load_persistent_saved_universes():
    """Initialize session saved universes from local state, then URL query params.

    Super Admin sees the global saved universe list. Campaign-scoped users see
    only the saved universes for their assigned campaign/account boundary.
    """
    section = saved_universe_state_section()
    session_key = f"saved_universes::{section}"
    if session_key not in st.session_state:
        state_saved = (_load_state().get(section) or {})
        if state_saved:
            st.session_state[session_key] = _json_safe_saved_universes(state_saved)
        else:
            try:
                raw = st.query_params.get(SAVED_UNIVERSES_PARAM, "") if is_super_admin() else ""
            except Exception:
                raw = ""
            st.session_state[session_key] = decode_saved_universes(raw)
    st.session_state["saved_universes"] = st.session_state.setdefault(session_key, {})
    return st.session_state["saved_universes"]


def persist_saved_universes(saved):
    """Persist saved universes into local state and the browser URL."""
    clean = _json_safe_saved_universes(saved)
    section = saved_universe_state_section()
    session_key = f"saved_universes::{section}"
    st.session_state[session_key] = clean
    st.session_state["saved_universes"] = clean
    try:
        _ = _persist_state_section(section, clean)
    except Exception:
        pass
    try:
        # Keep URL persistence only for Super Admin/global mode. Campaign users should
        # not leak campaign saved universes into a shareable browser URL.
        if is_super_admin():
            encoded = encode_saved_universes(clean)
            if encoded:
                st.query_params[SAVED_UNIVERSES_PARAM] = encoded
            elif SAVED_UNIVERSES_PARAM in st.query_params:
                del st.query_params[SAVED_UNIVERSES_PARAM]
    except Exception:
        pass


def load_saved_universe_into_widgets(data):
    """Reset current widget keys, then write saved filter values into the fresh keys."""
    old_token = int(st.session_state.get("filter_reset_token", 0))
    prefixes = (
        "filter_",
        "new_reg_months_",
        "vote_score_type_",
        "vote_history_score_range_",
        "election_years_",
        "election_types_",
        "election_methods_",
        "mb_prob_score_range_",
        "phone_reach_mode_",
    )
    for key in list(st.session_state.keys()):
        if key.startswith(prefixes) or key in {"quick_summary", "count_mode", "exact_summary"} or key.startswith("prepared_"):
            _ = st.session_state.pop(key, None)
    st.session_state["filter_reset_token"] = old_token + 1

    for f, vals in ((data or {}).get("filters") or {}).items():
        st.session_state[filter_key(f)] = vals

    sp = (data or {}).get("special") or {}
    for k, v in sp.items():
        if k == "__PhoneReach":
            st.session_state[special_key("phone_reach_mode")] = v
        elif k == "__ElectionFilters" and isinstance(v, dict):
            st.session_state[special_key("election_years")] = v.get("years", [])
            st.session_state[special_key("election_types")] = v.get("types", [])
            st.session_state[special_key("election_methods")] = v.get("methods", [])
        elif k == "RegistrationMonthsAgo" and isinstance(v, dict):
            st.session_state[special_key("new_reg_months")] = int(v.get("max", 0) or 0)
        elif k == "MB_Prob_Score" and isinstance(v, dict):
            st.session_state[special_key("mb_prob_score_range")] = (int(v.get("min", 0)), int(v.get("max", 4)))
        elif k in {"V4A", "V4G", "V4P"} and isinstance(v, dict):
            label = "All Elections" if k == "V4A" else ("General Elections" if k == "V4G" else "Primary Elections")
            st.session_state[special_key("vote_score_type")] = label
            st.session_state[special_key("vote_history_score_range")] = (int(v.get("min", 0)), int(v.get("max", 4)))

    st.session_state["left_section"] = "create_universe"
    st.session_state["view"] = "targeting"
    st.rerun()


def selected(field: str):
    return st.session_state.get(filter_key(field), [])

def clear_filter_state():
    """Reset all Create Universe widgets, saved count output, and force fresh widget keys."""
    old_token = int(st.session_state.get("filter_reset_token", 0))
    prefixes = (
        "filter_",
        "new_reg_months_",
        "vote_score_type_",
        "vote_history_score_range_",
        "election_years_",
        "election_types_",
        "election_methods_",
        "mb_prob_score_range_",
        "phone_reach_mode_",
    )
    for key in list(st.session_state.keys()):
        if key.startswith(prefixes) or key in {"quick_summary", "count_mode", "exact_summary"} or key.startswith("prepared_"):
            _ = st.session_state.pop(key, None)
    st.session_state["filter_reset_token"] = old_token + 1
    st.session_state["left_section"] = "create_universe"
    st.session_state["view"] = "targeting"
    st.rerun()

def active_filters() -> dict:
    out = {}
    for f in ALL_FILTER_FIELDS:
        vals = selected(f)
        if vals:
            out[f] = vals
    return enforce_security_scope(out)


def universe_label_from_filters(filters: dict) -> str:
    """Build a short human label for the currently applied universe."""
    filters = filters or {}
    priority = ["County", "Municipality", "Precinct", "School District", "School Region", "USC", "STS", "STH", "Party", "Gender", "Age_Range"]
    parts = []
    for field in priority:
        vals = filters.get(field) or []
        if vals:
            label = DISPLAY_LABELS.get(field, field)
            shown = ", ".join(map(str, vals[:3]))
            if len(vals) > 3:
                shown += f" +{len(vals)-3} more"
            parts.append(f"{label}: {shown}")
        if len(parts) >= 3:
            break
    return " | ".join(parts) if parts else "Statewide"


def save_current_universe(filters: dict, summary: dict | None = None, source: str = "Create Universe"):
    """Persist the latest applied Create Universe so other workspaces can use it."""
    clean_filters = {str(k): list(v or []) for k, v in (filters or {}).items() if v}
    st.session_state["current_universe_filters"] = clean_filters
    st.session_state["current_universe_label"] = universe_label_from_filters(clean_filters)
    st.session_state["current_universe_source"] = source
    st.session_state["current_universe_updated"] = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
    if summary is not None:
        st.session_state["current_universe_summary"] = summary


def get_current_universe_filters() -> dict:
    return dict(st.session_state.get("current_universe_filters") or {})


def has_current_universe() -> bool:
    return bool(get_current_universe_filters())


def active_geo_filters() -> dict:
    return {k: v for k, v in active_filters().items() if k in GEO_FIELDS}

def count_safe_filters(active: dict) -> dict:
    # v21: after Step 8 v18 rebuild, Save / Apply Current Universe supports the full targeting count cube.
    safe = set(GEO_FIELDS + [
        "Party", "Gender", "Age_Range", "CalculatedParty", "HH-Party",
        "V4A", "V4G", "V4P",
        "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "MB_Prob_Score",
        "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone",
        "RegistrationMonthsAgo",
    ])
    return {k: v for k, v in active.items() if k in safe}

def non_count_filters(active: dict) -> dict:
    safe = set(GEO_FIELDS + [
        "Party", "Gender", "Age_Range", "CalculatedParty", "HH-Party",
        "V4A", "V4G", "V4P",
        "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "MB_Prob_Score",
        "HasMobile", "HasLandline", "HasEmail", "HasApplicantPhone",
        "RegistrationMonthsAgo",
    ])
    return {k: v for k, v in active.items() if k not in safe}


def normalize_election_method_value(value) -> str:
    s = clean_value(value).upper()
    if not s:
        return ""
    if s in {"Y", "V", "VOTED", "YES"}:
        return "Voted"
    if s in {"A", "AB", "ABS", "ABSENTEE"} or "ABS" in s:
        return "Absentee"
    if s in {"M", "MB", "MAIL", "MAIL-IN", "MAIL IN", "MAILIN"} or "MAIL" in s:
        return "Mail"
    if s in {"P", "POLL", "POLLING", "IN PERSON", "IN-PERSON", "ELECTION DAY"} or "POLL" in s or "PERSON" in s:
        return "Polls"
    if s in {"PROV", "PROVISIONAL"} or "PROV" in s:
        return "Provisional"
    return clean_value(value).title()


def election_meta_from_col(col: str):
    raw = str(col).strip()
    u = re.sub(r"[^A-Z0-9]+", "_", raw.upper()).strip("_")
    patterns = [
        r"^([GP])_?((?:20)?\d{2})(?:_|$)",
        r"^(GENERAL|PRIMARY|GEN|PRI|PRIM)_?((?:20)?\d{2})(?:_|$)",
        r"^((?:20)?\d{2})_?(GENERAL|PRIMARY|GEN|PRI|PRIM)(?:_|$)",
        r"(?:^|_)([GP])_?((?:20)?\d{2})(?:_|$)",
        r"(?:^|_)(GENERAL|PRIMARY|GEN|PRI|PRIM)_?((?:20)?\d{2})(?:_|$)",
        r"(?:^|_)((?:20)?\d{2})_?(GENERAL|PRIMARY|GEN|PRI|PRIM)(?:_|$)",
    ]
    for pat in patterns:
        m = re.search(pat, u)
        if not m:
            continue
        a, b = m.group(1), m.group(2)
        if a.isdigit():
            yy, typ = a, b
        else:
            typ, yy = a, b
        try:
            year = int(yy) if len(str(yy)) == 4 else 2000 + int(yy)
        except Exception:
            continue
        if not (2000 <= year <= 2030):
            continue
        typ_u = str(typ).upper()
        if typ_u.startswith("G"):
            etype = "General"
        elif typ_u.startswith("P"):
            etype = "Primary"
        else:
            etype = str(typ).title()
        return {"column": raw, "year": str(year), "type": etype}
    return None


def election_columns_from_manifest() -> list[str]:
    try:
        m = load_manifest()
        cols = []
        for section in ["index", "schema", "detail"]:
            data = m.get(section, {}) if isinstance(m, dict) else {}
            for key in ["columns", "index_columns", "detail_columns"]:
                for c in data.get(key, []) or []:
                    if election_meta_from_col(c) and c not in cols:
                        cols.append(c)
        return cols
    except Exception:
        return []


def election_options():
    metas = [election_meta_from_col(c) for c in election_columns_from_manifest()]
    metas = [m for m in metas if m]
    years = sorted({m["year"] for m in metas}, key=lambda x: int(x), reverse=True)
    types = [t for t in ["General", "Primary"] if t in {m["type"] for m in metas}]
    methods = ["Voted", "Mail", "Absentee", "Polls", "Provisional"]
    return years, types, methods


def selected_election_columns(years=None, types=None) -> list[str]:
    years = set(years or [])
    types = set(types or [])
    cols = []
    for c in election_columns_from_manifest():
        meta = election_meta_from_col(c)
        if not meta:
            continue
        if years and meta["year"] not in years:
            continue
        if types and meta["type"] not in types:
            continue
        cols.append(c)
    return cols

def vote_score_field_from_selection() -> str:
    choice = st.session_state.get(special_key("vote_score_type"), "All Elections")
    if choice == "General Elections":
        return "V4G"
    if choice == "Primary Elections":
        return "V4P"
    return "V4A"


def active_special_filters() -> dict:
    special = {}

    # Newly registered slider, expressed against RegistrationMonthsAgo from Step 8 v18.
    new_reg_months = st.session_state.get(special_key("new_reg_months"), 0)
    if new_reg_months and int(new_reg_months) > 0:
        special["RegistrationMonthsAgo"] = {"max": int(new_reg_months)}

    vh_range = st.session_state.get(special_key("vote_history_score_range"), (0, 4))
    vh_field = vote_score_field_from_selection() if "vote_score_field_from_selection" in globals() else "V4A"
    if vh_range != (0, 4):
        special[vh_field] = {"min": int(vh_range[0]), "max": int(vh_range[1])}

    mb_prob = st.session_state.get(special_key("mb_prob_score_range"), (0, 4))
    if mb_prob != (0, 4):
        special["MB_Prob_Score"] = {"min": int(mb_prob[0]), "max": int(mb_prob[1])}

    phone_mode = st.session_state.get(special_key("phone_reach_mode"), "No phone filter")
    if phone_mode and phone_mode != "No phone filter":
        special["__PhoneReach"] = phone_mode

    election_years = st.session_state.get(special_key("election_years"), [])
    election_types = st.session_state.get(special_key("election_types"), [])
    election_methods = st.session_state.get(special_key("election_methods"), [])
    if election_years or election_types or election_methods:
        special["__ElectionFilters"] = {
            "years": list(election_years or []),
            "types": list(election_types or []),
            "methods": list(election_methods or []),
        }

    return special

def apply_special_filters(df: pd.DataFrame, special: dict) -> pd.DataFrame:
    out = df
    for field, rule in (special or {}).items():
        if out.empty:
            return out

        if field == "__PhoneReach":
            mobile = out["HasMobile"].astype(str).str.lower().eq("yes") if "HasMobile" in out.columns else pd.Series(False, index=out.index)
            landline = out["HasLandline"].astype(str).str.lower().eq("yes") if "HasLandline" in out.columns else pd.Series(False, index=out.index)
            mode = str(rule)
            if mode == "Mobile only":
                out = out[mobile]
            elif mode == "Landline only":
                out = out[landline]
            elif mode == "Mobile OR landline":
                out = out[mobile | landline]
            elif mode == "Mobile AND landline":
                out = out[mobile & landline]
            elif mode == "No mobile or landline":
                out = out[~(mobile | landline)]
            continue

        if field == "__ElectionFilters" and isinstance(rule, dict):
            years = rule.get("years") or []
            types = rule.get("types") or []
            methods = set(rule.get("methods") or [])
            cols = [c for c in selected_election_columns(years, types) if c in out.columns]
            if not cols:
                out = out.iloc[0:0]
                continue
            mask = pd.Series(False, index=out.index)
            for c in cols:
                vals = out[c].map(normalize_election_method_value)
                if methods:
                    mask = mask | vals.isin(methods)
                else:
                    mask = mask | vals.astype(str).str.strip().ne("")
            out = out[mask]
            continue

        if field in out.columns and isinstance(rule, dict):
            vals = pd.to_numeric(out[field], errors="coerce")
            if "min" in rule:
                out = out[vals >= float(rule["min"])]
                vals = pd.to_numeric(out[field], errors="coerce")
            if "max" in rule:
                out = out[vals <= float(rule["max"])]
    return out

def expand_filter_values(field, vals):
    # v21c: speed tables now use clean canonical labels, so no expansion is needed.
    # Kept as a safe helper because filtering code calls it.
    return vals

def apply_filters(df: pd.DataFrame, active: dict) -> pd.DataFrame:
    out = df
    try:
        for field, vals in (active or {}).items():
            if vals and field in out.columns:
                expanded_vals = expand_filter_values(field, vals)
                out = out[out[field].astype(str).isin([str(v) for v in expanded_vals])]
        return out
    except Exception:
        return df

def options_from_geo(df: pd.DataFrame, field: str, active: dict) -> list:
    try:
        if df is None or df.empty or field not in df.columns:
            return []
        relevant = {}
        for f, vals in (active or {}).items():
            if f == field:
                continue
            if vals and f in df.columns:
                relevant[f] = vals
        narrowed = apply_filters(df, relevant)
        vals = narrowed[field].astype(str).map(clean_value)
        return sorted([v for v in vals.unique().tolist() if not is_unusable_label(v)], key=smart_sort_key)
    except Exception:
        return []

def options_from_filter_table(filter_options: pd.DataFrame, field: str) -> list:
    try:
        if filter_options is None or filter_options.empty:
            return []
        if "field" not in filter_options.columns or "value" not in filter_options.columns:
            return []
        vals = filter_options.loc[filter_options["field"].astype(str).eq(str(field)), "value"].astype(str).map(clean_value)
        out = sorted([v for v in vals.unique().tolist() if not is_unusable_label(v)], key=smart_sort_key)
        return out
    except Exception:
        return []

def clean_yes_no_all_options():
    return ["Y", "N"]

def clean_mail_options(field: str):
    fixed = {
        "MB_App": ["Applied", "Not Applied"],
        "MB_App_Status": ["Approved", "Declined"],
        "MB_Sent": ["Sent", "Not Sent"],
        "MB_Status": ["Voted", "Not Voted"],
        "MB_PERM": ["Y", "N"],
        "HasMobile": ["Yes", "No"],
        "HasLandline": ["Yes", "No"],
        "HasEmail": ["Yes", "No"],
        "HasApplicantPhone": ["Yes", "No"],
    }
    return fixed.get(field, [])

def count_cube_option_filters(field: str, active: dict) -> dict:
    active = active or {}
    relevant = count_safe_filters(active)
    relevant.pop(field, None)
    return relevant


def options_from_count_cube(field: str, active: dict) -> list:
    try:
        relevant = count_cube_option_filters(field, active)
        needed = set(relevant.keys()) | {field}
        if not field or not needed:
            return []
        cube = load_count_cube_columns(tuple(sorted(needed)))
        narrowed = apply_filters(cube, relevant)
        if field not in narrowed.columns:
            return []
        vals = narrowed[field].astype(str).map(clean_value)
        return sorted([v for v in vals.unique().tolist() if not is_unusable_label(v)], key=smart_sort_key)
    except Exception:
        return []


def field_options(filter_options: pd.DataFrame, field: str, active: dict | None = None):
    """Return dropdown options for the left Create Universe filter pane.

    Campaign users must see only option values inside their assigned hard boundary.
    Geography options for campaign users now come from the current campaign mini
    dataset/count cube instead of the statewide fallback filter_options table.
    """
    try:
        scope = security_scope_filters()
        active = enforce_security_scope(active or {})

        if field in scope and scope.get(field):
            return sorted([str(v).strip() for v in (scope.get(field) or []) if str(v).strip()], key=smart_sort_key)

        fixed = clean_mail_options(field)
        if fixed:
            opts = fixed

        elif field in GEO_FIELDS:
            if is_campaign_scoped():
                # Critical fix: use the campaign dataset/count cube for downstream
                # geo fields like Precinct, USC, STS, STH, School District, etc.
                # Do not fall back to statewide values for campaign users.
                opts = options_from_count_cube(field, active) or []
            else:
                geo_df = load_geo_hierarchy_safe()
                opts = options_from_geo(geo_df, field, active) if geo_df is not None and not geo_df.empty else []
                if not opts:
                    opts = options_from_filter_table(filter_options, field)

        else:
            opts = options_from_filter_table(filter_options, field)

        current = [str(v) for v in (active or {}).get(field, []) if str(v).strip()]
        merged = list(opts)

        # Only Super Admin or non-geo fields preserve stale current selections.
        # Campaign geo dropdowns must not keep out-of-bound stale values.
        if is_super_admin() or field not in GEO_FIELDS:
            for v in current:
                if v not in merged:
                    merged.append(v)

        return sorted([v for v in merged if not is_unusable_label(v)], key=smart_sort_key)
    except Exception:
        return []


def is_cube_safe(active: dict) -> bool:
    # Geography + Party/Gender/Age_Range usually live in the count cube.
    # Anything else uses exact shard scan through the same Update Counts button.
    cube_safe = set(GEO_FIELDS + ["Party", "Gender", "Age_Range"])
    return all(k in cube_safe for k in active.keys())

def update_counts(active: dict):
    try:
        active = enforce_security_scope(active or {})
        special = active_special_filters()
        if requires_remote_index_count(active or {}, special or {}):
            # Tags and specific-election filters are row-level filters. Count them
            # remotely with DuckDB over R2 index shards so Streamlit does not
            # download shards or loop through them in Python.
            summary = duckdb_index_summary(
                json.dumps(active or {}, sort_keys=True),
                json.dumps(special or {}, sort_keys=True),
            )
            return summary, "remote-index", None

        safe_active = count_safe_filters(active)
        summary = duckdb_count_cube_summary(
            json.dumps(safe_active, sort_keys=True),
            json.dumps(special or {}, sort_keys=True),
        )
        return summary, "quick", None
    except Exception as e:
        return None, "unavailable", e


def pct(n, d):
    return "0.0%" if not d else f"{(n / d) * 100:.1f}%"


def confidence_level(active: dict) -> tuple[str, str]:
    count = sum(1 for v in active.values() if v)
    voter_count = sum(1 for k, v in active.items() if k in VOTER_FIELDS and v)
    if count <= 2 and voter_count == 0:
        return "High confidence", "Quick counts are expected to match final counts for simple geography filters."
    if count <= 4 and voter_count <= 1:
        return "High confidence", "Quick counts are built from the current dataset and are suitable for exploration. Export/download files are the final source for delivery lists."
    return "Advanced filters selected", "Many filters are combined. Export/download files are the final source for delivery lists."


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
        st.markdown(f'<div class="cc-metric"><div class="label">Total Voters</div><div class="value">{summary["total"]:,}</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="cc-metric"><div class="label">Republican</div><div class="value">{summary["r"]:,}</div><div class="sub">{pct(summary["r"], summary["total"])}</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="cc-metric blue"><div class="label">Democrat</div><div class="value">{summary["d"]:,}</div><div class="sub">{pct(summary["d"], summary["total"])}</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="cc-metric green"><div class="label">Other / Unaffiliated</div><div class="value">{summary["o"]:,}</div><div class="sub">{pct(summary["o"], summary["total"])}</div></div>', unsafe_allow_html=True)




def _cc_bar_component(title: str, rows: list[tuple[str, int, str]], total: int, key: str | None = None):
    """Party/Gender bars with no iframe and no markdown-code indentation."""
    total = int(total or 0)

    rendered_rows = []
    for label, value, color in rows:
        value = int(value or 0)
        pct_val = (value / total * 100) if total else 0
        width = max(1, min(100, pct_val))
        rendered_rows.append(
            f'<div style="display:grid;grid-template-columns:155px minmax(170px,1fr) 145px;gap:10px;align-items:center;min-height:24px;margin:8px 0;">'
            f'<div style="display:flex;align-items:center;gap:8px;color:#071d3a;font-weight:900;font-size:13px;line-height:1.1;white-space:nowrap;">'
            f'<span style="display:inline-block;width:11px;height:11px;min-width:11px;border-radius:50%;background:{color};"></span>'
            f'<span>{html.escape(str(label))}</span></div>'
            f'<div style="height:12px;border-radius:999px;background:#071d3a;overflow:hidden;">'
            f'<div style="height:100%;width:{width:.1f}%;border-radius:999px;background:{color};"></div></div>'
            f'<div style="color:#071d3a;font-weight:900;font-size:13px;line-height:1.1;white-space:nowrap;">{value:,} ({pct_val:.1f}%)</div>'
            f'</div>'
        )

    chart_html = (
        f'<div style="box-sizing:border-box;width:100%;background:#f8f4ea;border:1px solid #b9ad99;border-radius:12px;'
        f'box-shadow:0 8px 18px rgba(7,29,58,.12);padding:16px 20px 22px 20px;margin:0 0 16px 0;overflow:visible;">'
        f'<h3 style="margin:0 0 8px 0;color:#071d3a;font-size:22px;font-weight:950;line-height:1.1;">{html.escape(str(title))}</h3>'
        f'<div style="color:#071d3a;font-weight:950;font-size:15px;margin:0 0 10px 0;">'
        f'{total:,}<span style="color:#5f6b7a;font-size:12px;font-weight:800;margin-left:4px;">Total</span></div>'
        f'{"".join(rendered_rows)}'
        f'</div>'
    )
    st.markdown(chart_html, unsafe_allow_html=True)



def render_party_chart(summary, title="Party Breakdown"):
    total = int(summary.get("total", 0) or 0)
    rows = [("Republican", int(summary.get("r", 0) or 0), "#d51f2a"), ("Democrat", int(summary.get("d", 0) or 0), "#2454d6"), ("Other / Unaffiliated", int(summary.get("o", 0) or 0), "#4c9a2a")]
    _cc_bar_component(title, rows, total)


def render_gender_chart(summary, title="Voters by Gender"):
    total = int(summary.get("total", 0) or 0)
    rows = [("Female", int(summary.get("f", 0) or 0), "#d51f2a"), ("Male", int(summary.get("m", 0) or 0), "#2454d6"), ("Unknown / Other", int(summary.get("u", 0) or 0), "#4c9a2a")]
    _cc_bar_component(title, rows, total)


def render_quick_exact_comparison():
    q = st.session_state.get("quick_summary")
    e = st.session_state.get("exact_summary")
    if not q or not e:
        return
    comp = pd.DataFrame([
        {"Metric": "Total", "Quick": q["total"], "Exact": e["total"], "Difference": e["total"] - q["total"]},
        {"Metric": "Republican", "Quick": q["r"], "Exact": e["r"], "Difference": e["r"] - q["r"]},
        {"Metric": "Democrat", "Quick": q["d"], "Exact": e["d"], "Difference": e["d"] - q["d"]},
        {"Metric": "Other", "Quick": q["o"], "Exact": e["o"], "Difference": e["o"] - q["o"]},
    ])
    st.markdown("### Quick vs Verified Comparison")
    st.dataframe(comp, width="stretch", hide_index=True)


def set_view(name: str):
    st.session_state["view"] = name

def render_top_nav():
    if "view" not in st.session_state:
        st.session_state["view"] = "dashboard"

    n1, n2, n3, n4 = st.columns([1, 1, 1, 1])
    with n1:
        if st.button("🏠 Dashboard", width="stretch"):
            set_view("dashboard")
            st.rerun()
    with n2:
        if st.button("🎯 Targeting", width="stretch"):
            set_view("targeting")
            st.rerun()
    with n3:
        if st.button("📊 Analysis", width="stretch"):
            set_view("analysis")
            st.rerun()
    with n4:
        if st.button("📤 Export", width="stretch"):
            set_view("export")
            st.rerun()


@st.cache_data(ttl=300, show_spinner=False)
def duckdb_count_cube_group(field: str, limit: int = 12) -> pd.DataFrame:
    """Small remote group-by for the home dashboard. Never downloads the cube."""
    field = str(field)
    if not re.fullmatch(r"[A-Za-z0-9_ /-]+", field):
        return pd.DataFrame(columns=[field, "Voters"])
    url = count_cube_url()
    query = f"""
        SELECT CAST({sql_ident(field)} AS VARCHAR) AS label, SUM(Voters) AS Voters
        FROM read_parquet({sql_lit(url)})
        WHERE CAST({sql_ident(field)} AS VARCHAR) IS NOT NULL
          AND TRIM(CAST({sql_ident(field)} AS VARCHAR)) <> ''
        GROUP BY CAST({sql_ident(field)} AS VARCHAR)
        ORDER BY Voters DESC
        LIMIT {int(limit)}
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try: con.execute("LOAD httpfs;")
            except Exception: pass
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame(columns=["label", "Voters"])
    finally:
        try: con.close()
        except Exception: pass


@st.cache_data(ttl=300, show_spinner=False)
def duckdb_count_cube_group_filtered(active_json: str, special_json: str, field: str, limit: int = 20) -> pd.DataFrame:
    """Remote quick-count group by from the count cube. Does not scan detail/index shards."""
    active = enforce_security_scope(json.loads(active_json or "{}"))
    special = json.loads(special_json or "{}")
    if not re.fullmatch(r"[A-Za-z0-9_ /-]+", str(field)):
        return pd.DataFrame(columns=["label", "Voters"])
    url = count_cube_url()
    where = count_cube_where_sql(active, special)
    query = f"""
        SELECT CAST({sql_ident(field)} AS VARCHAR) AS label, SUM(Voters) AS Voters
        FROM read_parquet({sql_lit(url)})
        {where}
        GROUP BY CAST({sql_ident(field)} AS VARCHAR)
        HAVING SUM(Voters) > 0
        ORDER BY Voters DESC
        LIMIT {int(limit)}
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try: con.execute("LOAD httpfs;")
            except Exception: pass
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame(columns=["label", "Voters"])
    finally:
        try: con.close()
        except Exception: pass

@st.cache_data(ttl=300, show_spinner=False)
def duckdb_county_party_table_filtered(active_json: str = "{}", special_json: str = "{}", limit: int = 67) -> pd.DataFrame:
    """County by party table for the load screen from the remote count cube.

    Campaign-scoped users must never see a statewide county table on the home
    snapshot. The WHERE clause reuses the same security-scoped quick-count SQL
    path as Create Universe counts.
    """
    active = enforce_security_scope(json.loads(active_json or "{}"))
    special = json.loads(special_json or "{}")
    url = count_cube_url()
    where = count_cube_where_sql(active, special)
    extra = "CAST(County AS VARCHAR) IS NOT NULL AND TRIM(CAST(County AS VARCHAR)) <> ''"
    if where:
        where = where + " AND " + extra
    else:
        where = "WHERE " + extra
    query = f"""
        SELECT
            CAST(County AS VARCHAR) AS County,
            SUM(Voters) AS Total,
            SUM(CASE WHEN CAST(Party AS VARCHAR)='R' THEN Voters ELSE 0 END) AS Republican,
            SUM(CASE WHEN CAST(Party AS VARCHAR)='D' THEN Voters ELSE 0 END) AS Democrat,
            SUM(CASE WHEN CAST(Party AS VARCHAR) NOT IN ('R','D') THEN Voters ELSE 0 END) AS Other
        FROM read_parquet({sql_lit(url)})
        {where}
        GROUP BY CAST(County AS VARCHAR)
        ORDER BY County
        LIMIT {int(limit)}
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try: con.execute("LOAD httpfs;")
            except Exception: pass
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame(columns=["County","Total","Republican","Democrat","Other"])
    finally:
        try: con.close()
        except Exception: pass

def duckdb_county_party_table(limit: int = 67) -> pd.DataFrame:
    return duckdb_county_party_table_filtered(json.dumps(enforce_security_scope({}), sort_keys=True), json.dumps({}, sort_keys=True), limit)

def render_icon_metric(label: str, value: int, sub: str = "", icon: str = "●", klass: str = ""):
    html = f'<div class="cc-icon-metric {klass}"><div class="cc-icon-dot {klass}">{icon}</div><div><div class="cc-icon-label">{label}</div><div class="cc-icon-value">{int(value or 0):,}</div><div class="cc-icon-sub">{sub}</div></div></div>'
    st.markdown(html, unsafe_allow_html=True)

def render_home_age_card(total: int, active_scope: dict | None = None):
    active_scope = enforce_security_scope(active_scope or {})
    age = duckdb_count_cube_group_filtered(json.dumps(active_scope, sort_keys=True), json.dumps({}, sort_keys=True), "Age_Range", 12)
    if age.empty or "Voters" not in age.columns:
        st.markdown('<div class="cc-home-card"><h3>Voters by Age Range</h3><p>Age range quick-count data is not available.</p></div>', unsafe_allow_html=True)
        return
    rows = []
    order = {"18-24":1,"25-34":2,"35-44":3,"45-54":4,"55-64":5,"65+":6,"65-74":7,"75-84":8,"85+":9}
    age["label"] = age["label"].astype(str).str.strip()
    age = age[~age["label"].str.lower().isin(["", "(blank)", "blank", "nan", "none", "null"])]
    age = age[age["Voters"].fillna(0).astype(float) > 0]
    age["sort"] = age["label"].map(lambda x: order.get(str(x), 99))
    age = age.sort_values(["sort", "label"]).head(9)
    maxv = max(int(age["Voters"].max() or 1), 1)
    for _, r in age.iterrows():
        lab = str(r.get("label", ""))
        val = int(r.get("Voters", 0) or 0)
        p = (val / total * 100) if total else 0
        w = max(2, val / maxv * 100)
        rows.append(f'<div class="cc-age-row"><b>{lab}</b><div class="cc-age-bar-bg"><div class="cc-age-bar" style="width:{w:.1f}%"></div></div><span>{p:.1f}%</span></div>')
    html = '<div class="cc-home-card"><h3>Voters by Age Range</h3>' + ''.join(rows) + '<div style="color:#94a3b8;font-size:12px;margin-top:10px;">Universe: Campaign scope</div></div>'
    st.markdown(html, unsafe_allow_html=True)

def render_home_geo_table(summary: dict, active_scope: dict | None = None):
    active_scope = enforce_security_scope(active_scope or {})
    df = duckdb_county_party_table_filtered(json.dumps(active_scope, sort_keys=True), json.dumps({}, sort_keys=True), 67)
    if df.empty:
        st.markdown('<div class="cc-home-card"><h3>County Breakdown</h3><p>County quick-count data is not available.</p></div>', unsafe_allow_html=True)
        return
    show = df.copy()
    for c in ["Total","Republican","Democrat","Other"]:
        if c in show.columns:
            show[c] = show[c].fillna(0).astype(int).map(lambda x: f"{x:,}")
    st.markdown('<div class="cc-home-card"><h3>County Breakdown by Party</h3>', unsafe_allow_html=True)
    cc_table(show, height=235, key="home_county_breakdown")
    st.markdown('</div>', unsafe_allow_html=True)


def render_statewide_snapshot():
    st.markdown('<div class="cc-home-title">Voter Snapshot</div>', unsafe_allow_html=True)
    scoped_active = enforce_security_scope({})

    summary = None
    err = None
    try:
        summary, err = quick_counts({})
    except Exception as e:
        err = e

    if not summary:
        try:
            total = int(manifest.get("total_rows", 0)) if isinstance(manifest, dict) else 0
        except Exception:
            total = 0
        summary = {"total": total, "r": 0, "d": 0, "o": 0}

    total = int(summary.get("total", 0) or 0)
    r = int(summary.get("r", 0) or 0)
    d = int(summary.get("d", 0) or 0)
    o = int(summary.get("o", 0) or 0)

    c1, c2, c3, c4 = st.columns(4)
    with c1: render_icon_metric("Total Voters", total, "100% of universe", "👥", "")
    with c2: render_icon_metric("Republican", r, pct(r, total) + " of universe", "🐘", "")
    with c3: render_icon_metric("Democrat", d, pct(d, total) + " of universe", "🫏", "blue")
    with c4: render_icon_metric("Other / Unaffiliated", o, pct(o, total) + " of universe", "●", "green")

    left, right = st.columns([1.0, 1.25])
    with left:
        render_party_chart(summary, "Voters by Party")
        gdf = duckdb_count_cube_group_filtered(json.dumps(scoped_active, sort_keys=True), json.dumps({}, sort_keys=True), "Gender", 8)
        if not gdf.empty and "Voters" in gdf.columns:
            gf = {str(row.get("label", "")).upper(): int(row.get("Voters", 0) or 0) for _, row in gdf.iterrows()}
            gs = {"total": sum(gf.values()), "f": gf.get("F", 0), "m": gf.get("M", 0), "u": sum(v for k, v in gf.items() if k not in {"F", "M"})}
            render_gender_chart(gs, "Voters by Gender")
    with right:
        render_home_age_card(total, scoped_active)
        render_home_geo_table(summary, scoped_active)

    if err and not (r or d or o):
        st.warning("Quick-count statewide party numbers were not available, so the app showed the manifest total only.")
    st.caption("Use the sidebar to build a campaign universe, search voters, open Mail Ballot Center, or view Area Intelligence.")

def quick_counts(active: dict):
    # v21i: use DuckDB against the remote quick-count parquet so Streamlit does
    # not download the entire count_cube into memory when Gender or other voter
    # filters are selected.
    try:
        summary = duckdb_count_cube_summary(
            json.dumps(count_safe_filters(with_campaign_boundary(active or {})), sort_keys=True),
            json.dumps({}, sort_keys=True),
        )
        return summary, None
    except Exception as e:
        return None, e



def special_required_columns(special: dict) -> set[str]:
    cols = set()
    if not special:
        return cols
    if "__PhoneReach" in special:
        cols.update(["HasMobile", "HasLandline"])
    ef = special.get("__ElectionFilters")
    if isinstance(ef, dict):
        cols.update(selected_election_columns(ef.get("years") or [], ef.get("types") or []))
    for k in special.keys():
        if not str(k).startswith("__"):
            cols.add(k)
    return cols

def exact_counts(active: dict):
    special = active_special_filters()
    needed = set(["Party"])
    needed.update(active.keys())
    needed.update(special_required_columns(special))
    cols = tuple(sorted(needed))

    total = 0
    r_count = 0
    d_count = 0
    o_count = 0

    progress = st.progress(0)
    status = st.empty()

    shard_count = int((load_manifest().get("index", {}) or {}).get("count", DETAIL_SHARDS) or DETAIL_SHARDS)
    for i in range(shard_count):
        key = f"index/voters_index_{i:03d}.parquet"
        status.write(f"Counting index shard {i+1} of {shard_count}: {key}")
        df = load_index_columns(key, cols)

        for col, vals in active.items():
            if vals and col == "Tags" and col in df.columns:
                df = df[tag_contains_mask(df[col], vals)]
            elif vals and col in df.columns:
                expanded_vals = expand_filter_values(col, vals)
                df = df[df[col].astype(str).isin([str(v) for v in expanded_vals])]
            elif vals:
                df = df.iloc[0:0]

        df = apply_special_filters(df, special)

        total += len(df)

        if "Party" in df.columns and not df.empty:
            party = df["Party"].astype(str).str.upper().str.strip()
            r_count += int((party == "R").sum())
            d_count += int((party == "D").sum())
            o_count += int((~party.isin(["R", "D"])).sum())

        del df
        progress.progress((i + 1) / shard_count)

    status.empty()
    return {"total": total, "r": r_count, "d": d_count, "o": o_count}


def build_export(active: dict, columns: list[str]):
    special = active_special_filters()
    if not active and not special:
        raise RuntimeError("Please select at least one filter before exporting.")

    needed = set(columns)
    needed.update(active.keys())
    needed.update(special_required_columns(special))
    cols = tuple(sorted(needed))

    parts = []
    total = 0
    progress = st.progress(0)
    status = st.empty()

    detail_count = int((load_manifest().get("detail", {}) or {}).get("count", DETAIL_SHARDS) or DETAIL_SHARDS)
    for i in range(detail_count):
        key = f"detail/voters_detail_{i:03d}.parquet"
        status.write(f"Building export from shard {i+1} of {detail_count}: {key}")
        df = load_detail_columns(key, cols)

        for col, vals in active.items():
            if vals and col == "Tags" and col in df.columns:
                df = df[tag_contains_mask(df[col], vals)]
            elif vals and col in df.columns:
                expanded_vals = expand_filter_values(col, vals)
                df = df[df[col].astype(str).isin([str(v) for v in expanded_vals])]
            elif vals:
                df = df.iloc[0:0]

        df = apply_special_filters(df, special)

        if not df.empty:
            keep_cols = [c for c in columns if c in df.columns]
            if keep_cols:
                df = df[keep_cols]
            parts.append(df)
            total += len(df)
            if total > EXPORT_ROW_LIMIT:
                raise RuntimeError(f"Export exceeds {EXPORT_ROW_LIMIT:,} rows. Narrow filters before exporting.")

        progress.progress((i + 1) / detail_count)

    status.empty()
    if not parts:
        return pd.DataFrame(columns=columns)
    return pd.concat(parts, ignore_index=True)




# -----------------------------------------------------------------------------
# Restored workspace helpers v21r (safe, remote-query-first)
# -----------------------------------------------------------------------------
def cc_text(v):
    try:
        if pd.isna(v): return ""
    except Exception:
        pass
    s = str(v).strip()
    if s.lower() in {"nan", "none", "null"}: return ""
    return re.sub(r"\s+", " ", s)


def smart_title(value, keep_upper: set[str] | None = None) -> str:
    s = cc_text(value)
    if not s:
        return ""
    keep_upper = keep_upper or {"PA","USA","US","PO","P.O.","LLC","III","IV","II","JR","SR","MDJ","SD","TWP","USC","STS","STH"}
    def one_token(tok: str) -> str:
        raw = tok
        lead = re.match(r"^([^A-Za-z0-9#]*)(.*?)([^A-Za-z0-9]*)$", raw)
        if not lead:
            return raw
        pre, core, post = lead.groups()
        if not core:
            return raw
        up = core.upper().replace('.', '')
        if up in keep_upper or re.fullmatch(r"[IVXLCM]+", up):
            return pre + up + post
        if re.fullmatch(r"\d+[A-Z]?", core):
            return pre + core.upper() + post
        if "-" in core:
            return pre + "-".join(one_token(part) for part in core.split("-")) + post
        if "'" in core:
            return pre + "'".join(one_token(part) for part in core.split("'")) + post
        return pre + core[:1].upper() + core[1:].lower() + post
    return " ".join(one_token(t) for t in re.sub(r"\s+", " ", s).split(" ")).strip()


def normalize_name_suffix(value) -> str:
    s = cc_text(value).replace('.', '')
    if not s:
        return ""
    up = s.upper()
    if up in {"JR","SR","II","III","IV","V","VI"}:
        return up
    return smart_title(s)


def normalize_phone_digits(value) -> str:
    s = cc_text(value)
    digits = re.sub(r"\D+", "", s)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits or s



def is_unusable_label(value) -> bool:
    """Labels we never want to show in filters, charts, or report tables."""
    s = clean_value(value).strip()
    if not s:
        return True
    low = s.lower()
    if low in {"(blank)", "blank", "nan", "none", "null", "unknown", "n/a", "na"}:
        return True
    # Pure numeric precinct/coded values like 01100 are internal codes, not usable public labels.
    if re.fullmatch(r"\d{4,}", s):
        return True
    return False

def mark_downloaded(*keys):
    for k in keys:
        _ = st.session_state.pop(k, None)


def canonical_precinct_display(value, municipality=""):
    """Return a public precinct label, never an internal numeric code.

    Valid labels can look like ``Dormont 00 01``.  Invalid internal labels are
    pure numeric strings like ``01100``.  Those are hidden instead of displayed.
    """
    raw = cc_text(value)
    if not raw or is_unusable_label(raw):
        return ""
    return raw.strip()

def clean_apartment_and_address2(df: pd.DataFrame) -> pd.DataFrame:
    """Keep Apartment Number to true unit values only; move other extra address text to Address Line 2."""
    if df is None or df.empty:
        return df
    df = df.copy()
    for c in ["Apartment Number", "Address Line 2"]:
        if c not in df.columns:
            df[c] = ""
    apt = df["Apartment Number"].astype(str).replace({"nan":"", "None":"", "<NA>":""}).str.strip()
    line2 = df["Address Line 2"].astype(str).replace({"nan":"", "None":"", "<NA>":""}).str.strip()
    apt_unit = re.compile(r"^(APT|APARTMENT|UNIT|#)\s*[A-Z0-9][A-Z0-9\-]*$", re.I)
    bare_apt = re.compile(r"^[A-Z0-9][A-Z0-9\-]{0,6}$", re.I)
    address2_unit = re.compile(r"^(STE|SUITE|RM|ROOM|FL|FLOOR|BLDG|BUILDING|TRLR|TRAILER|LOT|PO BOX|P\.?O\.? BOX|BOX)\b", re.I)
    street_words = re.compile(r"\b(ST|STREET|RD|ROAD|DR|DRIVE|AVE|AVENUE|LN|LANE|CT|COURT|CIR|CIRCLE|BLVD|WAY|PIKE|HWY|HIGHWAY|PKWY|PARKWAY|TER|TERRACE|PL|PLACE)\b", re.I)
    new_apt=[]; new_line2=[]
    for a,l in zip(apt, line2):
        aa=a.strip(); ll=l.strip()
        if not aa:
            new_apt.append(""); new_line2.append(ll); continue
        # Keep Apartment Number strict: only true apartment/unit identifiers.
        # Suite, PO Box, building/floor/trailer/lot and stray street text belong in Address Line 2.
        if street_words.search(aa) and not apt_unit.search(aa):
            combined = (ll + " " + aa).strip() if ll else aa
            new_apt.append(""); new_line2.append(combined); continue
        if address2_unit.search(aa):
            combined = (ll + " " + aa).strip() if ll else aa
            new_apt.append(""); new_line2.append(combined); continue
        if apt_unit.search(aa) or bare_apt.fullmatch(aa):
            new_apt.append(smart_title(aa, keep_upper={"APT","UNIT","PO","PA","JR","SR","III","IV"}))
            new_line2.append(ll)
        else:
            combined = (ll + " " + aa).strip() if ll else aa
            new_apt.append(""); new_line2.append(combined)
    df["Apartment Number"] = new_apt
    df["Address Line 2"] = [smart_title(x) for x in new_line2]
    return df

def household_display_name(group: pd.DataFrame) -> str:
    voters = group.copy()
    voters["_fn"] = voters.apply(full_name, axis=1).map(smart_title)
    voters["_last"] = voters.get("LastName", pd.Series([""]*len(voters), index=voters.index)).map(lambda x: smart_title(x).strip())
    names = [x for x in voters["_fn"].tolist() if x]
    lasts = [x for x in voters["_last"].tolist() if x]
    uniq_lasts = sorted({x for x in lasts if x})
    if len(names) == 0:
        return "Current Resident"
    if len(names) == 1:
        return names[0]
    if len(uniq_lasts) == 1:
        return f"{uniq_lasts[0]} Household"
    if len(names) <= 3:
        return " & ".join(names)
    return f"{names[0]} & Family"


def household_for_mail(df: pd.DataFrame) -> pd.DataFrame:
    """One mail row per household/address using the local app household naming logic."""
    if df is None or df.empty:
        return df
    df = normalize_download_df(df).copy()
    for c in ["County","Municipality","House Number","Street Name","Apartment Number","City","State","Zip"]:
        if c not in df.columns: df[c] = ""
    key = (df["County"].astype(str).str.upper().str.strip()+"|"+
           df["Municipality"].astype(str).str.upper().str.strip()+"|"+
           df["House Number"].astype(str).str.upper().str.strip()+"|"+
           df["Street Name"].astype(str).str.upper().str.strip()+"|"+
           df["Apartment Number"].astype(str).str.upper().str.strip()+"|"+
           df["City"].astype(str).str.upper().str.strip()+"|"+
           df["State"].astype(str).str.upper().str.strip()+"|"+
           df["Zip"].astype(str).str.upper().str.strip())
    df["_HH_KEY"] = key
    df["HouseholdCount"] = df.groupby("_HH_KEY")["_HH_KEY"].transform("size")
    hh_names = df.groupby("_HH_KEY", sort=False).apply(household_display_name).to_dict()
    out = df.sort_values(["Street Name","House Number","LastName","FirstName"], kind="stable").drop_duplicates("_HH_KEY", keep="first").copy()
    out["HouseholdName"] = out["_HH_KEY"].map(hh_names).fillna("")
    out["FullName"] = out["HouseholdName"]
    out["FirstName"] = out["HouseholdName"]
    out["MiddleName"] = ""
    out["LastName"] = ""
    out["NameSuffix"] = ""
    return out.drop(columns=["_HH_KEY"], errors="ignore")

def full_name(row):
    """Best voter display name, with strong fallbacks for lookup/household cards."""
    def usable(v):
        t = cc_text(v).strip()
        return "" if t.lower() in {"unnamed voter", "unnamed", "unknown", "none", "nan", "null"} else t

    # Prefer already-canonical display names if present. This prevents blank/partial
    # segmented fields from overriding a good FullName/Name value in the speed shards.
    for c in ["FullName", "Full Name", "Name", "VoterName", "Voter Name"]:
        val = usable(row.get(c, ""))
        if val:
            return val

    first = usable(row.get("FirstName", "")) or usable(row.get("first_name", "")) or usable(row.get("FIRST_NAME", ""))
    middle = usable(row.get("MiddleName", "")) or usable(row.get("middle_name", "")) or usable(row.get("MIDDLE_NAME", ""))
    last = usable(row.get("LastName", "")) or usable(row.get("last_name", "")) or usable(row.get("LAST_NAME", ""))
    suffix = usable(row.get("NameSuffix", "")) or usable(row.get("suffix", "")) or usable(row.get("SUFFIX", ""))
    parts = [first, middle, last, suffix]
    name = " ".join([p for p in parts if p]).strip()
    if name:
        return name

    # Last-resort fallbacks that are still better than showing repeated Unnamed Voter.
    vid = usable(row.get("voter_id", ""))
    return f"Voter {vid}" if vid else "Household Voter"

def address_line(row):
    hn = cc_text(row.get("House Number", ""))
    stn = cc_text(row.get("Street Name", ""))
    apt = cc_text(row.get("Apartment Number", ""))
    line = " ".join([x for x in [hn, stn] if x])
    return (line + (f" Apt {apt}" if apt else "")).strip() or cc_text(row.get("res_address", ""))

def format_phone_number(value):
    s = cc_text(value)
    digits = re.sub(r"\D+", "", s)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return s

def phone_entries(row):
    entries = []
    mobile = cc_text(row.get("Mobile", "")) or cc_text(row.get("MobilePhone", ""))
    land = cc_text(row.get("Landline", "")) or cc_text(row.get("Phone", ""))
    app = cc_text(row.get("Current_ApplicantPhone", "")) or cc_text(row.get("ApplicantPhone", ""))
    if mobile:
        entries.append((format_phone_number(mobile), "m"))
    if land:
        entries.append((format_phone_number(land), "l"))
    if app and app not in {mobile, land}:
        entries.append((format_phone_number(app), "u"))
    # De-dupe exact label/type pairs while preserving order.
    seen = set(); clean = []
    for num, typ in entries:
        key = (num, typ)
        if num and key not in seen:
            seen.add(key); clean.append((num, typ))
    return clean

def phone_label(row):
    return " / ".join([f"{num} ({typ})" for num, typ in phone_entries(row)])

def first_existing_col(df: pd.DataFrame, candidates: list[str]):
    if df is None or df.empty:
        return None
    wanted = [re.sub(r"[^a-z0-9]+", "", str(x).lower()) for x in candidates]
    for w in wanted:
        for c in df.columns:
            if re.sub(r"[^a-z0-9]+", "", str(c).lower()) == w:
                return c
    return None

def matching_cols(df: pd.DataFrame, candidates: list[str]) -> list[str]:
    if df is None or df.empty:
        return []
    wanted = [re.sub(r"[^a-z0-9]+", "", str(x).lower()) for x in candidates]
    hits=[]
    for w in wanted:
        for c in df.columns:
            if c in hits:
                continue
            if re.sub(r"[^a-z0-9]+", "", str(c).lower()) == w:
                hits.append(c)
    return hits

def coalesce_columns(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    out = pd.Series([""] * len(df), index=df.index, dtype="object")
    # Use every matching column, not just the first normalized hit. This fixes shards
    # that contain both a blank display column (FirstName) and a populated source
    # column (first_name / First Name).
    for col in matching_cols(df, candidates):
        vals = df[col].astype(str).replace({"nan":"", "None":"", "<NA>":""}).str.strip()
        mask = out.astype(str).str.strip().eq("") & vals.ne("")
        out.loc[mask] = vals.loc[mask]
    return out

def normalize_download_df(df: pd.DataFrame) -> pd.DataFrame:
    """Repair/standardize downloaded fields from current detail shards, with vendor-friendly casing."""
    if df is None or df.empty:
        return pd.DataFrame(columns=DEFAULT_EXPORT_COLUMNS)
    df = df.copy()
    aliases = {
        "voter_id": ["voter_id", "VoterID", "Voter ID", "IDNumber", "ID Number", "PA ID Number", "PA_ID_Number", "SURE_ID", "StateVoterID"],
        "County": ["County", "county", "CountyName"],
        "Municipality": ["Municipality", "municipality", "municipality_clean", "Municipality_Clean"],
        "Precinct": ["precinct", "Precinct"],
        "FirstName": ["FirstName", "First Name", "First_Name", "FIRSTNAME", "FIRST_NAME", "first_name", "fname", "FName", "FNAME", "first", "FIRST", "GivenName", "Given Name", "Given_Name", "NameFirst", "Name First", "NAMEFIRST", "NAME_FIRST", "name_first", "Voter First Name", "VoterFirstName", "Voter_First_Name", "Registrant First Name", "RegistrantFirstName", "Registrant_First_Name", "Given", "Given_Name", "FirstNm", "First_Nm"],
        "MiddleName": ["MiddleName", "Middle Name", "Middle_Name", "MIDDLENAME", "MIDDLE_NAME", "middle_name", "middle", "MiddleInitial", "Middle Initial", "middle_initial", "MName", "MI", "NameMiddle", "Name Middle", "NAME_MIDDLE", "name_middle", "Voter Middle Name", "VoterMiddleName", "Voter_Middle_Name", "Registrant Middle Name", "RegistrantMiddleName", "Registrant_Middle_Name", "MiddleNm", "Middle_Nm"],
        "LastName": ["LastName", "Last Name", "Last_Name", "LASTNAME", "LAST_NAME", "last_name", "surname", "lname", "LName", "LNAME", "last", "LAST", "FamilyName", "Family Name", "NameLast", "Name Last", "NAMELAST", "NAME_LAST", "name_last", "Voter Last Name", "VoterLastName", "Voter_Last_Name", "Registrant Last Name", "RegistrantLastName", "Registrant_Last_Name", "Surname", "LastNm", "Last_Nm"],
        "NameSuffix": ["NameSuffix", "Name Suffix", "Name_Suffix", "NAMESUFFIX", "suffix", "Suffix", "surnsuffix", "SurnSuffix", "SuffixName", "NameSuffixCode", "Name Suffix Code", "Suffix_Code"],
        "FullName": ["FullName", "Full Name", "Full_Name", "FULLNAME", "Name", "name", "VoterName", "Voter Name", "Voter_Name", "Voter Full Name", "VoterFullName", "Voter_Full_Name", "Registrant Name", "RegistrantName", "Registrant_Name", "DisplayName", "Display Name"],
        "Party": ["Party", "party", "party_raw", "PartyCode", "RegisteredParty"],
        "Gender": ["Gender", "gender", "Sex", "sex"],
        "DOB": ["DOB", "DateOfBirth", "Date of Birth", "Date_of_Birth", "DATEOFBIRTH", "BirthDate", "Birth Date", "birth_date", "dob"],
        "Age": ["Age", "age", "Age_Calc"],
        "Age_Range": ["Age_Range", "age_group", "Age Group"],
        "RegistrationDate": ["RegistrationDate", "Registration Date", "registration_date"],
        "House Number": ["House Number", "HouseNumber", "house_number", "res_house_number", "house_num", "street_number"],
        "House Number Suffix": ["House Number Suffix", "HouseNumberSuffix", "house_number_suffix"],
        "Street Name": ["Street Name", "StreetName", "street_name", "res_street_name", "street", "address_street"],
        "Apartment Number": ["Apartment Number", "ApartmentNumber", "Unit", "Apt", "apartment_number"],
        "Address Line 2": ["Address Line 2", "AddressLine2", "Address2", "address_line_2"],
        "City": ["City", "city", "res_city", "Mail City"],
        "State": ["State", "state", "res_state", "Mail State"],
        "Zip": ["Zip", "ZIP", "ZipCode", "zipcode", "res_zip", "Mail Zip"],
        "Email": ["Email", "EMAIL", "Current_Email", "email"],
        "Mobile": ["Mobile", "MobilePhone", "mobile_phone", "Cell", "CellPhone"],
        "Landline": ["Landline", "Phone", "phone", "HomePhone"],
        "Current_ApplicantPhone": ["Current_ApplicantPhone", "ApplicantPhone", "Applicant Phone"],
    }
    for out_col, cands in aliases.items():
        if out_col not in df.columns or df[out_col].astype(str).replace({"nan":""}).str.strip().eq("").mean() > .80:
            df[out_col] = coalesce_columns(df, cands)

    for c in ["County","Municipality","FirstName","MiddleName","LastName","Street Name","Apartment Number","Address Line 2","City","School District","School Region"]:
        if c in df.columns:
            df[c] = df[c].map(smart_title)
    if "NameSuffix" in df.columns:
        df["NameSuffix"] = df["NameSuffix"].map(normalize_name_suffix)
    if "State" in df.columns:
        df["State"] = df["State"].map(lambda x: cc_text(x).upper())
    for c in ["Mobile","Landline","Current_ApplicantPhone"]:
        if c in df.columns:
            df[c] = df[c].map(normalize_phone_digits)

    parts = []
    for c in ["FirstName", "MiddleName", "LastName", "NameSuffix"]:
        parts.append(df.get(c, pd.Series([""]*len(df), index=df.index)).astype(str).replace({"nan":""}).str.strip())
    built_full = (parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]).str.replace(r"\s+", " ", regex=True).str.strip()
    df["FullName"] = built_full.where(built_full.str.strip().ne(""), df.get("FullName", pd.Series([""]*len(df), index=df.index)).map(smart_title))

    if "Party" in df.columns:
        df["Party"] = df["Party"].map(lambda x: "R" if str(x).strip().upper() in {"R","REP","REPUBLICAN"} else ("D" if str(x).strip().upper() in {"D","DEM","DEMOCRAT","DEMOCRATIC"} else ("O" if str(x).strip() else "")))
    if "Gender" in df.columns:
        df["Gender"] = df["Gender"].map(lambda x: "M" if str(x).strip().upper() in {"M","MALE"} else ("F" if str(x).strip().upper() in {"F","FEMALE"} else ("U" if str(x).strip() else "")))

    for c in DEFAULT_EXPORT_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    if "Precinct" in df.columns:
        muni_series = df["Municipality"] if "Municipality" in df.columns else pd.Series([""]*len(df), index=df.index)
        df["Precinct"] = [canonical_precinct_display(p, m) for p, m in zip(df["Precinct"], muni_series)]
    df = clean_apartment_and_address2(df)
    election_cols = [c for c in df.columns if re.match(r"^[GP]\d{2}(?:_\d+)?$", str(c)) or re.match(r"^[GP]\d{2}(?:_\d+)?_method$", str(c))]
    ordered = DEFAULT_EXPORT_COLUMNS + [c for c in election_cols if c not in DEFAULT_EXPORT_COLUMNS]
    return df[ordered]

def drop_all_blank_optional_columns(df: pd.DataFrame, required: list[str] | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    required = required or ["voter_id"]
    keep = []
    for c in df.columns:
        nonblank = df[c].astype(str).replace({"nan":"", "None":""}).str.strip().ne("").any()
        if nonblank or c in required:
            keep.append(c)
    return df[keep]



def source_alias_candidates():
    return {
        "voter_id": ["voter_id", "VoterID", "Voter ID", "IDNumber", "ID Number", "PA ID Number", "PA_ID_Number", "SURE_ID", "StateVoterID"],
        "County": ["County", "county", "CountyName"],
        "Municipality": ["Municipality", "municipality", "municipality_clean", "Municipality_Clean"],
        "Precinct": ["precinct", "Precinct"],
        "FirstName": ["FirstName", "First Name", "First_Name", "FIRSTNAME", "FIRST_NAME", "first_name", "fname", "FName", "FNAME", "first", "FIRST", "GivenName", "Given Name", "Given_Name", "NameFirst", "Name First", "NAMEFIRST", "NAME_FIRST", "name_first", "Voter First Name", "VoterFirstName", "Voter_First_Name", "Registrant First Name", "RegistrantFirstName", "Registrant_First_Name", "Given", "Given_Name", "FirstNm", "First_Nm"],
        "MiddleName": ["MiddleName", "Middle Name", "Middle_Name", "MIDDLENAME", "MIDDLE_NAME", "middle_name", "middle", "MiddleInitial", "Middle Initial", "middle_initial", "MName", "MI", "NameMiddle", "Name Middle", "NAME_MIDDLE", "name_middle", "Voter Middle Name", "VoterMiddleName", "Voter_Middle_Name", "Registrant Middle Name", "RegistrantMiddleName", "Registrant_Middle_Name", "MiddleNm", "Middle_Nm"],
        "LastName": ["LastName", "Last Name", "Last_Name", "LASTNAME", "LAST_NAME", "last_name", "surname", "lname", "LName", "LNAME", "last", "LAST", "FamilyName", "Family Name", "NameLast", "Name Last", "NAMELAST", "NAME_LAST", "name_last", "Voter Last Name", "VoterLastName", "Voter_Last_Name", "Registrant Last Name", "RegistrantLastName", "Registrant_Last_Name", "Surname", "LastNm", "Last_Nm"],
        "NameSuffix": ["NameSuffix", "Name Suffix", "Name_Suffix", "NAMESUFFIX", "suffix", "Suffix", "surnsuffix", "SurnSuffix", "SuffixName", "NameSuffixCode", "Name Suffix Code", "Suffix_Code"],
        "FullName": ["FullName", "Full Name", "Full_Name", "FULLNAME", "Name", "name", "VoterName", "Voter Name", "Voter_Name", "Voter Full Name", "VoterFullName", "Voter_Full_Name", "Registrant Name", "RegistrantName", "Registrant_Name", "DisplayName", "Display Name"],
        "Party": ["Party", "party", "party_raw", "PartyCode", "RegisteredParty"],
        "Gender": ["Gender", "gender", "Sex", "sex"],
        "DOB": ["DOB", "DateOfBirth", "Date of Birth", "Date_of_Birth", "DATEOFBIRTH", "BirthDate", "Birth Date", "birth_date", "dob"],
        "Age": ["Age", "age", "Age_Calc"],
        "Age_Range": ["Age_Range", "age_group", "Age Group"],
        "RegistrationDate": ["RegistrationDate", "Registration Date", "registration_date"],
        "House Number": ["House Number", "HouseNumber", "house_number", "res_house_number", "house_num", "street_number"],
        "House Number Suffix": ["House Number Suffix", "HouseNumberSuffix", "house_number_suffix"],
        "Street Name": ["Street Name", "StreetName", "street_name", "res_street_name", "street", "address_street"],
        "Apartment Number": ["Apartment Number", "ApartmentNumber", "Unit", "Apt", "apartment_number"],
        "Address Line 2": ["Address Line 2", "AddressLine2", "Address2", "address_line_2"],
        "City": ["City", "city", "res_city", "Mail City"],
        "State": ["State", "state", "res_state", "Mail State"],
        "Zip": ["Zip", "ZIP", "ZipCode", "zipcode", "res_zip", "Mail Zip"],
        "Email": ["Email", "EMAIL", "Current_Email", "email"],
        "Mobile": ["Mobile", "MobilePhone", "mobile_phone", "Cell", "CellPhone"],
        "Landline": ["Landline", "Phone", "phone", "HomePhone"],
        "Current_ApplicantPhone": ["Current_ApplicantPhone", "ApplicantPhone", "Applicant Phone"],
        "MB_PERM": ["MB_PERM", "MB Perm", "MBPerm", "PermanentMB", "MB_Perm"],
        "MB_App": ["MB_App", "MB App"],
        "MB_App_Status": ["MB_App_Status", "MB App Status"],
        "MB_Sent": ["MB_Sent", "MB Sent"],
        "MB_Status": ["MB_Status", "MB Status"],
        "Tags": ["Tags", "tags", "Tag", "tag"],
    }

@st.cache_data(ttl=900, show_spinner=False)
def remote_parquet_columns(urls) -> list[str]:
    # Fast schema check: shard schemas are consistent, so inspect only the first URL.
    # The old version inspected the full URL list, which made voter lookup feel stuck.
    one_url = urls[0] if isinstance(urls, (list, tuple)) and urls else urls
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        df0 = con.execute(f"SELECT * FROM read_parquet({one_url!r}, union_by_name=true) LIMIT 0").df()
        return list(df0.columns)
    finally:
        con.close()

def first_existing_column(existing_cols, candidates):
    existing_map = {str(c).lower(): c for c in existing_cols}
    for cand in candidates:
        if str(cand).lower() in existing_map:
            return existing_map[str(cand).lower()]
    return None

def safe_remote_select_exprs(existing_cols, out_cols):
    aliases = source_alias_candidates()
    exprs = []
    for out_col in out_cols:
        src = first_existing_column(existing_cols, aliases.get(out_col, [out_col]))
        if src:
            exprs.append(f"CAST({sql_ident(src)} AS VARCHAR) AS {sql_ident(out_col)}")
        else:
            exprs.append(f"CAST(NULL AS VARCHAR) AS {sql_ident(out_col)}")
    return ", ".join(exprs)

def safe_search_blob_expr(existing_cols):
    aliases = source_alias_candidates()
    search_out = ["FullName", "FirstName", "MiddleName", "LastName", "NameSuffix", "County", "Municipality", "Precinct", "voter_id", "Mobile", "Landline", "Email", "Street Name", "City", "Zip"]
    srcs = []
    seen = set()
    for out in search_out:
        src = first_existing_column(existing_cols, aliases.get(out, [out]))
        if src and src.lower() not in seen:
            srcs.append(src)
            seen.add(src.lower())
    if not srcs:
        return "''"
    return "CONCAT_WS(' ', " + ", ".join([f"CAST({sql_ident(c)} AS VARCHAR)" for c in srcs]) + ")"

def report_columns():
    return list(DEFAULT_EXPORT_COLUMNS)

@st.cache_data(ttl=600, show_spinner=False)
def remote_search_voters(term, max_rows=25):
    """Fast lookup against lightweight index shards using structured predicates.

    This avoids the slow all-column CONCAT/LIKE scan that made Voter Lookup feel stuck.
    For searches like "Elmer Bowman York", York is treated as a county filter,
    and first/last name predicates are applied directly when those columns exist.
    """
    raw_term = str(term or "").strip()
    urls = voter_search_urls_for_term(raw_term)
    base_lookup_cols = [
        "voter_id", "FullName", "FirstName", "MiddleName", "LastName", "NameSuffix",
        "House Number", "House Number Suffix", "Street Name", "Apartment Number", "Address Line 2",
        "City", "State", "Zip", "Precinct", "Municipality", "County",
        "Party", "Gender", "DOB", "RegistrationDate", "Age",
        "USC", "STS", "STH", "School District", "School Region",
        "Mobile", "Landline", "Current_ApplicantPhone", "Email",
        "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "Tags", "HH_LOOKUP_KEY"
    ]
    # Keep the search row intentionally small and fast. Vote history is loaded on demand.
    lookup_cols = base_lookup_cols
    if not raw_term:
        return pd.DataFrame(columns=lookup_cols)

    county_token, search_tokens = _lookup_county_token_and_search_tokens(raw_term)

    existing_cols = remote_parquet_columns(urls)
    aliases = source_alias_candidates()
    select_cols = safe_remote_select_exprs(existing_cols, lookup_cols)

    def src(out_name):
        return first_existing_column(existing_cols, aliases.get(out_name, [out_name]))

    c_voter = src("voter_id")
    c_full = src("FullName")
    c_first = src("FirstName")
    c_middle = src("MiddleName")
    c_last = src("LastName")
    c_county = src("County")
    c_house = src("House Number")
    c_street = src("Street Name")
    c_city = src("City")
    c_zip = src("Zip")
    c_mobile = src("Mobile")
    c_land = src("Landline")
    c_email = src("Email")

    where_parts = []
    if county_token and c_county:
        where_parts.append(f"LOWER(CAST({sql_ident(c_county)} AS VARCHAR)) = {sql_lit(county_token)}")

    digits = re.sub(r"\D+", "", raw_term)
    if len(digits) >= 6 and c_voter:
        where_parts.append(f"CAST({sql_ident(c_voter)} AS VARCHAR) LIKE {sql_lit('%' + digits + '%')}")
    elif "@" in raw_term and c_email:
        email_term = raw_term.lower().replace("'", "''")
        where_parts.append(f"LOWER(CAST({sql_ident(c_email)} AS VARCHAR)) LIKE {sql_lit('%' + email_term + '%')}")
    elif len(digits) >= 7 and (c_mobile or c_land):
        phone_parts = []
        for pc in [c_mobile, c_land]:
            if pc:
                phone_parts.append(f"REGEXP_REPLACE(CAST({sql_ident(pc)} AS VARCHAR), '[^0-9]', '', 'g') LIKE {sql_lit('%' + digits + '%')}")
        if phone_parts:
            where_parts.append("(" + " OR ".join(phone_parts) + ")")
    elif search_tokens:
        # Name-first search. Two tokens usually means first + last.
        if len(search_tokens) >= 2 and c_first and c_last:
            first_t = search_tokens[0]
            last_t = search_tokens[-1]
            where_parts.append(f"LOWER(CAST({sql_ident(c_first)} AS VARCHAR)) LIKE {sql_lit(first_t + '%')}")
            where_parts.append(f"LOWER(CAST({sql_ident(c_last)} AS VARCHAR)) LIKE {sql_lit(last_t + '%')}")
        else:
            searchable = [c_full, c_first, c_middle, c_last, c_house, c_street, c_city, c_zip]
            searchable = [c for c in searchable if c]
            if searchable:
                blob = "CONCAT_WS(' ', " + ", ".join([f"CAST({sql_ident(c)} AS VARCHAR)" for c in searchable]) + ")"
                for t in search_tokens:
                    where_parts.append(f"LOWER({blob}) LIKE {sql_lit('%' + t + '%')}")

    # Hard campaign boundary for scoped users. This prevents Voter Lookup from
    # returning voters outside the campaign's assigned office/universe.
    for scope_field, scope_vals in security_scope_filters().items():
        scope_col = src(scope_field)
        if scope_col and scope_vals:
            where_parts.append(
                f"CAST({sql_ident(scope_col)} AS VARCHAR) IN ("
                + ",".join(sql_lit(v) for v in scope_vals)
                + ")"
            )

    if not where_parts:
        return pd.DataFrame(columns=lookup_cols)

    order_parts = [c for c in [c_last, c_first, c_house, c_street] if c]
    order_sql = (" ORDER BY " + ", ".join(sql_ident(c) for c in order_parts)) if order_parts else ""
    where = " AND ".join(where_parts)

    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try:
                con.execute('LOAD httpfs;')
            except Exception:
                pass
        query = f"SELECT {select_cols} FROM read_parquet({urls!r}, union_by_name=true) WHERE {where}{order_sql} LIMIT {int(max_rows)}"
        df = con.execute(query).df()
        if df.empty:
            return pd.DataFrame(columns=lookup_cols)
        # Light normalization only; do not run export cleanup here.
        for c in df.columns:
            if c in {"FirstName","MiddleName","LastName","NameSuffix","FullName","Street Name","City","Municipality","County","Precinct"}:
                df[c] = df[c].map(smart_title)
        # Rebuild missing/placeholder names from segmented name fields so result cards
        # never fall back to Unnamed Voter when SURE has the pieces available.
        rebuilt = df.apply(full_name, axis=1).map(smart_title)
        if "FullName" not in df.columns:
            df["FullName"] = rebuilt
        else:
            bad = df["FullName"].astype(str).str.strip().eq("") | df["FullName"].astype(str).str.lower().isin(["unnamed voter", "nan", "none", "null"])
            df.loc[bad, "FullName"] = rebuilt.loc[bad]
        return df
    except Exception as e:
        st.error(f"Lookup query failed quickly instead of hanging: {e}")
        return pd.DataFrame(columns=lookup_cols)
    finally:
        con.close()

@st.cache_data(ttl=600, show_spinner=False)
def remote_voter_detail(voter_id: str) -> pd.Series:
    """Fetch one full voter record from detail shards by voter_id, then normalize display fields."""
    vid = cc_text(voter_id)
    if not vid:
        return pd.Series(dtype="object")
    urls = detail_urls_from_manifest()
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        df = con.execute(
            f"SELECT * FROM read_parquet({urls!r}, union_by_name=true) "
            f"WHERE CAST({sql_ident('voter_id')} AS VARCHAR) = {sql_lit(vid)} LIMIT 1"
        ).df()
        if df.empty:
            return pd.Series(dtype="object")
        df = normalize_download_df(df)
        return df.iloc[0]
    finally:
        con.close()


@st.cache_data(ttl=600, show_spinner=False)
def remote_voter_lookup_detail(voter_id: str) -> pd.Series:
    """Fetch the selected voter's display/detail row by exact voter_id.

    This is intentionally narrower than the full export cleanup path so the lookup
    page can show DOB, districts, household keys, and vote history without pulling
    the entire export schema.
    """
    vid = cc_text(voter_id)
    if not vid:
        return pd.Series(dtype="object")
    urls = voter_detail_lookup_urls_for_id(vid)
    existing_cols = remote_parquet_columns(urls)
    base_cols = [
        "voter_id", "FullName", "FirstName", "MiddleName", "LastName", "NameSuffix",
        "House Number", "House Number Suffix", "Street Name", "Apartment Number", "Address Line 2",
        "City", "State", "Zip", "Precinct", "Municipality", "County",
        "Party", "Gender", "DOB", "RegistrationDate", "Age",
        "USC", "STS", "STH", "School District", "School Region",
        "HH_ID", "Household_ID", "Household_Party", "HouseholdCount", "HH_LOOKUP_KEY",
        "Mobile", "Landline", "Current_ApplicantPhone", "Email",
        "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "Tags"
    ]
    # Pull every election/history column actually present in the detail shard, not just
    # columns listed in the manifest. Some builds store vote method in sibling fields
    # such as G24_method / G24_VoteMethod; if those are not selected the app can only
    # infer At Poll from the party row.
    manifest_election_cols = [c for c in election_columns_from_manifest() if c not in base_cols]
    existing_election_cols = [c for c in existing_cols if election_meta_from_col(c) and c not in base_cols]
    lookup_cols = base_cols + manifest_election_cols + [c for c in existing_election_cols if c not in manifest_election_cols]
    select_cols = safe_remote_select_exprs(existing_cols, lookup_cols)
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        id_col = first_existing_column(existing_cols, source_alias_candidates().get("voter_id", ["voter_id"])) or "voter_id"
        df = con.execute(
            f"SELECT {select_cols} FROM read_parquet({urls!r}, union_by_name=true) "
            f"WHERE CAST({sql_ident(id_col)} AS VARCHAR) = {sql_lit(vid)} LIMIT 1"
        ).df()
        if df.empty:
            return pd.Series(dtype="object")
        # Light display normalization.
        for c in ["FirstName","MiddleName","LastName","FullName","Street Name","City","Municipality","County","Precinct","School District","School Region"]:
            if c in df.columns:
                df[c] = df[c].map(smart_title)
        if "NameSuffix" in df.columns:
            df["NameSuffix"] = df["NameSuffix"].map(normalize_name_suffix)
        if "State" in df.columns:
            df["State"] = df["State"].map(lambda x: cc_text(x).upper())
        parts = []
        for c in ["FirstName", "MiddleName", "LastName", "NameSuffix"]:
            parts.append(df.get(c, pd.Series([""]*len(df), index=df.index)).astype(str).replace({"nan":""}).str.strip())
        built_full = (parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]).str.replace(r"\s+", " ", regex=True).str.strip()
        if "FullName" not in df.columns:
            df["FullName"] = built_full
        else:
            bad_full = df["FullName"].astype(str).str.strip().eq("") | df["FullName"].astype(str).str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null", "household voter"]) | df["FullName"].astype(str).str.lower().str.startswith("voter ")
            df.loc[bad_full, "FullName"] = built_full.loc[bad_full]
        if "Precinct" in df.columns:
            muni = df["Municipality"] if "Municipality" in df.columns else pd.Series([""]*len(df), index=df.index)
            df["Precinct"] = [canonical_precinct_display(p, m) for p, m in zip(df["Precinct"], muni)]
        return df.iloc[0]
    finally:
        con.close()



@st.cache_data(ttl=600, show_spinner=False)
def remote_voter_search_exact_by_id(voter_id: str) -> pd.Series:
    """Fetch a thin search-card row by voter_id from the fast last-name shards.

    This is used as a name fallback for household cards because the household
    shard is intentionally thin and older speed builds may have blank FullName
    for non-selected household members.
    """
    vid = cc_text(voter_id)
    if not vid:
        return pd.Series(dtype="object")
    urls = voter_search_all_urls() or voter_lookup_or_detail_urls()
    if not urls:
        return pd.Series(dtype="object")
    existing_cols = remote_parquet_columns(urls)
    aliases = source_alias_candidates()
    id_col = first_existing_column(existing_cols, aliases.get("voter_id", ["voter_id"])) or "voter_id"
    cols = [
        "voter_id", "FullName", "Name", "FirstName", "MiddleName", "LastName", "NameSuffix",
        "Party", "Gender", "Age", "DOB", "House Number", "Street Name", "City", "State", "Zip",
        "County", "Municipality", "Precinct", "HH_LOOKUP_KEY"
    ]
    select_cols = safe_remote_select_exprs(existing_cols, cols)
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        df = con.execute(
            f"SELECT {select_cols} FROM read_parquet({urls!r}, union_by_name=true) "
            f"WHERE CAST({sql_ident(id_col)} AS VARCHAR) = {sql_lit(vid)} LIMIT 1"
        ).df()
        if df.empty:
            return pd.Series(dtype="object")
        for c in ["FirstName", "MiddleName", "LastName", "NameSuffix", "FullName", "Name", "Street Name", "City", "Municipality", "County", "Precinct"]:
            if c in df.columns:
                df[c] = df[c].map(smart_title)
        if "NameSuffix" in df.columns:
            df["NameSuffix"] = df["NameSuffix"].map(normalize_name_suffix)
        rebuilt = df.apply(full_name, axis=1).map(smart_title)
        if "FullName" not in df.columns:
            df["FullName"] = rebuilt
        else:
            bad = df["FullName"].astype(str).str.strip().eq("") | df["FullName"].astype(str).str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null"])
            df.loc[bad, "FullName"] = rebuilt.loc[bad]
        return df.iloc[0]
    finally:
        con.close()


def _draw_branded_header(c, title: str, subtitle: str = ""):
    """Simple branded PDF header. Keeps PDF generation from depending on app-only helpers."""
    w, h = landscape(letter)
    c.setFillColorRGB(0.50, 0.05, 0.12)
    c.roundRect(28, h - 54, w - 56, 32, 6, stroke=0, fill=1)
    c.setFillColorRGB(1, 1, 1)
    c.setFont("Helvetica-Bold", 15)
    c.drawString(42, h - 43, title)
    if subtitle:
        c.setFont("Helvetica", 8)
        c.drawRightString(w - 42, h - 42, str(subtitle)[:60])
    c.setFillColorRGB(0,0,0)
    return h - 70


def _election_short_label(col: str) -> str:
    meta = election_meta_from_col(col) or {}
    typ = str(meta.get("type", "") or "").upper()
    year = str(meta.get("year", "") or "")
    if year.startswith("20") and len(year) == 4:
        yy = year[-2:]
    else:
        m = re.search(r"(\d{2})(?!.*\d)", str(col))
        yy = m.group(1) if m else year[-2:]
    prefix = "G" if typ.startswith("GENERAL") else ("P" if typ.startswith("PRIMARY") else ("S" if typ.startswith("SPECIAL") else str(col).strip()[:1].upper()))
    return f"{prefix}{yy}" if yy else str(col).split("_")[0].upper()


def _is_method_column_name(col: str) -> bool:
    u = re.sub(r"[^A-Z0-9]+", "_", str(col).upper())
    return any(tok in u for tok in ["METHOD", "VOTE_METHOD", "VOTEMETHOD", "BALLOT", "VOTE_TYPE", "VOTETYPE", "MODE", "CAST"])


def _history_groups_for_row(row: pd.Series, prefix: str) -> list[tuple[str, list[str]]]:
    """Return [(G24/P24 label, candidate columns)] with all party and method sibling fields."""
    row = row if isinstance(row, pd.Series) else pd.Series(row or {})
    row_cols = [c for c in row.index if election_meta_from_col(c)]
    # Fall back to manifest order when the row came from an older thin source.
    if not row_cols:
        row_cols = election_columns_from_manifest()
    groups = {}
    for c in row_cols:
        short = _election_short_label(c)
        if not short.upper().startswith(prefix):
            continue
        groups.setdefault(short, [])
        if c not in groups[short]:
            groups[short].append(c)
    def key(item):
        short = item[0]
        m = re.search(r"(\d{2})", short)
        return int(m.group(1)) if m else -1
    return sorted(groups.items(), key=key, reverse=True)


def _party_from_history_group(row: pd.Series, cols: list[str]) -> str:
    # Prefer non-method columns because they usually store the party at time/current party code.
    ordered = sorted(cols, key=lambda c: 1 if _is_method_column_name(c) else 0)
    for c in ordered:
        raw = cc_text(row.get(c, "")).upper()
        if raw in {"R", "D"}:
            return raw
        if raw in {"REP", "REPUBLICAN"}:
            return "R"
        if raw in {"DEM", "DEMOCRAT", "DEMOCRATIC"}:
            return "D"
    # If the only populated value is a vote method, use the voter current party as the displayed party.
    for c in cols:
        if normalize_vote_method(row.get(c, "")):
            p = cc_text(row.get("Party", "")).upper()
            if p in {"R", "D"}:
                return p
            if p in {"REP", "REPUBLICAN"}:
                return "R"
            if p in {"DEM", "DEMOCRAT", "DEMOCRATIC"}:
                return "D"
            if p:
                return "O"
    return ""


def _method_from_history_group(row: pd.Series, cols: list[str]) -> str:
    # Prefer explicitly named method/ballot columns, then any value that normalizes to a method.
    method_cols = [c for c in cols if _is_method_column_name(c)]
    other_cols = [c for c in cols if c not in method_cols]
    for c in method_cols + other_cols:
        m = normalize_vote_method(row.get(c, ""))
        if m:
            return m
    # Only infer At Poll if there is a party/vote record but no explicit method.
    return "At Poll" if _party_from_history_group(row, cols) else ""


def _history_payload(row: pd.Series, limit: int | None = None):
    """Return de-duplicated election history tuples for PDF: (label, party, method)."""
    row = row if isinstance(row, pd.Series) else pd.Series(row or {})
    def build(prefix: str):
        groups = _history_groups_for_row(row, prefix)
        if limit is not None:
            groups = groups[:int(limit)]
        return [(short, _party_from_history_group(row, cols), _method_from_history_group(row, cols)) for short, cols in groups]
    return build("G"), build("P")

def make_voter_lookup_pdf(row: pd.Series, household: pd.DataFrame | None = None) -> bytes:
    """Branded voter lookup report with full available vote history."""
    if canvas is None:
        return b"PDF support unavailable."
    bio = io.BytesIO()
    c = canvas.Canvas(bio, pagesize=landscape(letter))
    w, h = landscape(letter)
    y = _draw_branded_header(c, "Voter Lookup Report", datetime.now().strftime("%m/%d/%Y")) - 18

    name = smart_title(full_name(row)) or "Selected Voter"
    c.setFillColorRGB(0.50, 0.05, 0.12)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(36, y, name[:60]); y -= 16
    c.setFillColorRGB(0.05,0.05,0.05)
    c.setFont("Helvetica", 9)
    c.drawString(36, y, smart_title(address_line(row))[:95]); y -= 22

    c.setFont("Helvetica-Bold", 8)
    metrics = [("Party", row.get("Party","")), ("Gender", row.get("Gender","")), ("Age", row.get("Age","")), ("PA ID", row.get("voter_id",""))]
    x=36
    for lab,val in metrics:
        c.setFillColorRGB(0.35,0.35,0.35); c.drawString(x,y,lab)
        c.setFillColorRGB(0,0,0); c.setFont("Helvetica-Bold", 11); c.drawString(x,y-13,cc_text(val) or "—")
        c.setFont("Helvetica-Bold", 8); x += 130
    y -= 36

    def table_box(title, rows, x, y, width, row_h=12):
        c.setFillColorRGB(0.56,0.06,0.13); c.roundRect(x, y-12, width, 15, 3, fill=1, stroke=0)
        c.setFillColorRGB(1,1,1); c.setFont("Helvetica-Bold", 8); c.drawString(x+5, y-8, title)
        y -= 17
        c.setFont("Helvetica", 6.8); c.setFillColorRGB(0,0,0)
        for i,(a,b) in enumerate(rows):
            if y < 55: break
            if i % 2 == 0:
                c.setFillColorRGB(0.96,0.90,0.91); c.rect(x, y-row_h+3, width, row_h, stroke=0, fill=1)
            c.setFillColorRGB(0,0,0)
            c.setFont("Helvetica-Bold", 6.5); c.drawString(x+4, y-7, str(a)[:25])
            c.setFont("Helvetica", 6.5); c.drawString(x+108, y-7, str(b)[:52])
            y -= row_h
        return y

    details = [
        ("Date of Birth", row.get("DOB", "")), ("Registration Date", row.get("RegistrationDate", "")),
        ("Registered Party", row.get("Party", "")), ("County", row.get("County", "")),
        ("Municipality", row.get("Municipality", "")), ("Precinct", row.get("Precinct", "")),
        ("Congressional", row.get("USC", "")), ("State Senate", row.get("STS", "")),
        ("State House", row.get("STH", "")), ("School District", row.get("School District", "")),
        ("School Region", row.get("School Region", "")),
    ]
    contact = [
        ("Mobile", format_phone_number(row.get("Mobile", ""))), ("Landline", format_phone_number(row.get("Landline", ""))),
        ("Mail Ballot Application Phone", format_phone_number(row.get("Current_ApplicantPhone", ""))), ("Email", row.get("Email", "")),
        ("Mail Ballot Applied", row.get("MB_App", "")), ("Application Status", row.get("MB_App_Status", "")),
        ("Ballot Sent", row.get("MB_Sent", "")), ("Ballot Status", row.get("MB_Status", "")),
        ("Permanent MB", row.get("MB_PERM", "")), ("Tags", row.get("Tags", "")),
    ]
    y_left = table_box("Voter Details", details, 36, y, 340)
    y_right = table_box("Contact + Mail Ballot", contact, 410, y, 340)
    y = min(y_left, y_right) - 12

    if household is not None and not household.empty and y > 105:
        c.setFillColorRGB(0.56,0.06,0.13); c.roundRect(36, y-12, w-72, 15, 3, fill=1, stroke=0)
        c.setFillColorRGB(1,1,1); c.setFont("Helvetica-Bold", 8); c.drawString(41, y-8, "Household Members")
        y -= 18
        c.setFillColorRGB(0,0,0); c.setFont("Helvetica-Bold", 6.5)
        c.drawString(40, y, "Name"); c.drawString(250, y, "Party"); c.drawString(295, y, "Gender"); c.drawString(345, y, "Age"); y -= 10
        for _, rr in household.head(6).iterrows():
            nm = smart_title(full_name(rr))
            if nm.lower() == "unnamed voter":
                nm = smart_title(cc_text(rr.get("FullName", ""))) or "Unnamed Voter"
            mark = "✓ " if cc_text(rr.get("voter_id", "")) == cc_text(row.get("voter_id", "")) else ""
            c.setFont("Helvetica", 6.5)
            c.drawString(40, y, (mark + nm)[:45])
            c.drawString(250, y, cc_text(rr.get("Party", ""))[:5])
            c.drawString(295, y, cc_text(rr.get("Gender", ""))[:5])
            c.drawString(345, y, cc_text(rr.get("Age", ""))[:5])
            y -= 9
        y -= 8

    def ensure_space(needed=95):
        nonlocal y
        if y < needed:
            c.showPage()
            y = _draw_branded_header(c, "Voter Lookup Report", "Election History") - 18

    general, primary = _history_payload(row, limit=None)
    ensure_space(115)
    c.setFillColorRGB(0.56,0.06,0.13); c.roundRect(36, y-12, w-72, 15, 3, fill=1, stroke=0)
    c.setFillColorRGB(1,1,1); c.setFont("Helvetica-Bold", 8); c.drawString(41, y-8, "Full Election History")
    y -= 22

    def draw_hist(title, items, x, y, max_cols=10):
        c.setFillColorRGB(0,0,0); c.setFont("Helvetica-Bold", 7.2); c.drawString(x,y,title); y-=10
        if not items:
            c.setFont("Helvetica", 6.5); c.drawString(x, y, "No history found."); return y-10
        for start in range(0, len(items), max_cols):
            chunk = items[start:start+max_cols]
            if y < 48:
                c.showPage(); y = _draw_branded_header(c, "Voter Lookup Report", "Election History") - 18
            x0=x+42; step=20
            c.setFont("Helvetica-Bold", 6.2)
            for i,it in enumerate(chunk): c.drawCentredString(x0+i*step, y, it[0])
            y-=9
            c.drawString(x,y,"Party")
            for i,it in enumerate(chunk): c.drawCentredString(x0+i*step, y, it[1])
            y-=9
            c.drawString(x,y,"Method")
            for i,it in enumerate(chunk): c.drawCentredString(x0+i*step, y, vote_method_pdf_label(it[2]))
            y-=13
        return y

    def draw_hist_wide(title, items, y, max_cols=20):
        """Compact, boxed election history grid for PDF readability."""
        if not items:
            c.setFillColorRGB(0,0,0); c.setFont("Helvetica-Bold", 8); c.drawString(36, y, title)
            c.setFont("Helvetica", 6.5); c.drawString(92, y, "No history found.")
            return y - 14

        usable_w = w - 72
        left_label_w = 44
        row_h = 12

        for start in range(0, len(items), max_cols):
            chunk = items[start:start+max_cols]
            if y < 58:
                c.showPage(); y = _draw_branded_header(c, "Voter Lookup Report", "Election History") - 18

            cols = max(1, len(chunk))
            cell_w = (usable_w - left_label_w) / cols

            c.setFillColorRGB(0.56,0.06,0.13)
            c.roundRect(36, y-12, usable_w, 15, 3, fill=1, stroke=0)
            c.setFillColorRGB(1,1,1); c.setFont("Helvetica-Bold", 7.5)
            c.drawString(42, y-8, title)
            y -= 18

            x = 36
            table_w = left_label_w + cell_w * len(chunk)
            table_h = row_h * 3

            c.setFillColorRGB(0.88,0.90,0.94); c.rect(x, y-row_h, table_w, row_h, stroke=0, fill=1)
            c.setFillColorRGB(0.97,0.97,0.98); c.rect(x, y-row_h*2, table_w, row_h, stroke=0, fill=1)
            c.setFillColorRGB(1,1,1); c.rect(x, y-row_h*3, table_w, row_h, stroke=0, fill=1)

            c.setStrokeColorRGB(0.72,0.72,0.76); c.setLineWidth(0.35)
            for rline in range(4):
                yy = y - row_h*rline
                c.line(x, yy, x+table_w, yy)
            c.line(x, y, x, y-table_h)
            c.line(x+left_label_w, y, x+left_label_w, y-table_h)
            for i in range(len(chunk)+1):
                xx = x + left_label_w + cell_w*i
                c.line(xx, y, xx, y-table_h)

            c.setFillColorRGB(0.08,0.08,0.08)
            c.setFont("Helvetica-Bold", 5.8)
            c.drawCentredString(x + left_label_w/2, y-row_h+3.2, "Election")
            c.drawCentredString(x + left_label_w/2, y-row_h*2+3.2, "Party")
            c.drawCentredString(x + left_label_w/2, y-row_h*3+3.2, "Method")
            for i,it in enumerate(chunk):
                cx = x + left_label_w + cell_w*i + cell_w/2
                c.setFont("Helvetica-Bold", 5.8)
                c.drawCentredString(cx, y-row_h+3.2, cc_text(it[0])[:4])
                c.setFont("Helvetica-Bold", 5.8)
                c.drawCentredString(cx, y-row_h*2+3.2, cc_text(it[1])[:2])
                c.setFont("Helvetica", 5.8)
                c.drawCentredString(cx, y-row_h*3+3.2, vote_method_pdf_label(it[2])[:2])

            y -= table_h + 10
        return y

    y = draw_hist_wide("General Elections", general, y, max_cols=20) - 4
    y = draw_hist_wide("Primary Elections", primary, y, max_cols=20) - 6
    if y < 36:
        c.showPage(); y = 70
    c.setFont("Helvetica", 6.3); c.setFillColorRGB(0.25,0.25,0.25)
    c.drawString(36, max(28, y), "Legend: MB = Mail Ballot   AP = At Poll   PV = Provisional   blank = Did Not Vote / no record")
    c.save(); bio.seek(0); return bio.getvalue()

def remote_household_members(row: pd.Series, max_rows: int = 25) -> pd.DataFrame:
    """Find household members from one household-hash shard.

    Step 8 v23 writes speed/voter_household_hash_00..63.parquet with HH_LOOKUP_KEY.
    That avoids scanning every last-name shard just to find people at the same address.
    """
    hh_key_value = cc_text(row.get("HH_LOOKUP_KEY", "")) or household_lookup_key(row)
    cols = [
        "voter_id", "FullName", "Name", "FirstName", "MiddleName", "LastName", "NameSuffix",
        "first_name", "middle_name", "last_name", "suffix",
        "Party", "Gender", "Age", "DOB", "House Number", "Street Name", "Apartment Number",
        "City", "State", "Zip", "County", "Municipality", "Precinct", "HH_LOOKUP_KEY"
    ]
    if not hh_key_value or hh_key_value.count("|") < 4:
        return pd.DataFrame(columns=cols)

    hh_url = voter_household_lookup_url(row)
    if hh_url:
        urls = [hh_url]
        existing_cols = remote_parquet_columns(urls)
        key_col = first_existing_column(existing_cols, ["HH_LOOKUP_KEY"])
        if key_col:
            where = f"CAST({sql_ident(key_col)} AS VARCHAR) = {sql_lit(hh_key_value)}"
        else:
            where = "FALSE"
    else:
        # Fallback for an older shard build: slower, but still works.
        urls = voter_search_all_urls() or voter_lookup_or_detail_urls()
        existing_cols = remote_parquet_columns(urls)
        aliases = source_alias_candidates()
        conditions = []
        for field in ["County", "House Number", "Street Name", "Zip"]:
            val = cc_text(row.get(field, ""))
            src = first_existing_column(existing_cols, aliases.get(field, [field]))
            if val and src:
                conditions.append(f"LOWER(CAST({sql_ident(src)} AS VARCHAR)) = {sql_lit(val.lower())}")
        apt_val = cc_text(row.get("Apartment Number", ""))
        apt_src = first_existing_column(existing_cols, aliases.get("Apartment Number", ["Apartment Number"]))
        if apt_val and apt_src:
            conditions.append(f"LOWER(CAST({sql_ident(apt_src)} AS VARCHAR)) = {sql_lit(apt_val.lower())}")
        if len(conditions) < 3:
            return pd.DataFrame(columns=cols)
        where = " AND ".join(conditions)

    select_cols = safe_remote_select_exprs(existing_cols, cols)
    aliases = source_alias_candidates()
    order_parts = []
    for out in ["LastName", "FirstName"]:
        src = first_existing_column(existing_cols, aliases.get(out, [out]))
        if src:
            order_parts.append(sql_ident(src))
    order_sql = (" ORDER BY " + ", ".join(order_parts)) if order_parts else ""
    con = duckdb.connect(database=':memory:')
    try:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
        except Exception:
            try: con.execute('LOAD httpfs;')
            except Exception: pass
        df = con.execute(
            f"SELECT {select_cols} FROM read_parquet({urls!r}, union_by_name=true) "
            f"WHERE {where}{order_sql} LIMIT {int(max_rows)}"
        ).df()
        if df.empty:
            return df
        for c in ["FirstName", "MiddleName", "LastName", "NameSuffix", "FullName", "Name", "Street Name", "City", "Municipality", "County", "Precinct"]:
            if c in df.columns:
                df[c] = df[c].map(smart_title)
        if "NameSuffix" in df.columns:
            df["NameSuffix"] = df["NameSuffix"].map(normalize_name_suffix)
        rebuilt = df.apply(full_name, axis=1).map(smart_title)
        existing_full = df.get("FullName", pd.Series([""]*len(df), index=df.index)).astype(str).str.strip()
        df["FullName"] = existing_full
        mask = df["FullName"].eq("") | df["FullName"].str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null"])
        df.loc[mask, "FullName"] = rebuilt.loc[mask]
        # If the name still is not available, show a useful address/person label instead of repeating Unnamed Voter.
        missing = df["FullName"].astype(str).str.strip().eq("") | df["FullName"].astype(str).str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null"])
        df.loc[missing, "FullName"] = df.loc[missing].apply(lambda rr: f"Voter {cc_text(rr.get('voter_id',''))}" if cc_text(rr.get('voter_id','')) else "Household Voter", axis=1)
        return df
    finally:
        con.close()


def correction_store() -> dict:
    """Saved voter corrections with layered persistence.

    Durability order:
      1) R2 app_state/voter_record_corrections.json uploaded by Pipeline Manager
      2) local JSON state when running locally
      3) browser URL query parameter for refresh/reboot survival of recent edits
    """
    if "voter_corrections" not in st.session_state or not isinstance(st.session_state.get("voter_corrections"), dict):
        state_corr = _json_safe_corrections(_load_state().get("voter_corrections") or {})
        try:
            url_corr = decode_corrections(st.query_params.get(CORRECTIONS_PARAM, ""))
        except Exception:
            url_corr = {}
        state_corr.update(url_corr or {})
        st.session_state["voter_corrections"] = state_corr
    return st.session_state["voter_corrections"]


def persist_corrections():
    clean = _json_safe_corrections(st.session_state.get("voter_corrections", {}) or {})
    st.session_state["voter_corrections"] = clean
    try:
        _ = _persist_state_section("voter_corrections", clean)
    except Exception:
        pass
    # Also store in the browser URL so refresh/reboot keeps recent edits until they are committed to app_state/R2.
    try:
        encoded = encode_corrections(clean)
        if encoded and len(encoded) < 7000:
            st.query_params[CORRECTIONS_PARAM] = encoded
        elif CORRECTIONS_PARAM in st.query_params:
            del st.query_params[CORRECTIONS_PARAM]
    except Exception:
        pass


def correction_rows_df() -> pd.DataFrame:
    rows = []
    for vid, payload in correction_store().items():
        base = {"voter_id": vid, "updated_at": payload.get("updated_at", ""), "notes": payload.get("notes", "")}
        base.update(payload.get("fields", {}))
        rows.append(base)
    return pd.DataFrame(rows)


def apply_local_correction(row: pd.Series) -> pd.Series:
    vid = cc_text(row.get("voter_id", ""))
    payload = correction_store().get(vid)
    if not payload:
        return row
    out = row.copy()
    for k, v in (payload.get("fields", {}) or {}).items():
        out[k] = v
    return out




def _blank_vote_value(val) -> bool:
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except Exception:
        pass
    s = str(val).strip()
    return s == "" or s.upper() in {"0", "N", "NO", "NONE", "NULL", "NAN", "FALSE", "DID NOT VOTE", "DNV"}


def normalize_vote_method(val):
    """Normalize SURE/election vote-method values for voter lookup election history.

    Important: Pennsylvania/SURE-style history fields commonly use A/AB/ABS for
    absentee/mail-style voting and P/POLL for polling-place voting. Older builds
    were treating bare "A" as At Poll, which made mail/absentee voters look like
    they always voted at the polls.
    """
    if _blank_vote_value(val):
        return ""
    v = str(val).strip()
    vu = v.upper().replace("-", " ").replace("_", " ")
    vu = re.sub(r"\s+", " ", vu).strip()
    if vu in {"M", "MB", "MAIL", "MAIL BALLOT", "MAIL IN", "MAILIN", "MAIL BALLOT", "MAIL IN BALLOT", "MAIL-IN BALLOT"} or "MAIL" in vu:
        return "Mail Ballot"
    if vu in {"A", "AB", "ABS", "ABSENTEE", "ABSENTEE BALLOT"} or "ABSENT" in vu:
        return "Mail Ballot"
    if vu in {"PV", "PROV", "PROVISIONAL"} or "PROV" in vu:
        return "Provisional"
    if vu in {"P", "AP", "AT POLL", "AT POLLS", "POLL", "POLLS", "POLLING", "IN PERSON", "IP", "ELECTION DAY", "VOTED"} or "POLL" in vu or "PERSON" in vu:
        return "At Poll"
    # A bare party value is not a method. The history payload may still use it to
    # infer At Poll only when it is a real party code, not blank/zero.
    if vu in {"R", "D", "O", "I", "NP", "REP", "DEM", "REPUBLICAN", "DEMOCRAT", "DEMOCRATIC"}:
        return ""
    return v.title()


def vote_method_icon(method: str) -> str:
    m = normalize_vote_method(method)
    if m == "Mail Ballot":
        return "✉️"
    if m == "At Poll":
        return "🗳️"
    if m == "Provisional":
        return "🟨"
    return ""


def vote_method_pdf_label(method_or_icon: str) -> str:
    """ReportLab base fonts do not reliably render emoji, so the PDF uses short labels."""
    v = cc_text(method_or_icon)
    if v in {"✉", "✉️"}:
        return "MB"
    if v in {"🗳", "🗳️"}:
        return "AP"
    if v in {"🟨"}:
        return "PV"
    m = normalize_vote_method(v)
    if m == "Mail Ballot":
        return "MB"
    if m == "At Poll":
        return "AP"
    if m == "Provisional":
        return "PV"
    return ""



def _dedup_history_cols_for_row(cols, row: pd.Series, prefix: str):
    """Keep one column per displayed election code, preferring the column with data for this voter."""
    raw_cols = [c for c in (cols or []) if str(c).upper().startswith(prefix)]
    raw_cols = sorted(raw_cols, key=lambda c: str(c).upper(), reverse=True)
    chosen = {}
    order = []
    for c in raw_cols:
        short = str(c).split("_")[0].upper()
        if short not in chosen:
            chosen[short] = c
            order.append(short)
        else:
            prev = chosen[short]
            if _blank_vote_value(row.get(prev, "")) and not _blank_vote_value(row.get(c, "")):
                chosen[short] = c
    return [chosen[k] for k in order]


def render_election_history_table(row: pd.Series):
    """Draw visible blank/no-vote years and correct method icons for the selected voter."""
    row = row if isinstance(row, pd.Series) else pd.Series(row or {})

    def draw(label, prefix):
        st.markdown(f"**{label} Elections**")
        groups = _history_groups_for_row(row, prefix)
        if not groups:
            st.caption("No election history columns available in this shard build.")
            return
        party_row = {"Row": "Party"}
        method_row = {"Row": "Method"}
        for short, cols in groups:
            party_row[short] = _party_from_history_group(row, cols)
            m = _method_from_history_group(row, cols)
            method_row[short] = vote_method_icon(m) or vote_method_pdf_label(m)
        hist_df = pd.DataFrame([party_row, method_row])

        def _cell(v):
            return cc_text(v).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        headers = list(hist_df.columns)
        html = [
            '<div class="cc-history-scroll"><table class="cc-history-table">',
            '<thead><tr>' + ''.join(f'<th>{_cell(h)}</th>' for h in headers) + '</tr></thead>',
            '<tbody>'
        ]
        for _, rr in hist_df.iterrows():
            html.append('<tr>' + ''.join(f'<td>{_cell(rr.get(h, ""))}</td>' for h in headers) + '</tr>')
        html.append('</tbody></table></div>')
        css = """
<style>
.cc-history-scroll { max-width: 100%; overflow-x: auto; margin: 6px 0 14px 0; }
.cc-history-table { border-collapse: collapse; min-width: 760px; background: #0b0f19; color: #f8fafc; font-size: 12px; }
.cc-history-table th, .cc-history-table td { border: 1px solid rgba(148,163,184,.28); text-align: center !important; vertical-align: middle !important; padding: 8px 10px; line-height: 1.45; min-width: 38px; height: 36px; }
.cc-history-table th:first-child, .cc-history-table td:first-child { position: sticky; left: 0; z-index: 2; background: #111827 !important; color:#ffffff !important; -webkit-text-fill-color:#ffffff !important; min-width: 58px; font-weight: 800; }
.cc-history-table th:first-child *, .cc-history-table td:first-child * { color:#ffffff !important; -webkit-text-fill-color:#ffffff !important; }
.cc-history-table th { position: sticky; top: 0; z-index: 3; background: #1f2430; color:#ffffff !important; font-weight: 800; }
.cc-history-table tr:nth-child(even) td { background: #0f1724; }
.cc-history-table tr:nth-child(odd) td { background: #090d16; }
</style>
"""
        st.markdown(css + ''.join(html), unsafe_allow_html=True)

    draw("General", "G")
    draw("Primary", "P")
    st.caption("Legend: ✉️ = Mail Ballot · 🗳️ = At Poll · 🟨 = Provisional · blank = Did Not Vote / no record")

def render_voter_lookup_workspace():
    st.markdown("## Voter Lookup")
    q = st.session_state.get(special_key("lookup_query"), "")
    maxn = st.session_state.get(special_key("lookup_max"), 25)
    if not q:
        st.info("Enter a voter name, address, PA ID, phone, or email in the left pane.")
        return

    with st.spinner("Searching voters..."):
        df = remote_search_voters(q, maxn)
    st.caption(f"{len(df)} result(s) found for: {q}")
    if df.empty:
        st.warning("No voters found.")
        return

    if "lookup_selected_id" not in st.session_state or not cc_text(st.session_state.get("lookup_selected_id", "")):
        st.session_state["lookup_selected_id"] = cc_text(df.iloc[0].get("voter_id", ""))

    left, right = st.columns([0.85, 1.8])
    with left:
        st.markdown("### Search Results")
        pass

        for i, r0 in df.iterrows():
            vid = cc_text(r0.get("voter_id", ""))
            nm = smart_title(full_name(r0))
            age = cc_text(r0.get("Age", ""))
            first_line = f"{nm}, {age}" if age else nm
            addr = smart_title(address_line(r0))
            county = cc_text(r0.get('County',''))
            label = f"{first_line}\n{addr}\n{county} County"
            btn_type = "primary" if vid == st.session_state.get("lookup_selected_id") else "secondary"
            if st.button(label, key=f"lookup_pick_{vid or i}", width="stretch", type=btn_type):
                st.session_state["lookup_selected_id"] = vid
                # Clicking a card loads the full selected voter detail immediately.
                try:
                    full_detail = remote_voter_lookup_detail(vid) if vid else r0
                    st.session_state[f"lookup_detail_row_{vid}"] = pd.DataFrame([full_detail if len(full_detail) else r0]).iloc[0].to_dict()
                except Exception:
                    st.session_state[f"lookup_detail_row_{vid}"] = pd.DataFrame([r0]).iloc[0].to_dict()
                # Clear stale per-voter display sections for the previous selection.
                for k in list(st.session_state.keys()):
                    if str(k).startswith(("hh_df_", "vote_history_row_", "voter_pdf_bytes_", "voter_pdf_name_")):
                        _ = st.session_state.pop(k, None)
                st.rerun()

    with right:
        selected_id = cc_text(st.session_state.get("lookup_selected_id", ""))
        match = df[df["voter_id"].astype(str) == selected_id] if "voter_id" in df.columns else pd.DataFrame()
        cached_selected = st.session_state.get(f"lookup_detail_row_{selected_id}") if selected_id else None
        if not match.empty:
            index_row = match.iloc[0]
        elif isinstance(cached_selected, dict) and cached_selected:
            index_row = pd.Series(cached_selected)
        else:
            index_row = df.iloc[0]
        # Keep the selected voter view fast: use the lightweight lookup row first.
        # Full detail/election history/PDF are loaded only when the user asks for them.
        detail_key = f"lookup_detail_row_{selected_id}"
        if detail_key in st.session_state and isinstance(st.session_state.get(detail_key), dict):
            detail = pd.Series(st.session_state[detail_key])
        else:
            # First visible row also gets full detail so DOB/history fields are restored without another click.
            try:
                detail = remote_voter_lookup_detail(selected_id) if selected_id else index_row
                if detail is None or len(detail) == 0:
                    detail = index_row
                st.session_state[detail_key] = pd.DataFrame([detail]).iloc[0].to_dict()
            except Exception:
                detail = index_row
        r = apply_local_correction(pd.DataFrame([detail]).iloc[0])

        st.markdown(f"## {smart_title(full_name(r))}")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Party", cc_text(r.get("Party", "")) or "—")
        m2.metric("Gender", cc_text(r.get("Gender", "")) or "—")
        m3.metric("Age", cc_text(r.get("Age", "")) or "—")
        m4.metric("DOB", cc_text(r.get("DOB", "")) or "—")
        pdf_key = f"voter_pdf_bytes_{selected_id}"
        pdf_name_key = f"voter_pdf_name_{selected_id}"
        pc1, pc2 = st.columns([0.35, 1.65])
        with pc1:
            if st.button("Prepare PDF Report", key=f"prepare_voter_pdf_{selected_id}"):
                with st.spinner("Building voter PDF..."):
                    full_r = remote_voter_lookup_detail(selected_id) if selected_id else r
                    if full_r is None or len(full_r) == 0:
                        full_r = r
                    full_r = apply_local_correction(pd.DataFrame([full_r]).iloc[0])
                    try:
                        pdf_hh = remote_household_members(full_r)
                    except Exception:
                        pdf_hh = None
                    st.session_state[pdf_key] = make_voter_lookup_pdf(full_r, pdf_hh)
                    st.session_state[pdf_name_key] = f"candidate_connect_voter_report_{selected_id or 'voter'}.pdf"
                    st.rerun()
        if st.session_state.get(pdf_key):
            with pc2:
                st.download_button(
                    "Download PDF Report",
                    st.session_state[pdf_key],
                    file_name=st.session_state.get(pdf_name_key, f"candidate_connect_voter_report_{selected_id or 'voter'}.pdf"),
                    mime="application/pdf",
                    key=f"download_voter_pdf_{selected_id}",
                )

        d1, d2 = st.columns(2)
        with d1:
            st.markdown("### Voter Details")
            voter_rows = [
                ["Date of Birth", r.get("DOB", "")],
                ["Registration Date", r.get("RegistrationDate", "")],
                ["Registered Party", r.get("Party", "")],
                ["County", r.get("County", "")],
                ["Municipality", r.get("Municipality", "")],
                ["Precinct", r.get("Precinct", "")],
                ["Congressional", r.get("USC", "")],
                ["State Senate", r.get("STS", "")],
                ["State House", r.get("STH", "")],
                ["School District", r.get("School District", "")],
                ["School Region", r.get("School Region", "")],
            ]
            cc_table(pd.DataFrame(voter_rows, columns=["Field", "Value"]), height=360, key=f"voter_details_tbl_{selected_id}")
        with d2:
            st.markdown("### Contact + Mail Ballot")
            contact_rows = [
                ["Mobile", format_phone_number(r.get("Mobile", ""))],
                ["Landline", format_phone_number(r.get("Landline", ""))],
                ["Mail Ballot Application Phone", format_phone_number(r.get("Current_ApplicantPhone", ""))],
                ["Email", r.get("Email", "")],
                ["Mail Ballot Applied", r.get("MB_App", "")],
                ["Application Status", r.get("MB_App_Status", "")],
                ["Ballot Sent", r.get("MB_Sent", "")],
                ["Ballot Status", r.get("MB_Status", "")],
                ["Permanent MB", r.get("MB_PERM", "")],
                ["Tags", r.get("Tags", "")],
            ]
            cc_table(pd.DataFrame(contact_rows, columns=["Field", "Value"]), height=330, key=f"voter_contact_tbl_{selected_id}")

        with st.expander("Edit / Correct This Voter Record", expanded=False):
            if selected_id in correction_store():
                st.info("This voter currently has a saved correction in this browser session. Download the correction CSV and place it in the pipeline correction folder before the next pipeline run.")
            else:
                st.caption("Corrections are stored in this browser session and can be downloaded as a CSV for the pipeline correction workflow.")

            fields = [
                "FirstName", "MiddleName", "LastName", "NameSuffix",
                "Gender", "Party", "DOB", "RegistrationDate",
                "House Number", "House Number Suffix", "Street Name", "Apartment Number", "Address Line 2", "City", "State", "Zip",
                "County", "Municipality", "Precinct", "School District", "School Region", "USC", "STS", "STH",
                "Mobile", "Landline", "Current_ApplicantPhone", "Email",
                "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "Tags",
            ]
            existing_payload = correction_store().get(selected_id, {})
            existing_fields = existing_payload.get("fields", {}) or {}
            edits = {}
            group_specs = [
                ("Name", fields[0:4]),
                ("Voter Details", fields[4:8]),
                ("Address", fields[8:16]),
                ("Geography", fields[16:24]),
                ("Contact + Mail Ballot", fields[24:]),
            ]
            for title, group_fields in group_specs:
                st.markdown(f"**{title}**")
                cols = st.columns(4)
                for j, field in enumerate(group_fields):
                    val = cc_text(existing_fields.get(field, r.get(field, "")))
                    with cols[j % 4]:
                        edits[field] = st.text_input(field, value=val, key=f"edit_{selected_id}_{field}")
            notes = st.text_area("Correction Notes", value=existing_payload.get("notes", ""), key=f"edit_{selected_id}_notes")
            ca, cb, cc = st.columns([1, 1, 1])
            with ca:
                if st.button("Save Voter Correction", type="primary", key=f"save_corr_{selected_id}"):
                    correction_store()[selected_id] = {"updated_at": datetime.now().isoformat(timespec="seconds"), "fields": edits, "notes": notes}
                    persist_corrections()
                    current = dict(st.session_state.get(detail_key, {}) or {})
                    current.update(edits)
                    st.session_state[detail_key] = current
                    st.success("Correction saved and is available for correction CSV download.")
                    st.rerun()
            with cb:
                if st.button("Remove Saved Correction", key=f"remove_corr_{selected_id}"):
                    correction_store().pop(selected_id, None)
                    persist_corrections()
                    st.success("Saved correction removed.")
                    st.rerun()
            with cc:
                one_payload = {"voter_id": selected_id, "updated_at": datetime.now().isoformat(timespec="seconds"), "notes": notes, **edits}
                st.download_button("Download This Correction", pd.DataFrame([one_payload]).to_csv(index=False).encode(), file_name=f"voter_correction_{selected_id or 'unknown'}.csv", mime="text/csv")

            all_corr = correction_rows_df()
            if not all_corr.empty:
                st.download_button("Download All Saved Corrections CSV", all_corr.to_csv(index=False).encode(), file_name="candidate_connect_voter_corrections.csv", mime="text/csv", width="stretch")

        st.markdown("### Election History")
        vh_key = f"vote_history_row_{selected_id}"
        if vh_key not in st.session_state:
            st.session_state[vh_key] = pd.DataFrame([r]).iloc[0].to_dict()
        if st.button("Refresh Vote History", key=f"load_vote_history_{selected_id}"):
            with st.spinner("Loading vote history..."):
                full_r = remote_voter_lookup_detail(selected_id) if selected_id else r
                if full_r is None or len(full_r) == 0:
                    full_r = r
                st.session_state[vh_key] = pd.DataFrame([full_r]).iloc[0].to_dict()
                st.session_state[detail_key] = st.session_state[vh_key]
                st.rerun()
        render_election_history_table(pd.Series(st.session_state[vh_key]))

        st.markdown("### Household Members")
        hh_key = f"hh_df_{selected_id}"
        if hh_key not in st.session_state:
            with st.spinner("Household members are loading below..."):
                st.session_state[hh_key] = remote_household_members(r).to_dict("records")
        if st.button("Refresh Household Members", key=f"load_hh_{selected_id}"):
            with st.spinner("Household members are loading below..."):
                st.session_state[hh_key] = remote_household_members(r).to_dict("records")
        if st.session_state.get(hh_key):
            hh = pd.DataFrame(st.session_state.get(hh_key) or [])
            if not hh.empty:
                view = hh[[c for c in ["voter_id", "FullName", "Name", "FirstName", "MiddleName", "LastName", "NameSuffix", "first_name", "middle_name", "last_name", "suffix", "Party", "Gender", "Age", "County", "City", "State"] if c in hh.columns]].copy()
                # Do not show a table here. Build household member cards and make each
                # non-current card directly clickable to load that voter.
                view["DisplayName"] = hh.apply(full_name, axis=1).map(smart_title)
                view["DisplayName"] = view["DisplayName"].replace({"Unnamed Voter": "", "Unnamed voter": ""})
                missing_names = view["DisplayName"].astype(str).str.strip().eq("") | view["DisplayName"].astype(str).str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null"])
                # If the fast household shard has IDs/party/age but not name pieces,
                # fill names from the exact voter detail hash before falling back to an ID label.
                for idx in view.index[missing_names]:
                    hvid_for_name = cc_text(view.at[idx, "voter_id"]) if "voter_id" in view.columns else ""
                    if not hvid_for_name:
                        continue
                    nm = ""
                    try:
                        # Fastest name fallback first: search-card shard by exact voter_id.
                        # Then try the detail hash, and finally the older full detail shards.
                        for fetcher in (remote_voter_search_exact_by_id, remote_voter_lookup_detail, remote_voter_detail):
                            detail_name_row = fetcher(hvid_for_name)
                            nm = smart_title(full_name(detail_name_row))
                            if nm and nm.lower() not in {"unnamed voter", "unnamed", "nan", "none", "null"} and not nm.lower().startswith("voter "):
                                break
                    except Exception:
                        nm = ""
                    if nm and nm.lower() not in {"unnamed voter", "unnamed", "nan", "none", "null"}:
                        view.at[idx, "DisplayName"] = nm
                missing_names = view["DisplayName"].astype(str).str.strip().eq("") | view["DisplayName"].astype(str).str.lower().isin(["unnamed voter", "unnamed", "nan", "none", "null"])
                view.loc[missing_names, "DisplayName"] = view.loc[missing_names].apply(lambda rr: f"Voter {cc_text(rr.get('voter_id',''))}" if cc_text(rr.get('voter_id','')) else "Household Voter", axis=1)

                pass


                st.caption("Household members — click a card to open that voter.")
                for j, hhrow in view.iterrows():
                    hvid = cc_text(hhrow.get("voter_id", ""))
                    hname = smart_title(cc_text(hhrow.get("DisplayName", ""))) or hvid or "Household Voter"
                    is_current = hvid == selected_id
                    sub_bits = [x for x in [cc_text(hhrow.get("Party", "")), cc_text(hhrow.get("Gender", "")), ("Age " + cc_text(hhrow.get("Age", "")) if cc_text(hhrow.get("Age", "")) else "")] if x]
                    sub = " · ".join(sub_bits)
                    label = ("✓ " if is_current else "") + hname + (f"\n{sub}" if sub else "")
                    _card_col, _spacer_r = st.columns([0.42, 0.58])
                    with _card_col:
                        if is_current:
                            st.button(label, key=f"hh_current_{selected_id}_{j}_{hvid}", width="stretch", disabled=True)
                        else:
                            if st.button(label, key=f"open_household_card_{selected_id}_{j}_{hvid}", width="stretch"):
                                st.session_state["lookup_selected_id"] = hvid
                                try:
                                    hd = remote_voter_lookup_detail(hvid)
                                    st.session_state[f"lookup_detail_row_{hvid}"] = pd.DataFrame([hd]).iloc[0].to_dict()
                                except Exception:
                                    pass
                                for k in list(st.session_state.keys()):
                                    if str(k).startswith(("hh_df_", "vote_history_row_", "voter_pdf_bytes_", "voter_pdf_name_")):
                                        _ = st.session_state.pop(k, None)
                                st.rerun()
        else:
            st.caption("No household members found from the selected address.")



def safe_filtered_df(active: dict | None, max_rows: int = EXPORT_ROW_LIMIT) -> pd.DataFrame:
    active = enforce_security_scope(active or {})
    special = active_special_filters() if "active_special_filters" in globals() else {}
    try:
        df = duckdb_detail_filtered_df(active, special, int(max_rows))
    except Exception as exc:
        st.warning(f"Could not prepare filtered voter file: {exc}")
        return pd.DataFrame()
    try:
        return normalize_download_df(df)
    except Exception:
        return df


def _mb_total_from_summary(summary: dict | None) -> int:
    if not summary:
        return 0
    for k in ["total", "Total", "TOTAL", "voters", "Voters"]:
        try:
            if k in summary:
                return int(float(summary.get(k) or 0))
        except Exception:
            pass
    try:
        return int(float(summary.get("R", 0) or 0) + float(summary.get("D", 0) or 0) + float(summary.get("O", 0) or 0))
    except Exception:
        return 0


def _mb_special_filters() -> dict:
    """Mail Ballot Center-only special filters.

    Do not use the Create Universe/global special filters here, because this
    workspace should be controlled by the Mail Ballot widgets shown on this page.
    """
    special = {}
    score = st.session_state.get(special_key("mb_score_center"), (0, 4))
    try:
        lo, hi = int(score[0]), int(score[1])
        if lo > 0 or hi < 4:
            special["MB_Prob_Score"] = {"min": lo, "max": hi}
    except Exception:
        pass
    return special


@st.cache_data(ttl=300, show_spinner=False)
def _mb_index_group_cached(active_json: str, special_json: str, field: str, limit: int = 12) -> pd.DataFrame:
    active = enforce_security_scope(json.loads(active_json or "{}"))
    special = json.loads(special_json or "{}")
    urls = index_urls_from_manifest()
    url_list = "[" + ",".join(sql_lit(u) for u in urls) + "]"
    where = index_where_sql(active or {}, special or {})
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try:
                con.execute("LOAD httpfs;")
            except Exception:
                pass
        q = f"""
            SELECT CAST({sql_ident(field)} AS VARCHAR) AS Category, COUNT(*) AS Voters
            FROM read_parquet({url_list}, union_by_name=true)
            {where}
            GROUP BY CAST({sql_ident(field)} AS VARCHAR)
            ORDER BY Voters DESC
            LIMIT {int(limit)}
        """
        return con.execute(q).df()
    finally:
        try:
            con.close()
        except Exception:
            pass


@st.cache_data(ttl=300, show_spinner=False)
def _mb_summary_cached(active_json: str, special_json: str) -> tuple[dict | None, str, str]:
    """Cached Mail Ballot summary. Returns plain strings only so Streamlit never prints debug objects."""
    active = enforce_security_scope(json.loads(active_json or "{}"))
    try:
        summary = duckdb_count_cube_summary(
            json.dumps(active or {}, sort_keys=True),
            special_json or "{}",
        )
        return summary, "mail-ballot-cube", ""
    except Exception as e:
        try:
            summary = duckdb_index_summary(
                json.dumps(active or {}, sort_keys=True),
                special_json or "{}",
            )
            return summary, "mail-ballot-index-fallback", ""
        except Exception:
            return None, "unavailable", str(e)


def _mb_summary(active: dict) -> tuple[dict | None, str, Exception | None]:
    active = enforce_security_scope(active or {})
    summary, mode, err_text = _mb_summary_cached(
        json.dumps(active or {}, sort_keys=True),
        json.dumps(_mb_special_filters(), sort_keys=True),
    )
    return summary, mode, Exception(err_text) if err_text else None


@st.cache_data(ttl=300, show_spinner=False)
def _mb_snapshot_bundle_cached(active_json: str, special_json: str) -> dict:
    """One cached Mail Ballot snapshot query instead of several separate count scans."""
    active = enforce_security_scope(json.loads(active_json or "{}"))
    special = json.loads(special_json or "{}")
    url = count_cube_url()
    where = count_cube_where_sql(active or {}, special or {})

    def norm_expr(field: str) -> str:
        return f"UPPER(TRIM(COALESCE(CAST({sql_ident(field)} AS VARCHAR), '')))"

    mb_app = norm_expr("MB_App")
    mb_app_status = norm_expr("MB_App_Status")
    mb_sent = norm_expr("MB_Sent")
    mb_status = norm_expr("MB_Status")

    q = f"""
        SELECT
            SUM(Voters) AS total,
            SUM(CASE WHEN {mb_app} IN ('Y','YES','TRUE','T','1','APPLIED')
                      OR {mb_app_status} IN ('APPLIED','APPROVED','PENDING')
                     THEN Voters ELSE 0 END) AS applied,
            SUM(CASE WHEN {mb_sent} IN ('Y','YES','TRUE','T','1','SENT')
                     THEN Voters ELSE 0 END) AS sent_count,
            SUM(CASE WHEN {mb_status} IN ('VOTED','RETURNED','BALLOT RETURNED')
                     THEN Voters ELSE 0 END) AS returned
        FROM read_parquet({sql_lit(url)})
        {where}
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try:
                con.execute("LOAD httpfs;")
            except Exception:
                pass
        row = con.execute(q).fetchone()
    finally:
        try:
            con.close()
        except Exception:
            pass

    total = int(row[0] or 0) if row else 0
    applied = int(row[1] or 0) if row else 0
    sent_count = int(row[2] or 0) if row else 0
    returned = int(row[3] or 0) if row else 0
    not_applied = max(0, total - applied)
    chase = max(0, sent_count - returned)
    return {
        "total": total,
        "applied": applied,
        "not_applied": not_applied,
        "sent_count": sent_count,
        "returned": returned,
        "chase": chase,
    }


def _mb_snapshot_counts(active: dict) -> dict:
    active = enforce_security_scope(active or {})
    try:
        return _mb_snapshot_bundle_cached(
            json.dumps(active or {}, sort_keys=True),
            json.dumps(_mb_special_filters(), sort_keys=True),
        )
    except Exception:
        summary, _mode, _err = _mb_summary(active)
        total = _mb_total_from_summary(summary)
        applied = _mb_count(active, {"MB_App": ["Yes", "Y", "Applied"]}) or _mb_count(active, {"MB_App_Status": ["Applied", "Approved", "Pending"]})
        sent_count = _mb_count(active, {"MB_Sent": ["Yes", "Y", "Sent"]})
        returned = _mb_count(active, {"MB_Status": ["Voted", "Returned", "Ballot Returned"]})
        return {
            "total": total,
            "applied": applied,
            "not_applied": max(0, total - applied),
            "sent_count": sent_count,
            "returned": returned,
            "chase": max(0, sent_count - returned),
        }


def _mb_count(active: dict, extra: dict | None = None) -> int:
    a = enforce_security_scope(active or {})
    for k, v in (extra or {}).items():
        a[k] = v if isinstance(v, list) else [v]
    try:
        summary, _mode, _err = _mb_summary(a)
        return _mb_total_from_summary(summary)
    except Exception:
        return 0


def _mb_group_df(active: dict, field: str, limit: int = 12) -> pd.DataFrame:
    active = enforce_security_scope(active or {})
    try:
        df = duckdb_count_cube_group_filtered(
            json.dumps(active or {}, sort_keys=True),
            json.dumps(_mb_special_filters(), sort_keys=True),
            field,
            limit,
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=["Category", "Voters", "%"])
        df = df.rename(columns={"label": "Category"}).copy()
        df["Category"] = df["Category"].astype(str).replace({"": "(blank)", "nan": "(blank)", "None": "(blank)"})
        df = _drop_unusable_rows(df)
        df["Voters"] = pd.to_numeric(df["Voters"], errors="coerce").fillna(0).astype(int)
        total = max(1, int(df["Voters"].sum()))
        df["%"] = (df["Voters"] / total * 100).map(lambda x: f"{x:.1f}%")
        return df[["Category", "Voters", "%"]]
    except Exception:
        # Fallback only if the cube is missing a field.
        try:
            df = _mb_index_group_cached(
                json.dumps(active or {}, sort_keys=True),
                json.dumps(_mb_special_filters(), sort_keys=True),
                field,
                limit,
            )
            if df is None or df.empty:
                return pd.DataFrame(columns=["Category", "Voters", "%"])
            df = df.copy()
            df["Category"] = df["Category"].astype(str).replace({"": "(blank)", "nan": "(blank)", "None": "(blank)"})
            df["Voters"] = pd.to_numeric(df["Voters"], errors="coerce").fillna(0).astype(int)
            total = max(1, int(df["Voters"].sum()))
            df["%"] = (df["Voters"] / total * 100).map(lambda x: f"{x:.1f}%")
            df = df[["Category", "Voters", "%"]]
            return _sort_category_df(df, field)
        except Exception:
            return pd.DataFrame(columns=["Category", "Voters", "%"])


def _drop_unusable_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with blank/unusable labels from app tables and charts."""
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    out = df.copy()
    label_candidates = [c for c in ["Category", "Area", "County", "Municipality", "Precinct", "School District", "School Region"] if c in out.columns]
    if label_candidates:
        c = label_candidates[0]
        out = out[~out[c].map(is_unusable_label)].copy()
    return out



def cc_iframe_html(html_doc: str, height: int = 360, scrolling: bool = False):
    """Render isolated sortable-table HTML without the deprecated components.html call.

    Streamlit is removing st.components.v1.html. New DEV builds expose st.iframe,
    but older local builds may not, so keep a guarded fallback that will not affect
    DEV when st.iframe is available.
    """
    if hasattr(st, "iframe"):
        try:
            return st.iframe(srcdoc=html_doc, height=height, scrolling=scrolling)
        except TypeError:
            try:
                return st.iframe(html_doc, height=height, scrolling=scrolling)
            except TypeError:
                pass
        except Exception:
            pass
    if components is not None:
        return components.html(html_doc, height=height, scrolling=scrolling)
    # Last-resort non-interactive fallback instead of crashing the app.
    try:
        return st.html(html_doc)
    except Exception:
        return st.markdown(html_doc, unsafe_allow_html=True)


def cc_table(df: pd.DataFrame, height: int | None = None, key: str | None = None, sticky_first_col: bool = False):
    """Sortable, zebra-striped, sticky-header table in an isolated component."""
    if df is None:
        df = pd.DataFrame()
    show = _drop_unusable_rows(df.copy())
    if show.empty:
        st.markdown('<div class="cc-empty-table">No rows to display.</div>', unsafe_allow_html=True)
        return None
    for col in show.columns:
        if col in {"Voters", "Total", "Count", "Rows", "Households", "Republican", "Democrat", "Other", "R", "D", "O", "Female", "Male", "Age65Plus", "StrongGeneral", "StrongAll", "MBProspects", "MBApplicants", "MBSent", "MBReturned"} or str(col).lower().endswith(" voters"):
            raw = show[col].astype(str).str.replace(",", "", regex=False).str.strip()
            nums = pd.to_numeric(raw, errors="coerce")
            show[col] = nums.map(lambda x: "" if pd.isna(x) else f"{int(x):,}")
    max_h = int(height or 360)
    if len(show) <= 12:
        max_h = min(max_h, max(120, 48 + 38 * (len(show) + 1)))
    table_html = show.to_html(index=False, escape=True, classes="cc-sort-table", border=0)
    sticky_cls = " sticky-first" if sticky_first_col else ""
    html_doc = f"""
<html><head><style>
html, body {{ margin:0; padding:0; background:#efe8d8; color:#071d3a; font-family:Arial, Helvetica, sans-serif; font-size:13px; }}
.table-shell {{ max-height:{max_h}px; overflow:auto; border:1px solid #9f151c; border-radius:10px; background:#ffffff; }}
table {{ border-collapse:separate; border-spacing:0; width:100%; table-layout:auto; }}
th, td {{ padding:9px 11px; border-right:1px solid #eadfce; border-bottom:1px solid #eadfce; text-align:center; color:#071d3a; white-space:nowrap; }}
th {{ position:sticky; top:0; z-index:3; background:#9f151c; color:#fff; font-weight:900; cursor:pointer; user-select:none; }}
th:hover {{ background:#7f1016; }}
tbody tr:nth-child(even) td {{ background:#f3eadc; }}
tbody tr:nth-child(odd) td {{ background:#ffffff; }}
.sticky-first th:first-child {{ left:0; z-index:5; min-width:max-content; }}
.sticky-first td:first-child {{ position:sticky; left:0; z-index:2; font-weight:800; text-align:left; min-width:max-content; }}
.sticky-first tbody tr:nth-child(even) td:first-child {{ background:#f3eadc; }}
.sticky-first tbody tr:nth-child(odd) td:first-child {{ background:#ffffff; }}
th.sort-asc::after {{ content:' ▲'; font-size:10px; }}
th.sort-desc::after {{ content:' ▼'; font-size:10px; }}
</style></head><body>
<div class="table-shell{sticky_cls}">{table_html}</div>
<script>
function cellVal(row, idx) {{ return row.children[idx].innerText.trim(); }}
function asNum(v) {{ var n = Number(v.replace(/[%,$,]/g,'')); return isNaN(n) ? null : n; }}
document.querySelectorAll('th').forEach(function(th, idx) {{
  th.addEventListener('click', function() {{
    var table = th.closest('table'); var tbody = table.querySelector('tbody'); var rows = Array.from(tbody.querySelectorAll('tr'));
    var desc = !th.classList.contains('sort-desc'); table.querySelectorAll('th').forEach(h => h.classList.remove('sort-asc','sort-desc')); th.classList.add(desc ? 'sort-desc' : 'sort-asc');
    rows.sort(function(a,b) {{ var av = cellVal(a, idx), bv = cellVal(b, idx); var an = asNum(av), bn = asNum(bv); var cmp = (an !== null && bn !== null) ? (an - bn) : av.localeCompare(bv, undefined, {{numeric:true, sensitivity:'base'}}); return desc ? -cmp : cmp; }});
    rows.forEach(r => tbody.appendChild(r));
  }});
}});
</script>
</body></html>"""
    cc_iframe_html(html_doc, height=max_h + 8, scrolling=False)
    return None

def _mb_render_metric(label: str, value: int, note: str = "", color_class: str = ""):
    st.markdown(
        f"""
        <div class="cc-icon-metric {color_class}">
          <div>
            <div class="cc-icon-label">{label}</div>
            <div class="cc-icon-value">{int(value or 0):,}</div>
            <div class="cc-icon-sub">{note}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _mb_prepare_download(active: dict, label: str, file_prefix: str, max_rows: int = 50000):
    active = enforce_security_scope(active or {})
    key = special_key("mb_export_" + re.sub(r"[^a-z0-9]+", "_", file_prefix.lower()))
    if st.button(f"Prepare {label}", key=key + "_btn", width="stretch"):
        with st.spinner(f"Preparing {label}..."):
            df = duckdb_detail_filtered_df(active, _mb_special_filters(), int(max_rows))
            keep = [c for c in [
                "voter_id", "FirstName", "MiddleName", "LastName", "NameSuffix", "FullName",
                "Party", "Gender", "Age", "Age_Range", "County", "Municipality", "Precinct",
                "House Number", "House Number Suffix", "Street Name", "Apartment Number", "Address Line 2", "City", "State", "Zip",
                "Email", "Mobile", "Landline", "Current_ApplicantPhone",
                "MB_App", "MB_App_Status", "MB_Sent", "MB_Status", "MB_PERM", "MB_Prob_Score",
                "Current_App_Return_Date", "Current_Ballot_Sent_Date", "Current_Ballot_Returned_Date", "Tags"
            ] if c in df.columns]
            if keep:
                df = df[keep].copy()
            st.session_state[key + "_csv"] = df.to_csv(index=False).encode()
            st.session_state[key + "_rows"] = len(df)
    if key + "_csv" in st.session_state:
        st.download_button(
            f"Download {label} ({st.session_state.get(key + '_rows', 0):,} rows)",
            st.session_state[key + "_csv"],
            f"{file_prefix}.csv",
            "text/csv",
            width="stretch",
        )


def render_mail_ballot_workspace():
    st.markdown("## Mail Ballot Center")
    st.caption("Strategic mail ballot operations: cultivate applications, message applicants, chase outstanding ballots, and build targeted files.")

    # Sidebar owns this checkbox. Mail Ballot Center reads the last applied Create Universe,
    # not whatever happens to be visible in the Create Universe widgets.
    start_from_current = bool(st.session_state.get(special_key("mb_start_current"), False))
    saved_universe = get_current_universe_filters()
    base = dict(saved_universe) if (start_from_current and saved_universe) else {}
    base = enforce_security_scope(base)
    if is_campaign_scoped():
        st.info(f"Campaign boundary enforced: {security_scope_label()}")
    if start_from_current and saved_universe:
        st.info(f"Starting from current universe: {st.session_state.get('current_universe_label', 'Selected universe')}")
    elif start_from_current and not saved_universe:
        st.warning("No current universe has been applied yet. Showing your assigned campaign scope.")

    preset = st.selectbox(
        "Mail ballot mission",
        [
            "Snapshot / Custom",
            "Cultivate new mail ballot applications",
            "Message ballot applicants",
            "Chase sent ballots not returned",
            "Cure / problem ballot follow-up",
            "Permanent mail ballot growth",
        ],
        key=special_key("mb_mission"),
        help="This changes only the Mail Ballot Center filters. It does not send you back to Create Universe.",
    )

    mission_defaults = {}
    if preset == "Cultivate new mail ballot applications":
        mission_defaults = {"MB_App": ["No", "N", "DNA", "Not Applied"]}
    elif preset == "Message ballot applicants":
        mission_defaults = {"MB_App": ["Yes", "Y", "Applied"], "MB_Sent": ["No", "N", "Not Sent"]}
    elif preset == "Chase sent ballots not returned":
        mission_defaults = {"MB_Sent": ["Yes", "Y", "Sent"], "MB_Status": ["Not Voted", "Not Returned", "No", "N"]}
    elif preset == "Cure / problem ballot follow-up":
        mission_defaults = {"MB_Status": ["Cancelled", "Pending", "Rejected", "Challenged", "Cure", "Problem"]}
    elif preset == "Permanent mail ballot growth":
        mission_defaults = {"MB_PERM": ["No", "N", "0", "False"]}

    c1, c2, c3, c4 = st.columns(4)
    party = c1.multiselect("Party", field_options(filter_options, "Party", base), default=base.get("Party", []), key=special_key("mb_party"))
    gender = c2.multiselect("Gender", field_options(filter_options, "Gender", base), default=base.get("Gender", []), key=special_key("mb_gender"))
    age = c3.multiselect("Age Range", field_options(filter_options, "Age_Range", base), default=base.get("Age_Range", []), key=special_key("mb_age"))
    score = c4.slider("MB Probability Score", 0, 4, st.session_state.get(special_key("mb_score_center"), (0, 4)), key=special_key("mb_score_center"))

    # Separate "did they apply?" from "what status is the application?"
    # This matters for cultivation work: users need to target DNA / Not Applied voters
    # without accidentally selecting Approved or Declined application statuses.
    def _default_mb_vals(field, candidates):
        valid = list(field_options(filter_options, field, base))
        return [v for v in (candidates or []) if v in valid]

    c5, c6, c7, c8, c9 = st.columns(5)
    app_filed = c5.multiselect(
        "Mail Ballot Application",
        field_options(filter_options, "MB_App", base),
        default=_default_mb_vals("MB_App", mission_defaults.get("MB_App", [])),
        key=special_key("mb_app_filed"),
        help="Use this for application cultivation. Choose DNA / No / Not Applied to exclude voters who already applied.",
    )
    app = c6.multiselect(
        "Application Status",
        field_options(filter_options, "MB_App_Status", base),
        default=_default_mb_vals("MB_App_Status", mission_defaults.get("MB_App_Status", [])),
        key=special_key("mb_app_status"),
        help="Use this after an application exists, for example Approved or Declined.",
    )
    sent = c7.multiselect("Ballot Sent", field_options(filter_options, "MB_Sent", base), key=special_key("mb_sent"))
    ret = c8.multiselect("Ballot Status", field_options(filter_options, "MB_Status", base), key=special_key("mb_status"))
    perm = c9.multiselect("Permanent MB", field_options(filter_options, "MB_PERM", base), key=special_key("mb_perm"))

    c10, _sp1, _sp2 = st.columns([1.2, 1, 1])
    v4a = c10.multiselect("Vote History", field_options(filter_options, "V4A", base), key=special_key("mb_v4a"))

    mb_active = dict(base)
    for fld, vals in {"Party": party, "Gender": gender, "Age_Range": age, "MB_App": app_filed, "MB_App_Status": app, "MB_Sent": sent, "MB_Status": ret, "MB_PERM": perm, "V4A": v4a}.items():
        if vals:
            mb_active[fld] = vals
    for fld, vals in mission_defaults.items():
        if fld not in mb_active or not mb_active.get(fld):
            valid = set(field_options(filter_options, fld, base))
            matched = [v for v in vals if v in valid]
            if matched:
                mb_active[fld] = matched

    mb_active = enforce_security_scope(mb_active)
    st.session_state[special_key("mb_prob_score_range")] = score
    if st.button("Apply Mail Ballot Center Filters", width="stretch", type="primary"):
        st.session_state[special_key("mb_last_active")] = mb_active
        st.success("Mail Ballot Center filters applied here. Create Universe filters were not changed.")

    # One cached snapshot query keeps Mail Ballot Center from running several full count scans.
    mb_snapshot = _mb_snapshot_counts(mb_active)
    total = int(mb_snapshot.get("total", 0) or 0)
    applied = int(mb_snapshot.get("applied", 0) or 0)
    not_applied = int(mb_snapshot.get("not_applied", 0) or 0)
    sent_count = int(mb_snapshot.get("sent_count", 0) or 0)
    returned = int(mb_snapshot.get("returned", 0) or 0)
    chase = int(mb_snapshot.get("chase", 0) or 0)

    st.markdown("### Mail Ballot Snapshot")
    m1, m2, m3, m4, m5 = st.columns(5)
    with m1: _mb_render_metric("Current Universe", total, "After MB Center filters", "")
    with m2: _mb_render_metric("Likely App Targets", not_applied, "No/DNA/not applied", "green")
    with m3: _mb_render_metric("Applicants", applied, "Applied/approved/pending", "blue")
    with m4: _mb_render_metric("Ballots Sent", sent_count, "Sent to voters", "gold")
    with m5: _mb_render_metric("Chase Universe", chase, "Sent minus returned", "")

    tabs = st.tabs(["Plan", "Analyze", "Build Files", "Notes"])

    with tabs[0]:
        st.markdown("### Recommended workflow")
        st.markdown("""
**1. Cultivate applications:** start with high MB probability voters who have not applied. Prioritize reliable general-election voters and voters with phones/email.  
**2. Message applicants:** voters who applied but have not yet been sent a ballot need status updates and reminders.  
**3. Chase ballots:** voters with ballots sent but not returned are the highest-priority follow-up universe.  
**4. Cure/problem follow-up:** isolate rejected, pending, challenged, or cure-status ballots and handle separately.  
**5. Permanent MB growth:** after the election cycle, identify strong MB users who are not permanent.
""")
        st.info("This section stays inside Mail Ballot Center. It does not overwrite the main Create Universe filters unless we intentionally add a Send to Universe button later.")

    with tabs[1]:
        left, right = st.columns(2)
        with left:
            st.markdown("#### Party")
            cc_table(_mb_group_df(mb_active, "Party", 10), height=220, key=special_key("mb_tbl_party"))
            st.markdown("#### Gender")
            cc_table(_mb_group_df(mb_active, "Gender", 10), height=220, key=special_key("mb_tbl_gender"))
            st.markdown("#### Application Status")
            cc_table(_mb_group_df(mb_active, "MB_App_Status", 12), height=240, key=special_key("mb_tbl_app_status"))
        with right:
            st.markdown("#### Age Range")
            cc_table(_mb_group_df(mb_active, "Age_Range", 12), height=240, key=special_key("mb_tbl_age"))
            st.markdown("#### Vote History")
            cc_table(_mb_group_df(mb_active, "V4A", 8), height=220, key=special_key("mb_tbl_v4a"))
            st.markdown("#### Ballot Status")
            cc_table(_mb_group_df(mb_active, "MB_Status", 12), height=240, key=special_key("mb_tbl_mb_status"))

    with tabs[2]:
        st.markdown("### Build Mail Ballot Files")
        st.caption("Files are prepared only when you click a button, so the page stays fast. Each file respects the Mail Ballot Center filters currently shown above.")
        st.info("For application cultivation, use the Mail Ballot Application filter above and choose DNA / No / Not Applied. Application Status is for voters who already have an application record, such as Approved or Declined.")
        f1, f2, f3 = st.columns(3)
        with f1:
            st.markdown("**Application Cultivation File**")
            st.caption("Voters who look like good mail-ballot prospects but have not applied. Use for application mail, calls, texts, or digital follow-up.")
            cultivate = dict(mb_active)
            if "MB_App" not in cultivate and "MB_App_Status" not in cultivate:
                cultivate["MB_App"] = [v for v in ["No", "N", "DNA", "Not Applied"] if v in field_options(filter_options, "MB_App", base)] or cultivate.get("MB_App", [])
            _mb_prepare_download(cultivate, "Application Cultivation File", "mail_ballot_cultivate_apps", 50000)
        with f2:
            st.markdown("**Applicant Messaging File**")
            st.caption("Voters with an application/applicant status. Use for education, reminders, and ballot-arrival messaging.")
            applicants = dict(mb_active)
            if "MB_App" not in applicants and "MB_App_Status" not in applicants:
                applicants["MB_App"] = [v for v in ["Yes", "Y", "Applied"] if v in field_options(filter_options, "MB_App", base)] or applicants.get("MB_App", [])
            _mb_prepare_download(applicants, "Applicant Messaging File", "mail_ballot_applicant_message", 50000)
        with f3:
            st.markdown("**Ballot Chase File**")
            st.caption("Voters with ballots sent but not yet marked returned/voted. Use for chase calls, texts, and door follow-up.")
            chase_active = dict(mb_active)
            if "MB_Sent" not in chase_active:
                chase_active["MB_Sent"] = [v for v in ["Yes", "Y", "Sent"] if v in field_options(filter_options, "MB_Sent", base)] or chase_active.get("MB_Sent", [])
            if "MB_Status" not in chase_active:
                chase_active["MB_Status"] = [v for v in ["Not Voted", "Not Returned", "No", "N"] if v in field_options(filter_options, "MB_Status", base)] or chase_active.get("MB_Status", [])
            _mb_prepare_download(chase_active, "Ballot Chase File", "mail_ballot_chase", 50000)
        st.divider()
        st.markdown("**Current Mail Ballot Center Universe**")
        st.caption("A general-purpose export of exactly the current Mail Ballot Center universe after your mission and quick filters.")
        _mb_prepare_download(mb_active, "Current Mail Ballot Center Universe", "mail_ballot_center_current_universe", 100000)

    with tabs[3]:
        st.text_area("Mail ballot notes / plan", key=special_key("mb_notes"), height=180)



def _area_clean_label(value) -> str:
    s = str(value or "").strip()
    if not s or s.lower() in {"nan", "none", "null", "(blank)", "blank"}:
        return "(Blank)"
    return s


def _area_pct(n, d) -> str:
    try:
        n = float(n or 0); d = float(d or 0)
        return "0.0%" if d <= 0 else f"{(n/d)*100:.1f}%"
    except Exception:
        return "0.0%"



def _category_sort_value(label) -> float:
    """Sort numeric/vote-history categories in human order, keeping 65+ after the regular ranges."""
    txt = str(label or "").strip()
    if not txt:
        return 999999.0
    m = re.match(r"^(\d+)(?:\s*-\s*\d+)?", txt)
    if m:
        return float(m.group(1))
    m = re.search(r"-?\d+(?:\.\d+)?", txt)
    if m:
        return float(m.group(0))
    return 999999.0


def _sort_category_df(df: pd.DataFrame, field: str = "") -> pd.DataFrame:
    """For Age Range and Vote History tables/charts, sort ascending by the numeric category, not by voter count."""
    if df is None or df.empty or "Category" not in df.columns:
        return df
    if str(field) in {"Age_Range", "V4A", "V4G", "V4P"}:
        out = df.copy()
        out["__sort"] = out["Category"].map(_category_sort_value)
        out = out.sort_values(["__sort", "Category"], kind="mergesort").drop(columns=["__sort"])
        return out
    return df

def _area_group_df(active: dict, field: str, limit: int = 20) -> pd.DataFrame:
    """Fast cube-backed group table for Area Intelligence."""
    active = enforce_security_scope(active or {})
    try:
        special = {k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}
        df = duckdb_count_cube_group_filtered(
            json.dumps(count_safe_filters(with_campaign_boundary(active or {})), sort_keys=True),
            json.dumps(special or {}, sort_keys=True),
            field,
            int(limit),
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=["Category", "Voters", "%"])
        df = df.rename(columns={"label": "Category"}).copy()
        df["Category"] = df["Category"].map(_area_clean_label)
        df = _drop_unusable_rows(df)
        df["Voters"] = pd.to_numeric(df["Voters"], errors="coerce").fillna(0).astype(int)
        total = max(1, int(df["Voters"].sum()))
        df["%"] = df["Voters"].map(lambda x: _area_pct(x, total))
        df = df[["Category", "Voters", "%"]]
        return _sort_category_df(df, field)
    except Exception:
        return pd.DataFrame(columns=["Category", "Voters", "%"])


@st.cache_data(ttl=300, show_spinner=False)
def _count_cube_columns_for_base(base_url: str) -> list[str]:
    try:
        manifest = _load_manifest_from_base(base_url)
        key = ((manifest.get("speed", {}) or {}).get("tables", {}) or {}).get("count_cube", "speed/count_cube.parquet")
        url = f"{base_url.rstrip('/')}/{str(key).lstrip('/')}"
        con = duckdb.connect(database=":memory:")
        try:
            try:
                con.execute("INSTALL httpfs; LOAD httpfs;")
            except Exception:
                try:
                    con.execute("LOAD httpfs;")
                except Exception:
                    pass
            return [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_lit(url)})").fetchall()]
        finally:
            con.close()
    except Exception:
        return []


def _cube_col_name(requested: str, available_cols: list[str]) -> str:
    aliases = {
        "County": ["County", "county"],
        "Municipality": ["Municipality", "municipality", "Municipality_1"],
        "Precinct": ["Precinct", "precinct", "Precinct_1", "precinct_code"],
        "USC": ["USC", "congressional_num", "congressional_name"],
        "STS": ["STS", "state_senate_num", "state_senate_name"],
        "STH": ["STH", "state_house_num", "state_house_name"],
        "School District": ["School District", "school_district"],
        "School Region": ["School Region", "school_region"],
        "Party": ["Party", "Party_1", "party", "party_raw"],
        "Gender": ["Gender", "Gender_1", "gender"],
        "Age_Range": ["Age_Range", "age_group"],
        "V4A": ["V4A", "VoteHistory_Last4_All"],
        "V4G": ["V4G", "VoteHistory_Last4_General"],
        "V4P": ["V4P", "VoteHistory_Last4_Primary"],
        "MB_Prob_Score": ["MB_Prob_Score"],
        "MB_App": ["MB_App", "MIB_Applied"],
        "MB_App_Status": ["MB_App_Status"],
        "MB_Sent": ["MB_Sent", "BallotSentStatus", "MIB_BALLOT"],
        "MB_Status": ["MB_Status", "BallotReturnedStatus"],
    }
    lookup = {str(c).lower(): str(c) for c in available_cols or []}
    for cand in aliases.get(requested, [requested]):
        hit = lookup.get(str(cand).lower())
        if hit:
            return hit
    return ""


def _cube_expr(requested: str, available_cols: list[str], default: str = "NULL") -> str:
    col = _cube_col_name(requested, available_cols)
    return f"CAST({sql_ident(col)} AS VARCHAR)" if col else f"CAST({default} AS VARCHAR)"


@st.cache_data(ttl=300, show_spinner=False)
def _area_breakdown_cube(active_json: str, special_json: str, breakdown: str, limit: int = 250) -> pd.DataFrame:
    active = enforce_security_scope(json.loads(active_json or "{}"))
    special = json.loads(special_json or "{}")
    if not re.fullmatch(r"[A-Za-z0-9_ /-]+", str(breakdown)):
        return pd.DataFrame()

    url = count_cube_url()
    available = _count_cube_columns_for_base(current_data_base_url())
    actual_breakdown = _cube_col_name(breakdown, available)
    if not actual_breakdown:
        return pd.DataFrame()

    where = count_cube_where_sql(active, special)
    b = sql_ident(actual_breakdown)

    party_expr = _cube_expr("Party", available, "''")
    gender_expr = _cube_expr("Gender", available, "''")
    age_expr = _cube_expr("Age_Range", available, "''")
    v4g_expr = _cube_expr("V4G", available, "0")
    v4a_expr = _cube_expr("V4A", available, "0")
    mb_prob_expr = _cube_expr("MB_Prob_Score", available, "0")
    mb_app_expr = _cube_expr("MB_App", available, "''")
    mb_app_status_expr = _cube_expr("MB_App_Status", available, "''")
    mb_sent_expr = _cube_expr("MB_Sent", available, "''")
    mb_status_expr = _cube_expr("MB_Status", available, "''")

    q = f"""
        SELECT
            CAST({b} AS VARCHAR) AS Area,
            SUM(Voters) AS Total,
            SUM(CASE WHEN {party_expr} = 'R' THEN Voters ELSE 0 END) AS R,
            SUM(CASE WHEN {party_expr} = 'D' THEN Voters ELSE 0 END) AS D,
            SUM(CASE WHEN {party_expr} NOT IN ('R','D') THEN Voters ELSE 0 END) AS O,
            SUM(CASE WHEN {gender_expr} = 'F' THEN Voters ELSE 0 END) AS Female,
            SUM(CASE WHEN {gender_expr} = 'M' THEN Voters ELSE 0 END) AS Male,
            SUM(CASE WHEN {age_expr} IN ('65+', '65 Plus', '65 and over') THEN Voters ELSE 0 END) AS Age65Plus,
            SUM(CASE WHEN TRY_CAST({v4g_expr} AS DOUBLE) >= 3 THEN Voters ELSE 0 END) AS StrongGeneral,
            SUM(CASE WHEN TRY_CAST({v4a_expr} AS DOUBLE) >= 3 THEN Voters ELSE 0 END) AS StrongAll,
            SUM(CASE WHEN TRY_CAST({mb_prob_expr} AS DOUBLE) >= 3 THEN Voters ELSE 0 END) AS MBProspects,
            SUM(CASE WHEN UPPER({mb_app_expr}) IN ('Y','YES','APPLIED','TRUE','1') OR UPPER({mb_app_status_expr}) IN ('APPROVED','PENDING') THEN Voters ELSE 0 END) AS MBApplicants,
            SUM(CASE WHEN UPPER({mb_sent_expr}) IN ('Y','YES','SENT','TRUE','1') THEN Voters ELSE 0 END) AS MBSent,
            SUM(CASE WHEN UPPER({mb_status_expr}) IN ('VOTED','RETURNED','BALLOT RETURNED') THEN Voters ELSE 0 END) AS MBReturned
        FROM read_parquet({sql_lit(url)})
        {where}
        GROUP BY CAST({b} AS VARCHAR)
        HAVING SUM(Voters) > 0
        ORDER BY Total DESC
        LIMIT {int(limit)}
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try:
                con.execute("LOAD httpfs;")
            except Exception:
                pass
        df = con.execute(q).df()
        if df is None or df.empty:
            return pd.DataFrame()
        df["Area"] = df["Area"].map(_area_clean_label)
        df = df[df["Area"].astype(str).str.strip().ne("(Blank)")].copy()
        if df.empty:
            return pd.DataFrame()
        for c in [x for x in df.columns if x != "Area"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        df["R %"] = df.apply(lambda r: _area_pct(r["R"], r["Total"]), axis=1)
        df["D %"] = df.apply(lambda r: _area_pct(r["D"], r["Total"]), axis=1)
        df["O %"] = df.apply(lambda r: _area_pct(r["O"], r["Total"]), axis=1)
        df["65+ %"] = df.apply(lambda r: _area_pct(r["Age65Plus"], r["Total"]), axis=1)
        df["Strong Gen %"] = df.apply(lambda r: _area_pct(r["StrongGeneral"], r["Total"]), axis=1)
        df["MB Prospect %"] = df.apply(lambda r: _area_pct(r["MBProspects"], r["Total"]), axis=1)
        df["MB Return %"] = df.apply(lambda r: _area_pct(r["MBReturned"], r["MBSent"]), axis=1)
        return df[["Area", "Total", "R", "D", "O", "R %", "D %", "O %", "Female", "Male", "Age65Plus", "65+ %", "StrongGeneral", "Strong Gen %", "MBProspects", "MB Prospect %", "MBApplicants", "MBSent", "MBReturned", "MB Return %"]]
    except Exception:
        return pd.DataFrame()
    finally:
        try:
            con.close()
        except Exception:
            pass


def _area_default_breakdown(active: dict) -> str:
    active = active or {}
    # Exact user rule first.
    if len(active.get("Municipality") or []) == 1:
        return "Precinct"
    if len(active.get("County") or []) == 1:
        return "Municipality"
    # If district filter likely collapses to one county, try to detect it quickly.
    try:
        county_df = _area_group_df(active, "County", 5)
        nonblank = county_df[county_df["Category"].astype(str).str.strip().ne("(Blank)")]
        if len(nonblank) == 1:
            return "Municipality"
    except Exception:
        pass
    return "County"


def _area_universe_label(active: dict) -> str:
    if not active:
        return "Pennsylvania Statewide"
    try:
        return universe_label_from_filters(active)
    except Exception:
        parts = []
        for k, vals in active.items():
            if vals:
                v = vals[0] if len(vals) == 1 else f"{len(vals)} selected"
                parts.append(f"{DISPLAY_LABELS.get(k,k)}: {v}")
        return " · ".join(parts) if parts else "Selected Universe"


def _area_insights(summary: dict, party_df: pd.DataFrame, age_df: pd.DataFrame, mb_df: pd.DataFrame) -> list[str]:
    total = int(summary.get("total", 0) or 0)
    r = int(summary.get("r", 0) or 0); d = int(summary.get("d", 0) or 0); o = int(summary.get("o", 0) or 0)
    insights = []
    if total:
        if abs(r-d) / total >= 0.05:
            leader = "Republican" if r > d else "Democratic"
            margin = abs(r-d)
            insights.append(f"The universe has a {leader} registration advantage of {margin:,} voters ({_area_pct(margin, total)} of the universe).")
        else:
            insights.append("The partisan registration balance is relatively close, so turnout quality and voter-contact targeting may matter more than raw party advantage.")
        if o / total >= 0.15:
            insights.append(f"Other/unaffiliated voters are a meaningful bloc at {_area_pct(o, total)}, making persuasion and issue-based outreach important.")
    try:
        age65 = age_df[age_df["Category"].astype(str).str.contains("65", regex=False)]["Voters"].sum()
        if total and age65 / total >= 0.20:
            insights.append(f"Older voters are a major part of the universe ({_area_pct(age65, total)} age 65+), supporting mail, phone, and repeated direct-contact programs.")
    except Exception:
        pass
    try:
        applied = int(mb_df[mb_df["Category"].astype(str).str.upper().isin(["Y","YES","APPLIED"])] ["Voters"].sum())
        if total and applied / total < 0.25:
            insights.append("Mail-ballot application usage appears limited enough that cultivation can still grow the reachable vote universe.")
    except Exception:
        pass
    if not insights:
        insights.append("This universe is ready for a basic field strategy review using party, age, vote-history, and mail-ballot behavior below.")
    return insights[:5]


def _area_bar_html(df: pd.DataFrame, title: str, max_rows: int = 8) -> str:
    if df is None or df.empty:
        return f'<div class="cc-home-card"><h3>{title}</h3><div class="cc-sub">No data available.</div></div>'
    sort_field = "Age_Range" if "Age" in str(title) else ("V4A" if "Vote History" in str(title) else "")
    show = _sort_category_df(df.copy(), sort_field).head(max_rows).copy()
    maxv = max(1, int(pd.to_numeric(show["Voters"], errors="coerce").fillna(0).max()))
    rows = []
    for _, r in show.iterrows():
        lab = str(r["Category"])
        val = int(r["Voters"] or 0)
        w = max(2, val / maxv * 100)
        pct_s = str(r.get("%", ""))
        rows.append(f'<div class="cc-age-row"><b>{lab}</b><div class="cc-age-bar-bg"><div class="cc-age-bar" style="width:{w:.1f}%"></div></div><span>{val:,} ({pct_s})</span></div>')
    return f'<div class="cc-home-card"><h3>{title}</h3>' + ''.join(rows) + '</div>'



def _area_election_code(col: str) -> str:
    return str(col).split("_")[0].upper()


def _area_election_label_from_code(code: str) -> str:
    code = str(code or "").upper()
    m = re.match(r"^([GP])(\d{2})", code)
    if not m:
        return code
    year = 2000 + int(m.group(2))
    typ = "General" if m.group(1) == "G" else "Primary"
    return f"{year} {typ}"


def _area_election_sort_key(code: str):
    code = str(code or "").upper()
    m = re.match(r"^([GP])(\d{2})", code)
    if not m:
        return (0, 0)
    year = 2000 + int(m.group(2))
    # General before Primary within same year for readability.
    typ_order = 2 if m.group(1) == "G" else 1
    return (year, typ_order)


def _area_voted_sql_for_cols(cols: list[str]) -> str:
    """True only when a voter has an actual vote method for the election.

    The *_party columns in the source often contain O for no-vote rows, so they
    must not be used to decide turnout. Turnout is based on *_method only.
    """
    checks = []
    for c in cols:
        if not str(c).lower().endswith("_method"):
            continue
        expr = f"UPPER(TRIM(CAST({sql_ident(c)} AS VARCHAR)))"
        checks.append(f"({expr} NOT IN ('', 'NAN', 'NONE', 'NULL', '0', 'N', 'NO', 'FALSE', 'DID NOT VOTE', 'DNV'))")
    return "(" + " OR ".join(checks) + ")" if checks else "FALSE"


@st.cache_data(ttl=300, show_spinner=False)
def _turnout_cube_columns_for_base(base_url: str) -> list[str]:
    try:
        manifest = _load_manifest_from_base(base_url)
        key = ((manifest.get("speed", {}) or {}).get("tables", {}) or {}).get("turnout_cube", "speed/turnout_cube.parquet")
        url = f"{base_url.rstrip('/')}/{str(key).lstrip('/')}"
        con = duckdb.connect(database=":memory:")
        try:
            try:
                con.execute("INSTALL httpfs; LOAD httpfs;")
            except Exception:
                try: con.execute("LOAD httpfs;")
                except Exception: pass
            return [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_lit(url)})").fetchall()]
        finally:
            con.close()
    except Exception:
        return []


def turnout_cube_url() -> str:
    manifest = load_manifest()
    key = ((manifest.get("speed", {}) or {}).get("tables", {}) or {}).get("turnout_cube", "speed/turnout_cube.parquet")
    return r2_url(key)


def _turnout_where_sql(active: dict, special: dict | None = None, available_cols: list[str] | None = None) -> str:
    available = set(available_cols or [])
    clauses = []
    merged = {}
    merged.update(active or {})
    merged.update(special or {})
    for field, vals in merged.items():
        if str(field).startswith("__"):
            continue
        if field not in available:
            continue
        vals = [str(v).strip() for v in (vals if isinstance(vals, list) else [vals]) if str(v).strip()]
        if not vals:
            continue
        clauses.append(f"UPPER(TRIM(CAST({sql_ident(field)} AS VARCHAR))) IN (" + ",".join(sql_lit(v.upper()) for v in vals) + ")")
    return "WHERE " + " AND ".join(clauses) if clauses else ""


@st.cache_data(ttl=300, show_spinner=False)
def _area_turnout_by_party(active_json: str, special_json: str, limit: int = 12) -> pd.DataFrame:
    active = enforce_security_scope(json.loads(active_json or "{}"))
    special = json.loads(special_json or "{}")
    url = turnout_cube_url()
    cols = _turnout_cube_columns_for_base(current_data_base_url())
    if not cols:
        return pd.DataFrame(columns=["Election", "R", "D", "O", "Total"])
    where = _turnout_where_sql(active, special, cols)
    q = f"""
        SELECT Election, SUM(R) AS R, SUM(D) AS D, SUM(O) AS O, SUM(Total) AS Total
        FROM read_parquet({sql_lit(url)})
        {where}
        GROUP BY Election, SortOrder
        HAVING SUM(Total) > 0
        ORDER BY SortOrder DESC
        LIMIT {int(limit)}
    """
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try: con.execute("LOAD httpfs;")
            except Exception: pass
        df = con.execute(q).df()
        if df is None or df.empty:
            return pd.DataFrame(columns=["Election", "R", "D", "O", "Total"])
        for c in ["R","D","O","Total"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        return df[["Election", "R", "D", "O", "Total"]].reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=["Election", "R", "D", "O", "Total"])
    finally:
        try: con.close()
        except Exception: pass


@st.cache_data(ttl=300, show_spinner=False)
def _area_mail_ballot_turnout_by_party(active_json: str, special_json: str, limit: int = 12) -> pd.DataFrame:
    """Mail/absentee turnout by current party for Primary + General elections since 2020.

    This is used for the Area Intelligence report chart. It only counts rows where
    the election-history field itself indicates mail/absentee voting (M/MB/MAIL/A/AB/ABS).
    """
    active = enforce_security_scope(json.loads(active_json or "{}"))
    special = json.loads(special_json or "{}")
    cols = selected_election_columns(types=["General", "Primary"])
    groups = {}
    for c in cols:
        if not str(c).lower().endswith("_method"):
            continue
        code = _area_election_code(c)
        if not re.match(r"^[GP]\d{2}$", code):
            continue
        try:
            if 2000 + int(code[1:]) < 2020:
                continue
        except Exception:
            continue
        groups.setdefault(code, []).append(c)
    codes = sorted(groups.keys(), key=_area_election_sort_key, reverse=True)[:int(limit)]
    if not codes:
        return pd.DataFrame(columns=["Election", "R", "D", "O", "Total"])

    def mail_sql(cols_for_code):
        checks = []
        for c in cols_for_code:
            expr = f"UPPER(TRIM(CAST({sql_ident(c)} AS VARCHAR)))"
            checks.append(f"({expr} IN ('M','MB','MAIL','MAIL BALLOT','MAIL-IN','MAIL IN','MAILIN','A','AB','ABS','ABSENTEE') OR {expr} LIKE '%MAIL%' OR {expr} LIKE '%ABS%')")
        return "(" + " OR ".join(checks) + ")" if checks else "FALSE"

    urls = index_urls_from_manifest()
    url_list = "[" + ",".join(sql_lit(u) for u in urls) + "]"
    where = index_where_sql(active, special)
    selects = []
    for code in codes:
        voted_mail = mail_sql(groups[code])
        selects.append(f"""
        SELECT {sql_lit(_area_election_label_from_code(code))} AS Election,
               SUM(CASE WHEN {voted_mail} AND CAST(Party AS VARCHAR) = 'R' THEN 1 ELSE 0 END) AS R,
               SUM(CASE WHEN {voted_mail} AND CAST(Party AS VARCHAR) = 'D' THEN 1 ELSE 0 END) AS D,
               SUM(CASE WHEN {voted_mail} AND CAST(Party AS VARCHAR) NOT IN ('R','D') THEN 1 ELSE 0 END) AS O,
               SUM(CASE WHEN {voted_mail} THEN 1 ELSE 0 END) AS Total
        FROM read_parquet({url_list}, union_by_name=true)
        {where}
        """)
    q = " UNION ALL ".join(selects)
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try: con.execute("LOAD httpfs;")
            except Exception: pass
        df = con.execute(q).df()
        if df is None or df.empty:
            return pd.DataFrame(columns=["Election", "R", "D", "O", "Total"])
        for c in ["R","D","O","Total"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        return df[df["Total"] > 0].reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=["Election", "R", "D", "O", "Total"])
    finally:
        try: con.close()
        except Exception: pass

def _area_pdf_bytes(title: str, active: dict, summary: dict, insights: list[str], tables: dict[str, pd.DataFrame], breakdown_field: str) -> bytes:
    """Client-ready Area Intelligence PDF with branding, charts, turnout, and strategy pages."""
    bio = io.BytesIO()
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter, landscape
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
        from reportlab.lib.utils import ImageReader
        from reportlab.graphics.shapes import Drawing, String, Rect, Line
        from reportlab.graphics.charts.piecharts import Pie
        from reportlab.graphics.charts.barcharts import VerticalBarChart
    except Exception:
        return b""

    PAGE = landscape(letter)
    doc = SimpleDocTemplate(
        bio, pagesize=PAGE,
        rightMargin=0.42*inch, leftMargin=0.42*inch,
        topMargin=0.38*inch, bottomMargin=0.34*inch,
    )
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="CC_Title", fontName="Helvetica-Bold", fontSize=28, leading=34, textColor=colors.HexColor("#111827"), alignment=1))
    styles.add(ParagraphStyle(name="CC_Subtitle", fontName="Helvetica", fontSize=13, leading=18, textColor=colors.HexColor("#374151"), alignment=1))
    styles.add(ParagraphStyle(name="CC_H", fontName="Helvetica-Bold", fontSize=17, leading=22, textColor=colors.HexColor("#111827"), spaceBefore=8, spaceAfter=6))
    styles.add(ParagraphStyle(name="CC_H2", fontName="Helvetica-Bold", fontSize=12, leading=15, textColor=colors.HexColor("#111827"), spaceBefore=5, spaceAfter=4))
    styles.add(ParagraphStyle(name="CC_Body", fontName="Helvetica", fontSize=9.5, leading=13, textColor=colors.HexColor("#374151")))
    styles.add(ParagraphStyle(name="CC_Small", fontName="Helvetica", fontSize=7.8, leading=10, textColor=colors.HexColor("#4b5563")))
    styles.add(ParagraphStyle(name="CC_Cell", parent=styles["CC_Small"], alignment=1))
    styles.add(ParagraphStyle(name="CC_CellHead", parent=styles["CC_Small"], alignment=1, fontName="Helvetica-Bold", textColor=colors.white))

    red = colors.HexColor("#9f151c")
    blue = colors.HexColor("#1d4ed8")
    green = colors.HexColor("#4c9a2a")
    gold = colors.HexColor("#f2b84b")
    dark = colors.HexColor("#111827")
    light = colors.HexColor("#f3f4f6")

    story = []

    def safe_img(path, width=None, height=None, max_height=None):
        try:
            if path and file_exists(path):
                img = Image(path)
                iw = float(img.imageWidth or 1)
                ih = float(img.imageHeight or 1)
                if width and height:
                    img.drawWidth = width
                    img.drawHeight = height
                elif width:
                    img.drawWidth = width
                    img.drawHeight = width * ih / iw
                elif height:
                    img.drawHeight = height
                    img.drawWidth = height * iw / ih
                if max_height and img.drawHeight > max_height:
                    scale = max_height / float(img.drawHeight or 1)
                    img.drawHeight *= scale
                    img.drawWidth *= scale
                return img
        except Exception:
            return Paragraph("", styles["CC_Body"])
        return Paragraph("", styles["CC_Body"])

    # ---------------- Cover page ----------------
    logo_left = safe_img(LOGO_CANDIDATE_CONNECT, width=2.05*inch, max_height=0.75*inch)
    logo_right = safe_img(LOGO_TPTC, width=1.75*inch, max_height=0.75*inch)
    hdr = Table([[logo_left, "", logo_right]], colWidths=[2.4*inch, 5.4*inch, 2.2*inch])
    hdr.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'MIDDLE'), ('ALIGN',(0,0),(0,0),'LEFT'), ('ALIGN',(2,0),(2,0),'RIGHT')]))
    story.append(hdr)
    story.append(Spacer(1, 0.92*inch))
    story.append(Paragraph("Area Intelligence Report", styles["CC_Title"]))
    story.append(Spacer(1, 0.18*inch))
    story.append(Paragraph(cc_text(title), styles["CC_Subtitle"]))
    story.append(Spacer(1, 0.12*inch))
    story.append(Paragraph("Candidate Connect Voter Data & Engagement Platform", styles["CC_Subtitle"]))
    story.append(Spacer(1, 0.24*inch))
    date_s = datetime.now().strftime('%B %d, %Y')
    story.append(Paragraph(f"Prepared {date_s}", styles["CC_Subtitle"]))
    story.append(Spacer(1, 0.70*inch))
    story.append(Paragraph("Prepared for strategic planning, field targeting, direct voter contact, mail-ballot operations, and campaign resource allocation.", styles["CC_Subtitle"]))
    story.append(PageBreak())

    # Helpers
    def as_table_df(df, max_rows=20, max_cols=10, first_col_w=None, font_size=7.0, total_w=10.0*inch):
        if df is None or df.empty:
            return None
        show = df.head(max_rows).copy()
        if len(show.columns) > max_cols:
            show = show[list(show.columns[:max_cols])]
        cols = list(show.columns)
        data = [[Paragraph(str(c), styles["CC_CellHead"]) for c in cols]]
        for _, row in show.iterrows():
            vals = []
            for c in cols:
                v = row[c]
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    txt = f"{v:,.0f}"
                else:
                    txt = str(v)
                vals.append(Paragraph(txt, styles["CC_Cell"]))
            data.append(vals)
        n = max(1, len(cols))
        if first_col_w is None:
            first_col_w = 1.55*inch if n >= 6 else 2.0*inch
        if n == 1:
            colw = [total_w]
        else:
            rest = max(0.42*inch, (total_w - first_col_w) / (n-1))
            colw = [first_col_w] + [rest]*(n-1)
        tbl = Table(data, repeatRows=1, colWidths=colw)
        tbl.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),dark),('TEXTCOLOR',(0,0),(-1,0),colors.white),
            ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('ALIGN',(0,0),(-1,-1),'CENTER'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('GRID',(0,0),(-1,-1),0.25,colors.HexColor('#cbd5e1')),('FONTSIZE',(0,0),(-1,-1),font_size),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white, light]),('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),
            ('LEFTPADDING',(0,0),(-1,-1),2),('RIGHTPADDING',(0,0),(-1,-1),2),
        ]))
        return tbl

    def section_header(txt):
        return Table([[txt]], colWidths=[10.0*inch], style=TableStyle([
            ('BACKGROUND',(0,0),(-1,-1),red),('TEXTCOLOR',(0,0),(-1,-1),colors.white),
            ('FONTNAME',(0,0),(-1,-1),'Helvetica-Bold'),('FONTSIZE',(0,0),(-1,-1),13),
            ('ALIGN',(0,0),(-1,-1),'LEFT'),('LEFTPADDING',(0,0),(-1,-1),8),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),
        ]))

    def _clean_chart_df(df, max_rows=7):
        if df is None or df.empty:
            return pd.DataFrame(columns=["Category", "Voters", "%"])
        show = df.copy()
        if "Category" in show.columns:
            show["Category"] = show["Category"].astype(str)
            show = show[~show["Category"].str.strip().str.lower().isin(["", "(blank)", "blank", "nan", "none", "null"])]
        if "Voters" in show.columns:
            show["Voters"] = pd.to_numeric(show["Voters"], errors="coerce").fillna(0).astype(int)
            show = show[show["Voters"] > 0]
        return show.head(max_rows)

    def pie_chart(df, title_s, w=245, h=165):
        d = Drawing(w, h)
        d.add(String(8, h-14, title_s, fontName='Helvetica-Bold', fontSize=10, fillColor=dark))
        show = _clean_chart_df(df, 5)
        if show.empty:
            d.add(String(12, h/2, "No data", fontName='Helvetica', fontSize=8, fillColor=colors.HexColor('#6b7280')))
            return d
        vals = [int(x) for x in show["Voters"].tolist()]
        labs = [str(x) for x in show["Category"].tolist()]
        p = Pie(); p.x=18; p.y=54; p.width=88; p.height=88; p.data=vals; p.labels=[""]*len(vals)
        palette = [red, blue, green, gold, colors.HexColor('#64748b')]
        for i in range(len(vals)):
            p.slices[i].fillColor = palette[i % len(palette)]
        d.add(p)
        total=sum(vals) or 1
        y=h-38
        for i,(lab,val) in enumerate(zip(labs,vals)):
            d.add(Rect(118, y-4, 6, 6, fillColor=palette[i % len(palette)], strokeColor=None))
            d.add(String(128, y-3, f"{lab}: {val:,} ({val/total*100:.1f}%)", fontName='Helvetica', fontSize=7.2, fillColor=dark))
            y -= 14
        return d

    def bar_chart(df, title_s, w=245, h=165):
        d = Drawing(w, h)
        d.add(String(8, h-14, title_s, fontName='Helvetica-Bold', fontSize=10, fillColor=dark))
        show = _clean_chart_df(df, 6)
        if show.empty:
            d.add(String(12, h/2, "No data", fontName='Helvetica', fontSize=8, fillColor=colors.HexColor('#6b7280')))
            return d
        vals = [int(x) for x in show["Voters"].tolist()]
        labs = [str(x)[:11] for x in show["Category"].tolist()]
        maxv=max(vals) or 1
        left=58; top=h-34; bar_h=12; gap=8; bar_w=w-left-35
        for i,(lab,val) in enumerate(zip(labs, vals)):
            y=top-i*(bar_h+gap)
            d.add(String(8, y+2, lab, fontName='Helvetica', fontSize=6.8, fillColor=dark))
            d.add(Rect(left, y, bar_w, bar_h, fillColor=colors.HexColor('#f3f4f6'), strokeColor=colors.HexColor('#cbd5e1'), strokeWidth=.3))
            d.add(Rect(left, y, max(1, bar_w*val/maxv), bar_h, fillColor=red, strokeColor=None))
            d.add(String(left+bar_w+4, y+2, f"{val:,}", fontName='Helvetica', fontSize=6.5, fillColor=dark))
        return d

    def stacked_mail_chart(df, title_s="Mail Ballot Turnout by Party Since 2020", w=700, h=215):
        d = Drawing(w, h)
        d.add(String(8, h-14, title_s, fontName='Helvetica-Bold', fontSize=10, fillColor=dark))
        if df is None or df.empty:
            d.add(String(12, h/2, "No mail-ballot history data available in the current speed/index layer.", fontName='Helvetica', fontSize=8, fillColor=colors.HexColor('#6b7280')))
            return d
        show = df.head(12).copy()
        for c in ["R", "D", "O", "Total"]:
            show[c] = pd.to_numeric(show[c], errors="coerce").fillna(0).astype(int)
        maxv = int(show["Total"].max() or 1)
        left=94; right=46; top=h-36; bar_h=8; gap=5; bar_w=w-left-right
        colors_party = {"R": red, "D": blue, "O": green}
        for i, row in show.iterrows():
            y=top-i*(bar_h+gap)
            d.add(String(8, y+1, str(row.get("Election", ""))[:16], fontName='Helvetica', fontSize=6.7, fillColor=dark))
            x=left
            total=int(row["Total"] or 0)
            if total <= 0:
                d.add(Rect(left, y, bar_w, bar_h, fillColor=colors.HexColor('#f3f4f6'), strokeColor=colors.HexColor('#cbd5e1'), strokeWidth=.25))
            else:
                for party in ["R","D","O"]:
                    val=int(row[party] or 0)
                    seg = bar_w * val / maxv
                    if seg > 0:
                        d.add(Rect(x, y, seg, bar_h, fillColor=colors_party[party], strokeColor=None))
                        x += seg
            d.add(String(left+bar_w+4, y+1, f"{total:,}", fontName='Helvetica', fontSize=6.5, fillColor=dark))
        lx=left; ly=8
        for party in ["R","D","O"]:
            d.add(Rect(lx, ly, 7, 7, fillColor=colors_party[party], strokeColor=None))
            d.add(String(lx+10, ly, party, fontName='Helvetica', fontSize=7, fillColor=dark)); lx += 34
        return d

    # ---------------- Executive summary ----------------
    story.append(section_header("Executive Strategy Summary"))
    story.append(Spacer(1, 0.12*inch))
    for ins in insights:
        story.append(Paragraph(f"• {ins}", styles["CC_Body"]))
    story.append(Spacer(1, 0.12*inch))
    story.append(Paragraph("Recommended Strategic Posture", styles["CC_H2"]))
    story.append(Paragraph("Use this profile to decide whether the campaign should prioritize persuasion, turnout, mail-ballot growth, or direct-contact density. For most local campaigns, the immediate value is identifying where reliable voters live, where reachable voters concentrate, and where the campaign can win through door-to-door and phone contact.", styles["CC_Body"]))
    story.append(Spacer(1, 0.16*inch))

    # Keep page-2 visuals roomy: top row only, then continue charts on the next page.
    charts_top = Table(
        [[pie_chart(tables.get("Party"), "Party Composition", w=315, h=190), bar_chart(tables.get("Age"), "Age Distribution", w=315, h=190)]],
        colWidths=[5.0*inch, 5.0*inch], rowHeights=[2.25*inch]
    )
    charts_top.setStyle(TableStyle([
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('BOX',(0,0),(-1,-1),0.25,colors.HexColor('#e5e7eb')),
        ('INNERGRID',(0,0),(-1,-1),0.25,colors.HexColor('#e5e7eb')),
        ('TOPPADDING',(0,0),(-1,-1),8),('BOTTOMPADDING',(0,0),(-1,-1),8),
    ]))
    story.append(charts_top)
    story.append(PageBreak())

    story.append(section_header("Turnout and Vote Behavior"))
    story.append(Spacer(1, 0.10*inch))
    story.append(Table(
        [[bar_chart(tables.get("VoteHistory"), "Vote History - All Elections", w=700, h=150)]],
        colWidths=[10.0*inch], rowHeights=[1.9*inch],
        style=TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('BOX',(0,0),(-1,-1),0.25,colors.HexColor('#e5e7eb')),('TOPPADDING',(0,0),(-1,-1),8),('BOTTOMPADDING',(0,0),(-1,-1),8)])
    ))
    # Only include the mail-ballot-by-party history chart when the speed/index layer already has it.
    # This intentionally avoids falling back to a full voter-file scan, which would slow the app down.
    mb_party_df = tables.get("MailBallotByParty")
    if mb_party_df is not None and not getattr(mb_party_df, "empty", True):
        story.append(Spacer(1, 0.12*inch))
        story.append(Table(
            [[stacked_mail_chart(mb_party_df, w=700, h=215)]],
            colWidths=[10.0*inch], rowHeights=[2.75*inch],
            style=TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('BOX',(0,0),(-1,-1),0.25,colors.HexColor('#e5e7eb')),('TOPPADDING',(0,0),(-1,-1),8),('BOTTOMPADDING',(0,0),(-1,-1),8)])
        ))
    story.append(PageBreak())

    # ---------------- Profile tables ----------------
    story.append(section_header("Universe Profile"))
    story.append(Spacer(1, 0.10*inch))
    top = []
    for title_s, key in [("Party Composition", "Party"), ("Age Distribution", "Age"), ("Vote History - All Elections", "VoteHistory"), ("Mail Ballot Behavior", "MailBallot")]:
        tbl = as_table_df(tables.get(key), max_rows=10, max_cols=3, total_w=4.62*inch, first_col_w=2.05*inch, font_size=6.8)
        if tbl:
            top.append([Paragraph(title_s, styles["CC_H2"]), tbl])
    # two columns of mini tables
    rows = []
    for i in range(0, len(top), 2):
        left = [top[i][0], top[i][1]]
        right = [top[i+1][0], top[i+1][1]] if i+1 < len(top) else [Paragraph("", styles["CC_H2"]), Paragraph("", styles["CC_Body"])]
        rows.append([left, right])
    for row in rows:
        story.append(Table(row, colWidths=[4.8*inch, 4.8*inch], style=TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),0)])) )
        story.append(Spacer(1,0.1*inch))

    turnout_df = tables.get("Turnout")
    if turnout_df is not None and not turnout_df.empty:
        story.append(PageBreak())
        story.append(section_header("Historical Turnout by Current Party"))
        story.append(Spacer(1, 0.08*inch))
        story.append(Paragraph("Primary and general elections only. Counts require an actual vote-method record for that election; party columns reflect the voter file's current party grouping at report time.", styles["CC_Small"]))
        story.append(Spacer(1, 0.06*inch))
        story.append(as_table_df(turnout_df, max_rows=12, max_cols=5, first_col_w=2.1*inch, font_size=7.0, total_w=9.8*inch))
    story.append(PageBreak())

    # ---------------- Strategic breakdown ----------------
    breakdown_df = tables.get("Breakdown")
    story.append(section_header(f"Strategic Breakdown by {breakdown_field}"))
    story.append(Spacer(1, 0.10*inch))
    if breakdown_df is not None and not breakdown_df.empty:
        story.append(as_table_df(breakdown_df, max_rows=28, max_cols=12, first_col_w=1.85*inch, font_size=5.2, total_w=10.0*inch))
    else:
        story.append(Paragraph("No breakdown data available for this selection.", styles["CC_Body"]))
    story.append(PageBreak())

    # ---------------- Strategy page ----------------
    story.append(section_header("Campaign Strategy Notes"))
    story.append(Spacer(1, 0.12*inch))
    r = int(summary.get('r',0) or 0); d = int(summary.get('d',0) or 0); o = int(summary.get('o',0) or 0); total = max(1, int(summary.get('total',0) or 0))
    strengths = []
    risks = []
    actions = []
    if max(r,d,o) == r and r/total >= 0.40:
        strengths.append("Republican voters are a major share of the selected universe.")
        actions.append("Build a turnout universe of reliable Republican voters and prioritize door/contact completion by geography.")
    elif max(r,d,o) == d and d/total >= 0.40:
        strengths.append("Democratic voters are a major share of the selected universe.")
        actions.append("Identify persuasion and turnout pockets where direct contact can change the final margin.")
    if o/total >= 0.15:
        risks.append("Other/unaffiliated voters are large enough to influence the result if turnout is uneven.")
        actions.append("Create a persuasion universe using vote history, age, contact availability, and geography concentration.")
    try:
        age65 = int(pd.to_numeric(tables.get("Age", pd.DataFrame()).loc[tables.get("Age", pd.DataFrame()).get("Category", pd.Series(dtype=str)).astype(str).str.contains("65", regex=False), "Voters"], errors="coerce").fillna(0).sum())
        if age65/total >= 0.20:
            strengths.append("The area has a sizable older electorate, which usually rewards repeated direct contact and clear voting instructions.")
            actions.append("Use phone, mail, and volunteer follow-up to reinforce turnout among older high-propensity voters.")
    except Exception:
        pass
    if not actions:
        actions.append("Prioritize direct voter contact in the largest geography rows, then use vote history to separate turnout targets from persuasion targets.")
    for label, items in [("Strengths", strengths or ["The selected universe is structured enough for geography-based field planning." ]), ("Risks", risks or ["No major structural warning appears from the available profile; continue testing turnout and contact assumptions." ]), ("Action Plan", actions[:5])]:
        story.append(Paragraph(label, styles["CC_H"]))
        for item in items:
            story.append(Paragraph(f"• {item}", styles["CC_Body"]))
        story.append(Spacer(1, 0.08*inch))

    def _footer(canvas, doc):
        canvas.saveState()
        canvas.setFont('Helvetica', 7)
        canvas.setFillColor(colors.HexColor('#6b7280'))
        try:
            if file_exists(LOGO_CANDIDATE_CONNECT):
                canvas.drawImage(ImageReader(LOGO_CANDIDATE_CONNECT), 0.42*inch, 0.10*inch, width=0.42*inch, height=0.18*inch, preserveAspectRatio=True, mask='auto')
                canvas.drawString(0.90*inch, 0.17*inch, "Candidate Connect Area Intelligence")
            else:
                canvas.drawString(0.42*inch, 0.17*inch, "Candidate Connect Area Intelligence")
        except Exception:
            canvas.drawString(0.42*inch, 0.17*inch, "Candidate Connect Area Intelligence")
        canvas.drawRightString(10.58*inch, 0.17*inch, f"Page {doc.page}")
        canvas.restoreState()

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    bio.seek(0)
    return bio.getvalue()


def render_area_intelligence_workspace():
    st.markdown("## Area Intelligence")
    st.caption("Professional geography and jurisdiction profile for campaign strategy, targeting, and client-ready reports.")

    saved = get_current_universe_filters()
    default_use = bool(saved)
    use_current = st.checkbox(
        f"Use current universe: {st.session_state.get('current_universe_label', 'None')}",
        value=default_use,
        disabled=not bool(saved),
        key=special_key("area_use_current_universe"),
        help="Use the universe last applied in Create Universe. If unchecked, Area Intelligence starts statewide.",
    )
    active = dict(saved) if (use_current and saved) else {}
    active = enforce_security_scope(active)
    if is_campaign_scoped():
        st.info(f"Campaign boundary enforced: {security_scope_label()}")
    if active:
        st.info(f"Analyzing universe: {_area_universe_label(active)}")
    else:
        st.info("Analyzing statewide universe. Build/apply a Create Universe first to profile a district, county, municipality, or custom target universe.")

    # Optional quick geography focus inside Area Intel without changing Create Universe.
    with st.expander("Optional: focus this Area Intelligence report without changing Create Universe", expanded=False):
        f1, f2, f3, f4 = st.columns(4)
        area_filters = {}
        for col, fld in [(f1,"County"), (f2,"Municipality"), (f3,"STH"), (f4,"STS")]:
            vals = col.multiselect(DISPLAY_LABELS.get(fld, fld), field_options(filter_options, fld, active), key=special_key("area_focus_" + fld))
            if vals:
                area_filters[fld] = vals
        f5, f6, f7, f8 = st.columns(4)
        for col, fld in [(f5,"USC"), (f6,"School District"), (f7,"School Region"), (f8,"Precinct")]:
            vals = col.multiselect(DISPLAY_LABELS.get(fld, fld), field_options(filter_options, fld, {**active, **area_filters}), key=special_key("area_focus_" + re.sub(r'[^A-Za-z0-9]+','_',fld)))
            if vals:
                area_filters[fld] = vals
        if area_filters:
            active = enforce_security_scope({**active, **area_filters})

    active = enforce_security_scope(active)
    summary, mode, err = update_counts(active)
    if not summary:
        st.error(f"Area Intelligence counts are unavailable: {err}")
        return

    render_metrics(summary)

    party_df = _area_group_df(active, "Party", 8)
    gender_df = _area_group_df(active, "Gender", 8)
    age_df = _area_group_df(active, "Age_Range", 12)
    v4a_df = _area_group_df(active, "V4A", 8)
    mb_app_df = _area_group_df(active, "MB_App", 8)
    mb_status_df = _area_group_df(active, "MB_Status", 8)
    # Defer expensive historical turnout queries until Profile Tables or Report is actually opened.
    turnout_df = pd.DataFrame()
    mb_turnout_df = pd.DataFrame()
    insights = _area_insights(summary, party_df, age_df, mb_app_df)

    st.markdown("### Executive Strategy Readout")
    if insights:
        _insight_items = "".join([f"<li>{str(ins)}</li>" for ins in insights])
        st.markdown(f"<div class='cc-note cc-note-compact'><ul>{_insight_items}</ul></div>", unsafe_allow_html=True)

    c1, c2 = st.columns([1, 1])
    with c1:
        render_party_chart(summary, "Party Registration")
    with c2:
        st.markdown(_area_bar_html(age_df, "Age Range"), unsafe_allow_html=True)

    c3, c4 = st.columns([1, 1])
    with c3:
        st.markdown(_area_bar_html(v4a_df, "Vote History - All Elections"), unsafe_allow_html=True)
    with c4:
        st.markdown(_area_bar_html(mb_status_df, "Mail Ballot Status"), unsafe_allow_html=True)

    default_breakdown = _area_default_breakdown(active)
    breakdown_options = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District", "School Region"]
    default_idx = breakdown_options.index(default_breakdown) if default_breakdown in breakdown_options else 0
    breakdown = st.selectbox(
        "Break report down by",
        breakdown_options,
        index=default_idx,
        key=special_key("area_breakdown_by"),
        help="Default follows the next-area-down rule: statewide/multi-county → County; one county → Municipality; one municipality → Precinct. You can override it here.",
    )

    breakdown_df = _area_breakdown_cube(
        json.dumps(count_safe_filters(with_campaign_boundary(active or {})), sort_keys=True),
        json.dumps({k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}, sort_keys=True),
        breakdown,
        300,
    )

    st.markdown(f"### Strategic Breakdown by {DISPLAY_LABELS.get(breakdown, breakdown)}")
    if breakdown_df.empty:
        st.warning("No breakdown data available for this selection.")
    else:
        cc_table(breakdown_df, height=520, key=special_key("area_breakdown_table"), sticky_first_col=True)

    tabs = st.tabs(["Profile Tables", "Report", "Notes"])
    with tabs[0]:
        left, right = st.columns(2)
        with left:
            st.markdown("#### Party")
            cc_table(party_df, height=220, key=special_key("area_tbl_party"))
            st.markdown("#### Gender")
            cc_table(gender_df, height=220, key=special_key("area_tbl_gender"))
            st.markdown("#### Mail Ballot Application")
            cc_table(mb_app_df, height=220, key=special_key("area_tbl_mb_app"))
        with right:
            st.markdown("#### Age Range")
            cc_table(age_df, height=260, key=special_key("area_tbl_age"))
            st.markdown("#### Vote History - All Elections")
            cc_table(v4a_df, height=220, key=special_key("area_tbl_v4a"))
            st.markdown("#### Historical Turnout by Party")
            turnout_df = _area_turnout_by_party(
                json.dumps(count_safe_filters(with_campaign_boundary(active or {})), sort_keys=True),
                json.dumps({k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}, sort_keys=True),
                14,
            )
            cc_table(turnout_df, height=300, key=special_key("area_tbl_turnout"))
            st.markdown("#### Ballot Status")
            cc_table(mb_status_df, height=220, key=special_key("area_tbl_mb_status"))
    with tabs[1]:
        report_title = _area_universe_label(active)
        st.markdown("### Client-ready Area Intelligence Report")
        st.caption("This is built as a report, not a screenshot: cover/summary, strategy notes, profile tables, and the selected geography breakdown.")
        pdf_col1, pdf_col2, pdf_spacer = st.columns([0.9, 1.05, 4.0])
        with pdf_col1:
            prep_clicked = st.button("Prepare Area Intelligence PDF", key=special_key("area_pdf_btn"), type="primary")
        if prep_clicked:
            with st.spinner("Preparing Area Intelligence report..."):
                turnout_df = _area_turnout_by_party(
                    json.dumps(count_safe_filters(with_campaign_boundary(active or {})), sort_keys=True),
                    json.dumps({k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}, sort_keys=True),
                    14,
                )
                mb_turnout_df = _area_mail_ballot_turnout_by_party(
                    json.dumps(count_safe_filters(with_campaign_boundary(active or {})), sort_keys=True),
                    json.dumps({k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}, sort_keys=True),
                    12,
                )
                pdf = _area_pdf_bytes(
                    report_title,
                    active,
                    summary,
                    insights,
                    {
                        "Party": party_df,
                        "Age": age_df,
                        "VoteHistory": v4a_df,
                        "MailBallot": mb_status_df,
                        "Turnout": turnout_df,
                        "MailBallotByParty": mb_turnout_df,
                        "Breakdown": breakdown_df,
                    },
                    DISPLAY_LABELS.get(breakdown, breakdown),
                )
                st.session_state[special_key("area_pdf_bytes")] = pdf
        if st.session_state.get(special_key("area_pdf_bytes")):
            with pdf_col2:
                st.download_button(
                    "Download Area Intelligence PDF",
                    st.session_state[special_key("area_pdf_bytes")],
                    "candidate_connect_area_intelligence_report.pdf",
                    "application/pdf",
                )
    with tabs[2]:
        st.text_area("Area Intelligence notes / strategy", key=special_key("area_notes"), height=180)

def filtered_export_columns(df: pd.DataFrame) -> list[str]:
    base = ["voter_id","County","Municipality","Precinct","USC","STS","STH","School District","School Region",
            "FirstName","MiddleName","LastName","NameSuffix","FullName","Party","CalculatedParty","Gender","DOB","Age","Age_Range","RegistrationDate",
            "House Number","House Number Suffix","Street Name","Apartment Number","Address Line 2","City","State","Zip",
            "Email","Mobile","Landline","Current_ApplicantPhone","MB_App","MB_App_Status","MB_Sent","MB_Status","MB_PERM","MB_Prob_Score","Tags"]
    return [c for c in base if c in df.columns]



def safe_filtered_df(active: dict | None, max_rows: int = EXPORT_ROW_LIMIT) -> pd.DataFrame:
    """Live-safe detail export helper used by exports and Mail Ballot Center.

    Keeps heavy detail scans behind explicit download/prepare actions and applies
    the current special filters, including MB probability score and election filters.
    """
    active = enforce_security_scope(active or {})
    special = active_special_filters() if "active_special_filters" in globals() else {}
    try:
        df = duckdb_detail_filtered_df(active, special, int(max_rows))
    except Exception as exc:
        st.warning(f"Could not prepare filtered voter file: {exc}")
        return pd.DataFrame()
    try:
        return normalize_download_df(df)
    except Exception:
        return df

def texting_export_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["voter_id","FirstName","MiddleName","LastName","NameSuffix","FullName","Precinct","Mobile"])
    df = normalize_download_df(df)
    df = df[df.get("Mobile", pd.Series([""]*len(df), index=df.index)).astype(str).str.strip().ne("")]
    cols = ["voter_id","FirstName","MiddleName","LastName","NameSuffix","FullName","Precinct","Mobile"]
    return df[[c for c in cols if c in df.columns]].copy()


def mail_export_df(df: pd.DataFrame, mailing_mode: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = normalize_download_df(df)
    if mailing_mode == "Householded":
        df = household_for_mail(df)
    cols = ["voter_id","HouseholdName","FirstName","MiddleName","LastName","NameSuffix","FullName","HouseholdCount",
            "House Number","House Number Suffix","Street Name","Apartment Number","Address Line 2","City","State","Zip",
            "County","Municipality","Precinct","Party","Gender","Age"]
    return df[[c for c in cols if c in df.columns]].copy()

def labels_pdf(active: dict, mailing_mode: str = "Not Householded") -> bytes:
    """Generate Avery 5160-style mailing labels. Householded means one label per address."""
    if canvas is None or letter is None or inch is None:
        return b"ReportLab is not available in this environment."
    df = safe_filtered_df(active, EXPORT_ROW_LIMIT)
    if df is None or df.empty:
        df = pd.DataFrame()
    df = mail_export_df(df, "Householded" if mailing_mode == "Householded" else "Not Householded")
    bio = io.BytesIO()
    c = canvas.Canvas(bio, pagesize=letter)
    page_w, page_h = letter
    # Avery 5160: 3 columns x 10 rows, label 2.625 x 1.0, margins about .1875/.5
    left_margin = 0.1875 * inch
    top_margin = 0.50 * inch
    label_w = 2.625 * inch
    label_h = 1.00 * inch
    col_gap = 0.125 * inch
    rows_per_page = 10
    cols_per_page = 3

    def clean(v):
        return cc_text(v).strip()

    def _wrap_text_to_width(text_value: str, max_width: float, font_name: str, font_size: float, max_lines: int = 2) -> list[str]:
        """Wrap label text by actual PDF width so long household names do not get cut off."""
        text_value = clean(text_value)
        if not text_value:
            return []
        words = text_value.split()
        lines = []
        current = ""
        for word in words:
            test = (current + " " + word).strip()
            if c.stringWidth(test, font_name, font_size) <= max_width or not current:
                current = test
            else:
                lines.append(current)
                current = word
                if len(lines) >= max_lines:
                    break
        if current and len(lines) < max_lines:
            lines.append(current)
        # If the final line is still too wide, trim with an ellipsis rather than bleeding into next label.
        fixed = []
        for line in lines[:max_lines]:
            if c.stringWidth(line, font_name, font_size) <= max_width:
                fixed.append(line)
                continue
            ell = "…"
            while line and c.stringWidth(line + ell, font_name, font_size) > max_width:
                line = line[:-1].rstrip()
            fixed.append((line + ell) if line else ell)
        return fixed

    def label_payload(row):
        name = clean(row.get("HouseholdName")) if mailing_mode == "Householded" else ""
        if not name:
            name = clean(row.get("FullName")) or full_name(row)
        if not name:
            name = "Current Resident"
        house = clean(row.get("House Number"))
        suffix = clean(row.get("House Number Suffix"))
        street = clean(row.get("Street Name"))
        apt = clean(row.get("Apartment Number"))
        line2 = clean(row.get("Address Line 2"))
        addr = " ".join([x for x in [house, suffix, street] if x]).strip()
        if apt:
            addr = (addr + " " + apt).strip()
        city = clean(row.get("City")) or clean(row.get("res_city"))
        state = clean(row.get("State")) or clean(row.get("res_state")) or "PA"
        zipc = clean(row.get("Zip")) or clean(row.get("res_zip"))
        csz = ", ".join([x for x in [city, state] if x]).strip()
        if zipc:
            csz = (csz + " " + zipc).strip()
        return name, addr, line2, csz

    c.setTitle("Candidate Connect Mailing Labels")
    for idx, (_, row) in enumerate(df.iterrows()):
        pos = idx % (rows_per_page * cols_per_page)
        if idx and pos == 0:
            c.showPage()
        r = pos // cols_per_page
        col = pos % cols_per_page
        x = left_margin + col * (label_w + col_gap)
        y_top = page_h - top_margin - r * label_h
        name, addr, line2, csz = label_payload(row)

        # Avery 5160 has limited width. Use a bold name, wrap to two lines, and
        # slightly shrink very long names so householded labels remain clean.
        pad_x = 0.12 * inch
        usable_label_w = label_w - (0.22 * inch)
        name_font = "Helvetica-Bold"
        name_size = 8.8 if len(name) > 46 else 9.2
        normal_font = "Helvetica"
        normal_size = 8.8
        name_lines = _wrap_text_to_width(name, usable_label_w, name_font, name_size, max_lines=2)
        detail_lines = []
        if addr:
            detail_lines.extend(_wrap_text_to_width(addr, usable_label_w, normal_font, normal_size, max_lines=1))
        if line2:
            detail_lines.extend(_wrap_text_to_width(line2, usable_label_w, normal_font, normal_size, max_lines=1))
        if csz:
            detail_lines.extend(_wrap_text_to_width(csz, usable_label_w, normal_font, normal_size, max_lines=1))

        # Keep the whole label vertically balanced. If the name takes two lines,
        # there is still room for address + city/state/zip without overlap.
        total_lines = len(name_lines) + len(detail_lines)
        line_gap = 0.135 * inch
        y = y_top - 0.18 * inch
        if total_lines <= 3:
            y -= 0.03 * inch

        c.setFont(name_font, name_size)
        for line in name_lines:
            c.drawString(x + pad_x, y, line)
            y -= line_gap
        c.setFont(normal_font, normal_size)
        for line in detail_lines[: max(0, 4 - len(name_lines))]:
            c.drawString(x + pad_x, y, line)
            y -= line_gap
    if len(df) == 0:
        c.setFont("Helvetica-Bold", 12)
        c.drawCentredString(page_w/2, page_h/2, "No voters matched this export.")
    c.save()
    bio.seek(0)
    return bio.getvalue()


def zip_bytes(files: dict[str, bytes]) -> bytes:
    bio = io.BytesIO()
    import zipfile
    with zipfile.ZipFile(bio, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for name, data in files.items():
            z.writestr(name, data)
    bio.seek(0)
    return bio.getvalue()


def auto_area_level_for_export(active: dict | None) -> str:
    """Pick the first Excel summary level automatically to remove a cluttering UI dropdown."""
    active = active or {}
    county = active.get("County") or []
    muni = active.get("Municipality") or []
    # If more than one county/municipality is in play, summarize by municipality.
    # If exactly one municipality is selected, summarize by precinct.
    if muni and len(muni) == 1:
        return "Precinct"
    if county or muni:
        return "Municipality"
    return "County"


def prepared_key_for(kind: str, ftype: str) -> str:
    safe = re.sub(r"[^a-z0-9]+", "_", f"{kind}_{ftype}".lower()).strip("_")
    return f"prepared_one_export_{safe}"



def _contact_tracking_cols() -> list[str]:
    return ["F", "A", "U", "NH", "Yard Sign"]


def _selected_filter_lines(active: dict | None) -> list[str]:
    """Small cover-page summary of the active universe."""
    lines = []
    active = active or {}
    for k in ALL_FILTER_FIELDS:
        vals = active.get(k) or []
        if vals:
            label = DISPLAY_LABELS.get(k, k)
            shown = ", ".join([cc_text(v) for v in vals[:8]])
            if len(vals) > 8:
                shown += f" +{len(vals)-8} more"
            lines.append(f"{label}: {shown}")
    try:
        special = active_special_filters() if "active_special_filters" in globals() else {}
        if special.get("__PhoneReach"):
            lines.append(f"Phone reach: {special.get('__PhoneReach')}")
    except Exception:
        pass
    return lines[:22]


def _street_pdf_rows(active: dict, call_mode: bool = False) -> pd.DataFrame:
    """Prepare the same voter rows for the polished street/call PDF."""
    df = safe_filtered_df(active, EXPORT_ROW_LIMIT)
    if df is None or df.empty:
        return pd.DataFrame()
    df = normalize_download_df(df)
    df = df.copy()
    df["_name"] = df.apply(full_name, axis=1).map(smart_title)
    df["_phone"] = df.apply(phone_label, axis=1)
    muni = df.get("Municipality", pd.Series([""] * len(df), index=df.index)).astype(str)
    df["_precinct"] = [canonical_precinct_display(p, m) for p, m in zip(df.get("Precinct", pd.Series([""] * len(df), index=df.index)), muni)]
    df["_precinct"] = df["_precinct"].replace("", "Unassigned")
    df["_street"] = df.get("Street Name", pd.Series([""] * len(df), index=df.index)).astype(str).map(smart_title).replace("", "Unknown Street")
    if call_mode:
        df = df[df["_phone"].astype(str).str.strip().ne("")].copy()
        if df.empty:
            return df
    df["_precinct_sort"] = df["_precinct"].astype(str).str.upper()
    df["_street_sort"] = df["_street"].astype(str).str.upper().str.replace(r"[^A-Z0-9 ]+", " ", regex=True).str.strip()
    df["_house_sort"] = pd.to_numeric(df.get("House Number", "").astype(str).str.extract(r"(\d+)")[0], errors="coerce").fillna(0)
    df["_apt_sort"] = df.get("Apartment Number", pd.Series([""] * len(df), index=df.index)).astype(str).str.upper()
    df["_last_sort"] = df.get("LastName", pd.Series([""] * len(df), index=df.index)).astype(str).str.upper()
    df["_first_sort"] = df.get("FirstName", pd.Series([""] * len(df), index=df.index)).astype(str).str.upper()
    return df.sort_values(["_precinct_sort", "_street_sort", "_house_sort", "_apt_sort", "_last_sort", "_first_sort"], kind="mergesort")


def _pdf_clean(v) -> str:
    return cc_text(v).replace("\n", " ").replace("\r", " ").strip()


def _pdf_logo_path(name: str) -> str | None:
    try:
        p = Path(name)
        if p.exists():
            return str(p)
        alt = Path(__file__).resolve().parent / name
        if alt.exists():
            return str(alt)
    except Exception:
        pass
    return None


def _pdf_draw_fit_text(c, text: str, x: float, y: float, max_width: float, font: str = "Helvetica", size: float = 8, min_size: float = 5.5):
    text = _pdf_clean(text)
    c.setFont(font, size)
    while text and c.stringWidth(text, font, size) > max_width and size > min_size:
        size -= 0.3
        c.setFont(font, size)
    if text and c.stringWidth(text, font, size) > max_width:
        while text and c.stringWidth(text + "…", font, size) > max_width:
            text = text[:-1].rstrip()
        text = text + "…" if text else ""
    if text:
        c.drawString(x, y, text)


def _build_street_pdf(active: dict, call_mode: bool = False) -> bytes:
    """Branded precinct/street separated street and call lists with cover, summary, bookmarks, and check boxes."""
    if canvas is None or letter is None or inch is None:
        return b"ReportLab is not available in this environment."

    df = _street_pdf_rows(active, call_mode=call_mode)
    title = "Candidate Connect Call List" if call_mode else "Candidate Connect Street List"
    report_title = "Voter Call List" if call_mode else "Voter Contact List"
    tracks = _contact_tracking_cols()

    bio = io.BytesIO()
    c = canvas.Canvas(bio, pagesize=landscape(letter))
    w, h = landscape(letter)
    mar_l, mar_r = 28, 28
    usable_w = w - mar_l - mar_r
    page_no = 0

    def safe_bookmark(name, title_text, level=0):
        try:
            c.bookmarkPage(name)
            c.addOutlineEntry(title_text[:80], name, level=level, closed=False)
        except Exception:
            pass

    def draw_branded_top(heading: str, subtitle: str = "") -> float:
        # header band
        c.setFillColorRGB(0.94, 0.91, 0.84)
        c.rect(0, h-72, w, 72, stroke=0, fill=1)
        c.setFillColorRGB(0.60, 0.05, 0.08)
        c.rect(0, h-8, w, 8, stroke=0, fill=1)
        logo_left = _pdf_logo_path(LOGO_CANDIDATE_CONNECT)
        logo_right = _pdf_logo_path(LOGO_TPTC)
        try:
            if logo_left:
                c.drawImage(logo_left, 40, h-61, width=130, height=44, preserveAspectRatio=True, mask='auto')
        except Exception:
            pass
        try:
            if logo_right:
                c.drawImage(logo_right, w-145, h-55, width=100, height=34, preserveAspectRatio=True, mask='auto')
        except Exception:
            pass
        c.setFillColorRGB(0.04, 0.12, 0.24)
        c.setFont("Helvetica-Bold", 15)
        c.drawCentredString(w/2, h-38, heading[:70])
        if subtitle:
            c.setFont("Helvetica", 8)
            c.drawCentredString(w/2, h-52, subtitle[:90])
        return h - 92

    def footer():
        nonlocal page_no
        c.setFont("Helvetica", 7)
        c.setFillColorRGB(0.35, 0.35, 0.35)
        c.drawString(mar_l, 18, "Powered by Candidate Connect")
        c.drawRightString(w-mar_r, 18, f"Page {page_no}   Updated: {datetime.now().strftime('%m/%d/%Y')}")

    def finish_page():
        footer()
        c.showPage()

    def new_page(heading: str, subtitle: str = "") -> float:
        nonlocal page_no
        page_no += 1
        return draw_branded_top(heading, subtitle)

    def draw_table_header(y: float) -> float:
        c.setFillColorRGB(0.60, 0.05, 0.08)
        c.roundRect(mar_l, y-15, usable_w, 18, 4, stroke=0, fill=1)
        c.setFillColorRGB(1, 1, 1)
        c.setFont("Helvetica-Bold", 7.2)
        if call_mode:
            cols = [(34, "Full Name"), (200, "Phone"), (350, "Party"), (380, "Sex"), (420, "Age"), (452, "Precinct"), (555, "F"), (580, "A"), (605, "U"), (630, "NH"), (655, "Yard Sign"), (730, "MB Perm")]
        else:
            cols = [(42, "House"), (78, "Full Name"), (225, "Mobile"), (315, "Landline"), (410, "Party"), (440, "Sex"), (470, "Age"), (500, "F"), (525, "A"), (550, "U"), (575, "NH"), (600, "Yard Sign"), (690, "MB Perm")]
        for x, lab in cols:
            c.drawString(x, y-8, lab)
        c.setFillColorRGB(0,0,0)
        return y - 26

    def draw_street_bar(street: str, y: float) -> float:
        c.setFillColorRGB(0.04, 0.12, 0.24)
        c.roundRect(mar_l, y-13, usable_w, 16, 3, stroke=0, fill=1)
        c.setFillColorRGB(1,1,1)
        c.setFont("Helvetica-Bold", 8.5)
        c.drawString(mar_l+8, y-8, smart_title(street)[:80])
        c.setFillColorRGB(0,0,0)
        # Larger gap after each street header so the first voter row does not crowd the bar.
        return y - 33

    def new_detail_page(precinct: str, cont: bool = False, bookmark: bool = False) -> float:
        if bookmark:
            safe_bookmark("pct_" + re.sub(r"[^A-Za-z0-9]+", "_", precinct)[:60], smart_title(precinct), 1)
        heading = f"{smart_title(precinct)[:58]}{' (cont)' if cont else ''}"
        y = new_page(report_title, heading)
        return draw_table_header(y)

    # Cover page.
    safe_bookmark("cover", title, 0)
    y = new_page(title, datetime.now().strftime("%m/%d/%Y"))
    c.setFillColorRGB(0.50, 0.05, 0.12)
    c.setFont("Helvetica-Bold", 22)
    c.drawString(40, y-8, title)
    c.setFillColorRGB(0.12, 0.12, 0.12)
    c.setFont("Helvetica", 10)
    c.drawString(40, y-31, "Prepared from the current Candidate Connect universe.")

    if df is None or df.empty:
        c.setFont("Helvetica-Bold", 14)
        c.drawString(40, y-70, "No voters matched this report.")
        finish_page()
        c.save(); bio.seek(0); return bio.getvalue()

    hh_key = (df.get("County", "").astype(str).str.upper() + "|" +
              df.get("Municipality", "").astype(str).str.upper() + "|" +
              df.get("House Number", "").astype(str).str.upper() + "|" +
              df.get("Street Name", "").astype(str).str.upper() + "|" +
              df.get("Apartment Number", "").astype(str).str.upper())
    households = int(hh_key.nunique()) if len(df) else 0
    c.setFont("Helvetica-Bold", 13)
    c.drawString(40, y-65, f"Individuals: {len(df):,}     Households: {households:,}")

    c.setFont("Helvetica-Bold", 11)
    c.drawString(40, y-100, "Selected Voters")
    c.setFont("Helvetica", 8.7)
    yy = y-118
    lines = _selected_filter_lines(active)
    if not lines:
        lines = ["All active selected voters"]
    for line in lines:
        c.drawString(54, yy, "• " + line[:95])
        yy -= 13
        if yy < 170:
            break

    yy -= 12
    c.setFillColorRGB(0.50, 0.05, 0.12)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(40, yy, "Legend")
    yy -= 15
    c.setFillColorRGB(0.12, 0.12, 0.12)
    c.setFont("Helvetica", 8.5)
    legend_lines = [
        "Phone labels: (m) mobile, (l) landline, (u) applicant/unknown.",
        "Contact boxes: F = Favorable, A = Against, U = Undecided, NH = Not Home, Yard Sign = sign requested/placed.",
        "MB prints Y for permanent mail ballot voters.",
        "Detail pages are sorted by precinct, street, house number, apartment, last name, and first name.",
    ]
    for line in legend_lines:
        c.drawString(54, yy, "• " + line)
        yy -= 13
    finish_page()

    # Precinct summary page.
    safe_bookmark("summary", "Precinct Summary", 0)
    y = new_page(title, "Precinct Summary")
    summary = df.groupby("_precinct", dropna=False).agg(
        Individuals=("voter_id", "count") if "voter_id" in df.columns else ("_name", "count"),
        Households=("_house_sort", "count"),
    ).reset_index()
    # Recalculate households per precinct using household key.
    try:
        tmp = df.copy()
        tmp["_hh"] = hh_key
        hh_summary = tmp.groupby("_precinct")["_hh"].nunique().reset_index(name="Households")
        summary = summary.drop(columns=["Households"], errors="ignore").merge(hh_summary, on="_precinct", how="left")
    except Exception:
        pass
    summary = summary.sort_values("_precinct")
    c.setFillColorRGB(0.50, 0.05, 0.12)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, y, "Precinct Summary")
    y -= 28
    c.setFillColorRGB(0.04, 0.12, 0.24)
    c.roundRect(36, y-14, 520, 18, 3, stroke=0, fill=1)
    c.setFillColorRGB(1,1,1)
    c.setFont("Helvetica-Bold", 8.5)
    c.drawString(46, y-8, "Precinct")
    c.drawRightString(430, y-8, "Individuals")
    c.drawRightString(535, y-8, "Households")
    y -= 26
    c.setFont("Helvetica", 8.2)
    for _, rr in summary.iterrows():
        if y < 42:
            finish_page(); y = new_page(title, "Precinct Summary (cont)")
        c.setFillColorRGB(0.12,0.12,0.12)
        c.drawString(46, y, smart_title(rr.get("_precinct", ""))[:60])
        c.drawRightString(430, y, f"{int(rr.get('Individuals',0)):,}")
        c.drawRightString(535, y, f"{int(rr.get('Households',0) or 0):,}")
        y -= 12
    finish_page()

    current_precinct = None
    current_street = None
    seen_precincts = set()
    y = None
    row_count = 0

    for _, r in df.iterrows():
        precinct = _pdf_clean(r.get("_precinct", "")) or "Unassigned"
        street = smart_title(r.get("_street", "")) or "Unknown Street"
        house_raw = _pdf_clean(r.get("House Number", ""))
        m_house = re.search(r"\d+", house_raw)
        house = m_house.group(0) if m_house else house_raw[:8]
        apt_raw = _pdf_clean(r.get("Apartment Number", ""))
        apt = ""
        if re.fullmatch(r"(?i)(?:apt|unit|ste|suite|#)?\s*[A-Z0-9-]{1,8}", apt_raw or "") and not re.search(r"(?i)\b(?:dr|rd|st|ave|ln|ct|cir|blvd|way|road|street|drive|lane)\b", apt_raw or ""):
            apt = re.sub(r"(?i)^(apt|unit|ste|suite)\s+", "", apt_raw).strip()
        house_text = (house + (f" Apt {apt}" if apt else "")).strip()

        new_precinct = precinct != current_precinct
        if y is None or new_precinct or y < 56:
            if y is not None:
                finish_page()
            y = new_detail_page(precinct, cont=False, bookmark=(precinct not in seen_precincts))
            seen_precincts.add(precinct)
            current_precinct = precinct
            current_street = None
        if not call_mode and street != current_street:
            if y < 82:
                finish_page()
                y = new_detail_page(precinct, cont=True, bookmark=False)
            y = draw_street_bar(street, y)
            current_street = street

        phone_items = phone_entries(r)
        phone_lines = [f"{num} ({typ})" for num, typ in phone_items]
        mobile_text = " / ".join([num for num, typ in phone_items if typ == "m"])
        landline_text = " / ".join([num for num, typ in phone_items if typ == "l"])
        other_phone_text = " / ".join([f"{num} ({typ})" for num, typ in phone_items if typ not in {"m", "l"}])
        row_h = 18
        if y - row_h < 34:
            finish_page()
            y = new_detail_page(precinct, cont=True, bookmark=False)
            if not call_mode:
                y = draw_street_bar(street, y)

        if row_count % 2 == 0:
            c.setFillColorRGB(0.965, 0.86, 0.88)
        else:
            c.setFillColorRGB(1, 1, 1)
        # Keep the alternating band aligned with the fixed voter row height.
        c.rect(mar_l+6, y-5, usable_w-12, 12, fill=1, stroke=0)
        c.setFillColorRGB(0.08,0.08,0.08)
        name = smart_title(r.get("_name", ""))
        age = _pdf_clean(r.get("Age", ""))[:3]
        party = _pdf_clean(r.get("Party", ""))[:1]
        gender = _pdf_clean(r.get("Gender", ""))[:1]
        if call_mode:
            c.setFont("Helvetica", 6.7)
            _pdf_draw_fit_text(c, name, 34, y, 152, "Helvetica", 6.7)
            _pdf_draw_fit_text(c, _pdf_clean(r.get("_phone", "")), 200, y, 140, "Helvetica", 6.7)
            c.drawString(350, y, party)
            c.drawString(380, y, gender)
            c.drawRightString(440, y, age)
            _pdf_draw_fit_text(c, smart_title(precinct), 452, y, 88, "Helvetica", 6.4)
            x = 555
            for t in tracks:
                c.rect(x, y-2, 6, 6, fill=0, stroke=1)
                x += 25 if t != "Yard Sign" else 66
            if str(r.get("MB_PERM", "")).strip().upper() in {"Y", "YES", "1", "TRUE"}:
                c.drawString(730, y, "Y")
        else:
            c.setFont("Helvetica-Bold", 7)
            c.drawString(42, y, house_text[:17])
            _pdf_draw_fit_text(c, name, 78, y, 138, "Helvetica", 6.8)
            _pdf_draw_fit_text(c, mobile_text or other_phone_text, 225, y, 82, "Helvetica", 6.6)
            _pdf_draw_fit_text(c, landline_text, 315, y, 82, "Helvetica", 6.6)
            c.setFont("Helvetica", 6.8)
            c.drawString(410, y, party)
            c.drawString(440, y, gender)
            c.drawRightString(486, y, age)
            x = 500
            for t in tracks:
                c.rect(x, y-2, 6, 6, fill=0, stroke=1)
                x += 25 if t != "Yard Sign" else 66
            if str(r.get("MB_PERM", "")).strip().upper() in {"Y", "YES", "1", "TRUE"}:
                c.drawString(690, y, "Y")
        y -= row_h
        row_count += 1

    if y is not None:
        finish_page()
    c.save()
    bio.seek(0)
    return bio.getvalue()


def street_list_pdf(active: dict) -> bytes:
    return _build_street_pdf(active, call_mode=False)


def call_list_pdf(active: dict) -> bytes:
    return _build_street_pdf(active, call_mode=True)


def build_single_export(active, export_kind: str, file_type: str, mailing_mode: str) -> tuple[str, bytes, str, int]:
    """Build one export/report at a time from a simple dropdown workflow."""
    area_level = auto_area_level_for_export(active)
    kind = export_kind.lower()
    ftype = file_type.lower()
    if export_kind in {"Full File", "Texting File", "Mail File"}:
        base_df = safe_filtered_df(active, EXPORT_ROW_LIMIT)
        if export_kind == "Texting File":
            out = texting_export_df(base_df)
            stem = "candidate_connect_texting"
        elif export_kind == "Mail File":
            out = mail_export_df(base_df, mailing_mode)
            stem = "candidate_connect_mail"
        else:
            out = base_df[[c for c in filtered_export_columns(base_df) if c in base_df.columns]].copy()
            out = drop_all_blank_optional_columns(out, required=["voter_id","FirstName","LastName","House Number","Street Name","City","State","Zip"])
            stem = "candidate_connect_filtered"
        if ftype == "csv":
            return f"{stem}.csv", out.to_csv(index=False).encode(), "text/csv", len(out)
        return f"{stem}.xlsx", dataframe_to_excel_bytes(out, area_level), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", len(out)
    if export_kind == "Street List PDF":
        return "candidate_connect_street_list.pdf", street_list_pdf(active), "application/pdf", 0
    if export_kind == "Call List PDF":
        return "candidate_connect_call_list.pdf", call_list_pdf(active), "application/pdf", 0
    if export_kind == "Mailing Labels PDF":
        return "candidate_connect_labels_avery5160.pdf", labels_pdf(active, mailing_mode), "application/pdf", 0
    raise ValueError(f"Unsupported export type: {export_kind}")


def contact_tracking_template(kind: str) -> bytes:
    if kind == "Street Results":
        cols = ["voter_id","FullName","Street Name","House Number","Apartment Number","Phone","F","A","U","NH","Yard Sign","Notes"]
    else:
        cols = ["voter_id","FullName","Phone","Contacted","Result","Support Level","Follow-Up","Notes"]
    return pd.DataFrame(columns=cols).to_csv(index=False).encode()

def render_output_buttons(active):
    if not user_can("exports_reports"):
        tabs = st.tabs(["Overview"])
        with tabs[0]:
            summary, mode, err = update_counts(active)
            if summary:
                render_metric_summary(summary)
            if err:
                st.warning("Counts are unavailable for this filter combination.")
                st.caption(str(err)[:500])
            st.info("Exports and reports are disabled for your role.")
        return

    tabs = st.tabs(["Overview", "Exports", "Reports"])
    with tabs[0]:
        summary, mode, err = update_counts(active)
        if summary:
            render_metrics(summary)
            c1, c2 = st.columns([1, 1])
            with c1:
                render_party_chart(summary, "Party Breakdown")
            with c2:
                st.markdown("### Counts by Area")
                area_level_ov = st.selectbox("Area table", ["County", "Municipality", "Precinct", "School District", "School Region"], key=special_key("output_overview_area"))
                area_df_ov = duckdb_count_cube_group_filtered(json.dumps(count_safe_filters(with_campaign_boundary(active or {})), sort_keys=True), json.dumps({k:v for k,v in active_special_filters().items() if not str(k).startswith("__Election")}, sort_keys=True), area_level_ov, 200)
                if not area_df_ov.empty:
                    area_df_ov = area_df_ov.rename(columns={"label": area_level_ov})
                    area_df_ov["Voters"] = area_df_ov["Voters"].fillna(0).astype(int)
                    if area_level_ov == "Precinct":
                        area_df_ov[area_level_ov] = area_df_ov[area_level_ov].map(canonical_precinct_display)
                        area_df_ov = area_df_ov.groupby(area_level_ov, as_index=False)["Voters"].sum()
                    area_df_ov = area_df_ov.sort_values(area_level_ov, kind="stable")
                    cc_table(area_df_ov, height=260, key=special_key("output_overview_area_table_display"))
        elif err:
            st.warning(err)

    # Export Center
    with tabs[1]:
        st.markdown("### Export Center")
        st.caption("Pick one output type and one file type, prepare it, then download. Excel summaries are chosen automatically: county/multi-municipality universes summarize by municipality; one municipality summarizes by precinct.")
        e1, e2, e3 = st.columns([1.2, .8, 1.0])
        with e1:
            export_kind = st.selectbox("Download type", ["Full File", "Texting File", "Mail File", "Street List PDF", "Call List PDF", "Mailing Labels PDF"], key=special_key("export_kind"))
        with e2:
            allowed_types = ["PDF"] if export_kind.endswith("PDF") else ["CSV", "Excel"]
            file_type = st.selectbox("File type", allowed_types, key=special_key("export_file_type"))
        with e3:
            mailing_mode = st.radio("Mailing mode", ["Not Householded", "Householded"], horizontal=True, key=special_key("mailing_mode"), disabled=(export_kind not in ["Mail File", "Mailing Labels PDF"]))
            if export_kind in ["Mail File", "Mailing Labels PDF"]:
                st.caption("Householded = one mail piece/label per household address.")

        key = prepared_key_for(export_kind, file_type)
        pcol, dcol = st.columns([1, 1])
        with pcol:
            if st.button("Prepare Download", width="stretch"):
                with st.spinner(f"Preparing {export_kind}..."):
                    filename, data, mime, row_count = build_single_export(active, export_kind, file_type, mailing_mode)
                    st.session_state[key] = {"filename": filename, "data": data, "mime": mime, "rows": row_count}
        with dcol:
            if key in st.session_state:
                item = st.session_state[key]
                label = f"Download {item['filename']}" + (f" ({item['rows']:,} rows)" if item.get("rows") else "")
                st.download_button(label, item["data"], item["filename"], item["mime"], width="stretch", on_click=mark_downloaded, args=(key,))
            else:
                st.button("Download", disabled=True, width="stretch")

        with st.expander("Batch ZIP export", expanded=False):
            selected_types = st.multiselect("Files to include", ["Full CSV", "Text CSV", "Mail CSV", "Full Excel", "Text Excel", "Mail Excel", "Street List PDF", "Call List PDF", "Mailing Labels PDF"], default=[], key=special_key("bulk_export_types"))
            zip_key = "prepared_export_zip"
            if st.button("Prepare Selected ZIP", width="stretch"):
                with st.spinner("Building selected ZIP..."):
                    base_df = safe_filtered_df(active, EXPORT_ROW_LIMIT)
                    files = {}
                    area_level = auto_area_level_for_export(active)
                    if "Full CSV" in selected_types:
                        fdf = base_df[[c for c in filtered_export_columns(base_df) if c in base_df.columns]]
                        files["candidate_connect_filtered.csv"] = fdf.to_csv(index=False).encode()
                    if "Text CSV" in selected_types:
                        files["candidate_connect_texting.csv"] = texting_export_df(base_df).to_csv(index=False).encode()
                    if "Mail CSV" in selected_types:
                        files["candidate_connect_mail.csv"] = mail_export_df(base_df, mailing_mode).to_csv(index=False).encode()
                    if "Full Excel" in selected_types:
                        fdf = base_df[[c for c in filtered_export_columns(base_df) if c in base_df.columns]]
                        files["candidate_connect_filtered.xlsx"] = dataframe_to_excel_bytes(fdf, area_level)
                    if "Text Excel" in selected_types:
                        files["candidate_connect_texting.xlsx"] = dataframe_to_excel_bytes(texting_export_df(base_df), area_level)
                    if "Mail Excel" in selected_types:
                        files["candidate_connect_mail.xlsx"] = dataframe_to_excel_bytes(mail_export_df(base_df, mailing_mode), area_level)
                    if "Street List PDF" in selected_types:
                        files["candidate_connect_street_list.pdf"] = street_list_pdf(active)
                    if "Call List PDF" in selected_types:
                        files["candidate_connect_call_list.pdf"] = call_list_pdf(active)
                    if "Mailing Labels PDF" in selected_types:
                        files["candidate_connect_labels_avery5160.pdf"] = labels_pdf(active, mailing_mode)
                    st.session_state[zip_key] = zip_bytes(files) if files else b""
            if st.session_state.get(zip_key):
                st.download_button("Download Selected ZIP", st.session_state[zip_key], "candidate_connect_exports.zip", "application/zip", width="stretch", on_click=mark_downloaded, args=(zip_key,))

    with tabs[2]:
        st.markdown("### Reports + Tracking")
        st.caption("Prepare one PDF/report at a time. Street and call lists are sorted like the local list and include mobile/landline phone labels.")

        # Clean report workflow: no stale download button and no wide, confusing buttons.
        r1, r2, spacer = st.columns([1.25, .7, 2.2])
        with r1:
            report_kind = st.selectbox(
                "Report type",
                ["Street List PDF", "Call List PDF", "Mailing Labels PDF"],
                key=special_key("report_kind_clean"),
            )
        with r2:
            file_type = st.selectbox("File type", ["PDF"], key=special_key("report_file_type_clean"))

        report_key = prepared_key_for(report_kind, "PDF")
        # If the user changes the report type, do not show an older report download as if it were ready.
        current_ready_key = st.session_state.get("prepared_report_ready_key")
        report_is_ready = current_ready_key == report_key and report_key in st.session_state

        b1, b2, b3 = st.columns([.9, 1.25, 3.0])
        with b1:
            prepare_clicked = st.button("Prepare Report", key=special_key("prepare_report_button"))
        if prepare_clicked:
            # Clear old report artifacts first so no stale download appears.
            for k in list(st.session_state.keys()):
                if str(k).startswith("prepared_one_export_street_list_pdf") or str(k).startswith("prepared_one_export_call_list_pdf") or str(k).startswith("prepared_one_export_mailing_labels_pdf"):
                    _ = st.session_state.pop(k, None)
            _ = st.session_state.pop("prepared_report_ready_key", None)
            with st.spinner(f"Building {report_kind}..."):
                filename, data, mime, row_count = build_single_export(
                    active,
                    report_kind,
                    "PDF",
                    st.session_state.get(special_key("mailing_mode"), "Not Householded"),
                )
                st.session_state[report_key] = {"filename": filename, "data": data, "mime": mime, "rows": row_count}
                st.session_state["prepared_report_ready_key"] = report_key
                report_is_ready = True
            st.session_state["prepared_report_message"] = f"Prepared {filename}"

        with b2:
            if report_is_ready:
                item = st.session_state[report_key]
                _ = st.download_button(
                    f"Download {item['filename']}",
                    item["data"],
                    item["filename"],
                    item["mime"],
                    key=special_key("download_prepared_report_button"),
                    on_click=mark_downloaded,
                    args=(report_key,),
                )

        st.markdown("---")
        st.markdown("#### Contact Tracking")
        t1, t2, t3 = st.columns([1.1, 1.1, 1.8])
        with t1:
            _ = st.download_button("Street Results CSV Template", contact_tracking_template("Street Results"), "street_results_template.csv", "text/csv")
        with t2:
            _ = st.download_button("Walk/Call Tracking CSV Template", contact_tracking_template("Walk Call"), "walk_call_tracking_template.csv", "text/csv")
        with t3:
            uploaded = st.file_uploader("Upload completed contact results", type=["csv", "xlsx"], key=special_key("contact_results_upload_clean"))
            if uploaded is not None:
                _ = st.success(f"Loaded {uploaded.name}. Contact update import will be applied in the pipeline pass.")



# ---------------------------------------------------------------------------
# Campaign Operations modules: Campaign Organization / Voter Outreach foundation
# ---------------------------------------------------------------------------
def _ops_slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", str(value or "").strip().lower()).strip("-") or "default"


def _campaign_records_for_current_user() -> dict:
    try:
        store = load_security_store()
        campaigns = store.get("campaigns") if isinstance(store.get("campaigns"), dict) else {}
        return campaigns or {}
    except Exception:
        return {}


def _current_campaign_ops_id() -> str:
    """Campaign-scoped app_state folder for operations modules.

    Super Admin can select a campaign inside the workspace. Campaign users are
    locked to their own campaign. This keeps volunteer/team/contact operations
    out of the statewide voter data and out of security_store.json.
    """
    try:
        u = current_user() or {}
        if is_super_admin():
            selected = st.session_state.get("ops_selected_campaign_id") or ""
            if selected:
                return _ops_slug(selected)
            campaigns = _campaign_records_for_current_user()
            if campaigns:
                first = sorted([_ops_slug(k) for k in campaigns.keys() if str(k).strip()])[0]
                st.session_state["ops_selected_campaign_id"] = first
                return first
            return "super-admin-sandbox"
        return _ops_slug(u.get("campaign_id") or u.get("campaign") or u.get("campaign_name") or current_username())
    except Exception:
        return "default"


def _ops_json_get(key: str, default):
    try:
        r = requests.get(root_r2_url(key), timeout=10)
        if r.ok:
            data = r.json()
            return data if data is not None else default
    except Exception:
        pass
    return default


def _team_people_key(campaign_id: str) -> str:
    return f"app_state/campaigns/{_ops_slug(campaign_id)}/team/people.json"


def _empty_team_people_store() -> dict:
    return {"version": 1, "updated_at": datetime.now().isoformat(timespec="seconds"), "people": []}


@st.cache_data(ttl=15, show_spinner=False)
def load_team_people_store(campaign_id: str) -> dict:
    data = _ops_json_get(_team_people_key(campaign_id), {})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("version", 1)
    data.setdefault("people", [])
    if not isinstance(data.get("people"), list):
        data["people"] = []
    return data


def save_team_people_store(campaign_id: str, store: dict) -> tuple[bool, str]:
    if not isinstance(store, dict):
        store = _empty_team_people_store()
    store["version"] = int(store.get("version") or 1)
    store["updated_at"] = datetime.now().isoformat(timespec="seconds")
    store.setdefault("people", [])
    ok, msg = _put_json_to_r2_key(_team_people_key(campaign_id), store)
    try:
        load_team_people_store.clear()
    except Exception:
        pass
    return ok, msg


def _team_person_id(name: str, email: str = "", mobile: str = "") -> str:
    raw = "|".join([str(name or "").strip().lower(), str(email or "").strip().lower(), re.sub(r"\D+", "", str(mobile or ""))])
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    return f"tm-{digest}"


def _clean_phone(value) -> str:
    s = clean_value(value) if 'clean_value' in globals() else str(value or "").strip()
    return re.sub(r"\s+", " ", s).strip()


def _normalize_team_person(raw: dict, campaign_id: str, source: str = "manual") -> dict:
    raw = raw or {}
    first = clean_value(raw.get("first_name") or raw.get("First Name") or raw.get("FirstName") or "") if 'clean_value' in globals() else str(raw.get("first_name") or "")
    last = clean_value(raw.get("last_name") or raw.get("Last Name") or raw.get("LastName") or "") if 'clean_value' in globals() else str(raw.get("last_name") or "")
    name = clean_value(raw.get("name") or raw.get("Name") or raw.get("Full Name") or raw.get("FullName") or " ".join([first, last]).strip()) if 'clean_value' in globals() else str(raw.get("name") or "")
    email_raw = raw.get("email") or raw.get("Email") or raw.get("E-mail") or ""
    email = clean_value(email_raw).lower() if 'clean_value' in globals() else str(email_raw or "").strip().lower()
    mobile = _clean_phone(raw.get("mobile") or raw.get("Mobile") or raw.get("Cell") or raw.get("Cell Phone") or raw.get("Phone") or "")
    landline = _clean_phone(raw.get("landline") or raw.get("Landline") or raw.get("Home Phone") or raw.get("HomePhone") or "")
    role = clean_value(raw.get("role") or raw.get("Role") or "Volunteer") if 'clean_value' in globals() else str(raw.get("role") or "Volunteer")
    status = clean_value(raw.get("status") or raw.get("Status") or "Active") if 'clean_value' in globals() else str(raw.get("status") or "Active")
    address = clean_value(raw.get("address") or raw.get("Address") or raw.get("Street Address") or "") if 'clean_value' in globals() else str(raw.get("address") or "")
    city = clean_value(raw.get("city") or raw.get("City") or "") if 'clean_value' in globals() else str(raw.get("city") or "")
    state = clean_value(raw.get("state") or raw.get("State") or "PA") if 'clean_value' in globals() else str(raw.get("state") or "PA")
    zip_code = clean_value(raw.get("zip") or raw.get("Zip") or raw.get("ZIP") or raw.get("Zip Code") or "") if 'clean_value' in globals() else str(raw.get("zip") or "")
    notes = clean_value(raw.get("notes") or raw.get("Notes") or "") if 'clean_value' in globals() else str(raw.get("notes") or "")
    skills = clean_value(raw.get("skills") or raw.get("Skills") or "") if 'clean_value' in globals() else str(raw.get("skills") or "")
    person_id = clean_value(raw.get("person_id") or raw.get("team_member_id") or "") if 'clean_value' in globals() else str(raw.get("person_id") or "")
    if not person_id:
        person_id = _team_person_id(name, email, mobile)
    return {
        "person_id": person_id,
        "campaign_id": _ops_slug(campaign_id),
        "name": name,
        "email": email,
        "mobile": mobile,
        "landline": landline,
        "address": address,
        "city": city,
        "state": state,
        "zip": zip_code,
        "role": role or "Volunteer",
        "status": status or "Active",
        "skills": skills,
        "notes": notes,
        "source": source,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _merge_team_people(existing: list, incoming: list) -> tuple[list, int, int]:
    by_id = {str(p.get("person_id")): dict(p) for p in (existing or []) if isinstance(p, dict) and p.get("person_id")}
    added = updated = 0
    for p in incoming or []:
        if not isinstance(p, dict) or not p.get("person_id"):
            continue
        pid = str(p.get("person_id"))
        if pid in by_id:
            old = by_id[pid]
            old.update({k: v for k, v in p.items() if v not in [None, ""] or k in {"status", "role"}})
            by_id[pid] = old
            updated += 1
        else:
            by_id[pid] = p
            added += 1
    people = sorted(by_id.values(), key=lambda r: (str(r.get("status", "")), str(r.get("name", "")).lower()))
    return people, added, updated


def _team_people_dataframe(people: list) -> pd.DataFrame:
    cols = ["person_id", "name", "role", "status", "mobile", "landline", "email", "address", "city", "state", "zip", "skills", "notes"]
    df = pd.DataFrame(people or [])
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df[cols]


def _select_ops_campaign_control(prefix: str = "ops") -> str:
    if not is_super_admin():
        return _current_campaign_ops_id()
    campaigns = _campaign_records_for_current_user()
    options = sorted([_ops_slug(k) for k in campaigns.keys() if str(k).strip()])
    if not options:
        st.info("No campaigns exist yet. Create/approve a campaign first, or use the Super Admin sandbox for testing.")
        st.session_state.setdefault("ops_selected_campaign_id", "super-admin-sandbox")
        return st.session_state.get("ops_selected_campaign_id")
    current = st.session_state.get("ops_selected_campaign_id")
    if current not in options:
        current = options[0]
        st.session_state["ops_selected_campaign_id"] = current
    selected = st.selectbox("Campaign", options, index=options.index(current), key=f"{prefix}_campaign_select")
    st.session_state["ops_selected_campaign_id"] = selected
    return selected


def render_campaign_organization_workspace():
    st.markdown("## Campaign Organization")
    st.caption("Manage the people who help the campaign: candidates, staff, volunteers, canvassers, phone bankers, drivers, poll workers, and Election Day helpers.")
    campaign_id = _select_ops_campaign_control("campaign_org")
    tab_team, tab_roles, tab_assign = st.tabs(["Team / Volunteers", "Roles", "Assignment Readiness"])
    with tab_team:
        render_team_volunteers_workspace(campaign_id)
    with tab_roles:
        st.markdown("### Roles")
        st.info("First pass: roles are saved on each team member. Later this becomes role-based permissions, training status, and Election Day jobs.")
        st.markdown("Common roles: Candidate, Campaign Manager, Volunteer, Canvasser, Phone Banker, Driver, Poll Worker, Observer, Data Entry, Election Day Captain.")
    with tab_assign:
        st.markdown("### Assignment Readiness")
        store = load_team_people_store(campaign_id)
        people = store.get("people") or []
        active_people = [p for p in people if str(p.get("status", "")).lower() == "active"]
        st.metric("Active team members", len(active_people))
        st.caption("Voter Outreach will assign door/phone/postcard lists to these team members, not directly to the voter database.")


def render_team_volunteers_workspace(campaign_id: str | None = None):
    campaign_id = _ops_slug(campaign_id or _current_campaign_ops_id())
    store = load_team_people_store(campaign_id)
    people = store.get("people") or []
    st.markdown("### Team / Volunteers")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total people", len(people))
    with c2:
        st.metric("Active", sum(1 for p in people if str(p.get("status", "")).lower() == "active"))
    with c3:
        st.metric("Canvassers", sum(1 for p in people if "canvass" in str(p.get("role", "")).lower()))

    tab_add, tab_upload, tab_roster = st.tabs(["Add Person", "Upload List", "Roster"])
    with tab_add:
        with st.form(f"team_add_form_{campaign_id}"):
            a, b = st.columns(2)
            with a:
                name = st.text_input("Name")
                role = st.selectbox("Role", ["Volunteer", "Canvasser", "Phone Banker", "Campaign Manager", "Candidate", "Driver", "Poll Worker", "Observer", "Data Entry", "Election Day Captain", "Other"])
                status = st.selectbox("Status", ["Active", "Prospect", "Inactive", "Do Not Contact"])
                mobile = st.text_input("Mobile")
                landline = st.text_input("Landline")
                email = st.text_input("Email")
            with b:
                address = st.text_input("Address")
                city = st.text_input("City")
                state_val = st.text_input("State", value="PA")
                zip_code = st.text_input("Zip")
                skills = st.text_input("Skills / interests")
                notes = st.text_area("Notes", height=90)
            submitted = st.form_submit_button("Add Team Member", type="primary")
        if submitted:
            if not str(name or "").strip():
                st.error("Name is required.")
            else:
                person = _normalize_team_person({"name": name, "role": role, "status": status, "mobile": mobile, "landline": landline, "email": email, "address": address, "city": city, "state": state_val, "zip": zip_code, "skills": skills, "notes": notes}, campaign_id, "manual")
                merged, added, updated = _merge_team_people(people, [person])
                store["people"] = merged
                ok, msg = save_team_people_store(campaign_id, store)
                if ok:
                    st.success(f"Saved team member. Added {added}, updated {updated}.")
                    st.rerun()
                else:
                    st.error(f"Could not save team member: {msg}")

    with tab_upload:
        st.caption("Upload an existing volunteer/worker list. Supported columns include Name, Email, Mobile/Cell/Phone, Landline/Home Phone, Address, City, State, Zip, Role, Status, Skills, Notes.")
        sample = pd.DataFrame([{"Name": "Jane Volunteer", "Email": "jane@example.com", "Mobile": "717-555-0100", "Landline": "", "Address": "123 Main St", "City": "York", "State": "PA", "Zip": "17401", "Role": "Canvasser", "Status": "Active", "Skills": "Doors, phones", "Notes": "Prefers weekends"}])
        st.download_button("Download volunteer upload template", sample.to_csv(index=False).encode("utf-8"), "candidate_connect_team_upload_template.csv", "text/csv")
        uploaded = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx", "xls"], key=f"team_upload_{campaign_id}")
        if uploaded is not None:
            try:
                if uploaded.name.lower().endswith(".csv"):
                    df = pd.read_csv(uploaded, dtype=str).fillna("")
                else:
                    df = pd.read_excel(uploaded, dtype=str).fillna("")
                st.write("Preview")
                st.dataframe(df.head(25), use_container_width=True, hide_index=True)
                if st.button("Import Team List", key=f"team_import_btn_{campaign_id}", type="primary"):
                    incoming = [_normalize_team_person(row.to_dict(), campaign_id, "upload") for _, row in df.iterrows()]
                    incoming = [p for p in incoming if str(p.get("name", "")).strip()]
                    merged, added, updated = _merge_team_people(people, incoming)
                    store["people"] = merged
                    ok, msg = save_team_people_store(campaign_id, store)
                    if ok:
                        st.success(f"Imported team list. Added {added}, updated {updated}.")
                        st.rerun()
                    else:
                        st.error(f"Could not save import: {msg}")
            except Exception as exc:
                st.error("Could not read that upload.")
                st.exception(exc)

    with tab_roster:
        df = _team_people_dataframe(people)
        if df.empty:
            st.info("No team members saved yet.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("Download roster CSV", df.to_csv(index=False).encode("utf-8"), f"team_roster_{campaign_id}.csv", "text/csv")
            st.markdown("#### Update / Delete")
            options = [f"{r.get('name','')} — {r.get('role','')} — {r.get('person_id','')}" for r in people]
            choice = st.selectbox("Select team member", [""] + options, key=f"team_edit_choice_{campaign_id}")
            if choice:
                pid = choice.rsplit(" — ", 1)[-1]
                rec = next((p for p in people if p.get("person_id") == pid), {})
                e1, e2 = st.columns(2)
                with e1:
                    new_status = st.selectbox("Status", ["Active", "Prospect", "Inactive", "Do Not Contact"], index=["Active", "Prospect", "Inactive", "Do Not Contact"].index(rec.get("status", "Active") if rec.get("status", "Active") in ["Active", "Prospect", "Inactive", "Do Not Contact"] else "Active"), key=f"team_status_{pid}")
                    if st.button("Update Status", key=f"team_update_{pid}"):
                        for p in people:
                            if p.get("person_id") == pid:
                                p["status"] = new_status
                                p["updated_at"] = datetime.now().isoformat(timespec="seconds")
                        store["people"] = people
                        ok, msg = save_team_people_store(campaign_id, store)
                        st.success("Updated.") if ok else st.error(msg)
                        st.rerun()
                with e2:
                    confirm = st.checkbox("Confirm delete", key=f"team_del_confirm_{pid}")
                    if st.button("Delete Team Member", key=f"team_delete_{pid}", disabled=not confirm):
                        store["people"] = [p for p in people if p.get("person_id") != pid]
                        ok, msg = save_team_people_store(campaign_id, store)
                        st.success("Deleted.") if ok else st.error(msg)
                        st.rerun()


def render_voter_outreach_workspace():
    st.markdown("## Voter Outreach")
    st.caption("Plan direct voter contact in Candidate Connect and execute it later from the offline mobile utility.")
    tab_door, tab_phone, tab_mail, tab_results = st.tabs(["Door-to-Door", "Phone Bank", "Postcards / Mail", "Results"])
    with tab_door:
        st.markdown("### Door-to-Door")
        st.info("Next build: create a walk/contact list from a saved universe and assign it to a team member for offline mobile download.")
        st.markdown("Workflow: Saved Universe → Contact List → Assignment → Mobile Download → Offline Contacts → Sync Back.")
    with tab_phone:
        st.markdown("### Phone Bank")
        st.caption("Uses the same Contact Program / Assignment / Contact Result architecture as door-to-door.")
    with tab_mail:
        st.markdown("### Postcards / Mail")
        st.caption("Future voter-to-voter postcards, email, text, and mail-ballot chase can reuse the same contact-history structure.")
    with tab_results:
        st.markdown("### Contact Results")
        st.caption("Future dashboard for contact attempts, support IDs, follow-up needs, and volunteer productivity.")


def render_election_day_workspace():
    st.markdown("## Election Day Operations")
    st.info("Placeholder for poll coverage, workers, drivers, turnout tracking, incident reports, and end-of-night results. Built later using the same Team / Assignment foundation.")

# Full-width branded header fixed across both the sidebar and main workspace.
_cc_logo_uri = img_data_uri(LOGO_CANDIDATE_CONNECT)
_tss_logo_uri = img_data_uri(LOGO_TPTC)
_cc_logo_html = f'<img class="cc-global-logo-center" src="{_cc_logo_uri}" />' if _cc_logo_uri else '<div class="cc-title">Candidate Connect</div>'
_tss_logo_html = f'<img class="cc-global-logo-right" src="{_tss_logo_uri}" />' if _tss_logo_uri else '<div class="cc-powered">Powered by<br><b>The Political Technology Company</b></div>'
st.markdown(f'''<div class="cc-global-header">
  <div class="cc-global-sidebar-fill"></div>
  <div class="cc-global-header-inner">
    <div class="cc-global-redbar"></div>
    <div class="cc-global-brand-row">
      <div class="cc-global-tagline"><span>Target.</span><span>Engage.</span><span>Win.</span></div>
      <div class="cc-global-logo-center-wrap">{_cc_logo_html}</div>
      {_tss_logo_html}
    </div>
  </div>
</div>
''', unsafe_allow_html=True)

# Require login before loading the full data/filter layer.
render_security_gate()

# If an admin reset this password, block all app access until changed.
if _user_must_change_password():
    render_password_change_panel(forced=True)
    st.stop()

try:
    with st.spinner("Loading filters from R2..."):
        manifest, filter_options, geo_hierarchy = load_filter_layer()
except Exception as e:
    st.error("Could not load the filter layer."); st.exception(e); st.stop()

if "filter_reset_token" not in st.session_state: st.session_state["filter_reset_token"] = 0
if "left_section" not in st.session_state: st.session_state["left_section"] = None
_filter_suffix = st.session_state["filter_reset_token"]

with st.sidebar:
    st.caption(f"Signed in: {current_username()} · {current_role()}")
    if is_campaign_scoped():
        st.caption(f"Campaign: {current_user().get('campaign', '')}")
        st.caption(campaign_dataset_status_label())
    elif is_super_admin():
        st.caption("Dataset: Statewide")

    if st.button("🏠 Dashboard", width="stretch"):
        st.session_state["left_section"] = None
        st.session_state["view"] = "dashboard"
        st.rerun()

    st.markdown("### 🧠 Voter Intelligence")
    if user_can("create_universe") and st.button("🎯 Create Universe", width="stretch"):
        st.session_state["left_section"]="create_universe"; st.session_state["view"]="targeting"; st.rerun()
    if user_can("voter_lookup") and st.button("🔎 Voter Lookup", width="stretch"):
        st.session_state["left_section"]="voter_lookup"; st.session_state["view"]="dashboard"; st.rerun()
    if user_can("mail_ballot_center") and st.button("📬 Mail Ballot Center", width="stretch"):
        st.session_state["left_section"]="mail_ballot_center"; st.session_state["view"]="dashboard"; st.rerun()
    if user_can("area_intelligence") and st.button("⌂ Area Intelligence", width="stretch"):
        st.session_state["left_section"]="area_intelligence"; st.session_state["view"]="dashboard"; st.rerun()

    st.markdown("### 👥 Campaign Organization")
    if st.button("👥 Team / Volunteers", width="stretch"):
        st.session_state["left_section"]="campaign_organization"; st.session_state["view"]="organization"; st.rerun()

    st.markdown("### 📣 Voter Outreach")
    if st.button("🚪 Door-to-Door / Contact Programs", width="stretch"):
        st.session_state["left_section"]="voter_outreach"; st.session_state["view"]="outreach"; st.rerun()

    st.markdown("### 🗳️ Election Day")
    if st.button("🗳️ Election Day Operations", width="stretch"):
        st.session_state["left_section"]="election_day"; st.session_state["view"]="election_day"; st.rerun()

    st.markdown("### ⚙️ Administration")
    if st.button("👤 My Account", width="stretch"):
        st.session_state["left_section"]="my_account"; st.session_state["view"]="account"; st.rerun()
    if user_can("account_admin") and st.button("🔐 Account Admin", width="stretch"):
        st.session_state["left_section"]="account_admin"; st.session_state["view"]="security"; st.rerun()
    if st.button("Log Out", width="stretch"):
        for _k in ["auth_user", "auth_username"]:
            st.session_state.pop(_k, None)
        st.rerun()
    st.divider()

    if st.session_state.get("left_section") == "create_universe":
        st.markdown("### Create Universe")
        with st.expander("Geography", expanded=False):
            for field in GEO_FIELDS:
                st.multiselect(DISPLAY_LABELS.get(field, field), options=field_options(filter_options, field, active_filters()), key=filter_key(field))
        with st.expander("Voter Details", expanded=False):
            for field in ["Party", "Gender", "Age_Range", "CalculatedParty", "HH-Party"]:
                opts = field_options(filter_options, field, active_filters())
                if opts: st.multiselect(DISPLAY_LABELS.get(field, field), options=opts, key=filter_key(field))
            st.slider("Newly Registered Within Last N Months",0,24,0,1,key=special_key("new_reg_months"))
        with st.expander("Vote History", expanded=False):
            st.selectbox("Vote History Type", ["All Elections","General Elections","Primary Elections"], key=special_key("vote_score_type"))
            st.slider("Vote History",0,4,(0,4),1,key=special_key("vote_history_score_range"))
            years, etypes, methods = election_options()
            st.multiselect("Election Year", years, key=special_key("election_years"))
            st.multiselect("Election Type", etypes, key=special_key("election_types"))
            st.multiselect("Vote Method", methods, key=special_key("election_methods"))
        with st.expander("Mail Ballot", expanded=False):
            for field in ["MB_App", "MB_App_Status", "MB_Sent", "MB_Status"]:
                st.multiselect(DISPLAY_LABELS.get(field, field), options=field_options(filter_options, field, active_filters()), key=filter_key(field))
            st.slider("Mail Ballot Probability Score",0,4,(0,4),1,key=special_key("mb_prob_score_range"))
        with st.expander("Contact Filters", expanded=False):
            st.selectbox("Mobile / Landline Reach", ["No phone filter","Mobile only","Landline only","Mobile OR landline","Mobile AND landline","No mobile or landline"], key=special_key("phone_reach_mode"))
            for field in ["HasEmail","HasApplicantPhone"]:
                st.multiselect(DISPLAY_LABELS.get(field, field), options=field_options(filter_options, field, active_filters()), key=filter_key(field))
        tag_opts=field_options(filter_options,"Tags",active_filters())
        if tag_opts:
            with st.expander("Tags", expanded=False): st.multiselect("Tags", tag_opts, key=filter_key("Tags"))
        with st.expander("Saved Universes", expanded=False):
            saved=load_persistent_saved_universes(); name=st.text_input("Save current filters as", key=special_key("save_universe_name"))
            if st.button("Save Universe", key=special_key("save_universe_button"), width="stretch"):
                if str(name).strip():
                    saved[str(name).strip()]={"filters":active_filters(),"special":active_special_filters()}; persist_saved_universes(saved); st.success("Saved.")
                else: st.warning("Enter a universe name first.")
            if saved:
                choice=st.selectbox("Load saved universe", [""]+sorted(saved.keys()), key=special_key("load_universe_choice"))
                ca,cb=st.columns(2)
                with ca:
                    if st.button("Load", key=special_key("load_universe_button"), width="stretch") and choice: load_saved_universe_into_widgets(saved.get(choice,{}))
                with cb:
                    if st.button("Delete", key=special_key("delete_universe_button"), width="stretch") and choice:
                        _ = saved.pop(choice,None); persist_saved_universes(saved); st.rerun()
            else: st.caption("No saved universes saved yet.")
    elif st.session_state.get("left_section") == "voter_lookup":
        st.markdown("### Voter Lookup")
        st.caption("Search the full statewide active voter file by name, address, PA ID, phone, or email.")
        st.text_input("Search voters", key=special_key("lookup_query"), placeholder="Name, county, address, PA ID, phone, email")
        st.selectbox("Max Results", [10,25,50,100], index=1, key=special_key("lookup_max"))
        ca, cb = st.columns(2)
        with ca:
            if st.button("Search", key=special_key("lookup_search_btn"), width="stretch"):
                st.rerun()
        with cb:
            if st.button("Clear Lookup", key=special_key("lookup_clear_btn"), width="stretch"):
                for k in [special_key("lookup_query"), "lookup_selected_id"]:
                    _ = st.session_state.pop(k, None)
                st.rerun()
    elif st.session_state.get("left_section") == "mail_ballot_center":
        st.markdown("### Mail Ballot Center")
        _has_universe = has_current_universe()
        _label = st.session_state.get("current_universe_label", "None")
        st.checkbox(
            f"Use current universe: {_label}",
            value=_has_universe,
            disabled=not _has_universe,
            key=special_key("mb_start_current"),
        )
        if _has_universe:
            st.caption(f"Last applied from Create Universe: {st.session_state.get('current_universe_updated', '')}")
        else:
            st.caption("Build a universe in Create Universe, click Save / Apply Current Universe, then return here to use it as your Mail Ballot base.")
    elif st.session_state.get("left_section") == "area_intelligence":
        st.markdown("### Area Intelligence")
        st.caption("Select the area on the right.")
    elif st.session_state.get("left_section") == "my_account":
        st.markdown("### My Account")
        st.caption("Change your password.")
    elif st.session_state.get("left_section") == "campaign_organization":
        st.markdown("### Campaign Organization")
        st.caption("Manage team members, volunteers, roles, and assignment readiness.")
    elif st.session_state.get("left_section") == "voter_outreach":
        st.markdown("### Voter Outreach")
        st.caption("Plan door-to-door, phone, postcard, text, email, and mail-ballot chase programs.")
    elif st.session_state.get("left_section") == "election_day":
        st.markdown("### Election Day")
        st.caption("Poll coverage, worker scheduling, turnout tracking, and incident reporting will live here.")
    elif st.session_state.get("left_section") == "account_admin":
        st.markdown("### Account Admin")
        st.caption("Manage Candidate Connect accounts and campaign scopes.")

active = active_filters()
section = st.session_state.get("left_section")

def render_enhanced_home():
    render_statewide_snapshot()

# Route protection. If a user lands on a page they do not have permission for,
# send them back to the dashboard instead of rendering unauthorized tools.
if section == "voter_lookup" and user_can("voter_lookup"): render_voter_lookup_workspace(); st.stop()
if section == "mail_ballot_center" and user_can("mail_ballot_center"): render_mail_ballot_workspace(); st.stop()
if section == "area_intelligence" and user_can("area_intelligence"): render_area_intelligence_workspace(); st.stop()
if section == "campaign_organization": render_campaign_organization_workspace(); st.stop()
if section == "voter_outreach": render_voter_outreach_workspace(); st.stop()
if section == "election_day": render_election_day_workspace(); st.stop()
if section == "account_admin" and user_can("account_admin"): render_account_admin_workspace(filter_options); st.stop()
if section == "my_account": render_my_account_workspace(); st.stop()
if section != "create_universe" or not user_can("create_universe"): render_enhanced_home(); st.stop()

st.session_state["view"]="targeting"
st.markdown("## Create Universe")
st.markdown("### Current Universe")
special_active = active_special_filters()
if active or special_active:
    chips=[]
    for k,vals in active.items(): chips.append(f"**{DISPLAY_LABELS.get(k,k)}:** {', '.join(map(str, vals[:6]))}{'…' if len(vals)>6 else ''}")
    if "RegistrationMonthsAgo" in special_active: chips.append(f"**Newly Registered:** last {special_active['RegistrationMonthsAgo']['max']} months")
    if "__PhoneReach" in special_active: chips.append(f"**Phone Reach:** {special_active['__PhoneReach']}")
    if "__ElectionFilters" in special_active:
        ef=special_active["__ElectionFilters"]; bits=[]
        if ef.get("years"): bits.append("Years "+", ".join(map(str,ef.get("years",[]))))
        if ef.get("types"): bits.append("Types "+", ".join(map(str,ef.get("types",[]))))
        if ef.get("methods"): bits.append("Methods "+", ".join(map(str,ef.get("methods",[]))))
        chips.append("**Specific Elections:** "+"; ".join(bits))
    for sf in ["V4A","V4G","V4P","MB_Prob_Score"]:
        if sf in special_active: chips.append(f"**{DISPLAY_LABELS.get(sf,sf)}:** {special_active[sf]['min']}–{special_active[sf]['max']}")
    st.markdown(" &nbsp; | &nbsp; ".join(chips), unsafe_allow_html=True)
else: st.info("No filters selected. Choose filters in the left pane.")


a1,a2,sp = st.columns([.85,.85,4.3])
with a1:
    if st.button("Save / Apply Current Universe", width="stretch"):
        with st.spinner("Updating counts..."):
            summary, mode, err = update_counts(active)
        if err:
            st.warning("Counts are unavailable for this filter combination.")
            st.caption(str(err)[:500])
        else:
            st.session_state["quick_summary"] = summary
            st.session_state["count_mode"] = mode
            save_current_universe(active, summary, source="Create Universe")
            st.success(f"Current universe saved: {st.session_state.get('current_universe_label', 'Selected universe')}")
with a2: st.button("Clear Filters", width="stretch", on_click=clear_filter_state)
if st.session_state.get("quick_summary"):
    st.caption("Counts updated. Use the Output Center tabs below for the overview, exports, and reports.")

_ = st.markdown("## Output Center")
_ = render_output_buttons(active)
_ = st.caption(f"Rendered at {datetime.now().isoformat(timespec='seconds')}")

# Absolute final UI safety lock: keep real action/download buttons red with white text,
# while keeping tabs/toggles/read-only labels navy and readable across Chrome/Safari.
pass


# Final Candidate Connect theme remediation based on the UI design template.
# This is intentionally loaded last so it fixes color/layout conflicts without changing app logic.
st.markdown("""
<style>
:root {
  color-scheme: light !important;
  --cc-red: #9f151c;
  --cc-red-dark: #6f0d13;
  --cc-red-hover: #7f1016;
  --cc-blue: #071d3a;
  --cc-blue-soft: #0b2545;
  --cc-green: #246b2f;
  --cc-beige: #efe8d8;
  --cc-beige-2: #f8f4ea;
  --cc-beige-row: #f3eadc;
  --cc-gray: #5f6b7a;
  --cc-border: #9f151c;
}

/* Page, content, and readable text */
html, body, .stApp, [data-testid="stAppViewContainer"] {
  background: var(--cc-beige) !important;
  color: var(--cc-blue) !important;
  font-family: Arial, Helvetica, sans-serif !important;
  font-size: 10pt !important;
}
.main .block-container, [data-testid="stMain"] .block-container, .block-container {
  max-width: 1320px !important;
  margin-left: 0 !important;
  margin-right: auto !important;
  padding-left: 1.5rem !important;
  padding-right: 1.25rem !important;
}
h1, h2, h3, h4, h5, h6, p, label, .stMarkdown, [data-testid="stMarkdownContainer"] {
  color: var(--cc-blue) !important;
}
small, .stCaption, [data-testid="stCaptionContainer"] {
  color: var(--cc-gray) !important;
}

/* Header and sidebar */
.cc-global-header {
  background: var(--cc-beige) !important;
  border-bottom: 2px solid var(--cc-red) !important;
}
.cc-global-redbar {
  background: var(--cc-red) !important;
  border-color: var(--cc-red-dark) !important;
}
.cc-global-tagline,
.cc-global-tagline span {
  font-family: Impact, Haettenschweiler, "Arial Narrow Bold", sans-serif !important;
  color: var(--cc-blue) !important;
}
[data-testid="stSidebar"], section[data-testid="stSidebar"] {
  background: #e6ddcc !important;
  color: var(--cc-blue) !important;
  border-right: 2px solid var(--cc-red) !important;
  min-width: 250px !important;
  width: 250px !important;
  max-width: 250px !important;
}
[data-testid="stSidebar"] *, [data-testid="stSidebar"] p, [data-testid="stSidebar"] label {
  color: var(--cc-blue) !important;
}

/* Action buttons: dark red background, white text. Does not affect tabs. */
.stButton > button:not(:disabled),
div[data-testid="stDownloadButton"] > button:not(:disabled),
button[data-testid="baseButton-primary"]:not(:disabled),
button[data-testid="baseButton-secondary"]:not(:disabled),
button[kind="primary"]:not(:disabled),
button[kind="secondary"]:not(:disabled) {
  background: linear-gradient(180deg, #b01822, var(--cc-red)) !important;
  background-color: var(--cc-red) !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  border: 1px solid var(--cc-red-dark) !important;
  border-radius: 9px !important;
  font-weight: 850 !important;
  text-shadow: none !important;
  box-shadow: none !important;
  min-height: 34px !important;
  padding: 6px 12px !important;
  line-height: 1.15 !important;
}
.stButton > button:not(:disabled) *,
div[data-testid="stDownloadButton"] > button:not(:disabled) *,
button[data-testid="baseButton-primary"]:not(:disabled) *,
button[data-testid="baseButton-secondary"]:not(:disabled) *,
button[kind="primary"]:not(:disabled) *,
button[kind="secondary"]:not(:disabled) * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
}
.stButton > button:not(:disabled):hover,
div[data-testid="stDownloadButton"] > button:not(:disabled):hover,
button[data-testid="baseButton-primary"]:not(:disabled):hover,
button[data-testid="baseButton-secondary"]:not(:disabled):hover {
  background: linear-gradient(180deg, var(--cc-red), var(--cc-red-dark)) !important;
  background-color: var(--cc-red-hover) !important;
}
.stButton > button:disabled,
div[data-testid="stDownloadButton"] > button:disabled,
button[data-testid="baseButton-primary"]:disabled,
button[data-testid="baseButton-secondary"]:disabled,
button[kind="primary"]:disabled,
button[kind="secondary"]:disabled {
  background: #d8cfc0 !important;
  background-color: #d8cfc0 !important;
  color: #111111 !important;
  -webkit-text-fill-color: #111111 !important;
  border: 1px solid #b9ad99 !important;
  opacity: 1 !important;
}
.stButton > button:disabled *,
div[data-testid="stDownloadButton"] > button:disabled * {
  color: #111111 !important;
  -webkit-text-fill-color: #111111 !important;
}

/* Sidebar nav buttons remain compact */
[data-testid="stSidebar"] .stButton > button {
  height: 38px !important;
  min-height: 38px !important;
  max-height: 38px !important;
  width: 100% !important;
  margin: 0 0 4px 0 !important;
  padding: 5px 9px !important;
  border-radius: 8px !important;
}

/* Download buttons can use same red unless disabled; keep readable */
div[data-testid="stDownloadButton"] > button:not(:disabled) {
  background: linear-gradient(180deg, #b01822, var(--cc-red)) !important;
}

/* Tabs must always look like tabs, not buttons */
div[data-testid="stTabs"] button,
div[data-testid="stTabs"] button *,
button[data-baseweb="tab"],
button[data-baseweb="tab"] *,
[role="tab"],
[role="tab"] * {
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
  color: var(--cc-blue) !important;
  -webkit-text-fill-color: var(--cc-blue) !important;
  font-weight: 900 !important;
}
div[data-testid="stTabs"] [data-baseweb="tab-highlight"] {
  background-color: var(--cc-red) !important;
  height: 4px !important;
}

/* Forms and dropdowns */
[data-baseweb="select"] > div,
[data-baseweb="input"] > div,
textarea,
input {
  background: #ffffff !important;
  color: #000000 !important;
  -webkit-text-fill-color: #000000 !important;
  border-color: #111111 !important;
  caret-color: #000000 !important;
}
input::placeholder,
textarea::placeholder,
[data-baseweb="input"] input::placeholder {
  color: #5f6b7a !important;
  -webkit-text-fill-color: #5f6b7a !important;
  opacity: 1 !important;
}
[data-baseweb="select"] input,
[data-baseweb="select"] span,
[data-baseweb="select"] div {
  color: var(--cc-blue) !important;
  -webkit-text-fill-color: var(--cc-blue) !important;
}
[data-baseweb="popover"], [data-baseweb="menu"], [role="listbox"], ul[role="listbox"] {
  background: #ffffff !important;
  color: var(--cc-blue) !important;
}
[role="option"], [role="option"] * {
  background: #ffffff !important;
  color: var(--cc-blue) !important;
  -webkit-text-fill-color: var(--cc-blue) !important;
}
[role="option"]:hover,
[role="option"][aria-selected="true"] {
  background: #f1e7d6 !important;
  color: var(--cc-blue) !important;
}
[data-baseweb="tag"] {
  background: #e5e0d8 !important;
  color: var(--cc-blue) !important;
  border: 1px solid #b9ad99 !important;
}
[data-baseweb="tag"] * {
  color: var(--cc-blue) !important;
  -webkit-text-fill-color: var(--cc-blue) !important;
}
[data-testid="stCheckbox"] label,
[data-testid="stCheckbox"] label *,
[data-testid="stRadio"] label,
[data-testid="stRadio"] label *,
[data-testid="stToggle"] label,
[data-testid="stToggle"] label * {
  color: var(--cc-blue) !important;
  -webkit-text-fill-color: var(--cc-blue) !important;
}
input[type="checkbox"], input[type="radio"] {
  accent-color: var(--cc-blue) !important;
}

/* Password eye icon and icon wells */
button[aria-label*="password"],
button[title*="password"],
[data-testid="stTextInputRootElement"] button,
[data-baseweb="input"] button {
  background: #ffffff !important;
  color: var(--cc-blue) !important;
  -webkit-text-fill-color: var(--cc-blue) !important;
  border: 0 !important;
}
button[aria-label*="password"] *,
button[title*="password"] *,
[data-testid="stTextInputRootElement"] button *,
[data-baseweb="input"] button * {
  color: var(--cc-blue) !important;
  fill: var(--cc-blue) !important;
  -webkit-text-fill-color: var(--cc-blue) !important;
}

/* Expander headers: beige/light, not black */
details,
div[data-testid="stExpander"],
div[data-testid="stExpander"] > details {
  background: var(--cc-beige-2) !important;
  color: var(--cc-blue) !important;
  border: 1px solid rgba(159,21,28,.35) !important;
  border-radius: 10px !important;
}
details > summary,
details > summary *,
div[data-testid="stExpander"] summary,
div[data-testid="stExpander"] summary * {
  background: var(--cc-beige-2) !important;
  color: var(--cc-blue) !important;
  -webkit-text-fill-color: var(--cc-blue) !important;
  font-weight: 850 !important;
}
details[open] > summary,
div[data-testid="stExpander"] details[open] > summary {
  border-bottom: 1px solid rgba(159,21,28,.25) !important;
}

/* Cards, alerts, info bubbles */
.cc-card, .cc-home-card, .cc-metric, .cc-icon-metric,
div[data-testid="stForm"] {
  background: var(--cc-beige-2) !important;
  color: var(--cc-blue) !important;
  border: 1px solid #b9ad99 !important;
  border-radius: 12px !important;
}
.cc-note, .cc-verify, .cc-empty-table, .stAlert {
  background: #d9e8f8 !important;
  color: var(--cc-blue) !important;
  border: 1px solid #8aa3bf !important;
}
.cc-note *, .cc-verify *, .cc-empty-table *, .stAlert * {
  color: var(--cc-blue) !important;
}

/* Donut charts: restore the actual colored segments */
.cc-donut {
  width: 150px !important;
  height: 150px !important;
  border-radius: 50% !important;
  position: relative !important;
  flex: 0 0 auto !important;
  background: conic-gradient(
    #d51f2a 0 calc(var(--r) * 1%),
    #2454d6 calc(var(--r) * 1%) calc((var(--r) + var(--d)) * 1%),
    #4c9a2a calc((var(--r) + var(--d)) * 1%) 100%
  ) !important;
  box-shadow: 0 12px 22px rgba(7,29,58,.18) !important;
}
.cc-donut:after {
  content: "" !important;
  position: absolute !important;
  inset: 40px !important;
  border-radius: 50% !important;
  background: var(--cc-blue) !important;
}
.cc-donut-center,
.cc-donut-center *,
.cc-donut-center div,
.cc-donut-center span {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  fill: #ffffff !important;
  opacity: 1 !important;
  text-shadow: 0 1px 2px rgba(0,0,0,.65) !important;
  position: relative !important;
  z-index: 2 !important;
}
.cc-swatch[style*="#d51f2a"] { background: #d51f2a !important; }
.cc-swatch[style*="#2454d6"] { background: #2454d6 !important; }
.cc-swatch[style*="#4c9a2a"] { background: #4c9a2a !important; }

/* Age bars/charts */
.cc-age-bar-bg {
  background: var(--cc-blue) !important;
}
.cc-age-bar {
  background: linear-gradient(90deg, #8b0d13, #ef4444) !important;
}
.cc-age-row, .cc-age-row *, .cc-legend-row, .cc-legend-row * {
  color: var(--cc-blue) !important;
}

/* Tables */
.cc-table-wrap, .cc-scroll-table, [data-testid="stDataFrame"], [data-testid="stTable"] {
  background: #ffffff !important;
  border: 1px solid var(--cc-red) !important;
  border-radius: 10px !important;
}
.cc-html-table th, .cc-home-table th,
[data-testid="stDataFrame"] [role="columnheader"],
[data-testid="stTable"] th {
  background: var(--cc-red) !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  text-align: center !important;
  font-weight: 900 !important;
}
.cc-html-table td, .cc-home-table td,
[data-testid="stDataFrame"] [role="gridcell"],
[data-testid="stTable"] td {
  background: #ffffff !important;
  color: var(--cc-blue) !important;
  -webkit-text-fill-color: var(--cc-blue) !important;
  text-align: center !important;
}
.cc-html-table tbody tr:nth-child(even) td,
.cc-home-table tbody tr:nth-child(even) td,
[data-testid="stTable"] tr:nth-child(even) td {
  background: var(--cc-beige-row) !important;
}

/* Election history tables keep dark style but readable */
.cc-history-table {
  background: #111827 !important;
  color: #ffffff !important;
}
.cc-history-table th,
.cc-history-table td {
  background: #111827 !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  border-color: #374151 !important;
}
.cc-history-table th {
  background: #1f2937 !important;
}

/* File uploader */
[data-testid="stFileUploader"],
[data-testid="stFileUploader"] section {
  background: #fbf7ee !important;
  color: var(--cc-blue) !important;
  border-color: rgba(159,21,28,.35) !important;
}
[data-testid="stFileUploader"] *,
[data-testid="stFileUploader"] label,
[data-testid="stFileUploader"] span,
[data-testid="stFileUploader"] p,
[data-testid="stFileUploader"] small {
  color: var(--cc-blue) !important;
  -webkit-text-fill-color: var(--cc-blue) !important;
}
[data-testid="stFileUploader"] button,
[data-testid="stFileUploader"] button * {
  background: var(--cc-red) !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
}

/* Code blocks: keep dark, but readable */
pre, code, [data-testid="stCodeBlock"] {
  background: #111827 !important;
  color: #f8fafc !important;
}
pre *, code *, [data-testid="stCodeBlock"] * {
  color: #f8fafc !important;
}

/* Login/setup card center and readable controls */
.cc-login-title {
  color: var(--cc-blue) !important;
}
.cc-login-subtitle {
  color: var(--cc-gray) !important;
}
div[data-testid="stForm"] {
  background: var(--cc-beige-2) !important;
}

/* Prevent old broad CSS from hiding useful controls */
header[data-testid="stHeader"] {
  visibility: visible !important;
  height: auto !important;
  background: transparent !important;
}
#MainMenu, footer, [data-testid="stToolbar"], [data-testid="stDecoration"], [data-testid="stStatusWidget"] {
  visibility: hidden !important;
  height: 0 !important;
}

/* Responsive */
@media (max-width: 900px) {
  [data-testid="stSidebar"], section[data-testid="stSidebar"] {
    min-width: 230px !important;
    width: 230px !important;
    max-width: 230px !important;
  }
  .cc-donut {
    width: 135px !important;
    height: 135px !important;
  }
  .cc-donut:after {
    inset: 36px !important;
  }
}
</style>
""", unsafe_allow_html=True)

# Final production CSS lock: fixes login readability and removes broad dark-on-dark conflicts.
st.markdown("""
<style>
:root{--cc-red:#9f151c;--cc-red-dark:#6f0d13;--cc-blue:#071d3a;--cc-beige:#efe8d8;--cc-card:#f8f4ea;--cc-row:#f3eadc;--cc-gray:#5f6b7a;color-scheme:light!important;}
html,body,.stApp,[data-testid="stAppViewContainer"]{background:var(--cc-beige)!important;color:var(--cc-blue)!important;}
.block-container{max-width:1320px!important;margin-left:0!important;margin-right:auto!important;padding-left:1.5rem!important;padding-right:1.25rem!important;}
h1,h2,h3,h4,h5,h6,p,label,.stMarkdown,[data-testid="stMarkdownContainer"]{color:var(--cc-blue)!important;}
small,.stCaption,[data-testid="stCaptionContainer"]{color:var(--cc-gray)!important;}
[data-testid="stSidebar"]{background:#e6ddcc!important;border-right:2px solid var(--cc-red)!important;min-width:250px!important;width:250px!important;max-width:250px!important;}
[data-testid="stSidebar"] *{color:var(--cc-blue)!important;}
.stButton>button:not(:disabled),div[data-testid="stDownloadButton"]>button:not(:disabled),button[data-testid="baseButton-primary"]:not(:disabled),button[data-testid="baseButton-secondary"]:not(:disabled){background:linear-gradient(180deg,#b01822,var(--cc-red))!important;background-color:var(--cc-red)!important;color:#fff!important;-webkit-text-fill-color:#fff!important;border:1px solid var(--cc-red-dark)!important;border-radius:9px!important;font-weight:850!important;text-shadow:none!important;box-shadow:none!important;min-height:34px!important;padding:6px 12px!important;line-height:1.15!important;}
.stButton>button:not(:disabled) *,.stButton>button:not(:disabled) p,div[data-testid="stDownloadButton"]>button:not(:disabled) *,button[data-testid="baseButton-primary"]:not(:disabled) *,button[data-testid="baseButton-secondary"]:not(:disabled) *{color:#fff!important;-webkit-text-fill-color:#fff!important;}
[data-testid="stSidebar"] .stButton>button{height:38px!important;min-height:38px!important;max-height:38px!important;width:100%!important;margin:0 0 4px 0!important;padding:5px 9px!important;border-radius:8px!important;}
.stButton>button:disabled,div[data-testid="stDownloadButton"]>button:disabled{background:#d8cfc0!important;color:#111!important;-webkit-text-fill-color:#111!important;border:1px solid #b9ad99!important;opacity:1!important;}
div[data-testid="stTabs"] button,div[data-testid="stTabs"] button *,[role="tab"],[role="tab"] *{background:transparent!important;border:none!important;box-shadow:none!important;color:var(--cc-blue)!important;-webkit-text-fill-color:var(--cc-blue)!important;font-weight:900!important;}
div[data-testid="stTabs"] [data-baseweb="tab-highlight"]{background-color:var(--cc-red)!important;height:4px!important;}
[data-baseweb="select"]>div,[data-baseweb="input"]>div,textarea,input{background:#fff!important;color:#000!important;-webkit-text-fill-color:#000!important;border-color:#111!important;caret-color:#000!important;}
input::placeholder,textarea::placeholder{color:#5f6b7a!important;-webkit-text-fill-color:#5f6b7a!important;opacity:1!important;}
[data-baseweb="select"] input,[data-baseweb="select"] span,[data-baseweb="select"] div{color:var(--cc-blue)!important;-webkit-text-fill-color:var(--cc-blue)!important;}
button[aria-label*="password"],button[title*="password"],[data-testid="stTextInputRootElement"] button,[data-baseweb="input"] button{background:#fff!important;color:var(--cc-blue)!important;-webkit-text-fill-color:var(--cc-blue)!important;border:0!important;}
button[aria-label*="password"] *,button[title*="password"] *,[data-testid="stTextInputRootElement"] button *{color:var(--cc-blue)!important;fill:var(--cc-blue)!important;-webkit-text-fill-color:var(--cc-blue)!important;}
details,div[data-testid="stExpander"],div[data-testid="stExpander"]>details{background:var(--cc-card)!important;color:var(--cc-blue)!important;border:1px solid rgba(159,21,28,.35)!important;border-radius:10px!important;}
details>summary,details>summary *,div[data-testid="stExpander"] summary,div[data-testid="stExpander"] summary *{background:var(--cc-card)!important;color:var(--cc-blue)!important;-webkit-text-fill-color:var(--cc-blue)!important;font-weight:850!important;}
.cc-card,.cc-home-card,.cc-metric,.cc-icon-metric,div[data-testid="stForm"]{background:var(--cc-card)!important;color:var(--cc-blue)!important;border:1px solid #b9ad99!important;border-radius:12px!important;}
.cc-note,.cc-verify,.cc-empty-table,.stAlert{background:#d9e8f8!important;color:var(--cc-blue)!important;border:1px solid #8aa3bf!important;border-radius:10px!important;padding:14px 16px!important;}
.cc-note *,.cc-verify *,.cc-empty-table *,.stAlert *{color:var(--cc-blue)!important;}
[data-testid="stFileUploader"],[data-testid="stFileUploader"] section{background:#fbf7ee!important;color:var(--cc-blue)!important;border-color:rgba(159,21,28,.35)!important;}
[data-testid="stFileUploader"] *{color:var(--cc-blue)!important;-webkit-text-fill-color:var(--cc-blue)!important;}
[data-testid="stFileUploader"] button,[data-testid="stFileUploader"] button *{background:var(--cc-red)!important;color:#fff!important;-webkit-text-fill-color:#fff!important;}
pre,code,[data-testid="stCodeBlock"]{background:#111827!important;color:#f8fafc!important;}
pre *,code *,[data-testid="stCodeBlock"] *{color:#f8fafc!important;}
header[data-testid="stHeader"]{visibility:visible!important;height:auto!important;background:transparent!important;}
#MainMenu,footer,[data-testid="stToolbar"],[data-testid="stDecoration"],[data-testid="stStatusWidget"]{visibility:hidden!important;height:0!important;}
</style>
""", unsafe_allow_html=True)


# Final UI polish patch: login card, readable buttons/icons, compact party/gender bars.
st.markdown("""
<style>
/* Login/setup professional card layout */
div[data-testid="stForm"] {
    background: #f8f4ea !important;
    border: 1px solid #b9ad99 !important;
    border-radius: 16px !important;
    box-shadow: 0 12px 28px rgba(7,29,58,.12) !important;
    padding: 22px 26px !important;
}

/* Login/setup page content should be centered, not left-floating */
.cc-login-wrap,
.cc-login-card,
.cc-auth-card,
.cc-setup-card {
    max-width: 460px !important;
    margin: 0 auto !important;
}

/* Streamlit login form fallback: center the first form on auth pages */
[data-testid="stVerticalBlock"]:has(div[data-testid="stForm"]) {
    max-width: 520px !important;
}

/* Active buttons: always readable */
.stButton > button:not(:disabled),
div[data-testid="stDownloadButton"] > button:not(:disabled),
button[data-testid="baseButton-primary"]:not(:disabled),
button[data-testid="baseButton-secondary"]:not(:disabled),
button[kind="primary"]:not(:disabled),
button[kind="secondary"]:not(:disabled) {
    background: linear-gradient(180deg, #b01822, #9f151c) !important;
    background-color: #9f151c !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    border: 1px solid #6f0d13 !important;
    font-weight: 900 !important;
    opacity: 1 !important;
}
.stButton > button:not(:disabled) *,
div[data-testid="stDownloadButton"] > button:not(:disabled) *,
button[data-testid="baseButton-primary"]:not(:disabled) *,
button[data-testid="baseButton-secondary"]:not(:disabled) *,
button[kind="primary"]:not(:disabled) *,
button[kind="secondary"]:not(:disabled) * {
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    opacity: 1 !important;
}

/* Login button was dark navy with dark text; force login forms to red/white too */
div[data-testid="stForm"] .stButton > button:not(:disabled) {
    background: linear-gradient(180deg, #b01822, #9f151c) !important;
    background-color: #9f151c !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    min-width: 88px !important;
}
div[data-testid="stForm"] .stButton > button:not(:disabled) * {
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
}

/* Disabled buttons: readable but clearly disabled */
.stButton > button:disabled,
div[data-testid="stDownloadButton"] > button:disabled {
    background: #d8cfc0 !important;
    color: #222222 !important;
    -webkit-text-fill-color: #222222 !important;
    border: 1px solid #b9ad99 !important;
    opacity: .75 !important;
}
.stButton > button:disabled *,
div[data-testid="stDownloadButton"] > button:disabled * {
    color: #222222 !important;
    -webkit-text-fill-color: #222222 !important;
}

/* Password show/hide icon: readable navy on white */
button[aria-label*="password"],
button[title*="password"],
[data-testid="stTextInputRootElement"] button,
[data-baseweb="input"] button {
    background: #ffffff !important;
    color: #071d3a !important;
    -webkit-text-fill-color: #071d3a !important;
    border-left: 1px solid #d0c7b7 !important;
    opacity: 1 !important;
}
button[aria-label*="password"] svg,
button[title*="password"] svg,
[data-testid="stTextInputRootElement"] button svg,
[data-baseweb="input"] button svg {
    fill: #071d3a !important;
    color: #071d3a !important;
    opacity: 1 !important;
}

/* Compact party/gender bar charts. Each item stays on one row. */
.cc-party-bars {
    display: flex !important;
    flex-direction: column !important;
    gap: 8px !important;
    margin-top: 8px !important;
    width: 100% !important;
}
.cc-party-bar-row {
    display: grid !important;
    grid-template-columns: 150px minmax(180px, 1fr) 140px !important;
    align-items: center !important;
    gap: 10px !important;
    min-height: 24px !important;
    margin: 0 !important;
}
.cc-party-bar-label {
    display: flex !important;
    align-items: center !important;
    gap: 7px !important;
    font-size: 10pt !important;
    font-weight: 900 !important;
    line-height: 1.1 !important;
    color: #071d3a !important;
    white-space: nowrap !important;
}
.cc-party-bar-track {
    height: 12px !important;
    border-radius: 999px !important;
    background: #071d3a !important;
    overflow: hidden !important;
    min-width: 120px !important;
}
.cc-party-bar-fill {
    height: 100% !important;
    border-radius: 999px !important;
}
.cc-party-bar-value {
    font-size: 10pt !important;
    font-weight: 900 !important;
    line-height: 1.1 !important;
    color: #071d3a !important;
    white-space: nowrap !important;
    text-align: left !important;
}
.cc-swatch {
    width: 11px !important;
    height: 11px !important;
    min-width: 11px !important;
    border-radius: 50% !important;
    display: inline-block !important;
}

/* Give chart cards enough room, but not giant vertical waste */
.cc-chart-card,
.cc-card {
    overflow: visible !important;
}
.cc-chart-card .cc-party-bars,
.cc-card .cc-party-bars {
    padding-bottom: 4px !important;
}

/* Keep table colors light/zebra even with sortable dataframe fallback */
[data-testid="stDataFrame"] {
    background: #ffffff !important;
    border: 1px solid #9f151c !important;
    border-radius: 10px !important;
}
[data-testid="stDataFrame"] * {
    color: #071d3a !important;
}
[data-testid="stDataFrame"] [role="columnheader"],
[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] {
    background: #9f151c !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    font-weight: 900 !important;
}
</style>
""", unsafe_allow_html=True)


# Final focused patch v8: auth layout, readable controls, compact group bars.
st.markdown("""
<style>
div[data-testid="stForm"] {
    background: #f8f4ea !important;
    border: 1px solid #b9ad99 !important;
    border-radius: 16px !important;
    box-shadow: 0 12px 28px rgba(7,29,58,.12) !important;
    padding: 22px 26px !important;
    max-width: 460px !important;
    margin-left: auto !important;
    margin-right: auto !important;
}
.cc-login-title { text-align:center !important; color:#071d3a !important; font-size:18pt !important; font-weight:950 !important; }
.cc-login-subtitle { text-align:center !important; color:#5f6b7a !important; font-size:10pt !important; }
div[data-testid="stForm"] label, div[data-testid="stForm"] label *, div[data-testid="stForm"] p, div[data-testid="stForm"] span {
    color:#071d3a !important; -webkit-text-fill-color:#071d3a !important; opacity:1 !important;
}
input, textarea, [data-baseweb="input"] input {
    background:#ffffff !important; color:#000000 !important; -webkit-text-fill-color:#000000 !important; caret-color:#000000 !important;
}
.stButton > button:not(:disabled), div[data-testid="stDownloadButton"] > button:not(:disabled),
button[data-testid="baseButton-primary"]:not(:disabled), button[data-testid="baseButton-secondary"]:not(:disabled),
button[kind="primary"]:not(:disabled), button[kind="secondary"]:not(:disabled) {
    background:linear-gradient(180deg,#b01822,#9f151c) !important; background-color:#9f151c !important;
    color:#ffffff !important; -webkit-text-fill-color:#ffffff !important; border:1px solid #6f0d13 !important;
    font-weight:900 !important; opacity:1 !important;
}
.stButton > button:not(:disabled) *, div[data-testid="stDownloadButton"] > button:not(:disabled) *,
button[data-testid="baseButton-primary"]:not(:disabled) *, button[data-testid="baseButton-secondary"]:not(:disabled) * {
    color:#ffffff !important; -webkit-text-fill-color:#ffffff !important; opacity:1 !important;
}
.stButton > button:disabled, div[data-testid="stDownloadButton"] > button:disabled {
    background:#d8cfc0 !important; color:#222222 !important; -webkit-text-fill-color:#222222 !important;
    border:1px solid #b9ad99 !important; opacity:.75 !important;
}
button[aria-label*="password"], button[title*="password"], [data-testid="stTextInputRootElement"] button, [data-baseweb="input"] button {
    background:#ffffff !important; color:#071d3a !important; -webkit-text-fill-color:#071d3a !important;
    border-left:1px solid #d0c7b7 !important; opacity:1 !important;
}
button[aria-label*="password"] svg, button[title*="password"] svg, [data-testid="stTextInputRootElement"] button svg, [data-baseweb="input"] button svg {
    color:#071d3a !important; fill:#071d3a !important; opacity:1 !important;
}
.cc-group-bar-card { overflow:visible !important; padding-bottom:14px !important; }
.cc-total-line { color:#071d3a !important; font-weight:950 !important; font-size:11pt !important; margin:2px 0 8px 0 !important; }
.cc-total-line span { color:#5f6b7a !important; font-size:9pt !important; margin-left:3px !important; }
.cc-one-line-bars { display:flex !important; flex-direction:column !important; gap:8px !important; width:100% !important; padding-bottom:8px !important; }
.cc-one-line-bar-row {
    display:grid !important; grid-template-columns:150px minmax(170px,1fr) 140px !important;
    gap:10px !important; align-items:center !important; min-height:22px !important;
}
.cc-one-line-bar-label {
    display:flex !important; align-items:center !important; gap:7px !important; color:#071d3a !important;
    font-weight:900 !important; font-size:10pt !important; line-height:1.1 !important; white-space:nowrap !important;
}
.cc-one-line-bar-track { height:12px !important; border-radius:999px !important; background:#071d3a !important; overflow:hidden !important; }
.cc-one-line-bar-fill { height:100% !important; border-radius:999px !important; }
.cc-one-line-bar-value { color:#071d3a !important; font-weight:900 !important; font-size:10pt !important; line-height:1.1 !important; white-space:nowrap !important; }
.cc-swatch { width:11px !important; height:11px !important; min-width:11px !important; border-radius:50% !important; display:inline-block !important; }
</style>
""", unsafe_allow_html=True)


# Final source-level UI fixes v9.
st.markdown("""
<style>
/* Login card: true centered professional card */
div[data-testid="stForm"] {
  background: #f8f4ea !important;
  border: 1px solid #b9ad99 !important;
  border-radius: 16px !important;
  box-shadow: 0 12px 28px rgba(7,29,58,.12) !important;
  padding: 22px 26px !important;
}

/* ALL buttons, including form submit and BaseWeb buttons */
button:not(:disabled),
.stButton > button:not(:disabled),
.stFormSubmitButton > button:not(:disabled),
div[data-testid="stFormSubmitButton"] button:not(:disabled),
div[data-testid="stDownloadButton"] > button:not(:disabled),
button[data-testid*="baseButton"]:not(:disabled) {
  background: linear-gradient(180deg,#b01822,#9f151c) !important;
  background-color: #9f151c !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  border: 1px solid #6f0d13 !important;
  font-weight: 900 !important;
  opacity: 1 !important;
  text-shadow: none !important;
}
button:not(:disabled) *,
.stButton > button:not(:disabled) *,
.stFormSubmitButton > button:not(:disabled) *,
div[data-testid="stFormSubmitButton"] button:not(:disabled) *,
div[data-testid="stDownloadButton"] > button:not(:disabled) *,
button[data-testid*="baseButton"]:not(:disabled) * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  fill: #ffffff !important;
  opacity: 1 !important;
}

/* Do NOT turn password eye into red button */
button[aria-label*="password"],
button[title*="password"],
[data-testid="stTextInputRootElement"] button,
[data-baseweb="input"] button {
  background: #ffffff !important;
  background-color: #ffffff !important;
  color: #071d3a !important;
  -webkit-text-fill-color: #071d3a !important;
  border: 0 !important;
  border-left: 1px solid #d0c7b7 !important;
  opacity: 1 !important;
}
button[aria-label*="password"] *,
button[title*="password"] *,
[data-testid="stTextInputRootElement"] button *,
[data-baseweb="input"] button * {
  color: #071d3a !important;
  -webkit-text-fill-color: #071d3a !important;
  fill: #071d3a !important;
  opacity: 1 !important;
}

/* Disabled buttons readable */
button:disabled,
.stButton > button:disabled,
.stFormSubmitButton > button:disabled,
div[data-testid="stFormSubmitButton"] button:disabled,
div[data-testid="stDownloadButton"] > button:disabled {
  background: #d8cfc0 !important;
  background-color: #d8cfc0 !important;
  color: #222222 !important;
  -webkit-text-fill-color: #222222 !important;
  border: 1px solid #b9ad99 !important;
  opacity: .75 !important;
}
button:disabled *,
.stButton > button:disabled *,
.stFormSubmitButton > button:disabled *,
div[data-testid="stFormSubmitButton"] button:disabled *,
div[data-testid="stDownloadButton"] > button:disabled * {
  color: #222222 !important;
  -webkit-text-fill-color: #222222 !important;
}

/* Compact, non-clipped party/gender bars */
.cc-group-bar-card {
  min-height: 230px !important;
  height: auto !important;
  overflow: visible !important;
  padding: 18px 22px 22px 22px !important;
}
.cc-one-line-bars {
  display: flex !important;
  flex-direction: column !important;
  gap: 10px !important;
  width: 100% !important;
  padding-bottom: 14px !important;
}
.cc-one-line-bar-row {
  display: grid !important;
  grid-template-columns: 150px minmax(180px, 1fr) 145px !important;
  gap: 10px !important;
  align-items: center !important;
  min-height: 24px !important;
}
.cc-one-line-bar-label {
  display: flex !important;
  align-items: center !important;
  gap: 8px !important;
  color: #071d3a !important;
  font-weight: 900 !important;
  font-size: 10pt !important;
  white-space: nowrap !important;
}
.cc-one-line-bar-track {
  height: 12px !important;
  border-radius: 999px !important;
  background: #071d3a !important;
  overflow: hidden !important;
}
.cc-one-line-bar-fill { height: 100% !important; border-radius: 999px !important; }
.cc-one-line-bar-value {
  color: #071d3a !important;
  font-weight: 900 !important;
  font-size: 10pt !important;
  white-space: nowrap !important;
}
.cc-total-line {
  color: #071d3a !important;
  font-weight: 950 !important;
  font-size: 11pt !important;
  margin: 2px 0 12px 0 !important;
}
.cc-total-line span {
  color: #5f6b7a !important;
  font-size: 9pt !important;
}
.cc-swatch {
  width: 11px !important;
  height: 11px !important;
  min-width: 11px !important;
  border-radius: 50% !important;
  display: inline-block !important;
}

/* Account admin: no raw black JSON-looking blocks unless real code requested */
.cc-boundary-summary {
  background: #d9e8f8 !important;
  color: #071d3a !important;
  border: 1px solid #8aa3bf !important;
  border-radius: 10px !important;
  padding: 10px 12px !important;
  margin: 8px 0 12px 0 !important;
}
</style>
""", unsafe_allow_html=True)


# Final polish v10: buttons, password icon, unclipped charts.
st.markdown("""
<style>
/* NEVER dark text on dark buttons, including account admin/save form buttons */
.stButton button:not(:disabled),
.stFormSubmitButton button:not(:disabled),
div[data-testid="stFormSubmitButton"] button:not(:disabled),
div[data-testid="stDownloadButton"] button:not(:disabled),
button[data-testid*="baseButton"]:not(:disabled) {
  background: linear-gradient(180deg,#b01822,#9f151c) !important;
  background-color: #9f151c !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  border: 1px solid #6f0d13 !important;
  font-weight: 900 !important;
  opacity: 1 !important;
  text-shadow: none !important;
}
.stButton button:not(:disabled) *,
.stFormSubmitButton button:not(:disabled) *,
div[data-testid="stFormSubmitButton"] button:not(:disabled) *,
div[data-testid="stDownloadButton"] button:not(:disabled) *,
button[data-testid*="baseButton"]:not(:disabled) * {
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  fill: #ffffff !important;
  opacity: 1 !important;
}

/* Disabled buttons remain readable */
.stButton button:disabled,
.stFormSubmitButton button:disabled,
div[data-testid="stFormSubmitButton"] button:disabled,
div[data-testid="stDownloadButton"] button:disabled {
  background: #d8cfc0 !important;
  color: #222222 !important;
  -webkit-text-fill-color: #222222 !important;
  border: 1px solid #b9ad99 !important;
  opacity: .8 !important;
}

/* Password icon stays readable and is NOT styled like a red button */
button[aria-label*="password"],
button[title*="password"],
[data-testid="stTextInputRootElement"] button,
[data-baseweb="input"] button {
  background: #ffffff !important;
  background-color: #ffffff !important;
  color: #071d3a !important;
  -webkit-text-fill-color: #071d3a !important;
  border: 0 !important;
  border-left: 1px solid #d0c7b7 !important;
  opacity: 1 !important;
}
button[aria-label*="password"] *,
button[title*="password"] *,
[data-testid="stTextInputRootElement"] button *,
[data-baseweb="input"] button * {
  color: #071d3a !important;
  -webkit-text-fill-color: #071d3a !important;
  fill: #071d3a !important;
  opacity: 1 !important;
}

/* Party/Gender chart cards: stop Streamlit/parent containers from clipping */
.cc-group-bar-card,
.cc-home-card:has(.cc-one-line-bars),
.cc-card:has(.cc-one-line-bars) {
  height: auto !important;
  min-height: 285px !important;
  max-height: none !important;
  overflow: visible !important;
  padding: 18px 22px 28px 22px !important;
  margin-bottom: 16px !important;
}
.cc-one-line-bars {
  display: flex !important;
  flex-direction: column !important;
  gap: 6px !important;
  width: 100% !important;
  overflow: visible !important;
  padding-bottom: 18px !important;
}
.cc-one-line-bar-row {
  display: grid !important;
  grid-template-columns: 145px minmax(160px, 1fr) 132px !important;
  gap: 8px !important;
  align-items: center !important;
  min-height: 20px !important;
  margin-bottom: 2px !important;
}
.cc-one-line-bar-label {
  display: flex !important;
  align-items: center !important;
  gap: 7px !important;
  color: #071d3a !important;
  font-weight: 900 !important;
  font-size: 9.5pt !important;
  line-height: 1.05 !important;
  white-space: nowrap !important;
}
.cc-one-line-bar-track {
  height: 10px !important;
  border-radius: 999px !important;
  background: #071d3a !important;
  overflow: hidden !important;
}
.cc-one-line-bar-fill {
  height: 100% !important;
  border-radius: 999px !important;
}
.cc-one-line-bar-value {
  color: #071d3a !important;
  font-weight: 900 !important;
  font-size: 9.5pt !important;
  line-height: 1.05 !important;
  white-space: nowrap !important;
}
.cc-total-line {
  color: #071d3a !important;
  font-weight: 950 !important;
  font-size: 10.5pt !important;
  margin: 2px 0 8px 0 !important;
}
.cc-total-line span {
  color: #5f6b7a !important;
  font-size: 8.5pt !important;
}
.cc-swatch {
  width: 10px !important;
  height: 10px !important;
  min-width: 10px !important;
  border-radius: 50% !important;
  display: inline-block !important;
}

/* Login form card */
div[data-testid="stForm"] {
  background: #f8f4ea !important;
  border: 1px solid #b9ad99 !important;
  border-radius: 16px !important;
  box-shadow: 0 12px 28px rgba(7,29,58,.12) !important;
}
</style>
""", unsafe_allow_html=True)


# Final chart iframe polish v12.
st.markdown("""
<style>
iframe[title="streamlit.components.v1.html"] {
  width: 100% !important;
  border: 0 !important;
  background: transparent !important;
  margin-bottom: 12px !important;
}
</style>
""", unsafe_allow_html=True)


# Native Party/Gender chart rows: no iframe, no clipping.
st.markdown("""
<style>
.cc-native-total {
    color: #071d3a !important;
    font-weight: 950 !important;
    font-size: 15px !important;
    margin: 0 0 10px 0 !important;
}
.cc-native-total span {
    color: #5f6b7a !important;
    font-size: 12px !important;
    font-weight: 800 !important;
}
.cc-native-bar-row {
    display: grid !important;
    grid-template-columns: 155px minmax(160px, 1fr) 140px !important;
    gap: 10px !important;
    align-items: center !important;
    margin: 8px 0 10px 0 !important;
    min-height: 22px !important;
}
.cc-native-bar-label {
    display: flex !important;
    align-items: center !important;
    gap: 7px !important;
    color: #071d3a !important;
    font-weight: 900 !important;
    font-size: 13px !important;
    white-space: nowrap !important;
}
.cc-native-dot {
    width: 11px !important;
    height: 11px !important;
    min-width: 11px !important;
    border-radius: 50% !important;
    display: inline-block !important;
}
.cc-native-bar-track {
    height: 11px !important;
    border-radius: 999px !important;
    background: #071d3a !important;
    overflow: hidden !important;
}
.cc-native-bar-fill {
    height: 100% !important;
    border-radius: 999px !important;
}
.cc-native-bar-value {
    color: #071d3a !important;
    font-weight: 900 !important;
    font-size: 13px !important;
    white-space: nowrap !important;
}
@media (max-width: 850px) {
    .cc-native-bar-row {
        grid-template-columns: 1fr !important;
        gap: 4px !important;
    }
}
</style>
""", unsafe_allow_html=True)


# Readable tooltip/help popovers only. No theme/color redesign.
st.markdown("""
<style>
div[data-testid="stTooltipContent"],
div[role="tooltip"],
[data-baseweb="popover"] {
    background: #ffffff !important;
    color: #071d3a !important;
    -webkit-text-fill-color: #071d3a !important;
    border: 1px solid #b9ad99 !important;
    box-shadow: 0 8px 24px rgba(7,29,58,.18) !important;
}
div[data-testid="stTooltipContent"] *,
div[role="tooltip"] *,
[data-baseweb="popover"] * {
    color: #071d3a !important;
    -webkit-text-fill-color: #071d3a !important;
    opacity: 1 !important;
}
</style>
""", unsafe_allow_html=True)



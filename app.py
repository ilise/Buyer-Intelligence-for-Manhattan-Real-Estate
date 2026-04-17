"""
app.py — Manhattan Development Tracker
Modern search-first UI with inline filter chips and card grid.

Run with:
    streamlit run app.py
"""

import sqlite3
import os
import base64
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from datetime import datetime
from manhattan_dev_tracker import get_projects

# ---------------------------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Manhattan Dev Tracker",
    page_icon="🏗️",
    layout="wide",
)

# ---------------------------------------------------------------------------
# OUTREACH DATABASE
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "outreach.db")

OUTREACH_STATUSES = [
    "New",
    "Contacted",
    "In Conversation",
    "On Viewing List",
    "Not Interested",
]

def init_outreach_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS outreach (
            address    TEXT PRIMARY KEY,
            status     TEXT DEFAULT 'New',
            email      TEXT DEFAULT '',
            notes      TEXT DEFAULT '',
            updated_at TEXT
        )
    """)
    try:
        conn.execute("ALTER TABLE outreach ADD COLUMN email TEXT DEFAULT ''")
    except Exception:
        pass
    # Migrate old status strings to new plain-English labels
    for old, new in [
        ("not contacted",           "New"),
        ("contacted — no response", "Contacted"),
        ("contacted — responded",   "In Conversation"),
        ("on viewing list",         "On Viewing List"),
        ("not interested",          "Not Interested"),
    ]:
        conn.execute("UPDATE outreach SET status=? WHERE status=?", (new, old))
    conn.commit()
    conn.close()

def get_outreach(address):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT status, email, notes FROM outreach WHERE address = ?", (address,)
    ).fetchone()
    conn.close()
    return (
        {"status": row[0], "email": row[1] or "", "notes": row[2]}
        if row else
        {"status": "not contacted", "email": "", "notes": ""}
    )

def save_outreach(address, status, email, notes):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO outreach (address, status, email, notes, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            status=excluded.status, email=excluded.email,
            notes=excluded.notes, updated_at=excluded.updated_at
    """, (address, status, email, notes, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def get_all_outreach():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT address, status, email, notes FROM outreach").fetchall()
    conn.close()
    return {r[0]: {"status": r[1], "email": r[2] or "", "notes": r[3]} for r in rows}

init_outreach_db()

# ---------------------------------------------------------------------------
# CARD IMAGE — neighborhood SVG graphic (architectural silhouette style)
# ---------------------------------------------------------------------------

HOOD_ICONS = {
    "Tribeca":                ("🏛", "#1a1714", "#c9b49a"),
    "Tribeca / SoHo":         ("🏛", "#1a1714", "#c9b49a"),
    "SoHo / NoHo":            ("🎨", "#141210", "#d4b896"),
    "West Village":           ("🌿", "#12180e", "#a8c890"),
    "Chelsea":                ("🏗", "#1a1410", "#c4b09a"),
    "Chelsea / Hudson Yards": ("🏙", "#0e1218", "#90a8c4"),
    "Upper East Side":        ("🏰", "#18140a", "#c8b488"),
    "Upper West Side":        ("🌳", "#0e180e", "#90c490"),
    "Midtown East":           ("🗽", "#141014", "#c490c4"),
    "Midtown West / Hell's Kitchen": ("🎭", "#181014", "#c49090"),
    "Hell's Kitchen":         ("🎭", "#181014", "#c49090"),
    "Gramercy":               ("🏡", "#12140e", "#b4c490"),
    "Murray Hill":            ("🏢", "#101418", "#90a8b4"),
    "Financial District":     ("💹", "#0e1014", "#a490c4"),
    "Battery Park City":      ("⚓", "#0e1218", "#90b4c4"),
    "Lower East Side":        ("🎸", "#18100e", "#c4a090"),
    "East Village":           ("☕", "#14100a", "#c4b078"),
    "East Village / Gramercy":("☕", "#14100a", "#c4b078"),
    "Midtown / Garment District": ("✂️", "#141010", "#c49898"),
    "Upper East Side / Yorkville": ("🏰", "#18140a", "#c8b488"),
}
HOOD_DEFAULT = ("🏙", "#1a1714", "#c9b49a")

def card_image_svg(address, neighborhood, score, category):
    """Generate a clean architectural SVG for the card — neighborhood icon + typography."""
    icon, bg, fg = HOOD_ICONS.get(neighborhood or "", HOOD_DEFAULT)
    hood    = (neighborhood or "Manhattan")[:24].upper()
    addr    = address[:28] if len(address) > 28 else address
    cat_label = {"high_priority": "HIGH PRIORITY", "watch": "WATCH", "maybe": "PROSPECT"}.get(category, "")
    cat_color = {"high_priority": "#e8ddd0", "watch": "#c4b09a", "maybe": "#a89060"}.get(category, fg)

    # Build a grid-like architectural background pattern
    svg = f"""<svg xmlns='http://www.w3.org/2000/svg' width='400' height='200'>
  <defs>
    <linearGradient id='bg' x1='0' y1='0' x2='1' y2='1'>
      <stop offset='0%' stop-color='{bg}'/>
      <stop offset='100%' stop-color='#0a0806'/>
    </linearGradient>
    <pattern id='grid' width='40' height='40' patternUnits='userSpaceOnUse'>
      <path d='M 40 0 L 0 0 0 40' fill='none' stroke='{fg}' stroke-width='0.3' opacity='0.12'/>
    </pattern>
  </defs>
  <rect width='400' height='200' fill='url(#bg)'/>
  <rect width='400' height='200' fill='url(#grid)'/>
  <text x='200' y='115' font-family='Apple Color Emoji,Segoe UI Emoji,sans-serif'
        font-size='54' text-anchor='middle' opacity='0.18'>{icon}</text>
  <rect x='0' y='0' width='3' height='200' fill='{cat_color}' opacity='0.7'/>
  <text x='18' y='34' font-family='Arial,sans-serif' font-size='8'
        fill='{fg}' opacity='0.55' letter-spacing='3'>{hood}</text>
  <line x1='18' y1='42' x2='382' y2='42' stroke='{fg}' stroke-opacity='0.1' stroke-width='0.5'/>
  <text x='18' y='76' font-family='Georgia,serif' font-size='16'
        fill='#f5f0e8' letter-spacing='0.3'>{addr}</text>
  <line x1='18' y1='90' x2='382' y2='90' stroke='{fg}' stroke-opacity='0.1' stroke-width='0.5'/>
  <text x='18' y='128' font-family='Georgia,serif' font-size='44'
        fill='#ffffff' opacity='0.92'>{score}</text>
  <text x='18' y='148' font-family='Arial,sans-serif' font-size='7.5'
        fill='{fg}' opacity='0.4' letter-spacing='2.5'>SIGNAL SCORE</text>
  <rect x='74' y='108' width='0.5' height='44' fill='{fg}' opacity='0.2'/>
  <text x='84' y='127' font-family='Arial,sans-serif' font-size='7.5'
        fill='{cat_color}' font-weight='600' letter-spacing='2'>{cat_label}</text>
  <text x='382' y='192' font-family='Georgia,serif' font-size='10'
        fill='{fg}' opacity='0.15' text-anchor='end'>Manhattan</text>
</svg>"""
    encoded = base64.b64encode(svg.encode()).decode()
    return f"data:image/svg+xml;base64,{encoded}"

def maps_url(address):
    encoded = f"{address}, Manhattan NY".replace(" ", "+")
    return f"https://www.google.com/maps/search/?api=1&query={encoded}"

# ---------------------------------------------------------------------------
# STYLING
# ---------------------------------------------------------------------------

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@300;400;500;600&family=Inter:wght@300;400;500;600&display=swap');

:root {
    --ink:        #1a1714;
    --ink-soft:   #3d3530;
    --stone:      #8c7d6e;
    --stone-lt:   #c4b09a;
    --champagne:  #e8ddd0;
    --parchment:  #f5f0e8;
    --cream:      #faf7f3;
    --border:     #e0d6cc;
    --border-dk:  #c4b09a;
    --gold:       #a89060;
    --gold-lt:    #c4aa78;
}

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background: var(--cream);
    color: var(--ink);
}

.block-container {
    padding-top: 0 !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
    max-width: 1400px;
    background: var(--cream);
}

#MainMenu, footer, .stDeployButton, header { visibility: hidden; }

/* ── Hero ── */
.hero {
    background: var(--ink);
    padding: 3rem 2.5rem 2.5rem;
    margin: -1rem -2rem 2.5rem;
    border-bottom: 1px solid rgba(200,176,154,0.2);
}
.hero-eyebrow {
    font-family: 'Inter', sans-serif;
    font-size: 0.68rem;
    font-weight: 500;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--stone-lt);
    margin-bottom: 0.75rem;
}
.hero-title {
    font-family: 'Cormorant Garamond', Georgia, serif;
    font-size: 2.4rem;
    font-weight: 300;
    color: var(--parchment);
    letter-spacing: 0.02em;
    line-height: 1.1;
    margin-bottom: 0.4rem;
}
.hero-sub {
    font-size: 0.78rem;
    color: var(--stone);
    letter-spacing: 0.04em;
}

/* ── Search ── */
div[data-testid="stTextInput"] input {
    border-radius: 2px !important;
    border: 1px solid var(--border-dk) !important;
    background: #ffffff !important;
    color: var(--ink) !important;
    font-size: 0.9rem !important;
    font-family: 'Inter', sans-serif !important;
    padding: 0.65rem 1rem !important;
}
div[data-testid="stTextInput"] input:focus {
    border-color: var(--gold) !important;
    box-shadow: 0 0 0 2px rgba(168,144,96,0.15) !important;
    outline: none !important;
}
div[data-testid="stTextInput"] input::placeholder {
    color: var(--stone) !important;
}

/* ── Filter label ── */
.filter-heading {
    font-size: 0.68rem;
    font-weight: 500;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: var(--stone);
    margin-bottom: 0.5rem;
    margin-top: 1rem;
}

/* ── Stats bar ── */
.stats-bar {
    display: flex;
    gap: 0;
    padding: 1.25rem 0;
    margin-bottom: 1.5rem;
    border-top: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
}
.stat-item {
    flex: 1;
    text-align: center;
    padding: 0 1rem;
    border-right: 1px solid var(--border);
}
.stat-item:last-child { border-right: none; }
.stat-num {
    font-family: 'Cormorant Garamond', Georgia, serif;
    font-size: 2.2rem;
    font-weight: 300;
    color: var(--ink);
    line-height: 1;
}
.stat-num-high  { color: var(--ink); }
.stat-num-watch { color: var(--ink-soft); }
.stat-num-list  { color: var(--gold); }
.stat-label {
    font-size: 0.62rem;
    color: var(--stone);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-top: 4px;
}
.stat-updated {
    font-size: 0.68rem;
    color: var(--stone);
    letter-spacing: 0.05em;
    padding-top: 0.4rem;
}

/* ── Cards ── */
.card {
    background: #ffffff;
    border: 1px solid var(--border);
    border-radius: 2px;
    overflow: hidden;
    margin-bottom: 1.25rem;
    box-shadow: 0 1px 4px rgba(26,23,20,0.06);
    transition: transform 0.2s, box-shadow 0.2s;
}
.card:hover {
    transform: translateY(-3px);
    box-shadow: 0 8px 28px rgba(26,23,20,0.10);
}
.card-img { width:100%; height:220px; object-fit:cover; display:block; }
.card-body { padding: 1.1rem 1.2rem 0.9rem; }
.card-hood {
    font-size: 0.62rem;
    font-weight: 500;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--stone);
    margin-bottom: 5px;
}
.card-address {
    font-family: 'Cormorant Garamond', Georgia, serif;
    font-size: 1.1rem;
    font-weight: 400;
    color: var(--ink);
    margin-bottom: 10px;
    line-height: 1.3;
    letter-spacing: 0.01em;
}
.card-tags { display:flex; gap:5px; flex-wrap:wrap; margin-bottom:9px; }
.tag {
    font-size: 0.6rem;
    font-weight: 600;
    padding: 2px 8px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    white-space: nowrap;
    border-radius: 1px;
}
.tag-high    { background: var(--ink);       color: var(--champagne); }
.tag-watch   { background: var(--ink-soft);  color: var(--champagne); }
.tag-maybe   { background: var(--champagne); color: var(--ink-soft);  }
.tag-neutral { background: var(--parchment); color: var(--stone);     }
.tag-launch  { background: var(--parchment); color: var(--stone);
               border: 1px solid var(--border); }
.card-dev {
    font-size: 0.72rem;
    color: var(--stone);
    margin-bottom: 7px;
    letter-spacing: 0.02em;
}
.card-meta {
    font-size: 0.7rem;
    color: var(--stone);
    line-height: 1.6;
    padding-top: 8px;
    border-top: 1px solid var(--border);
}
.card-footer {
    display:flex; gap:10px; margin-top:9px; align-items:center;
}
.card-link {
    font-size: 0.7rem;
    color: var(--gold);
    text-decoration: none;
    font-weight: 500;
    letter-spacing: 0.04em;
}
.card-link:hover { color: var(--ink); }
.outreach-dot {
    width:7px; height:7px; border-radius:50%;
    display:inline-block; margin-right:4px;
}
.dot-none    { background: var(--border-dk); }
.dot-no-resp { background: var(--stone-lt); }
.dot-resp    { background: var(--gold); }
.dot-list    { background: var(--ink); }
.dot-no      { background: #b08070; }

/* Streamlit button overrides — minimal */
div[data-testid="stButton"] button {
    border-radius: 2px !important;
    border: 1px solid var(--border-dk) !important;
    background: transparent !important;
    color: var(--ink-soft) !important;
    font-size: 0.75rem !important;
    letter-spacing: 0.08em !important;
    font-weight: 500 !important;
    padding: 0.4rem 1rem !important;
    transition: background 0.15s, color 0.15s !important;
}
div[data-testid="stButton"] button:hover {
    background: var(--ink) !important;
    color: var(--parchment) !important;
    border-color: var(--ink) !important;
}

/* ── Multiselect pills — outline style, no dark background ── */
span[data-baseweb="tag"] {
    background-color: transparent !important;
    border: 1px solid var(--border-dk) !important;
    border-radius: 2px !important;
}
span[data-baseweb="tag"] span {
    color: var(--ink-soft) !important;
    font-size: 0.7rem !important;
    letter-spacing: 0.04em !important;
}
/* The X button on pills */
span[data-baseweb="tag"] button {
    border: none !important;
    background: transparent !important;
    color: var(--stone) !important;
    padding: 0 !important;
}
span[data-baseweb="tag"] button:hover {
    background: transparent !important;
    color: var(--ink) !important;
}
/* Multiselect container */
div[data-baseweb="select"] {
    border-radius: 2px !important;
}
div[data-baseweb="select"] > div {
    border-color: var(--border-dk) !important;
    background: #ffffff !important;
    border-radius: 2px !important;
}

/* ── Slider — remove red, use gold ── */
div[data-testid="stSlider"] div[role="slider"] {
    background-color: var(--gold) !important;
    border-color: var(--gold) !important;
}
div[data-testid="stSlider"] > div > div > div {
    background: var(--gold) !important;
}
div[data-testid="stSlider"] [data-testid="stTickBar"] {
    color: var(--stone) !important;
}

/* ── Smaller card when table is active ── */
.card-sm .card-img { height: 120px !important; }
.card-sm .card-body { padding: 0.6rem 0.8rem 0.5rem !important; }
.card-sm .card-address { font-size: 0.85rem !important; margin-bottom: 5px !important; }
.card-sm .card-hood { font-size: 0.58rem !important; margin-bottom: 3px !important; }
.card-sm .card-meta { font-size: 0.65rem !important; padding-top: 5px !important; }

/* Divider */
hr { border-color: var(--border) !important; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load():
    return get_projects()

# ---------------------------------------------------------------------------
# HERO HEADER + SEARCH
# ---------------------------------------------------------------------------

st.markdown("""
<div class="hero">
  <div class="hero-eyebrow">Private Buyer Intelligence · Manhattan</div>
  <div class="hero-title">Development Tracker</div>
  <div class="hero-sub">Early-stage residential developments · NYC DOB permits &amp; filings</div>
  <div style="margin-top:1.25rem;display:flex;gap:1.5rem;">
    <a href="#developer-sites" style="font-size:0.72rem;color:var(--stone-lt);text-decoration:none;
       letter-spacing:0.1em;text-transform:uppercase;border-bottom:1px solid rgba(196,176,154,0.3);
       padding-bottom:2px;">
      ↓ Developer Sites
    </a>
    <a href="#draft-email" style="font-size:0.72rem;color:var(--stone-lt);text-decoration:none;
       letter-spacing:0.1em;text-transform:uppercase;border-bottom:1px solid rgba(196,176,154,0.3);
       padding-bottom:2px;">
      ↓ Draft Email
    </a>
  </div>
</div>
""", unsafe_allow_html=True)

# Search bar — prominent, at the top
search_col, refresh_col = st.columns([5, 1])
with search_col:
    search = st.text_input(
        "",
        placeholder="🔍  Search by address, neighborhood, or developer...",
        label_visibility="collapsed",
        key="main_search",
    )
with refresh_col:
    if st.button("↺  Refresh", use_container_width=True):
        st.cache_data.clear()
        if "outreach_map" in st.session_state:
            del st.session_state["outreach_map"]
        st.session_state["refreshed"] = True
        st.rerun()

# ---------------------------------------------------------------------------
# LOAD & PREPARE DATA
# ---------------------------------------------------------------------------

with st.spinner("Loading projects..."):
    try:
        projects = load()
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        st.stop()

if not projects:
    st.warning("No projects returned. Run `python3 manhattan_dev_tracker.py` in Terminal to diagnose.")
    st.stop()

df = pd.DataFrame(projects)

# Keep outreach_map in session state so saves immediately reflect everywhere
if "outreach_map" not in st.session_state:
    st.session_state["outreach_map"] = get_all_outreach()
outreach_map = st.session_state["outreach_map"]

df["outreach_status"] = df["address"].map(
    lambda a: outreach_map.get(a, {}).get("status", "not contacted")
)

ALL_NEIGHBORHOODS = sorted([n for n in df["neighborhood"].unique() if n])
ALL_LAUNCH_STAGES = ["launched / permitted", "in review", "early / filed"]

# ---------------------------------------------------------------------------
# VIEWING LIST STRIP — addresses you've flagged as On Viewing List
# ---------------------------------------------------------------------------

viewing_list_items = [
    addr for addr, data in outreach_map.items()
    if data.get("status") == "On Viewing List"
    and addr in df["address"].values
]

if viewing_list_items:
    vl_label = " &nbsp;·&nbsp; ".join(
        f'<span style="color:#1a1714;font-weight:600;">{a.title()}</span>'
        for a in viewing_list_items
    )
    st.markdown(
        f'<div style="background:#1a1714;color:#e8ddd0;border-radius:2px;'
        f'padding:0.6rem 1.2rem;margin-bottom:1rem;font-size:0.78rem;">'
        f'<span style="font-weight:600;letter-spacing:0.1em;text-transform:uppercase;'
        f'font-size:0.62rem;color:#c4b09a;">Viewing List &nbsp; </span>'
        f'<span style="color:#e8ddd0;">{vl_label}</span>'
        f'</div>',
        unsafe_allow_html=True
    )

# ---------------------------------------------------------------------------
# FILTER BAR — compact, clear, easy to reset
# ---------------------------------------------------------------------------

st.markdown(
    '<div style="display:flex;align-items:center;justify-content:space-between;'
    'margin-bottom:0.5rem;">'
    '<span class="filter-heading" style="margin:0;">Filters</span>'
    '</div>',
    unsafe_allow_html=True
)

fc1, fc2, fc3, fc4, fc5 = st.columns([2, 2, 3, 2, 1])

# Hard-coded min score — no slider
min_score = 0

with fc1:
    selected_categories = st.multiselect(
        "Priority",
        options=["high_priority", "watch", "maybe"],
        default=["high_priority", "watch"],
        format_func=lambda c: {
            "high_priority": "⬛ High Priority",
            "watch":         "◾ Watch",
            "maybe":         "▫ Prospect",
        }[c],
        placeholder="All priorities",
    )

with fc2:
    selected_launch = st.multiselect(
        "Launch stage",
        options=ALL_LAUNCH_STAGES,
        default=ALL_LAUNCH_STAGES,
        placeholder="All stages",
    )

with fc3:
    selected_hoods = st.multiselect(
        "Neighborhood",
        options=ALL_NEIGHBORHOODS,
        default=[],
        placeholder="All neighborhoods",
    )

with fc4:
    status_filter = st.multiselect(
        "My status",
        options=["New", "Contacted", "In Conversation", "On Viewing List", "Not Interested"],
        default=[],
        placeholder="Any status",
    )

with fc5:
    watched_only = st.checkbox("★ Watched devs only", value=False)

# Active filter count badge
active = []
if set(selected_categories) != {"high_priority","watch","maybe"}: active.append("Priority")
if set(selected_launch) != set(ALL_LAUNCH_STAGES): active.append("Stage")
if selected_hoods: active.append("Neighborhood")
if status_filter: active.append("Status")
if watched_only: active.append("Watched")
if search: active.append("Search")

if active:
    st.markdown(
        f'<div style="font-size:0.68rem;color:#a89060;margin-bottom:0.5rem;">'
        f'Active filters: {" · ".join(active)} &nbsp;'
        f'<span style="color:#8c7d6e;">(clear above to reset)</span>'
        f'</div>',
        unsafe_allow_html=True
    )

# ---------------------------------------------------------------------------
# APPLY FILTERS
# ---------------------------------------------------------------------------

filtered = df.copy()
filtered = filtered[filtered["score"] >= min_score]

if selected_categories:
    filtered = filtered[filtered["category"].isin(selected_categories)]

if selected_launch:
    filtered = filtered[filtered["launch_stage"].isin(selected_launch)]

if selected_hoods:
    # When neighborhoods are explicitly selected, filter to only those
    filtered = filtered[filtered["neighborhood"].isin(selected_hoods)]

if status_filter:
    filtered = filtered[filtered["outreach_status"].isin(status_filter)]

if watched_only:
    filtered = filtered[filtered["watched_developer"].astype(str).str.len() > 0]

if search:
    q = search.upper()
    filtered = filtered[
        filtered["address"].str.contains(q, na=False) |
        filtered["neighborhood"].str.contains(q, na=False) |
        filtered["developer_name"].str.contains(q, na=False)
    ]

filtered = filtered.sort_values("score", ascending=False).reset_index(drop=True)

# ---------------------------------------------------------------------------
# STATS BAR
# ---------------------------------------------------------------------------

hp  = len(filtered[filtered["category"] == "high_priority"])
wtch = len(filtered[filtered["category"] == "watch"])
onlist = len(filtered[filtered["outreach_status"] == "On Viewing List"])

st.markdown(f"""
<div class="stats-bar">
  <div class="stat-item">
    <div class="stat-num">{len(filtered)}</div>
    <div class="stat-label">Projects</div>
  </div>
  <div class="stat-item">
    <div class="stat-num stat-num-high">{hp}</div>
    <div class="stat-label">High Priority</div>
  </div>
  <div class="stat-item">
    <div class="stat-num stat-num-watch">{wtch}</div>
    <div class="stat-label">Watch</div>
  </div>
  <div class="stat-item">
    <div class="stat-num stat-num-list">{onlist}</div>
    <div class="stat-label">On Viewing List</div>
  </div>
  <div class="stat-item" style="border-right:none;display:flex;align-items:center;justify-content:flex-end;padding-right:0;">
    <div class="stat-updated">Updated {datetime.now().strftime('%b %d · %I:%M %p')}</div>
  </div>
</div>
""", unsafe_allow_html=True)

if filtered.empty:
    st.info("No projects match your search or filters. Try clearing some filters above.")
    st.stop()

# ---------------------------------------------------------------------------
# RECENTLY LAUNCHED SECTION — Certificate of Occupancy buildings
# ---------------------------------------------------------------------------

co_projects = [p for p in projects if p.get("source_dataset") == "DOB Certificate of Occupancy"]

if co_projects:
    st.markdown("""
    <div style="background:#1a1714;border-radius:2px;padding:1.25rem 1.5rem;margin-bottom:1rem;">
      <div style="font-size:0.65rem;font-weight:600;letter-spacing:0.18em;
                  text-transform:uppercase;color:#c4b09a;margin-bottom:0.4rem;">
        Recently Launched
      </div>
      <div style="font-family:'Cormorant Garamond',Georgia,serif;font-size:1.3rem;
                  font-weight:300;color:#f5f0e8;margin-bottom:0.25rem;">
        New Residential Buildings · Certificate of Occupancy Issued
      </div>
      <div style="font-size:0.72rem;color:#8c7d6e;">
        Buildings that completed construction and received a CO in the last 12 months
      </div>
    </div>
    """, unsafe_allow_html=True)

    co_cols = st.columns(3)
    for i, p in enumerate(co_projects[:9]):
        with co_cols[i % 3]:
            hood  = p.get("neighborhood", "") or "Manhattan"
            dev   = p.get("developer_name", "") or "—"
            date  = p.get("latest_event_date", "") or "—"
            mlink = maps_url(p["address"])
            ostat = outreach_map.get(p["address"], {}).get("status", "New")

            st.markdown(
                f'<div style="border:1px solid #e0d6cc;border-radius:2px;'
                f'padding:1rem 1.1rem;background:#fff;margin-bottom:0.75rem;">'
                f'<div style="font-size:0.6rem;letter-spacing:0.14em;text-transform:uppercase;'
                f'color:#8c7d6e;margin-bottom:4px;">{hood}</div>'
                f'<div style="font-family:Cormorant Garamond,Georgia,serif;font-size:1rem;'
                f'color:#1a1714;margin-bottom:8px;font-weight:400;">{p["address"]}</div>'
                f'<div style="margin-bottom:8px;">'
                f'<span style="font-size:0.6rem;font-weight:600;padding:2px 8px;'
                f'letter-spacing:0.08em;text-transform:uppercase;background:#a89060;'
                f'color:#fff;border-radius:1px;">CO Issued</span>'
                f'</div>'
                f'<div style="font-size:0.7rem;color:#8c7d6e;line-height:1.7;'
                f'border-top:1px solid #e0d6cc;padding-top:7px;">'
                f'&#128100; {dev}<br>'
                f'CO issued: {date}<br>'
                f'<a href="{mlink}" target="_blank" style="color:#a89060;'
                f'text-decoration:none;font-weight:500;">&#128205; Map</a>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True
            )

            safe_key = "co_" + p["address"].replace(" ", "_").replace("/", "_")
            already_listed = ostat == "On Viewing List"
            btn_label = "★ Remove from List" if already_listed else "＋ Add to List"
            if st.button(btn_label, key=f"vl_{safe_key}", use_container_width=True):
                existing = outreach_map.get(p["address"], {})
                new_s = "New" if already_listed else "On Viewing List"
                save_outreach(p["address"], new_s, existing.get("email", ""), existing.get("notes", ""))
                st.session_state["outreach_map"][p["address"]] = {**existing, "status": new_s}
                st.rerun()

    st.divider()

# ---------------------------------------------------------------------------
# VIEW TOGGLE — two buttons, no radio
# ---------------------------------------------------------------------------

if "view_mode" not in st.session_state:
    st.session_state["view_mode"] = "Cards"

tog1, tog2, _ = st.columns([1, 1, 5])
with tog1:
    if st.button("Cards", use_container_width=True,
                 type="primary" if st.session_state["view_mode"] == "Cards" else "secondary"):
        st.session_state["view_mode"] = "Cards"
        st.rerun()
with tog2:
    if st.button("Table", use_container_width=True,
                 type="primary" if st.session_state["view_mode"] == "Table" else "secondary"):
        st.session_state["view_mode"] = "Table"
        st.rerun()

view_mode = st.session_state["view_mode"]

# ---------------------------------------------------------------------------
# TABLE VIEW
# ---------------------------------------------------------------------------

if view_mode == "Table":
    st.markdown("""
    <style>
    .proj-table { width:100%; border-collapse:collapse; font-size:0.82rem; }
    .proj-table th {
        text-align:left; padding:10px 12px; cursor:pointer; user-select:none;
        font-size:0.65rem; font-weight:600; letter-spacing:0.1em;
        text-transform:uppercase; color:#8c7d6e;
        border-bottom:2px solid #e0d6cc; background:#faf7f3;
        white-space:nowrap;
    }
    .proj-table th:hover { color:#1a1714; }
    .proj-table th.sorted-asc::after  { content:" ↑"; }
    .proj-table th.sorted-desc::after { content:" ↓"; }
    .proj-table td {
        padding:10px 12px; border-bottom:1px solid #e0d6cc;
        color:#1a1714; vertical-align:middle;
    }
    .proj-table tr:hover td { background:#f7f2ec; }
    .proj-table .addr { font-weight:600; color:#1a1714; }
    .proj-table .hood { color:#8c7d6e; font-size:0.75rem; }
    .proj-table .score-num {
        font-family:'Cormorant Garamond',Georgia,serif;
        font-size:1.3rem; font-weight:400; color:#1a1714;
    }
    .tag-hp  { background:#1a1714; color:#e8ddd0;
               font-size:0.6rem; padding:2px 7px; letter-spacing:0.08em; text-transform:uppercase; }
    .tag-w   { background:#3d3530; color:#e8ddd0;
               font-size:0.6rem; padding:2px 7px; letter-spacing:0.08em; text-transform:uppercase; }
    .tag-m   { background:#f5f0e8; color:#3d3530;
               font-size:0.6rem; padding:2px 7px; letter-spacing:0.08em;
               text-transform:uppercase; border:1px solid #e0d6cc; }
    .status-pill { font-size:0.65rem; padding:2px 8px; letter-spacing:0.06em;
                   text-transform:uppercase; white-space:nowrap; border-radius:1px; }
    .map-link { color:#a89060; text-decoration:none; font-size:0.72rem; font-weight:500; }
    </style>
    """, unsafe_allow_html=True)

    # Map icon SVG — modern location pin
    map_icon = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" '
        'fill="none" stroke="#a89060" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
        'style="vertical-align:middle;margin-right:3px;">'
        '<path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/>'
        '<circle cx="12" cy="10" r="3"/>'
        '</svg>'
    )

    status_style_map = {
        "New":              ("#f5f0e8", "#8c7d6e"),
        "Contacted":        ("#e8ddd0", "#3d3530"),
        "In Conversation":  ("#a89060", "#ffffff"),
        "On Viewing List":  ("#1a1714", "#e8ddd0"),
        "Not Interested":   ("#c4a090", "#5a3828"),
    }

    # Build rows
    rows_data = []
    for _, row in filtered.iterrows():
        cat      = row["category"]
        ostat    = outreach_map.get(row["address"], {}).get("status", "New")
        raw_dev  = row.get("developer_name","") or "—"
        star     = "&#9733; " if row.get("watched_developer") else ""
        hood     = row.get("neighborhood","") or "Manhattan"
        date     = row.get("latest_event_date","") or "—"
        launch   = row.get("launch_stage","") or "—"
        mlink    = maps_url(row["address"])
        email    = outreach_map.get(row["address"], {}).get("email", "")
        mylist   = ""  # watchlist removed — tracked internally for amenities only

        cat_html = {
            "high_priority": '<span class="tag-hp">High Priority</span>',
            "watch":         '<span class="tag-w">Watch</span>',
            "maybe":         '<span class="tag-m">Prospect</span>',
        }.get(cat, cat)

        sbg, sfg = status_style_map.get(ostat, ("#f5f0e8", "#8c7d6e"))
        status_pill = f'<span class="status-pill" style="background:{sbg};color:{sfg};">{ostat}</span>'
        email_cell  = f'<a href="mailto:{email}" style="color:#a89060;font-size:0.72rem;">&#9993; {email}</a>' if email else '<span style="color:#c4b09a;font-size:0.72rem;">—</span>'
        cat_sort    = {"high_priority": 0, "watch": 1, "maybe": 2}.get(cat, 3)

        rows_data.append({
            "addr_raw":    row["address"],
            "addr_html":   f'<div class="addr">{row["address"]}{mylist}</div>',
            "hood":        hood,
            "score":       row["score"],
            "cat_sort":    cat_sort,
            "cat_html":    cat_html,
            "launch":      launch,
            "dev":         f'{star}{raw_dev}',
            "email_cell":  email_cell,
            "status_pill": status_pill,
            "date":        date,
            "map":         f'<a class="map-link" href="{mlink}" target="_blank">{map_icon} Map</a>',
        })

    rows_html = ""
    selected_addr = st.session_state.get("selected_address","")
    for r in rows_data:
        is_sel   = r['addr_raw'] == selected_addr
        row_bg   = 'background:#f5f0e8;border-left:3px solid #a89060;' if is_sel else ''
        addr_style = 'color:#a89060;font-weight:700;cursor:pointer;' if is_sel else 'color:#1a1714;font-weight:700;cursor:pointer;'
        rows_html += f"""<tr style="cursor:pointer;{row_bg}"
          onclick="selectProject(this)"
          data-address="{r['addr_raw']}"
          data-score="{r['score']}"
          data-cat="{r['cat_sort']}"
          data-launch="{r['launch']}"
          data-date="{r['date']}">
          <td>
            <div style="{addr_style}text-decoration:underline;">{r['addr_raw']}</div>
          </td>
          <td style="color:#8c7d6e;font-size:0.78rem;">{r['hood']}</td>
          <td><span class="score-num">{r['score']}</span></td>
          <td>{r['cat_html']}</td>
          <td>{r['launch']}</td>
          <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{r['dev']}</td>
          <td>{r['email_cell']}</td>
          <td style="color:#8c7d6e;font-size:0.75rem;">{r['date']}</td>
          <td>{r['map']}</td>
        </tr>"""

    # Score + Priority legend
    st.markdown(
        '<div style="font-size:0.68rem;color:#8c7d6e;margin-bottom:0.5rem;line-height:1.8;">'
        '<strong style="color:#1a1714;letter-spacing:0.06em;">SCORE</strong> &nbsp; Signal confidence: &nbsp;'
        '<strong style="color:#1a1714;">80+</strong> High Priority &nbsp;·&nbsp; '
        '<strong style="color:#3d3530;">60–79</strong> Watch &nbsp;·&nbsp; '
        '<strong style="color:#8c7d6e;">40–59</strong> Prospect'
        '<br>'
        '<strong style="color:#1a1714;letter-spacing:0.06em;">PRIORITY</strong> &nbsp; '
        '<strong style="color:#1a1714;">High Priority</strong> = demo permit + new building permit both filed — strongest signal &nbsp;·&nbsp; '
        '<strong style="color:#3d3530;">Watch</strong> = one clear signal (demo or NB), not yet corroborated &nbsp;·&nbsp; '
        '<strong style="color:#8c7d6e;">Prospect</strong> = early filing only, worth monitoring'
        '</div>',
        unsafe_allow_html=True
    )

    table_html = f"""
    <style>
    body {{ margin:0; font-family:'Inter',sans-serif; background:#faf7f3; }}
    .proj-table {{ width:100%; border-collapse:collapse; font-size:0.82rem; }}
    .proj-table th {{
        text-align:left; padding:10px 12px; cursor:pointer; user-select:none;
        font-size:0.65rem; font-weight:600; letter-spacing:0.1em;
        text-transform:uppercase; color:#8c7d6e;
        border-bottom:2px solid #e0d6cc; background:#faf7f3;
        white-space:nowrap;
    }}
    .proj-table th:hover {{ color:#1a1714; }}
    .proj-table th.sorted-asc::after  {{ content:" ↑"; }}
    .proj-table th.sorted-desc::after {{ content:" ↓"; }}
    .proj-table td {{
        padding:10px 12px; border-bottom:1px solid #e0d6cc;
        color:#1a1714; vertical-align:middle; background:#fff;
    }}
    .proj-table tr {{ cursor:pointer; }}
    .proj-table tr:hover td {{ background:#f7f2ec; }}
    .proj-table tr.selected td {{ background:#f5f0e8 !important; border-left:3px solid #a89060; }}
    .addr {{ font-weight:600; color:#1a1714; font-size:0.82rem; }}
    .hood {{ color:#8c7d6e; font-size:0.72rem; }}
    .score-num {{ font-family:Georgia,serif; font-size:1.2rem; font-weight:400; color:#1a1714; }}
    .tag-hp  {{ background:#1a1714; color:#e8ddd0; font-size:0.6rem; padding:2px 7px; letter-spacing:0.08em; text-transform:uppercase; }}
    .tag-w   {{ background:#3d3530; color:#e8ddd0; font-size:0.6rem; padding:2px 7px; letter-spacing:0.08em; text-transform:uppercase; }}
    .tag-m   {{ background:#f5f0e8; color:#3d3530; font-size:0.6rem; padding:2px 7px; letter-spacing:0.08em; text-transform:uppercase; border:1px solid #e0d6cc; }}
    .status-pill {{ font-size:0.65rem; padding:2px 8px; letter-spacing:0.06em; text-transform:uppercase; white-space:nowrap; border-radius:1px; }}
    .map-link {{ color:#a89060; text-decoration:none; font-size:0.72rem; font-weight:500; }}
    a {{ color:#a89060; }}
    </style>
    <table class="proj-table" id="projTable">
      <thead><tr>
        <th onclick="sortTable(0,'str')">Address</th>
        <th onclick="sortTable(1,'str')">Neighborhood</th>
        <th onclick="sortTable(2,'num')">Score</th>
        <th onclick="sortTable(3,'cat')">Priority</th>
        <th onclick="sortTable(4,'str')">Launch Stage</th>
        <th onclick="sortTable(5,'str')">Owner / Developer</th>
        <th>Email</th>
        <th onclick="sortTable(7,'str')">Latest Filing</th>
        <th></th>
      </tr></thead>
      <tbody id="projBody">{rows_html}</tbody>
    </table>
    <script>
    var sortDir = {{}};
    function selectProject(row) {{
      document.querySelectorAll('.proj-table tr').forEach(function(r){{ r.classList.remove('selected'); }});
      row.classList.add('selected');
      var addr = row.getAttribute('data-address');
      window.parent.postMessage({{type:'streamlit:setComponentValue', value: addr}}, '*');
    }}
    function sortTable(col, type) {{
      var tbody = document.getElementById('projBody');
      var rows  = Array.from(tbody.querySelectorAll('tr'));
      var asc   = sortDir[col] !== true;
      sortDir[col] = asc;
      rows.sort(function(a, b) {{
        var av = a.cells[col] ? a.cells[col].innerText.trim() : '';
        var bv = b.cells[col] ? b.cells[col].innerText.trim() : '';
        if (type === 'num') {{
          return asc ? (parseFloat(av)||0) - (parseFloat(bv)||0) : (parseFloat(bv)||0) - (parseFloat(av)||0);
        }}
        if (type === 'cat') {{
          var m = {{'HIGH PRIORITY':0,'WATCH':1,'PROSPECT':2}};
          var ai = m[av.toUpperCase()]; var bi = m[bv.toUpperCase()];
          ai = ai !== undefined ? ai : 9; bi = bi !== undefined ? bi : 9;
          return asc ? ai - bi : bi - ai;
        }}
        return asc ? av.localeCompare(bv) : bv.localeCompare(av);
      }});
      rows.forEach(function(r){{ tbody.appendChild(r); }});
      document.querySelectorAll('.proj-table th').forEach(function(th,i){{
        th.classList.remove('sorted-asc','sorted-desc');
        if(i===col) th.classList.add(asc?'sorted-asc':'sorted-desc');
      }});
    }}
    </script>
    """
    components.html(table_html, height=min(60 + len(rows_data) * 52, 700), scrolling=True)
    st.caption(f"{len(filtered)} projects · Click a row to select it in the detail panel below · Click column headers to sort.")

# ---------------------------------------------------------------------------
# CARD VIEW
# ---------------------------------------------------------------------------

else:
    # Section header for cards
    st.markdown(
        '<div id="card-grid" style="border-top:2px solid #e0d6cc;padding-top:1rem;margin-bottom:1rem;">'
        '<span style="font-size:0.65rem;font-weight:600;letter-spacing:0.14em;'
        'text-transform:uppercase;color:#8c7d6e;">Project Cards</span>'
        '</div>',
        unsafe_allow_html=True
    )

    # Scroll to card grid if triggered from watchlist or table click
    if st.session_state.get("scroll_to_cards"):
        st.session_state["scroll_to_cards"] = False
        components.html("""
        <script>
        window.parent.document.getElementById('card-grid')
          .scrollIntoView({behavior:'smooth', block:'start'});
        </script>
        """, height=0)

    CARDS_PER_ROW = 3

    def cat_tag(cat):
        cls = {"high_priority":"tag-high","watch":"tag-watch","maybe":"tag-maybe"}.get(cat,"tag-neutral")
        lbl = {"high_priority":"High Priority","watch":"Watch","maybe":"Maybe"}.get(cat, cat)
        return f'<span class="tag {cls}">{lbl}</span>'

    def launch_tag(s):
        return f'<span class="tag tag-launch">{s}</span>' if s else ""

    def outreach_dot(status):
        cls = {
            "not contacted":         "dot-none",
            "contacted — no response": "dot-no-resp",
            "contacted — responded":   "dot-resp",
            "on viewing list":         "dot-list",
            "not interested":          "dot-no",
        }.get(status, "dot-none")
        return f'<span class="outreach-dot {cls}"></span>'

    if "contact_open" not in st.session_state:
        st.session_state["contact_open"] = None

    rows = [filtered.iloc[i:i+CARDS_PER_ROW] for i in range(0, len(filtered), CARDS_PER_ROW)]

    for row_df in rows:
        cols = st.columns(CARDS_PER_ROW)
        for col, (_, p) in zip(cols, row_df.iterrows()):
            with col:
                img_h    = "120px" if view_mode == "Table" else "200px"
                safe_key = p['address'].replace(" ", "_").replace("/", "_")
                ostat    = outreach_map.get(p["address"],{}).get("status","New")
                oemail   = outreach_map.get(p["address"],{}).get("email","")
                dev      = p.get("developer_name","")
                hood     = p.get("neighborhood","") or "Manhattan"
                mlink    = maps_url(p["address"])

                # Neighborhood SVG card image
                card_svg = card_image_svg(p["address"], hood, p["score"], p["category"])

                st.markdown(
                    f'<img src="{card_svg}" style="width:100%;height:{img_h};'
                    f'object-fit:cover;display:block;border-radius:2px 2px 0 0;'
                    f'border:1px solid #e0d6cc;border-bottom:none;" alt="{p["address"]}"/>',
                    unsafe_allow_html=True
                )

                # Card body — all native Streamlit, no HTML injection
                with st.container():
                    st.markdown(
                        f'<div style="border:1px solid #e0d6cc;border-top:none;'
                        f'border-radius:0 0 2px 2px;padding:0.9rem 1rem 0.75rem;'
                        f'background:#fff;margin-bottom:0.5rem;">',
                        unsafe_allow_html=True
                    )

                    # Neighborhood + address
                    st.markdown(
                        f'<div style="font-size:0.62rem;letter-spacing:0.14em;'
                        f'text-transform:uppercase;color:#8c7d6e;">{hood}</div>'
                        f'<div style="font-family:Cormorant Garamond,Georgia,serif;'
                        f'font-size:1rem;color:#1a1714;margin:3px 0 8px;">{p["address"]}</div>',
                        unsafe_allow_html=True
                    )

                    # Tags — build safe flat string, no CSS vars
                    cat_colors = {
                        "high_priority": ("background:#1a1714;color:#e8ddd0;",  "High Priority"),
                        "watch":         ("background:#3d3530;color:#e8ddd0;",  "Watch"),
                        "maybe":         ("background:#f5f0e8;color:#3d3530;border:1px solid #e0d6cc;", "Prospect"),
                    }
                    cat_style, cat_label = cat_colors.get(p["category"], ("background:#f5f0e8;color:#3d3530;", p["category"]))
                    tag_style = "font-size:0.6rem;font-weight:600;padding:2px 8px;letter-spacing:0.08em;text-transform:uppercase;margin-right:4px;border-radius:1px;"
                    ls = p.get("launch_stage","")
                    watched_span = f'<span style="{tag_style}border:1px solid #c4b09a;color:#3d3530;">★ Watched</span>' if p.get("watched_developer") else ""
                    tags_html = (
                        f'<div style="margin-bottom:8px;">'
                        f'<span style="{tag_style}{cat_style}">{cat_label}</span>'
                        f'<span style="{tag_style}background:#f5f0e8;color:#8c7d6e;border:1px solid #e0d6cc;">{ls}</span>'
                        f'<span style="{tag_style}background:#f5f0e8;color:#8c7d6e;border:1px solid #e0d6cc;">Score {p["score"]}</span>'
                        f'{watched_span}'
                        f'</div>'
                    )
                    st.markdown(tags_html, unsafe_allow_html=True)

                    # Developer
                    if dev:
                        st.markdown(
                            f'<div style="font-size:0.72rem;color:#8c7d6e;margin-bottom:6px;">&#128100; {dev}</div>',
                            unsafe_allow_html=True
                        )

                    # Outreach status
                    status_styles = {
                        "New":             "background:#f5f0e8;color:#8c7d6e;",
                        "Contacted":       "background:#e8ddd0;color:#3d3530;",
                        "In Conversation": "background:#a89060;color:#fff;",
                        "On Viewing List": "background:#1a1714;color:#e8ddd0;",
                        "Not Interested":  "background:#c4a090;color:#5a3828;",
                    }
                    ss = status_styles.get(ostat, "background:#f5f0e8;color:#8c7d6e;")
                    contacted = ostat in ("Contacted", "In Conversation", "On Viewing List")
                    check = "&#10003; " if contacted else ""
                    st.markdown(
                        f'<div style="margin-bottom:6px;">'
                        f'<span style="font-size:0.6rem;font-weight:600;letter-spacing:0.08em;'
                        f'text-transform:uppercase;padding:2px 8px;border-radius:1px;{ss}">{check}{ostat}</span>'
                        f'</div>',
                        unsafe_allow_html=True
                    )

                    # Email if saved
                    if oemail:
                        st.markdown(
                            f'<div style="font-size:0.7rem;margin-bottom:6px;">'
                            f'<a href="mailto:{oemail}" style="color:#a89060;text-decoration:none;">&#9993; {oemail}</a>'
                            f'</div>',
                            unsafe_allow_html=True
                        )

                    # Meta
                    expl = p.get("explanation","") or ""
                    date = p.get("latest_event_date","") or "—"
                    nf   = p.get("num_filings","")
                    avail = p.get("availability","")
                    price = p.get("price_range","")
                    meta_lines = f"{expl}<br>Filed: {date} &middot; {nf} filing(s)"
                    if avail:
                        meta_lines += f"<br><strong style='color:#3d3530;'>Availability:</strong> {avail}"
                    if price:
                        meta_lines += f"<br><strong style='color:#3d3530;'>Pricing:</strong> {price}"
                    st.markdown(
                        f'<div style="font-size:0.7rem;color:#8c7d6e;border-top:1px solid #e0d6cc;'
                        f'padding-top:7px;line-height:1.6;">{meta_lines}</div>',
                        unsafe_allow_html=True
                    )

                    # Map link with SVG pin
                    map_svg = (
                        '<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" '
                        'fill="none" stroke="#a89060" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" '
                        'style="vertical-align:middle;margin-right:3px;">'
                        '<path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/>'
                        '<circle cx="12" cy="10" r="3"/></svg>'
                    )
                    st.markdown(
                        f'<div style="margin-top:8px;">'
                        f'<a href="{mlink}" target="_blank" style="font-size:0.7rem;color:#a89060;'
                        f'text-decoration:none;font-weight:500;">{map_svg} Map</a>'
                        f'</div>',
                        unsafe_allow_html=True
                    )

                    st.markdown('</div>', unsafe_allow_html=True)

                # Two action buttons below card — fully independent
                btn1, btn2 = st.columns(2)
                with btn1:
                    # Contacted toggle — only checks Contacted / In Conversation, NOT Viewing List
                    contacted = ostat in ("Contacted", "In Conversation")
                    contact_label = "✓ Contacted" if contacted else "Mark Contacted"
                    if st.button(contact_label, key=f"mc_{safe_key}", use_container_width=True):
                        new_s = "New" if contacted else "Contacted"
                        existing = outreach_map.get(p["address"], {})
                        save_outreach(p["address"], new_s, existing.get("email",""), existing.get("notes",""))
                        st.session_state["outreach_map"][p["address"]] = {**existing, "status": new_s}
                        st.rerun()
                with btn2:
                    already_listed = ostat == "On Viewing List"
                    btn_label = "★ Remove from List" if already_listed else "＋ Add to List"
                    if st.button(btn_label, key=f"vl_{safe_key}", use_container_width=True):
                        existing = outreach_map.get(p["address"], {})
                        new_s = "New" if already_listed else "On Viewing List"
                        save_outreach(p["address"], new_s,
                                      existing.get("email",""), existing.get("notes",""))
                        st.session_state["outreach_map"][p["address"]] = {
                            **existing, "status": new_s
                        }
                        st.rerun()

                # Email Draft — pre-populates detail panel below and scrolls to it
                if st.button("✉ Email Draft", key=f"ed_{safe_key}", use_container_width=True):
                    st.session_state["selected_address"] = p["address"]
                    st.session_state["scroll_to_detail"] = True
                    st.rerun()

# ---------------------------------------------------------------------------
# DETAIL + OUTREACH PANEL
# ---------------------------------------------------------------------------

st.divider()
st.markdown('<div id="project-detail-outreach"></div>', unsafe_allow_html=True)
st.subheader("Project Detail & Outreach")
st.caption("Select any project to log your outreach status and notes.")

# Auto-scroll to this section if triggered by Email Draft button
if st.session_state.get("scroll_to_detail"):
    st.session_state["scroll_to_detail"] = False
    components.html("""
    <script>
    window.parent.document.getElementById('project-detail-outreach')
      .scrollIntoView({behavior:'smooth', block:'start'});
    </script>
    """, height=0)

if filtered.empty:
    st.stop()

selected_addresses = filtered["address"].tolist()

# Set the selectbox value via session state key (Streamlit's proper API)
# This avoids the index/key conflict that caused wrong addresses to show
_nav_addr = st.session_state.get("selected_address", "")
if _nav_addr and _nav_addr in selected_addresses:
    st.session_state["detail_select"] = _nav_addr

selected = st.selectbox(
    "Project",
    options=selected_addresses,
    label_visibility="collapsed",
    key="detail_select",
)

if selected:
    p       = filtered[filtered["address"] == selected].iloc[0]
    current = get_outreach(selected)

    left, right = st.columns([3, 2], gap="large")

    with left:
        c1, c2, c3 = st.columns(3)
        c1.metric("Score",    p["score"],
                  help="Signal confidence. 80+ = both demo and new building permits on file.")
        c2.metric("Category", p["category"].replace("_"," ").title())
        c3.metric("Stage",    p.get("launch_stage","").title())

        st.markdown(f"**{p['address']}**, Manhattan")
        if p.get("neighborhood"):
            st.markdown(f"📍 {p['neighborhood']}")
        st.markdown(f"**Why flagged:** {p.get('explanation','—')}")
        st.markdown(
            f"**Latest filing:** {p.get('latest_event_date','—')}"
            f"  ·  **{p.get('num_filings','—')} filing(s)**"
        )

        st.divider()
        st.markdown("**👤 Owner / Developer on file**")
        dev_name = p.get("developer_name","")
        if dev_name:
            st.markdown(f"**{dev_name}**")
            st.caption(
                "Search this name in LinkedIn, the NYS entity registry (dos.ny.gov), "
                "or Google to find the principal and contact details."
            )
        else:
            st.caption("No owner name found in DOB records for this address.")

        # Availability, pricing, website — shown for watchlist/launched buildings
        avail   = p.get("availability","")
        price   = p.get("price_range","")
        website = p.get("website","")
        if avail or price or website:
            st.divider()
            st.markdown("**📊 Availability & Pricing**")
            if avail:
                st.markdown(f"**Status:** {avail}")
            if price:
                st.markdown(f"**Pricing:** {price}")
            if website:
                st.markdown(f"[🌐 Building website]({website})")

        # Amenities — shown when available
        amenities     = p.get("amenities", [])
        amenity_src   = p.get("amenity_source", "")
        if amenities:
            st.divider()
            st.markdown("**🏢 Building Amenities**")
            cols_a = st.columns(2)
            for i, amenity in enumerate(amenities):
                with cols_a[i % 2]:
                    st.markdown(
                        f'<div style="font-size:0.75rem;color:#3d3530;padding:3px 0;'
                        f'border-bottom:1px solid #f5f0e8;">&#10003; {amenity}</div>',
                        unsafe_allow_html=True
                    )
            if amenity_src:
                st.caption(f"Source: {amenity_src}")

    with right:
        st.markdown("**Track Your Outreach**")
        st.caption("Optional — update as you make contact. Use Viewing List to flag sites you want access to.")

        cur_status = current.get("status", "New")
        if cur_status not in OUTREACH_STATUSES:
            cur_status = "New"
        new_status = st.selectbox(
            "Status",
            options=OUTREACH_STATUSES,
            index=OUTREACH_STATUSES.index(cur_status),
            key=f"status_{selected}",
            help="New · Contacted · In Conversation · On Viewing List · Not Interested",
        )
        new_email = st.text_input(
            "Contact email",
            value=current.get("email", ""),
            placeholder="Paste when you find it",
            key=f"email_{selected}",
        )
        new_notes = st.text_area(
            "Notes",
            value=current["notes"],
            height=90,
            placeholder="e.g. Called sales office 4/10, left voicemail.",
            key=f"notes_{selected}",
        )
        if st.button("Save", use_container_width=True, key=f"save_{selected}"):
            save_outreach(selected, new_status, new_email, new_notes)
            st.session_state["outreach_map"][selected] = {
                "status": new_status, "email": new_email, "notes": new_notes
            }
            st.rerun()

        if new_email:
            st.markdown(
                f'<div style="font-size:0.75rem;color:var(--gold);margin-top:0.25rem;">'
                f'✉ <a href="mailto:{new_email}" style="color:var(--gold);">{new_email}</a></div>',
                unsafe_allow_html=True,
            )

        st.divider()
        st.markdown('<div id="draft-email"></div>', unsafe_allow_html=True)
        st.markdown("**✉ Draft Outreach**")
        dev_display = dev_name or "your team"
        to_line = f"To: {new_email}\n\n" if new_email else ""
        template = (
            f"{to_line}"
            f"Subject: Direct Buyer Inquiry — {selected}\n\n"
            f"Hi,\n\n"
            f"I hope this finds you well. I came across {selected} and wanted to reach out directly — "
            f"I understand {dev_display} is behind the project.\n\n"
            f"I'm a serious buyer actively looking in Manhattan and would love to learn more about the "
            f"development when the time is right. I prefer building relationships directly with development "
            f"teams and would welcome the chance to connect early.\n\n"
            f"If there's someone on your team handling buyer interest or pre-launch inquiries, "
            f"I'd be grateful for an introduction.\n\n"
            f"Thank you for your time,\n"
            f"[Your name]\n"
            f"[Your phone]\n"
            f"[Your email]"
        )
        st.markdown(
            '<div style="background:#f5f0e8;border:1px solid #e0d6cc;border-left:3px solid #a89060;'
            'border-radius:2px;padding:0.5rem 0.75rem;margin-bottom:0.5rem;font-size:0.7rem;color:#8c7d6e;">'
            'Edit the draft below, then copy and paste into your email client.'
            '</div>',
            unsafe_allow_html=True
        )
        st.text_area("", value=template, height=260,
                     key=f"draft_{selected}", label_visibility="collapsed")

# ---------------------------------------------------------------------------
# DEVELOPER REFERENCE — watched firm websites
# ---------------------------------------------------------------------------

st.divider()
st.markdown('<div id="developer-sites"></div>', unsafe_allow_html=True)
with st.expander("📋 Watched Developer Sites", expanded=False):
    st.caption("Direct links and contact emails for your watched developers. Click a site to research, then paste the email into the outreach tracker above.")
    # (name, url, email or "")
    dev_sites = [
        ("Rybak Development",        "https://rybakdev.com",                              "office@rybakdev.com"),
        ("Extell Development",        "https://extell.com/contact",                        ""),
        ("Related Companies",         "https://www.related.com/contact",                   ""),
        ("Naftali Group",             "https://www.naftaligroup.com/contact",              "info@naftaligroup.com"),
        ("Zeckendorf Development",    "https://www.zeckendorf.com",                        ""),
        ("Macklowe Properties",       "https://macklowe.com",                              ""),
        ("Witkoff Group",             "https://witkoff.com",                               ""),
        ("Silverstein Properties",    "https://www.silversteinproperties.com/contact",     "info@silversteinproperties.com"),
        ("Tishman Speyer",            "https://www.tishmanspeyer.com/contact",             ""),
        ("Two Trees Management",      "https://www.twotreesny.com",                        "info@twotreesny.com"),
        ("Durst Organization",        "https://www.durst.org/contact",                     ""),
        ("Rudin Management",          "https://www.rudin.com/contact",                     "info@rudin.com"),
        ("RFR Realty",                "https://www.rfrholding.com/contact",                ""),
        ("Toll Brothers City Living", "https://www.tollbrothers.com/luxury-homes/New-York",""),
        ("Domain Companies",          "https://www.domaincompanies.com",                   "info@domaincompanies.com"),
        ("Broad Street Development",  "https://broadstreetdev.com",                        ""),
        ("Ian Schrager Company",      "https://www.ianschragercompany.com",                ""),
        ("Rockefeller Group",         "https://www.rockefellergroup.com",                  ""),
        ("Gotham Organization",       "https://www.gothamorg.com",                         "info@gothamorg.com"),
        ("Atlas Capital Group",       "https://www.atlascapitalgroup.net",                 ""),
        ("Alchemy Properties",        "https://www.alchemyproperties.com",                 "info@alchemyproperties.com"),
        ("Vornado Realty",            "https://www.vno.com/contact",                       ""),
        ("SL Green Realty",           "https://www.slgreen.com/contact",                   ""),
        ("New Empire Corp",           "https://www.newempirecorp.com",                     ""),
        ("Steiner NYC",               "https://www.steinernyc.com",                        ""),
        ("Continuum Company",         "https://www.thecontinuumcompany.com",               ""),
    ]
    cols = st.columns(3)
    for i, (name, url, email) in enumerate(dev_sites):
        with cols[i % 3]:
            email_line = (
                f'<div style="font-size:0.68rem;margin-top:2px;">'
                f'<a href="mailto:{email}" style="color:#a89060;text-decoration:none;">&#9993; {email}</a>'
                f'</div>'
            ) if email else ""
            st.markdown(
                f'<div style="padding:8px 0;border-bottom:1px solid #e0d6cc;">'
                f'<a href="{url}" target="_blank" style="color:#a89060;text-decoration:none;'
                f'font-weight:500;font-size:0.8rem;">{name}</a>'
                f'<div style="font-size:0.67rem;color:#8c7d6e;overflow:hidden;'
                f'text-overflow:ellipsis;white-space:nowrap;margin-top:1px;">'
                f'{url.replace("https://","")}</div>'
                f'{email_line}'
                f'</div>',
                unsafe_allow_html=True
            )

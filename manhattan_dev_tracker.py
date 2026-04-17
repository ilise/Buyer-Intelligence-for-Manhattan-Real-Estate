#!/usr/bin/env python3
"""
manhattan_dev_tracker.py
------------------------
Backend for the Manhattan Development Tracker.
Pulls real NYC DOB permit and filing data, groups by address,
scores each project, and returns a filtered list via get_projects().

Official data sources:
  - DOB Permit Issuance:  https://data.cityofnewyork.us/resource/ipu4-2q9a.json
  - DOB Job Filings:      https://data.cityofnewyork.us/resource/ic3t-wcy2.json

Run directly to test:
    python3 manhattan_dev_tracker.py
"""

import requests
from datetime import datetime, timedelta
from collections import defaultdict

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DEBUG        = False  # Set True to print a full pipeline report to terminal
LOOKBACK_DAYS = 730   # How far back to pull records (730 = 2 years, catches early-stage projects)
LIMIT         = 1000  # Max records per API call
MIN_SCORE     = 0     # Show all projects — use the slider in the dashboard to filter

# Neighborhoods to exclude entirely — filtered before scoring.
# Uses prefix matching so "Harlem" also catches "East Harlem".
# Set to [] to disable. Values must match entries in ZIP_TO_NEIGHBORHOOD.
EXCLUDED_NEIGHBORHOODS = [
    "Harlem",
    "East Harlem",
    "Washington Heights",
    "Inwood",
    "Hamilton Heights",
]

# ---------------------------------------------------------------------------
# WATCHED DEVELOPERS
# Any project whose owner/applicant name contains one of these fragments
# gets a score boost and a "Watched Developer" badge in the dashboard.
# Purely additive — does not restrict or exclude any other developers.
# Fragment matching is case-insensitive against the DOB owner field.
# ---------------------------------------------------------------------------

WATCHED_DEVELOPERS = {
    # Fragment to match    : display name
    "RYBAK":               "Rybak Development",
    "EXTELL":              "Extell Development",
    "RELATED":             "Related Companies",
    "NAFTALI":             "Naftali Group",
    "ZECKENDORF":          "Zeckendorf Development",
    "MACKLOWE":            "Macklowe Properties",
    "WITKOFF":             "Witkoff Group",
    "SILVERSTEIN":         "Silverstein Properties",
    "TISHMAN":             "Tishman Speyer",
    "TWO TREES":           "Two Trees Management",
    "DURST":               "Durst Organization",
    "RUDIN":               "Rudin Management",
    "RFR":                 "RFR Realty",
    "TOLL BROTHERS":       "Toll Brothers City Living",
    "DOM":                 "Domain Companies",
    "BROAD STREET":        "Broad Street Development",
    "IAN SCHRAGER":        "Ian Schrager Company",
    "ROCKEFELLER GROUP":   "Rockefeller Group",
    "GOTHAM":              "Gotham Organization",
    "ATLAS":               "Atlas Capital Group",
    "ALCHEMY":             "Alchemy Properties",
    "HFZ":                 "HFZ Capital Group",
    "VORNADO":             "Vornado Realty",
    "SL GREEN":            "SL Green Realty",
    "SUMAIDA":             "Sumaida + Khurana",
    "SHOP ARCHITECT":      "SHoP Architects (developer)",
    "NEW EMPIRE":          "New Empire Corp",
    "STEINER":             "Steiner NYC",
    "CONTINUUM":           "Continuum Company",
    "PLAZA":               "Plaza Construction",
}

# ---------------------------------------------------------------------------
# MANUAL WATCHLIST
# Addresses you want tracked regardless of DOB activity.
# These appear in the dashboard with a "Manually Watched" tag.
# Set score and stage yourself — the tool won't override them.
# ---------------------------------------------------------------------------

WATCHLIST = [
    {
        "address":        "250 EAST 62ND STREET",
        "neighborhood":   "Upper East Side",
        "zip_code":       "10065",
        "stage":          "pre-filing",
        "launch_stage":   "early / filed",
        "score":          75,
        "category":       "watch",
        "developer_name": "Rybak Development",
        "developer_status": "confirmed",
        "notes":          "Rybak acquired Sept 2025 for $9.7M from Catholic Near East Welfare Assoc. No DOB filing yet. Pre-demo.",
        "source":         "Manual",
        "amenities":      [],
        "amenity_source": None,
    },
    {
        "address":        "300 EAST 64TH STREET",
        "neighborhood":   "Upper East Side",
        "zip_code":       "10065",
        "stage":          "existing condo",
        "launch_stage":   "launched / permitted",
        "score":          72,
        "category":       "watch",
        "developer_name": "RFR Realty (Aby Rosen)",
        "developer_status": "confirmed",
        "notes":          "27-story condo (Sixtyfour). Built 1996 by RFR, converted to condo 2015. 103 units. Architect: SLCE / Stonehill & Taylor.",
        "source":         "Manual",
        "amenities": [
            "24-hour doorman & concierge",
            "Marble lobby",
            "Landscaped roof deck with grilling station & wet bar",
            "Fitness center with yoga studio",
            "Residents' lounge with library & media room",
            "Movie screening room",
            "Children's playroom",
            "Spa / treatment room",
            "Bike room",
            "Private storage",
            "Laundry lounge",
            "Barbecue area",
            "Common garden",
            "Pet friendly",
            "Pied-à-terre & sublets allowed",
        ],
        "amenity_source": "CityRealty / Compass / Quintessentially Estates",
    },
    {
        "address":        "200 EAST 75TH STREET",
        "neighborhood":   "Upper East Side",
        "zip_code":       "10021",
        "stage":          "launched / selling",
        "launch_stage":   "launched / permitted",
        "score":          88,
        "category":       "high_priority",
        "developer_name": "EJS Group",
        "developer_status": "confirmed",
        "notes":          "18-story condo, 35 units, 2–6BR. Beyer Blinder Belle architecture, Yellow House interiors. 70%+ sold. Closings summer 2026.",
        "source":         "Manual",
        "amenities": [
            "Full athletic suite & fitness center",
            "Landscaped courtyard",
            "Rooftop terrace",
            "Private outdoor space (select units)",
            "Direct elevator entry (select units)",
            "Herringbone oak floors",
            "Oversized windows",
            "State-of-the-art kitchens",
        ],
        "amenity_source": "CityRealty / 200e75.com",
        "availability":   "~10 units remaining · closings summer 2026",
        "price_range":    "From ~$3M",
        "website":        "https://200e75.com",
    },
    {
        "address":        "255 EAST 77TH STREET",
        "neighborhood":   "Upper East Side",
        "zip_code":       "10075",
        "stage":          "under construction / selling",
        "launch_stage":   "in review",
        "score":          85,
        "category":       "high_priority",
        "developer_name": "Naftali Group",
        "developer_status": "confirmed",
        "notes":          "62-unit condo by Naftali Group + Robert A.M. Stern Architects. Max 4 units/floor. Topped out May 2025. Est. completion 2026.",
        "source":         "Manual",
        "amenities": [
            "Fitness center",
            "Swimming pool",
            "Library",
            "Music room",
            "Children's playroom",
            "Lounge",
            "Roof deck",
            "Porte-cochere & automated parking",
            "Honed Calacatta marble kitchens",
            "Rain showers",
            "In-unit washer/dryer",
        ],
        "amenity_source": "CityRealty",
        "availability":   "Active sales · among Manhattan's top contracts",
        "price_range":    "Contact Naftali Group",
        "website":        "https://255east77.com",
    },
    {
        "address":        "1122 MADISON AVENUE",
        "neighborhood":   "Upper East Side",
        "zip_code":       "10028",
        "stage":          "selling / near sellout",
        "launch_stage":   "launched / permitted",
        "score":          92,
        "category":       "high_priority",
        "developer_name": "Legion Investment Group / Nahla Capital",
        "developer_status": "confirmed",
        "notes":          "26-unit ultra-luxury by Studio Sofield. NE corner Madison & 84th, 1 block from Central Park & The Met. ~2 units left. Penthouse closed $89.5M (UES record). Prices raised 4x since Jan 2026 launch.",
        "source":         "Manual",
        "amenities": [
            "24-hour doorman & concierge",
            "Studio Sofield interiors",
            "Private elevator entry",
            "Full-floor & half-floor residences",
            "Central Park & Met Museum views",
            "1 block to Central Park",
        ],
        "amenity_source": "CityRealty / Wall Street Journal",
        "availability":   "~2 units remaining · near sellout",
        "price_range":    "~$10M–$89.5M",
        "website":        "https://1122madison.com",
    },
    # ── Chelsea / West Chelsea ──────────────────────────────────────────────
    {
        "address":        "500 WEST 18TH STREET",
        "neighborhood":   "Chelsea / Hudson Yards",
        "zip_code":       "10011",
        "stage":          "launched / selling",
        "launch_stage":   "launched / permitted",
        "score":          90,
        "category":       "high_priority",
        "developer_name": "Witkoff / Access Industries",
        "developer_status": "confirmed",
        "notes":          "One High Line — two twisting travertine towers by Bjarke Ingels Group (BIG). 236 residences, full city block between 17th & 18th St. One of Manhattan's top-selling buildings 2025.",
        "source":         "Manual",
        "amenities": [
            "75-foot swimming pool with skyline views",
            "4,000 sq ft fitness center",
            "Wine tasting room",
            "Social lounge with billiards",
            "Teen room & children's playroom",
            "Bridge lounge (glass skybridge)",
            "Landscaped porte-cochère by Enzo Enea",
            "Concierge & doorman",
            "Bjarke Ingels Group architecture",
            "Gabellini Sheppard / Gilles & Boissier interiors",
        ],
        "amenity_source": "Wikipedia / onehighlineresidences.com",
        "availability":   "Active sales · 21 units currently listed",
        "price_range":    "From ~$2.8M",
        "website":        "https://www.onehighlineresidences.com",
    },
    {
        "address":        "550 WEST 21ST STREET",
        "neighborhood":   "Chelsea",
        "zip_code":       "10011",
        "stage":          "pre-sales / under construction",
        "launch_stage":   "in review",
        "score":          82,
        "category":       "high_priority",
        "developer_name": "Legion Investment Group",
        "developer_status": "confirmed",
        "notes":          "22-story limestone tower by Thomas Juul-Hansen. 83 residences between Hudson River Park and the High Line. Sales launching 2026, completion late 2027.",
        "source":         "Manual",
        "amenities": [
            "Thomas Juul-Hansen architecture",
            "Between Hudson River Park & High Line",
            "Limestone façade",
            "83 residences",
        ],
        "amenity_source": "6sqft",
        "availability":   "Sales launching 2026",
        "price_range":    "From $2.5M",
        "website":        "https://550west21.com",
    },
    # ── West Village / Downtown ─────────────────────────────────────────────
    {
        "address":        "140 JANE STREET",
        "neighborhood":   "West Village",
        "zip_code":       "10014",
        "stage":          "selling",
        "launch_stage":   "launched / permitted",
        "score":          88,
        "category":       "high_priority",
        "developer_name": "Aurora Capital Associates",
        "developer_status": "confirmed",
        "notes":          "11-story boutique by BKSK Architects & Leroy Street Studio. 15 luxury apartments on cobblestone West Village street. Duplex penthouse entered contract at $87.5M — possible record for downtown Manhattan.",
        "source":         "Manual",
        "amenities": [
            "15 full-floor & duplex residences",
            "BKSK Architects exterior",
            "Leroy Street Studio interiors",
            "Cobblestone West Village location",
            "Concierge service",
        ],
        "amenity_source": "6sqft / CityRealty",
        "availability":   "Active sales · penthouse in contract",
        "price_range":    "Contact developer",
        "website":        "https://140janestreet.com",
    },
    # ── Tribeca ────────────────────────────────────────────────────────────
    {
        "address":        "65 FRANKLIN STREET",
        "neighborhood":   "Tribeca / SoHo",
        "zip_code":       "10013",
        "stage":          "under construction",
        "launch_stage":   "in review",
        "score":          80,
        "category":       "high_priority",
        "developer_name": "HAP Investments",
        "developer_status": "confirmed",
        "notes":          "19-story, 41-unit condo on corner of Franklin & Broadway. Developer acquired site for $46M in 2018. Construction underway after long delay.",
        "source":         "Manual",
        "amenities": [
            "41 residences",
            "Corner of Franklin & Broadway",
            "Tribeca location",
        ],
        "amenity_source": "CityRealty",
        "availability":   "Under construction · contact developer",
        "price_range":    "Contact developer",
        "website":        "https://www.hapinvestments.com",
    },
    # ── Upper West Side ────────────────────────────────────────────────────
    {
        "address":        "720 WEST END AVENUE",
        "neighborhood":   "Upper West Side",
        "zip_code":       "10025",
        "stage":          "launched / selling",
        "launch_stage":   "launched / permitted",
        "score":          85,
        "category":       "high_priority",
        "developer_name": "Naftali Group",
        "developer_status": "confirmed",
        "notes":          "The Henry — Robert A.M. Stern Architects exterior, Thomas Juul-Hansen interiors. 18 stories, half- and full-floor residences, townhouses and penthouses. Hand-laid brick and Indiana limestone. One of NYC's top Q1 2026 sellers.",
        "source":         "Manual",
        "amenities": [
            "Robert A.M. Stern architecture",
            "Thomas Juul-Hansen interiors",
            "Half- and full-floor residences",
            "Townhouses & penthouses",
            "Hand-laid brick & Indiana limestone façade",
            "Fitness center",
            "Landscaped terrace",
            "Concierge & doorman",
        ],
        "amenity_source": "CityRealty",
        "availability":   "Active sales · among NYC top sellers Q1 2026",
        "price_range":    "From ~$3M",
        "website":        "https://720westend.com",
    },
    # ── Sutton Place / Midtown East ────────────────────────────────────────
    {
        "address":        "430 EAST 58TH STREET",
        "neighborhood":   "Midtown East",
        "zip_code":       "10022",
        "stage":          "launched / selling",
        "launch_stage":   "launched / permitted",
        "score":          84,
        "category":       "high_priority",
        "developer_name": "Gamma Real Estate",
        "developer_status": "confirmed",
        "notes":          "Sutton Tower — Manhattan's tallest residential tower in Sutton Place. 80-story glass tower. Duplex penthouse listed at $65M. Sales ongoing, availability from $1.825M. Yabu Pushelberg amenities across multiple floors.",
        "source":         "Manual",
        "amenities": [
            "75-foot swimming pool with skyline views",
            "Recording studio",
            "Private cinema",
            "Multi-sport simulator",
            "Yabu Pushelberg-designed amenities",
            "80-story tower",
            "Concierge & doorman",
            "Fitness center & spa",
        ],
        "amenity_source": "6sqft",
        "availability":   "Active sales from $1.825M · duplex penthouse at $65M",
        "price_range":    "From $1.825M",
        "website":        "https://suttontower.com",
    },
]

# ---------------------------------------------------------------------------
# DATA SOURCE URLS
# Add new source URLs here later — fetch functions stay separate below.
# ---------------------------------------------------------------------------

PERMITS_URL = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"
FILINGS_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"
PLUTO_URL   = "https://data.cityofnewyork.us/resource/64uk-42ks.json"  # MapPLUTO
CO_URL      = "https://data.cityofnewyork.us/resource/bs8b-p36w.json"  # DOB Certificates of Occupancy

# Set False to skip PLUTO enrichment (faster load, fewer API calls)
PLUTO_ENABLED = False

# ---------------------------------------------------------------------------
# NEIGHBORHOOD LOOKUP by zip code
# ---------------------------------------------------------------------------

ZIP_TO_NEIGHBORHOOD = {
    "10001": "Chelsea / Hudson Yards",
    "10002": "Lower East Side",
    "10003": "East Village / Gramercy",
    "10004": "Financial District",
    "10005": "Financial District",
    "10006": "Financial District",
    "10007": "Tribeca",
    "10009": "East Village",
    "10010": "Gramercy",
    "10011": "Chelsea",
    "10012": "SoHo / NoHo",
    "10013": "Tribeca / SoHo",
    "10014": "West Village",
    "10016": "Murray Hill",
    "10017": "Midtown East",
    "10018": "Midtown / Garment District",
    "10019": "Midtown West / Hell's Kitchen",
    "10021": "Upper East Side",
    "10022": "Midtown East",
    "10023": "Upper West Side",
    "10024": "Upper West Side",
    "10025": "Upper West Side",
    "10026": "Harlem",
    "10027": "Harlem",
    "10028": "Upper East Side",
    "10029": "East Harlem",
    "10030": "Harlem",
    "10031": "Washington Heights",
    "10032": "Washington Heights",
    "10033": "Washington Heights",
    "10034": "Inwood",
    "10035": "East Harlem",
    "10036": "Hell's Kitchen",
    "10038": "Financial District",
    "10065": "Upper East Side",
    "10075": "Upper East Side",
    "10128": "Upper East Side / Yorkville",
    "10280": "Battery Park City",
    "10282": "Battery Park City",
}

PREMIUM_NEIGHBORHOODS = {
    "Upper East Side",
    "West Village",
    "Tribeca",
    "Tribeca / SoHo",
    "Chelsea",
    "SoHo / NoHo",
    "Gramercy",
    "Chelsea / Hudson Yards",
    "Midtown East",
    "Battery Park City",
}

# ---------------------------------------------------------------------------
# RESIDENTIAL FILTER
# We exclude records that are explicitly non-residential.
# Blank building type = keep (most DOB records don't populate this field).
# ---------------------------------------------------------------------------

NON_RESIDENTIAL_BUILDING_TYPES = {
    "HOTEL",
    "GARAGE",
    "OFFICE",
    "COMMERCIAL",
    "FACTORY",
    "WAREHOUSE",
    "PARKING",
    "PLACE OF ASSEMBLY",
    "STORAGE",
}

# ---------------------------------------------------------------------------
# INSTITUTIONAL OWNER EXCLUSIONS
# These owner name fragments indicate the permit is for an institution,
# government body, or non-profit — not a private residential developer.
# Any record whose owner contains one of these strings is dropped.
# Add more as you spot them in the results.
# ---------------------------------------------------------------------------

EXCLUDED_OWNER_FRAGMENTS = {
    # Government & housing
    "SALVATION ARMY", "NEW YORK CITY", "NYC HOUSING", "NYCHA",
    "HOUSING AUTHORITY", "DEPT OF", "DEPARTMENT OF", "CITY OF NEW YORK",
    "NYC DEPT", "HRA", "ACS", "DSNY", "FDNY", "NYPD",
    # Universities & schools
    "CUNY", "CITY UNIVERSITY", "COLUMBIA UNIVERSITY", "NYU",
    "NEW YORK UNIVERSITY", "FORDHAM", "PACE UNIVERSITY", "HUNTER COLLEGE",
    "BARUCH COLLEGE", "SCHOOL", "ACADEMY", "INSTITUTE OF",
    "BOARD OF EDUCATION", "DEPT OF EDUCATION",
    # Hospitals & healthcare
    "MOUNT SINAI", "PRESBYTERIAN", "HOSPITAL", "MEDICAL CENTER",
    "HEALTH SYSTEM", "HEALTH CARE", "CLINIC", "MONTEFIORE",
    "LENOX HILL", "NYP ", "NYPHD",
    # Cultural & civic
    "MUSEUM", "LIBRARY", "YMCA", "YWCA",
    # Religious
    "CHURCH", "SYNAGOGUE", "MOSQUE", "ARCHDIOCESE", "DIOCESE",
    "CATHOLIC", "METHODIST", "BAPTIST", "LUTHERAN", "EPISCOPAL",
    "JEWISH", "ISLAMIC CENTER", "TEMPLE ", "CONGREGATION",
    # Non-profits & affordable housing
    "HABITAT FOR HUMANITY", "COMMUNITY LAND TRUST", "AFFORDABLE HOUSING",
    "SETTLEMENT HOUSE", "NEIGHBORHOOD ASSOC",
}

# Permit/filing statuses that indicate a project is recently launched or
# actively under way (vs. just filed and stalled).
LAUNCHED_STATUSES = {
    "ISSUED",
    "PERMIT ISSUED",
    "APPROVED",
    "PLAN EXAMINATION - APPROVED",
    "FULLY PERMITTED",
    "SIGNED OFF",
}


# ===========================================================================
# FETCH FUNCTIONS
# To add a new source later, write a new fetch_*() function and call it
# inside get_projects() alongside the existing ones.
# ===========================================================================

def _get(url, params):
    """Shared HTTP helper. Returns list of records or [] on error."""
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  [WARN] API error ({url}): {e}")
        return []


def fetch_permits():
    """
    DOB Permit Issuance — issued DM and NB permits for Manhattan.
    These are permits already approved and issued by DOB.
    """
    params = {
        "$where": "borough='MANHATTAN' AND job_type IN('DM','NB')",
        "$limit": LIMIT,
        "$order": "filing_date DESC",
    }
    records = _get(PERMITS_URL, params)
    if DEBUG:
        print(f"  [permits]  {len(records)} raw records")
    return records


def fetch_filings():
    """
    DOB Job Application Filings — NB and DM filings for Manhattan.
    These appear earlier in the pipeline than issued permits,
    giving earlier visibility into planned development.
    """
    params = {
        "$where": "borough='MANHATTAN' AND job_type IN('NB','DM')",
        "$limit": LIMIT,
        "$order": "pre__filing_date DESC",
    }
    records = _get(FILINGS_URL, params)
    if DEBUG:
        print(f"  [filings]  {len(records)} raw records")
    return records


def fetch_certificates_of_occupancy():
    """
    DOB Certificates of Occupancy — recently completed residential buildings in Manhattan.
    Filters for COs issued in the last 12 months, residential occupancy types.
    These represent buildings that finished construction and are entering the market.
    """
    cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT00:00:00")
    params = {
        "$where": (
            f"borough='Manhattan' "
            f"AND co_issue_date >= '{cutoff}' "
            f"AND (occupancy_type LIKE '%RESIDENTIAL%' "
            f"     OR occupancy_type LIKE '%DWELLING%' "
            f"     OR occupancy_type LIKE '%APARTMENT%')"
        ),
        "$limit": LIMIT,
        "$order": "co_issue_date DESC",
    }
    records = _get(CO_URL, params)
    if DEBUG:
        print(f"  [COs]  {len(records)} raw records")
    return records


def parse_co(rec):
    """Parse one raw Certificate of Occupancy record. Returns normalized dict or None."""
    house  = (rec.get("house_no") or rec.get("house__") or "").strip()
    street = (rec.get("street_name") or "").strip()
    if not house or not street:
        return None
    raw_date = rec.get("co_issue_date") or ""
    owner    = (rec.get("owner_name") or rec.get("applicant_name") or "").strip()
    zip_code = (rec.get("zip_code") or "").strip()
    job_num  = (rec.get("job_no") or rec.get("job__") or "").strip()
    occ_type = (rec.get("occupancy_type") or "").strip()
    neighborhood = ZIP_TO_NEIGHBORHOOD.get(zip_code, "")
    # Skip excluded neighborhoods
    if EXCLUDED_NEIGHBORHOODS and neighborhood:
        if any(neighborhood.startswith(ex) for ex in EXCLUDED_NEIGHBORHOODS):
            return None
    # Skip institutional owners
    if not _is_private_developer(owner, ""):
        return None
    return {
        "address":        f"{house} {street}",
        "zip_code":       zip_code,
        "job_type":       "CO",
        "building_type":  occ_type,
        "is_residential": True,
        "status":         "CERTIFICATE OF OCCUPANCY",
        "date":           _parse_date(raw_date),
        "job_number":     job_num,
        "owner":          owner,
        "applicant":      "",
        "source":         "DOB Certificate of Occupancy",
        "source_link":    "",
    }


# ===========================================================================
# PARSE FUNCTIONS
# ===========================================================================

def _parse_date(raw):
    """Convert MM/DD/YYYY or YYYY-MM-DD to YYYY-MM-DD. Returns '' on failure."""
    if not raw:
        return ""
    s = str(raw).strip()[:10]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _resolve_developer(owner, applicant):
    """
    Return the raw owner/applicant name from DOB data as the developer name.
    No hardcoded list — whatever is on file is shown directly.
    Returns (developer_name, developer_status, contact_dict).
    """
    empty_contact = {"contact_url": "", "contact_email": "", "contact_phone": "", "notes": ""}
    name = (owner or applicant or "").strip().title()
    status = "suspected" if name else "unknown"
    return name, status, empty_contact


def _is_residential(bldg_type):
    """
    Return False only if the building type is explicitly non-residential.
    Blank or unknown = True (keep). This avoids dropping the many DOB
    records where building type is not populated.
    """
    if not bldg_type or not bldg_type.strip():
        return True  # blank = keep
    bt = bldg_type.strip().upper()
    for excluded in NON_RESIDENTIAL_BUILDING_TYPES:
        if excluded in bt:
            return False
    return True  # anything else = keep


def _is_private_developer(owner, applicant):
    """
    Return False if the owner or applicant name matches a known institutional
    or government entity that is not a private residential developer.
    Blank owner = True (keep — don't drop records with no owner on file).
    """
    combined = f"{owner} {applicant}".upper()
    if not combined.strip():
        return True  # no name = keep
    for fragment in EXCLUDED_OWNER_FRAGMENTS:
        if fragment in combined:
            return False
    return True


def parse_permit(rec):
    """Parse one raw permit record. Returns normalized dict or None."""
    house  = rec.get("house__", "").strip()
    street = rec.get("street_name", "").strip()
    if not house or not street:
        return None
    job_type = (rec.get("job_type") or "").upper()
    if job_type not in ("DM", "NB"):
        return None

    raw_date   = rec.get("issuance_date") or rec.get("filing_date") or ""
    owner      = rec.get("owner_s_business_name") or rec.get("owner_s_last_name") or ""
    applicant  = rec.get("applicant_s_last_name") or ""
    zip_code   = rec.get("zip_code", "").strip()
    job_num    = rec.get("job__", "").strip()
    bldg_type  = rec.get("bldg_type", "").strip()
    status     = rec.get("permit_status", "").strip()

    return {
        "address":        f"{house} {street}",
        "zip_code":       zip_code,
        "job_type":       job_type,
        "building_type":  bldg_type,
        "is_residential": _is_residential(bldg_type),
        "status":         status,
        "date":           _parse_date(raw_date),
        "job_number":     job_num,
        "owner":          owner.strip(),
        "applicant":      applicant.strip(),
        "source":         "DOB Permits",
        "source_link":    "",
    }


def parse_filing(rec):
    """Parse one raw filing record. Returns normalized dict or None."""
    house  = rec.get("house__", "").strip()
    street = rec.get("street_name", "").strip()
    if not house or not street:
        return None
    job_type = (rec.get("job_type") or "").upper()
    if job_type not in ("DM", "NB"):
        return None

    raw_date   = rec.get("pre__filing_date") or rec.get("filing_date") or ""
    owner      = rec.get("owner_s_business_name") or rec.get("owner_s_last_name") or ""
    applicant  = rec.get("applicant_s_last_name") or ""
    zip_code   = rec.get("zip_code", "").strip()
    job_num    = rec.get("job__", "").strip()
    bldg_type  = rec.get("building_type", "").strip()
    status     = (rec.get("job_status_descrp") or "").strip()

    return {
        "address":        f"{house} {street}",
        "zip_code":       zip_code,
        "job_type":       job_type,
        "building_type":  bldg_type,
        "is_residential": _is_residential(bldg_type),
        "status":         status,
        "date":           _parse_date(raw_date),
        "job_number":     job_num,
        "owner":          owner.strip(),
        "applicant":      applicant.strip(),
        "source":         "DOB Filings",
        "source_link":    "",
    }


# ===========================================================================
# GROUPING
# ===========================================================================

def group_by_address(events):
    """Group parsed events by normalized address. Returns {address: [events]}."""
    buckets = defaultdict(list)
    for e in events:
        key = " ".join(e["address"].upper().split())
        buckets[key].append(e)
    return buckets


# ===========================================================================
# SCORING
# ===========================================================================

def score_project(events, address_key):
    """
    Score a grouped project. Returns (score, category, explanation).
    """
    score   = 0
    reasons = []
    now     = datetime.now()
    types   = [e["job_type"] for e in events]

    # Signal strength
    if "DM" in types:
        score += 30
        reasons.append("demolition signal")
    if "NB" in types:
        score += 35
        reasons.append("new building filing")
    if len(events) > 1:
        score += 10
        reasons.append(f"{len(events)} filings at this address")
    elif len(events) == 1:
        score -= 15

    # Location
    zip_code     = events[0].get("zip_code", "")
    neighborhood = ZIP_TO_NEIGHBORHOOD.get(zip_code, "")
    is_premium   = any(neighborhood.startswith(n) for n in PREMIUM_NEIGHBORHOODS)
    score       += 15 if is_premium else 8

    # Building assumption
    score += 10

    # Recency — broader windows to catch early-stage projects filed 1-2 years ago.
    # A demolition permit from 18 months ago is still a live signal.
    for e in events:
        if not e["date"]:
            continue
        try:
            days_ago = (now - datetime.strptime(e["date"], "%Y-%m-%d")).days
            if days_ago <= 90:
                score += 10
                reasons.append("activity within 90 days")
                break
            elif days_ago <= 365:
                score += 7
                reasons.append("activity within 1 year")
                break
            elif days_ago <= 730:
                score += 4
                reasons.append("activity within 2 years")
                break
        except ValueError:
            continue

    # Watched developer boost
    owner     = events[0].get("owner", "").upper()
    applicant = events[0].get("applicant", "").upper()
    combined  = f"{owner} {applicant}"
    for fragment in WATCHED_DEVELOPERS:
        if fragment in combined:
            score += 15
            reasons.append(f"watched developer: {WATCHED_DEVELOPERS[fragment]}")
            break

    # Category
    if score >= 80:
        category = "high_priority"
    elif score >= 60:
        category = "watch"
    elif score >= 40:
        category = "maybe"
    else:
        category = "ignore"

    return score, category, ", ".join(reasons) if reasons else "development activity"


# ===========================================================================
# BUILD PROJECT
# ===========================================================================

def build_project(address_key, events):
    """Assemble a project dict from a group of events at the same address."""
    score, category, explanation = score_project(events, address_key)

    # Representative event = most recent with a date
    dated = sorted([e for e in events if e["date"]], key=lambda e: e["date"], reverse=True)
    rep   = dated[0] if dated else events[0]

    types = [e["job_type"] for e in events]
    if "DM" in types and "NB" in types:
        stage = "demo + new building"
    elif "DM" in types:
        stage = "demolition"
    else:
        stage = "new building"

    # Launch stage: based on the most advanced status seen across all events
    all_statuses = [e.get("status", "").upper() for e in events]
    if any(s in LAUNCHED_STATUSES for s in all_statuses):
        launch_stage = "launched / permitted"
    elif any("EXAM" in s or "APPROV" in s for s in all_statuses):
        launch_stage = "in review"
    else:
        launch_stage = "early / filed"

    zip_code     = rep.get("zip_code", "")
    neighborhood = ZIP_TO_NEIGHBORHOOD.get(zip_code, "")
    dev_name, dev_status, contact = _resolve_developer(rep.get("owner", ""), rep.get("applicant", ""))

    # Check if this is a watched developer
    owner_upper = rep.get("owner", "").upper()
    appl_upper  = rep.get("applicant", "").upper()
    combined    = f"{owner_upper} {appl_upper}"
    watched_dev_name = ""
    for fragment, display_name in WATCHED_DEVELOPERS.items():
        if fragment in combined:
            watched_dev_name = display_name
            break

    return {
        "address":           address_key,
        "borough":           "Manhattan",
        "neighborhood":      neighborhood,
        "stage":             stage,
        "launch_stage":      launch_stage,
        "building_type":     rep.get("building_type", ""),
        "score":             score,
        "category":          category,
        "explanation":       explanation,
        "developer_name":    dev_name,
        "developer_status":  dev_status,
        "developer_url":     contact["contact_url"],
        "developer_email":   contact["contact_email"],
        "developer_phone":   contact["contact_phone"],
        "developer_notes":   contact["notes"],
        "owner_name":        rep.get("owner", ""),
        "applicant_name":    rep.get("applicant", ""),
        "latest_event_date": rep.get("date", ""),
        "job_number":        rep.get("job_number", ""),
        "num_filings":       len(events),
        "source_dataset":    rep.get("source", ""),
        "source_link":       rep.get("source_link", ""),
        "watched_developer": watched_dev_name,
        "is_watchlist":      False,
        "amenities":         [],
        "amenity_source":    None,
    }


# ===========================================================================
# PLUTO ENRICHMENT
# Runs after scoring, on projects that already passed MIN_SCORE.
# Looks up lot-level data by address and adds it to each project dict.
# If a lookup fails, the project is kept — just without PLUTO fields.
#
# PLUTO fields added to each project:
#   pluto_lot_area     — lot size in sq ft
#   pluto_bld_area     — existing building gross sq ft
#   pluto_res_area     — existing residential sq ft
#   pluto_units_total  — existing number of units
#   pluto_year_built   — year existing building was built
#   pluto_zoning       — zoning district (e.g. R8, C6-2)
#   pluto_far          — max allowed floor area ratio
#   pluto_land_use     — land use code description
#   pluto_owner        — owner of record from city assessment rolls
#   pluto_found        — True if PLUTO returned a match, False if not
# ===========================================================================

# MapPLUTO land use codes -> readable labels
PLUTO_LAND_USE = {
    "01": "One & Two Family",
    "02": "Multi-Family Walkup",
    "03": "Multi-Family Elevator",
    "04": "Mixed Residential & Commercial",
    "05": "Commercial & Office",
    "06": "Industrial & Manufacturing",
    "07": "Transportation & Utility",
    "08": "Public Facilities & Institutions",
    "09": "Open Space & Recreation",
    "10": "Parking",
    "11": "Vacant Land",
}


def fetch_pluto_for_address(house_number, street_name):
    """
    Query PLUTO by house number and street name for Manhattan (borocode=1).
    Returns the first matching PLUTO record as a dict, or None if not found.
    """
    params = {
        "$where": (
            f"borocode='1' "
            f"AND address LIKE '{house_number} {street_name.upper()}%'"
        ),
        "$limit": 1,
    }
    results = _get(PLUTO_URL, params)
    return results[0] if results else None


def _parse_address_parts(address_key):
    """
    Split a normalized address string like '350 WEST 42ND ST' into
    (house_number, street_name). Returns ('', '') if it can't be parsed.
    """
    parts = address_key.strip().split(" ", 1)
    if len(parts) == 2 and parts[0].isdigit():
        return parts[0], parts[1]
    return "", ""


def enrich_with_pluto(projects):
    """
    For each project, look up its lot in PLUTO and attach the fields.
    Returns the same list with PLUTO fields added in place.
    Failed lookups set pluto_found=False and leave other fields blank.
    """
    for project in projects:
        house, street = _parse_address_parts(project["address"])
        pluto = None

        if house and street:
            pluto = fetch_pluto_for_address(house, street)

        if pluto:
            land_use_code = (pluto.get("landuse") or "").strip()
            project.update({
                "pluto_found":      True,
                "pluto_lot_area":   pluto.get("lotarea", ""),
                "pluto_bld_area":   pluto.get("bldgarea", ""),
                "pluto_res_area":   pluto.get("resarea", ""),
                "pluto_units_total": pluto.get("unitstotal", ""),
                "pluto_year_built": pluto.get("yearbuilt", ""),
                "pluto_zoning":     pluto.get("zonedist1", ""),
                "pluto_far":        pluto.get("residfar", ""),
                "pluto_land_use":   PLUTO_LAND_USE.get(land_use_code, land_use_code),
                "pluto_owner":      (pluto.get("ownername") or "").strip().title(),
            })
            if DEBUG:
                print(f"  [PLUTO] {project['address']} — "
                      f"lot {pluto.get('lotarea','?')} sqft, "
                      f"zoning {pluto.get('zonedist1','?')}, "
                      f"built {pluto.get('yearbuilt','?')}")
        else:
            project.update({
                "pluto_found":      False,
                "pluto_lot_area":   "",
                "pluto_bld_area":   "",
                "pluto_res_area":   "",
                "pluto_units_total": "",
                "pluto_year_built": "",
                "pluto_zoning":     "",
                "pluto_far":        "",
                "pluto_land_use":   "",
                "pluto_owner":      "",
            })
            if DEBUG:
                print(f"  [PLUTO] {project['address']} — no match")

    return projects


# ===========================================================================
# MAIN ENTRY POINT
# ===========================================================================

def get_projects():
    """
    Fetch, parse, group, score, and filter projects.
    Returns a list of project dicts sorted by score descending.
    Only includes projects with score >= MIN_SCORE.
    """
    if DEBUG:
        print("\n" + "=" * 55)
        print("Manhattan Dev Tracker — DEBUG")
        print("=" * 55)

    # Fetch from all sources
    raw_permits = fetch_permits()
    raw_filings = fetch_filings()
    raw_cos     = fetch_certificates_of_occupancy()

    total_raw = len(raw_permits) + len(raw_filings)
    if DEBUG:
        print(f"  Total raw records: {total_raw}")

    # Parse
    events = []
    for rec in raw_permits:
        p = parse_permit(rec)
        if p:
            events.append(p)
    for rec in raw_filings:
        p = parse_filing(rec)
        if p:
            events.append(p)

    # Parse CO records separately — they become their own project list
    co_events = []
    for rec in raw_cos:
        p = parse_co(rec)
        if p:
            co_events.append(p)

    # No date filter — include all records. The API returns the 1000 most
    # recent per endpoint, so recency is already handled by the API ordering.

    # Residential filter — only drop explicitly non-residential building types
    before = len(events)
    events = [e for e in events if e.get("is_residential", True)]
    if DEBUG:
        print(f"  Residential filter: {before} -> {len(events)} events")

    # Institutional owner filter — drop government, hospitals, universities etc.
    before = len(events)
    events = [e for e in events if _is_private_developer(e.get("owner", ""), e.get("applicant", ""))]
    if DEBUG:
        print(f"  Institutional filter: {before} -> {len(events)} events")

    # Group by address
    buckets = group_by_address(events)

    # Neighborhood exclusion (only if list is non-empty)
    if EXCLUDED_NEIGHBORHOODS:
        filtered_buckets = {}
        for address_key, addr_events in buckets.items():
            zip_code     = addr_events[0].get("zip_code", "")
            neighborhood = ZIP_TO_NEIGHBORHOOD.get(zip_code, "")
            excluded = neighborhood and any(
                neighborhood.startswith(ex) for ex in EXCLUDED_NEIGHBORHOODS
            )
            if not excluded:
                filtered_buckets[address_key] = addr_events
            elif DEBUG:
                print(f"  [excluded] {address_key} — {neighborhood}")
        buckets = filtered_buckets

    if DEBUG:
        print(f"  Unique addresses after exclusions: {len(buckets)}")
        print(f"\n  {'ADDRESS':<42} {'SCORE':>5}  {'CAT':<14}  OUTCOME")
        print("  " + "-" * 78)

    # Score — keep ALL projects (MIN_SCORE acts as soft floor, shown in UI)
    projects = []
    for address_key, addr_events in sorted(buckets.items()):
        project = build_project(address_key, addr_events)
        kept    = project["score"] >= MIN_SCORE
        if DEBUG:
            outcome = "KEPT" if kept else f"skip (< {MIN_SCORE})"
            print(f"  {address_key[:42]:<42} {project['score']:>5}  "
                  f"{project['category']:<14}  {outcome}")
        if kept:
            projects.append(project)

    projects.sort(key=lambda p: p["score"], reverse=True)

    # Inject manual watchlist entries
    existing_addresses = {p["address"].upper() for p in projects}
    for w in WATCHLIST:
        addr = " ".join(w["address"].upper().split())
        if addr in existing_addresses:
            for p in projects:
                if p["address"].upper() == addr:
                    p["is_watchlist"]    = True
                    p["developer_notes"] = w.get("notes", "")
                    p["amenities"]       = w.get("amenities", [])
                    p["amenity_source"]  = w.get("amenity_source", None)
                    p["availability"]    = w.get("availability", "")
                    p["price_range"]     = w.get("price_range", "")
                    p["website"]         = w.get("website", "")
        else:
            projects.append({
                "address":           addr,
                "borough":           "Manhattan",
                "neighborhood":      w.get("neighborhood", ""),
                "zip_code":          w.get("zip_code", ""),
                "stage":             w.get("stage", "pre-filing"),
                "launch_stage":      w.get("launch_stage", "early / filed"),
                "building_type":     "",
                "score":             w.get("score", 60),
                "category":          w.get("category", "watch"),
                "explanation":       w.get("notes", "Manually tracked"),
                "developer_name":    w.get("developer_name", ""),
                "developer_status":  w.get("developer_status", "suspected"),
                "developer_url":     "",
                "developer_email":   "",
                "developer_phone":   "",
                "developer_notes":   w.get("notes", ""),
                "owner_name":        "",
                "applicant_name":    "",
                "latest_event_date": "",
                "job_number":        "",
                "num_filings":       0,
                "source_dataset":    "Manual Watchlist",
                "source_link":       "",
                "watched_developer": w.get("developer_name", ""),
                "is_watchlist":      True,
                "amenities":         w.get("amenities", []),
                "amenity_source":    w.get("amenity_source", None),
                "availability":      w.get("availability", ""),
                "price_range":       w.get("price_range", ""),
                "website":           w.get("website", ""),
                "pluto_found": False, "pluto_lot_area": "", "pluto_bld_area": "",
                "pluto_res_area": "", "pluto_units_total": "", "pluto_year_built": "",
                "pluto_zoning": "", "pluto_far": "", "pluto_land_use": "", "pluto_owner": "",
            })

    # Re-sort after injecting watchlist
    projects.sort(key=lambda p: p["score"], reverse=True)

    # Enrich with PLUTO lot data (one API call per project).
    # Set PLUTO_ENABLED = False at the top of the file for faster loads.
    if PLUTO_ENABLED:
        if DEBUG:
            print(f"\n  Running PLUTO enrichment for {len(projects)} projects...")
        projects = enrich_with_pluto(projects)
    else:
        # Add empty PLUTO fields so the dashboard doesn't error
        for p in projects:
            p.update({
                "pluto_found": False, "pluto_lot_area": "", "pluto_bld_area": "",
                "pluto_res_area": "", "pluto_units_total": "", "pluto_year_built": "",
                "pluto_zoning": "", "pluto_far": "", "pluto_land_use": "", "pluto_owner": "",
            })

    if DEBUG:
        print(f"\n  Returning {len(projects)} projects (score >= {MIN_SCORE})")
        print("=" * 55 + "\n")

    # Build CO projects from certificate of occupancy records
    co_projects = []
    co_buckets  = group_by_address(co_events)
    existing_addresses = {p["address"].upper() for p in projects}
    for address_key, addr_events in co_buckets.items():
        if address_key in existing_addresses:
            continue  # already tracked via permits/filings
        rep          = addr_events[0]
        zip_code     = rep.get("zip_code", "")
        neighborhood = ZIP_TO_NEIGHBORHOOD.get(zip_code, "")
        dev_name, dev_status, _ = _resolve_developer(rep.get("owner", ""), "")
        co_projects.append({
            "address":           address_key,
            "borough":           "Manhattan",
            "neighborhood":      neighborhood,
            "zip_code":          zip_code,
            "stage":             "completed / CO issued",
            "launch_stage":      "launched / permitted",
            "building_type":     rep.get("building_type", ""),
            "score":             70,
            "category":          "watch",
            "explanation":       "Certificate of Occupancy issued — building completed",
            "developer_name":    dev_name,
            "developer_status":  dev_status,
            "developer_url":     "",
            "developer_email":   "",
            "developer_phone":   "",
            "developer_notes":   "",
            "owner_name":        rep.get("owner", ""),
            "applicant_name":    "",
            "latest_event_date": rep.get("date", ""),
            "job_number":        rep.get("job_number", ""),
            "num_filings":       len(addr_events),
            "source_dataset":    "DOB Certificate of Occupancy",
            "source_link":       "",
            "watched_developer": "",
            "is_watchlist":      False,
            "amenities":         [],
            "amenity_source":    None,
            "availability":      "",
            "price_range":       "",
            "website":           "",
            "pluto_found": False, "pluto_lot_area": "", "pluto_bld_area": "",
            "pluto_res_area": "", "pluto_units_total": "", "pluto_year_built": "",
            "pluto_zoning": "", "pluto_far": "", "pluto_land_use": "", "pluto_owner": "",
        })

    co_projects.sort(key=lambda p: p["latest_event_date"], reverse=True)

    return projects + co_projects


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    DEBUG = True
    projects = get_projects()
    print(f"\n{'='*55}")
    print(f"RESULTS — {len(projects)} qualifying projects")
    print(f"{'='*55}\n")
    for p in projects:
        print(f"  {p['score']:>3}  [{p['category'].upper()}]  {p['address']}")
        print(f"       {p['stage']}  |  {p['explanation']}")
        if p["developer_name"]:
            print(f"       Developer: {p['developer_name']} ({p['developer_status']})")
        print()

"""
Shared company name utilities for news search.

Canonical source for company name cleaning, generic-name detection,
and suffix stripping used by the backfill script, CRM reports, and scraper.
"""

# Suffixes to strip when generating search-friendly names
SUFFIXES = [
    " - hq",
    " - headquarters",
    " inc.",
    " inc",
    " corp.",
    " corp",
    " llc",
    " llp",
    " ltd",
    " co.",
    " co",
    " group",
    " company",
    " corporation",
]

# Companies with names too generic to search reliably
SKIP_NAMES = {
    "target",
    "compass",
    "summit",
    "frontier",
    "core",
    "legacy",
    "pinnacle",
    "premier",
    "sterling",
    "venture",
    "delta",
    "granite",
    "united",
    "national",
    "american",
    "pacific",
    "western",
    "southern",
    "central",
    "modern",
    "royal",
    "global",
    "metro",
    "universal",
    "general",
    "continental",
    "standard",
    "classic",
    "executive",
    "commercial",
}


def clean_company_name(name: str) -> str:
    """Strip common suffixes to get a search-friendly company name."""
    clean = name.strip()
    lower = clean.lower()
    for suffix in SUFFIXES:
        if lower.endswith(suffix):
            clean = clean[: -len(suffix)].strip()
            lower = clean.lower()
    return clean

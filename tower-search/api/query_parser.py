"""
Natural language query parser for tower search.
Handles typos (rapidfuzz), coordinates, tower IDs, city/state disambiguation.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from rapidfuzz import process, fuzz

from models import AmbiguousTerm, DisambiguationOption, ParsedFilters

# ---------------------------------------------------------------------------
# Static keyword maps
# ---------------------------------------------------------------------------

US_STATES: dict[str, str] = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}

STATE_ABBREV_TO_NAME: dict[str, str] = {v: k.title() for k, v in US_STATES.items()}
ALL_ABBREVS: set[str] = set(US_STATES.values())

# Generation: user input → filter prefix (True) or exact (False) + value
GENERATION_PATTERNS: list[tuple[str, str, bool]] = [
    # pattern, generation value, use_prefix
    ("5g standalone", "5G Standalone", False),
    ("5g non-standalone", "5G Non-Standalone", False),
    ("5g nsa", "5G Non-Standalone", False),
    ("4g advanced", "4G Advanced", False),
    ("4g lte advanced", "4G Advanced", False),
    ("lte advanced", "4G Advanced", False),
    ("5g nr", "5G", True),
    ("5g", "5G", True),
    ("nr", "5G Standalone", False),
    ("4g lte", "4G", True),
    ("lte", "4G", True),
    ("4g", "4G", True),
]

SITE_TYPE_KEYWORDS: dict[str, str] = {
    "macro": "Tower",
    "macro tower": "Tower",
    "cell tower": "Tower",
    "cell towers": "Tower",
    "small cell": "Small Cell",
    "small cells": "Small Cell",
    "smallcell": "Small Cell",
    "pico": "Pico Cell",
    "pico cell": "Pico Cell",
    "picocell": "Pico Cell",
    "das": "Distributed Antenna",
    "distributed antenna": "Distributed Antenna",
    "cow": "Cell on Wheels",
    "cell on wheels": "Cell on Wheels",
}

STATUS_KEYWORDS: dict[str, bool] = {
    "active": True,
    "inactive": False,
    "decommissioned": False,
    "offline": False,
}

AREA_KEYWORDS: dict[str, bool] = {
    "urban": False,     # rural=False means urban
    "suburban": False,
    "rural": True,
}

PROVIDER_KEYWORDS: dict[str, str] = {
    "t-mobile": "T-Mobile",
    "tmobile": "T-Mobile",
    "t mobile": "T-Mobile",
    "att": "AT&T",
    "at&t": "AT&T",
    "at t": "AT&T",
    "verizon": "Verizon",
    "vzw": "Verizon",
}

# Street address suffixes used for address detection.
# When one of these is found in a query, the preceding street name + suffix
# are extracted as an FTS address query instead of being fuzzy-matched as city names.
ADDRESS_SUFFIXES: frozenset[str] = frozenset({
    "street", "st",
    "avenue", "ave",
    "boulevard", "blvd",
    "road", "rd",
    "drive", "dr",
    "lane", "ln",
    "way",
    "place", "pl",
    "court", "ct",
    "circle", "cir",
    "parkway", "pkwy",
    "terrace", "ter",
    "highway", "hwy",
    "pike",
    "trail", "trl",
})

STOP_WORDS = {
    "towers", "tower", "in", "near", "around", "show", "find", "search",
    "me", "all", "the", "for", "and", "with", "of", "a", "an",
    # Full-form address suffixes: won't appear as real city names, safe to exclude
    # from city/state fuzzy matching. Short abbreviations (st, rd, dr…) are kept
    # out of STOP_WORDS to preserve multi-word city matches like "St. Louis".
    "street", "avenue", "boulevard", "road", "drive", "lane",
    "place", "court", "circle", "parkway", "terrace", "highway", "trail", "pike",
}

COORD_RE = re.compile(
    r"(-?\d{1,3}(?:\.\d+)?)\s*[,\s]\s*(-?\d{1,3}(?:\.\d+)?)"
    r"(?:\s+(?:within\s+)?(\d+(?:\.\d+)?)\s*(?:miles?|mi|km))?",
    re.IGNORECASE,
)

TOWER_ID_RE = re.compile(r"\b(\d{3}_\d{3}_\d+)\b")
SITE_ID_RE = re.compile(r"\b(\d{4,8})\b")
ZIPCODE_RE = re.compile(r"\b(\d{5})\b")

# Prefixes users may type before a numeric tower/site ID that should be stripped
# so the number is always treated as a site ID, not a zip code.
# Ordered longest-first so "site id" is removed before "site" or "id".
ID_PREFIX_RE = re.compile(
    r"\b(?:site\s+id|site\s+ID|site\s+Id|enb\s+id|enb\s+ID|enb|site|tower\s+id|tower\s+ID)\s*:?\s*",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# QueryParser
# ---------------------------------------------------------------------------

class QueryParser:
    """
    Parses free-text queries into structured filters.
    Requires DB-derived lookup dictionaries (call .init_from_db()).
    """

    def __init__(self) -> None:
        # city_name_lower → list of (city, state, count) — populated from DB
        self._city_index: dict[str, list[tuple[str, str, int]]] = {}
        # all unique city names (lowercased) in dataset
        self._all_cities: list[str] = []
        # states present in dataset (abbrevs)
        self._dataset_states: set[str] = set()

    def init_from_db(
        self,
        city_counts: list[tuple[str, str, int]],  # (city, state, count)
    ) -> None:
        """Call once at startup with rows from: SELECT city, state, count(*) …"""
        self._city_index = {}
        for city, state, count in city_counts:
            key = city.lower().strip()
            if key not in self._city_index:
                self._city_index[key] = []
            self._city_index[key].append((city, state, count))
            self._dataset_states.add(state)
        self._all_cities = list(self._city_index.keys())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse(self, raw: str, resolved: dict | None = None) -> tuple[ParsedFilters, list[AmbiguousTerm]]:  # noqa: C901
        """
        Returns (ParsedFilters, ambiguous_terms).
        If ambiguous_terms is non-empty the caller should show disambiguation UI.
        resolved: pre-resolved choices from the user e.g. {"city": "Portland", "state": "OR"}
        """
        resolved = resolved or {}
        text = raw.strip()
        filters = ParsedFilters()
        ambiguous: list[AmbiguousTerm] = []

        # 1. Coordinates
        coord_match = COORD_RE.search(text)
        if coord_match:
            try:
                lat = float(coord_match.group(1))
                lng = float(coord_match.group(2))
                if -90 <= lat <= 90 and -180 <= lng <= 180:
                    filters.lat = lat
                    filters.lng = lng
                    if coord_match.group(3):
                        filters.radius_miles = float(coord_match.group(3))
                    text = text[:coord_match.start()] + " " + text[coord_match.end():]
            except (ValueError, TypeError):
                pass

        # 2. Tower ID (format: MCC_MNC_SITEID)
        tid_match = TOWER_ID_RE.search(text)
        if tid_match:
            filters.tower_id = tid_match.group(1)
            text = text.replace(tid_match.group(0), " ")

        # 3 & 4. Numeric detection: site ID (4-8 digits) vs zipcode (exactly 5 digits).
        # Strip ID prefix keywords first (e.g. "ID", "eNB", "site id") so they don't
        # get mistaken for state abbreviations (ID = Idaho) and force zipcode mode.
        if not filters.tower_id:
            text, n_subs = ID_PREFIX_RE.subn(" ", text)
            # Also strip a bare "id" or "ID" token that immediately precedes digits
            if re.search(r'\bid\s+\d{4,8}\b', text, re.IGNORECASE):
                text = re.sub(r'\bid\s+', ' ', text, flags=re.IGNORECASE)

        # 5-digit numbers are ambiguous. Default to site ID unless there is explicit
        # location context (state name/abbreviation) elsewhere in the query, in which
        # case the number is more likely a zip code.
        if not filters.tower_id:
            sid_match = SITE_ID_RE.search(text)
            if sid_match:
                matched_num = sid_match.group(1)
                is_five_digit = len(matched_num) == 5
                if is_five_digit:
                    # Check for state context in the rest of the query
                    other_text = text.replace(sid_match.group(0), " ").strip()
                    other_lower = other_text.lower()
                    has_state_context = (
                        any(abbrev in other_text.upper().split() for abbrev in ALL_ABBREVS)
                        or any(name in other_lower for name in US_STATES)
                    )
                    if has_state_context:
                        filters.zipcode = matched_num
                    else:
                        filters.tower_id = matched_num
                else:
                    filters.tower_id = matched_num
                text = text.replace(sid_match.group(0), " ")

        # If we have a tower/site ID or zipcode as the whole query — return early
        if (filters.tower_id or filters.zipcode) and len(text.strip()) <= 3:
            return filters, ambiguous

        text_lower = text.lower()

        # 5. Generation (longest match first)
        for pattern, gen_value, use_prefix in GENERATION_PATTERNS:
            if pattern in text_lower:
                filters.generation = gen_value
                filters.generation_prefix = use_prefix
                text_lower = text_lower.replace(pattern, " ", 1)
                break

        # 6. Site type (longest match first, sorted by length desc, word-boundary aware)
        for kw, st_value in sorted(SITE_TYPE_KEYWORDS.items(), key=lambda x: -len(x[0])):
            # Use word boundaries to avoid "tower" matching inside "towers"
            pattern = r'\b' + re.escape(kw) + r's?\b'
            if re.search(pattern, text_lower):
                filters.site_type = st_value
                text_lower = re.sub(pattern, " ", text_lower, count=1)
                break

        # 7. Status
        for kw, active_val in STATUS_KEYWORDS.items():
            if re.search(r"\b" + kw + r"\b", text_lower):
                filters.active = active_val
                text_lower = re.sub(r"\b" + kw + r"\b", " ", text_lower)
                break

        # 8. Area
        for kw, rural_val in AREA_KEYWORDS.items():
            if re.search(r"\b" + kw + r"\b", text_lower):
                filters.rural = rural_val
                text_lower = re.sub(r"\b" + kw + r"\b", " ", text_lower)
                break

        # 8b. Provider (longest match first)
        for kw, provider_val in sorted(PROVIDER_KEYWORDS.items(), key=lambda x: -len(x[0])):
            if kw in text_lower:
                filters.provider = provider_val
                text_lower = text_lower.replace(kw, " ", 1)
                break

        # 8c. Address detection — extract street address fragment before city/state matching.
        # Prevents address words (e.g. "ellery", "street") from fuzzy-matching city names
        # (e.g. "ellery" → Keller TX, "street" → Street MD).
        # The extracted address is routed to fts_query; remaining tokens (city, state)
        # continue through normal location extraction below.
        if not filters.fts_query:
            _address_tokens, text_lower = self._extract_address_tokens(text_lower)
            if _address_tokens:
                filters.fts_query = " ".join(_address_tokens)

        # 9. Apply pre-resolved disambiguation choices
        if "state" in resolved:
            filters.state = resolved["state"]
        if "city" in resolved:
            filters.city = resolved["city"]

        # If both already resolved, skip further text matching
        if filters.state and filters.city:
            return filters, ambiguous

        # 10. State + city extraction from remaining text
        remaining = self._clean_tokens(text_lower)

        # Try multi-word combos (2-word then 1-word) against state names
        # Pass original (un-lowercased) text so abbreviation matching can check case
        filters, remaining, ambiguous = self._extract_location(
            filters, remaining, ambiguous, resolved, raw_text=text
        )

        # 11. If no structured match found and non-trivial text remains → FTS fallback
        # Don't overwrite fts_query already set by address detection (step 8c).
        leftover = " ".join(t for t in remaining if t not in STOP_WORDS)
        if leftover and not filters.fts_query and not any([
            filters.state, filters.city, filters.lat,
            filters.tower_id, filters.zipcode
        ]):
            filters.fts_query = leftover

        return filters, ambiguous

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _clean_tokens(self, text: str) -> list[str]:
        text = re.sub(r"[^\w\s\-]", " ", text)
        return [t for t in text.split() if t]

    def _extract_address_tokens(self, text: str) -> tuple[list[str], str]:
        """
        Find the first address suffix token in `text` and extract the street address
        fragment: the suffix plus any immediately preceding non-stop-word tokens
        (the street name, and optionally a house number).

        Returns (address_tokens, text_with_address_removed).
        Returns ([], text) if no suffix is found or no street name precedes it.

        Examples:
          "ellery street brooklyn ny"  → (["ellery", "street"], "brooklyn ny")
          "157 ellery st"              → (["157", "ellery", "st"], "")
          "happy jack rd"              → (["happy", "jack", "rd"], "")
          "brooklyn ny"                → ([], "brooklyn ny")   # no suffix
          "street"                     # bare suffix with no name → ([], …)
        """
        tokens = self._clean_tokens(text)

        # Find the index of the first address suffix token
        suffix_idx = next(
            (i for i, tok in enumerate(tokens) if tok in ADDRESS_SUFFIXES),
            None,
        )
        if suffix_idx is None:
            return [], text

        # Walk backwards from just before the suffix to find where the address starts.
        # Stop at stop words (e.g. "near", "in") or the beginning of the token list.
        address_start = 0
        for i in range(suffix_idx - 1, -1, -1):
            if tokens[i] in STOP_WORDS:
                address_start = i + 1
                break

        # Street name tokens (excluding the suffix itself).
        # The suffix is intentionally dropped from the FTS query: DB addresses use
        # abbreviated forms ("St", "Ave", "Blvd") while users type full words
        # ("street", "avenue"), so including the suffix would cause FTS misses.
        # The street name alone ("ellery", "happy jack") is sufficient to find matches.
        name_tokens = tokens[address_start: suffix_idx]

        # Require at least one street name token before the suffix.
        # Prevents a bare suffix like "st" with nothing before it from triggering
        # address mode.
        if not name_tokens:
            return [], text

        # Reconstruct text without all the address tokens (name + suffix)
        remaining_tokens = tokens[:address_start] + tokens[suffix_idx + 1:]
        remaining_text = " ".join(remaining_tokens)
        return name_tokens, remaining_text

    def _extract_location(
        self,
        filters: ParsedFilters,
        tokens: list[str],
        ambiguous: list[AmbiguousTerm],
        resolved: dict,
        raw_text: str = "",
    ) -> tuple[ParsedFilters, list[str], list[AmbiguousTerm]]:
        used: set[int] = set()

        # --- Try to find state abbreviation ---
        # Only match 2-letter abbreviations if they appear UPPERCASE in the original
        # query text (prevents "in" → Indiana, "or" → Oregon false matches)
        state_from_abbrev: str | None = None
        for i, tok in enumerate(tokens):
            upper = tok.upper()
            if upper in ALL_ABBREVS and upper in self._dataset_states:
                # Require it to be uppercase in the original text
                if raw_text and re.search(r'\b' + upper + r'\b', raw_text):
                    state_from_abbrev = upper
                    used.add(i)
                    break

        # --- Try 2-word then 1-word against known state names (fuzzy) ---
        if not state_from_abbrev and "state" not in resolved:
            n = len(tokens)
            for length in (3, 2, 1):
                for start in range(n - length + 1):
                    if any(j in used for j in range(start, start + length)):
                        continue
                    candidate = " ".join(tokens[start:start + length])
                    # Skip stop words and very short tokens (prevent "in" → Indiana)
                    if candidate in STOP_WORDS or len(candidate) < 3:
                        continue
                    if any(t in STOP_WORDS for t in tokens[start:start + length]):
                        continue
                    matched_abbrev = self._fuzzy_match_state(candidate)
                    if matched_abbrev and matched_abbrev in self._dataset_states:
                        filters.state = matched_abbrev
                        for j in range(start, start + length):
                            used.add(j)
                        break
                if filters.state:
                    break
            if not filters.state and state_from_abbrev:
                filters.state = state_from_abbrev
        elif state_from_abbrev and "state" not in resolved:
            filters.state = state_from_abbrev

        # --- City extraction from remaining tokens ---
        if "city" not in resolved and self._all_cities:
            remaining_tokens = [t for i, t in enumerate(tokens) if i not in used and t not in STOP_WORDS]
            city_result = self._extract_city(remaining_tokens, filters.state)
            if city_result is not None:
                city_str, city_used_idx, options = city_result
                if len(options) == 1 or filters.state:
                    # Unambiguous (or state already constrains it)
                    chosen = options[0] if len(options) == 1 else next(
                        (o for o in options if o[1] == filters.state), options[0]
                    )
                    filters.city = chosen[0]
                    if not filters.state:
                        filters.state = chosen[1]
                    for idx in city_used_idx:
                        used.add(idx)
                elif len(options) > 1:
                    # Multiple states — disambiguation needed
                    ambiguous.append(AmbiguousTerm(
                        term=city_str,
                        field="city",
                        options=[
                            DisambiguationOption(city=c, state=s, count=cnt)
                            for c, s, cnt in options[:6]
                        ],
                    ))
                    for idx in city_used_idx:
                        used.add(idx)

        remaining_out = [t for i, t in enumerate(tokens) if i not in used]
        return filters, remaining_out, ambiguous

    def _fuzzy_match_state(self, candidate: str) -> str | None:
        """Returns state abbreviation if candidate fuzzy-matches a US state name."""
        matches = process.extractOne(
            candidate,
            list(US_STATES.keys()),
            scorer=fuzz.WRatio,
            score_cutoff=82,
        )
        if matches:
            return US_STATES[matches[0]]
        return None

    def _extract_city(
        self,
        tokens: list[str],
        known_state: str | None,
    ) -> tuple[str, list[int], list[tuple[str, str, int]]] | None:
        """
        Try to match tokens (2-word then 1-word) against known city names.
        Returns (matched_city_string, token_indices_used, [(city, state, count), ...]) or None.
        The options list is ordered by count desc.
        """
        n = len(tokens)
        for length in (3, 2, 1):
            for start in range(n - length + 1):
                candidate = " ".join(tokens[start:start + length])
                if candidate in STOP_WORDS:
                    continue

                # Exact match first
                exact = candidate.lower()
                if exact in self._city_index:
                    options = sorted(self._city_index[exact], key=lambda x: -x[2])
                    # Filter to known state if given
                    if known_state:
                        state_options = [o for o in options if o[1] == known_state]
                        if state_options:
                            return candidate, list(range(start, start + length)), state_options
                    return candidate, list(range(start, start + length)), options

                # Fuzzy match (cutoff 78 handles common transpositions like "Mimai"→"Miami")
                best = process.extractOne(
                    exact,
                    self._all_cities,
                    scorer=fuzz.WRatio,
                    score_cutoff=78,
                )
                if best:
                    options = sorted(self._city_index[best[0]], key=lambda x: -x[2])
                    if known_state:
                        state_options = [o for o in options if o[1] == known_state]
                        if state_options:
                            return candidate, list(range(start, start + length)), state_options
                    return candidate, list(range(start, start + length)), options

        return None

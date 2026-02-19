import re
import requests
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any
from datetime import datetime

logger = logging.getLogger(__name__)

GAMMA_SEARCH_URL = "https://gamma-api.polymarket.com/public-search"


def _slugify(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"[^a-z0-9-]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.rstrip("-") or "unknown"


@dataclass
class MarketInfo:
    asset_id: str
    event_slug: str
    market_title: str
    event_title: str
    condition_id: str
    outcome_label: str = ""
    market_slug: str = field(init=False)

    def __post_init__(self):
        self.market_slug = _slugify(self.market_title)


class MarketDiscovery:
    def __init__(self):
        self.known_assets: dict[str, MarketInfo] = {}
        # stores cache_key -> (value, timestamp)
        self._details_cache: dict[str, Any] = {}

    def _cache_get(self, key: str):
        return self._details_cache.get(key)

    def _cache_set(self, key: str, value: Any):
        self._details_cache[key] = value

    def discover(self, queries: list[str]) -> list[MarketInfo]:
        new_markets = []
        for query in queries:
            try:
                events = self._search_events(query)
            except Exception:
                logger.exception(f"Error searching for query: {query}")
                continue

            slug_query = _slugify(query)
            for event in events:
                try:
                    # skip events that have no open markets
                    mkts = event.get("markets", []) or []
                    if not any(not (m.get("closed") or m.get("archived")) for m in mkts):
                        continue

                    new_from_event = self._extract_yes_tokens(event)
                    new_markets.extend(new_from_event)
                except Exception:
                    logger.exception(f"Error processing event {event.get('slug')} for query: {query}")
        return new_markets

    def _search_events(self, query: str) -> list[dict]:
        params = {
            "q": query,
            "limit_per_type": 50,
            "optimized": "true",
            "sort": "startTime",
            "ascending": "false",
            "events_status": "active",
            "keep_closed_markets": 0,
        }

        slug_query = _slugify(query)
        slug_like = (query == slug_query) and ("-" in query or "_" in query)

        open_events = []
        for page in range(1, 4):                    # page 1..3
            p = dict(params, page=page)
            resp = requests.get(GAMMA_SEARCH_URL, params=p, timeout=15)
            resp.raise_for_status()
            events = resp.json().get("events", []) or []
            for e in events:
                mkts = e.get("markets", []) or []
                if any(not (m.get("closed") or m.get("archived")) for m in mkts):
                    open_events.append(e)
            if open_events:
                break                              # stop early if we found open events

        return open_events



    def _load_json_if_str(self, value: Any, name: str = "") -> Any:
        if value is None:
            return []
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return [value]
        return value

    def _normalize_outcome(self, outcome: Any) -> str:
        if outcome is None:
            return ""
        if isinstance(outcome, dict):
            label = outcome.get("label") or outcome.get("name") or outcome.get("value") or outcome.get("id")
            if label is not None:
                return str(label).strip().lower()
            return str(outcome).strip().lower()
        if isinstance(outcome, bool):
            return "true" if outcome else "false"
        if isinstance(outcome, (int, float)):
            return str(outcome)
        return str(outcome).strip().lower()

    def _extract_yes_tokens(self, event: dict) -> list[MarketInfo]:
        new_markets = []
        event_slug = event.get("slug", "")
        # prefer numeric id for detail endpoints when available
        event_id = event.get("id") or event.get("eventId") or event.get("event_id")
        event_title = event.get("title", "") or ""

        # If any market is missing clobTokenIds, fetch all markets once for this event
        event_lookup_key = event_id or event_slug
        event_markets_map = None
        mkts_list = event.get("markets", []) or []
        # Removed: event-level fetch fallback for simplicity

        for market in mkts_list:
            try:
                if market.get("closed") or market.get("archived"):
                    continue

                # read market fields first
                market_title = market.get("groupItemTitle", market.get("question", "Unknown")) or "Unknown"
                condition_id = market.get("conditionId", "")

                raw_token_ids = market.get("clobTokenIds", [])
                raw_outcomes = market.get("outcomes", [])

                token_ids = self._load_json_if_str(raw_token_ids, "clobTokenIds") or []
                outcomes = self._load_json_if_str(raw_outcomes, "outcomes") or []

                # If token_ids empty, fetch market details to try to obtain them
                if not token_ids:
                    try:
                        fetched = self._fetch_market_details(event_id or event_slug, market_title)
                        token_ids = self._load_json_if_str(fetched, "fetched_clobTokenIds") or []
                    except Exception:
                        pass

                # Ensure lists
                if not isinstance(token_ids, list):
                    token_ids = [token_ids]
                if not isinstance(outcomes, list):
                    outcomes = [outcomes]

                for i, token_id in enumerate(token_ids):
                    try:
                        token_id_str = str(token_id).strip()
                        outcome = outcomes[i] if i < len(outcomes) else ""
                        outcome_norm = self._normalize_outcome(outcome)

                        if token_id_str and token_id_str not in self.known_assets:
                            info = MarketInfo(
                                asset_id=token_id_str,
                                event_slug=event_slug,
                                market_title=market_title,
                                event_title=event_title,
                                condition_id=condition_id,
                                outcome_label=outcome_norm,
                            )
                            self.known_assets[token_id_str] = info
                            new_markets.append(info)
                            logger.info("Discovered: %s / %s [%s]", event_title, market_title, outcome_norm)
                    except Exception:
                        logger.exception("Error processing token/outcome for market %s", market.get("id"))
                        continue
            except Exception:
                logger.exception("Error processing market in event %s", event.get("slug"))
                continue

        return new_markets

    def get_market_info(self, asset_id: str) -> MarketInfo | None:
        return self.known_assets.get(str(asset_id).strip())

    def _fetch_market_details(self, event_slug: str, market_title: str | None = None) -> list | None:
        """
        Fetch event details and return clobTokenIds for matching market or first market.
        """
        if not event_slug:
            return None
        market_key = _slugify(market_title) if market_title else ""
        cache_key = f"{event_slug}::{market_key}"
        val = self._cache_get(cache_key)
        if val is not None:
            return val

        try:
            url = f"https://gamma-api.polymarket.com/events/{event_slug}"
            resp = requests.get(url, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                mkts = data.get("markets", []) or []
                if market_title:
                    mt_lower = market_title.lower()
                    # Try various matching strategies
                    for m in mkts:
                        m_title = (m.get("groupItemTitle") or m.get("question") or "").lower()
                        m_slug = _slugify(m.get("slug") or m.get("groupItemTitle") or m.get("question") or "")
                        if (mt_lower == m_title or 
                            mt_lower in m_title or 
                            market_title in m_title or
                            m_slug == _slugify(market_title) or
                            _slugify(market_title) in m_slug):
                            val = m.get("clobTokenIds") or m.get("clob_token_ids") or None
                            self._cache_set(cache_key, val)
                            return val
                # fallback: first market
                if mkts:
                    val = mkts[0].get("clobTokenIds") or mkts[0].get("clob_token_ids") or None
                    self._cache_set(cache_key, val)
                    return val
        except Exception:
            pass

        self._cache_set(cache_key, None)
        return None

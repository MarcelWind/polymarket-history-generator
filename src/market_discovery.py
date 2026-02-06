import re
import requests
import json
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

GAMMA_SEARCH_URL = "https://gamma-api.polymarket.com/public-search"


def _slugify(text: str) -> str:
    text = text.lower()
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
    market_slug: str = field(init=False)

    def __post_init__(self):
        self.market_slug = _slugify(self.market_title)


class MarketDiscovery:
    def __init__(self):
        self.known_assets: dict[str, MarketInfo] = {}

    def discover(self, queries: list[str]) -> list[MarketInfo]:
        new_markets = []
        for query in queries:
            try:
                events = self._search_events(query)
                for event in events:
                    event_slug = event.get("slug", "")
                    if not event_slug.startswith(query):
                        continue
                    new_from_event = self._extract_yes_tokens(event)
                    new_markets.extend(new_from_event)
            except Exception:
                logger.exception(f"Error discovering markets for query: {query}")
        return new_markets

    def _search_events(self, query: str) -> list[dict]:
        resp = requests.get(
            GAMMA_SEARCH_URL,
            params={"q": query, "limit_per_type": 50},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("events", [])

    def _extract_yes_tokens(self, event: dict) -> list[MarketInfo]:
        new_markets = []
        event_slug = event.get("slug", "")
        event_title = event.get("title", "")

        for market in event.get("markets", []):
            if market.get("closed") or market.get("archived"):
                continue

            token_ids = json.loads(market.get("clobTokenIds", "[]"))
            outcomes = json.loads(market.get("outcomes", "[]"))
            market_title = market.get(
                "groupItemTitle", market.get("question", "Unknown")
            )
            condition_id = market.get("conditionId", "")

            for i, token_id in enumerate(token_ids):
                outcome = outcomes[i] if i < len(outcomes) else ""
                if outcome.lower() == "yes" and token_id not in self.known_assets:
                    info = MarketInfo(
                        asset_id=token_id,
                        event_slug=event_slug,
                        market_title=market_title,
                        event_title=event_title,
                        condition_id=condition_id,
                    )
                    self.known_assets[token_id] = info
                    new_markets.append(info)
                    logger.info(
                        f"Discovered: {event_title} / {market_title}"
                    )

        return new_markets

    def get_market_info(self, asset_id: str) -> MarketInfo | None:
        return self.known_assets.get(asset_id)

"""The Focus-market filter (issue #108): keep only wallets specialized in a
market — a whole category (CRYPTO / STOCKS / METALS / ENERGY) or one specific
ticker.

The two modes deliberately mean different things. A category qualifies a
wallet when more than half of its completed round-trips are in that category's
tickers — "trades mostly metals" is majority share. A specific ticker
qualifies a wallet when the ticker sits in its top-2 most-played coins per the
shared #80 ranking (epigone.plays) — the same ranking the profile's "Most
played" line shows. Wallets with no fine data never qualify in either mode.

The ticker→category map is curated in code, like the starter presets
(epigone.criteria_presets): versioned, recalibrated by normal PR. It was built
from Hyperliquid's live instrument universe on 2026-07-24 — the core perp meta
plus every HIP-3 perp dex (xyz, flx, vntl, hyna, km, abcd, cash, para, mkts),
delisted tickers included because stored round-trips still reference them.
Two safe defaults keep it honest: any core (unprefixed) coin is CRYPTO
automatically, and a dex-prefixed ticker missing from the map is
uncategorized — it counts toward no category, never silently miscounted,
until a PR maps it. The map keys on the full prefixed name because the same
bare ticker can mean different markets per venue (flx:GAS was natural gas;
core GAS is Neo Gas).

Classification follows the instrument, not the theme: an energy-sector ETF
(xyz:XLE), a gold-miners index (km:GLDMINE) or a pre-IPO listing
(vntl:OPENAI) is STOCKS; only commodity contracts land in METALS/ENERGY;
crypto-market indices (para:BTCD, para:TOTAL2) are CRYPTO; FX, rates, vol and
agriculture fit no category and stay unmapped.

A Focus-market filter stores its choice in the Filter's threshold string —
``cat:METALS`` or ``tick:SILVER`` — so the existing JSONB shape and saved
criteria re-runs carry it unchanged.
"""

import re
from enum import Enum

import asyncpg

from epigone.plays import RANKED_PLAYS_SQL

# The Metric Library key of the focus-market filter — the one non-numeric
# filter, so criteria_store and the builder branch on it.
FOCUS_MARKET_KEY = "focus_market"

# Ticker mode passes when the ticker is within this rank of the wallet's
# most-played coins (#80's ranking, top-2 per the issue).
TOP_PLAYED_RANK = 2


class Category(Enum):
    """A market a wallet can specialize in. The value is what the threshold
    string carries (cat:METALS) — stable once shipped, like a preset key."""

    CRYPTO = "CRYPTO"
    STOCKS = "STOCKS"
    METALS = "METALS"
    ENERGY = "ENERGY"

    @property
    def label(self) -> str:
        return self.name.capitalize()

    @property
    def emoji(self) -> str:
        return _CATEGORY_EMOJI[self]


_CATEGORY_EMOJI = {
    Category.CRYPTO: "🪙",
    Category.STOCKS: "📈",
    Category.METALS: "🥇",
    Category.ENERGY: "🛢",
}


def _market(category: Category, dex: str, *tickers: str) -> dict[str, Category]:
    return {f"{dex}:{t}": category for t in tickers}


def _stocks(dex: str, *tickers: str) -> dict[str, Category]:
    return _market(Category.STOCKS, dex, *tickers)


def _crypto(dex: str, *tickers: str) -> dict[str, Category]:
    return _market(Category.CRYPTO, dex, *tickers)


def _metals(dex: str, *tickers: str) -> dict[str, Category]:
    return _market(Category.METALS, dex, *tickers)


def _energy(dex: str, *tickers: str) -> dict[str, Category]:
    return _market(Category.ENERGY, dex, *tickers)


def _kinetiq(dex: str) -> dict[str, Category]:
    """Markets by Kinetiq listed the same instruments under two dex names (km,
    mkts) — one definition, fanned to both, so the twins can't drift."""
    return {
        **_stocks(
            dex,
            *"AAPL BABA BMNR GLDMINE GOOGL JPN225 MU NVDA PLTR RTX SEMI SMALL2000".split(),
            *"TENCENT TSLA US500 USENERGY USTECH XIAOMI".split(),
        ),
        **_metals(dex, "GOLD", "SILVER"),
        **_energy(dex, "USOIL"),
    }


# Deliberately unmapped (uncategorized): FX and rates (xyz:EUR/GBP/JPY/KRW/DXY,
# km/mkts:EUR/USBOND), agriculture (xyz:CORN/WHEAT, vntl:SOY/WHEAT), volatility
# (xyz:VIX/VOL), tech-commodity price indices (xyz:DRAM, xyz:H100, para:H100)
# and tickers whose underlying we couldn't pin down (xyz:BOT/SHAZ/PURRDAT,
# cash:CAR). They count toward no category until a PR maps them.
DEX_TICKER_CATEGORIES: dict[str, Category] = {
    # xyz — equities, equity indices/ETFs, pre-IPO names, and the commodity row
    **_stocks(
        "xyz",
        *"AAPL AMAT AMD AMZN ARM ASML AVGO BABA BB BE BIRD BX CBRS COIN COST CRCL".split(),
        *"CRWV CXMT DELL DKNG EBAY EWJ EWT EWY EWZ GEV GIGADEV GME GOOGL HIMS HOOD".split(),
        *"HYUNDAI IBIDEN IBM IBOV INTC JP225 KIOXIA KR200 KSTR LITE LLY META MINIMAX".split(),
        *"MRVL MSFT MSTR MU NBIS NFLX NIFTY NOK NOW NVDA ORCL PLTR QCOM QNT RIVN RKLB".split(),
        *"SKHX SKHY SMH SMSN SNDK SOFTBANK SP500 SPCX STRC TSLA TSM URNM USAR WDC XLE".split(),
        *"XYZ100 ZHIPU ZM".split(),
    ),
    **_metals("xyz", "GOLD", "SILVER", "COPPER", "PLATINUM", "PALLADIUM", "ALUMINIUM"),
    **_energy("xyz", "CL", "BRENTOIL", "NATGAS", "TTF", "URANIUM"),
    # hyna — a crypto dex, plus its retired gold/silver markets
    **_crypto(
        "hyna",
        *"1000PEPE ADA BASED BCH BNB BTC DOGE ENA ETH FARTCOIN HYPE IP LIGHTER LINK".split(),
        *"LIT LTC PUMP SOL SUI XMR XPL XRP ZEC".split(),
    ),
    **_metals("hyna", "GOLD", "SILVER"),
    # para — semis/tech equities plus crypto-market indices
    **_stocks("para", "AVGO", "COHR", "CRDO", "GLW", "IREN", "LRCX", "NET", "STX"),
    **_crypto("para", "BTCD", "OTHERS", "TOTAL2"),
    # km and mkts — the same Markets-by-Kinetiq listings under two dex names
    **_kinetiq("km"),
    **_kinetiq("mkts"),
    # flx — retired Felix Exchange markets
    **_crypto("flx", "BTC", "USDE", "XMR"),
    **_stocks("flx", "COIN", "CRCL", "NVDA", "TSLA", "USA100", "USA500"),
    **_metals("flx", "COPPER", "GOLD", "PALLADIUM", "PLATINUM", "SILVER"),
    **_energy("flx", "GAS", "OIL"),
    # vntl — retired Ventuals pre-IPO and thematic-index markets
    **_stocks(
        "vntl",
        *"ANTHROPIC BIOTECH DEFENSE ENERGY INFOTECH MAG7 NUCLEAR OPENAI ROBOT".split(),
        *"SEMIS SPACEX".split(),
    ),
    **_metals("vntl", "GOLDJM", "SILVERJM"),
    # cash — retired dreamcash markets
    **_stocks(
        "cash", *"AMZN EWY GOOGL HOOD INTC KWEB META MSFT NVDA TSLA USA500".split()
    ),
    **_crypto("cash", "BTC", "ETH"),
    **_metals("cash", "GOLD", "SILVER"),
    **_energy("cash", "WTI"),
    # abcd — one retired market
    **_stocks("abcd", "USA500"),
}

_BY_UPPER_NAME = {name.upper(): category for name, category in DEX_TICKER_CATEGORIES.items()}


def categorize(coin: str) -> Category | None:
    """A stored coin → its market, or None for uncategorized. Core coins are
    CRYPTO by rule; dex-prefixed tickers come from the curated map."""
    if ":" not in coin:
        return Category.CRYPTO
    return _BY_UPPER_NAME.get(coin.upper())


def category_coins(category: Category) -> list[str]:
    """The category's curated dex tickers, uppercased for the SQL match. Core
    coins are not listed — CRYPTO adds them by the no-prefix rule in SQL."""
    return sorted(name for name, c in _BY_UPPER_NAME.items() if c is category)


# --- threshold encoding (the Filter's JSONB threshold string) -----------------

_CATEGORY_PREFIX = "cat:"
_TICKER_PREFIX = "tick:"


def category_threshold(category: Category) -> str:
    return f"{_CATEGORY_PREFIX}{category.value}"


def ticker_threshold(ticker: str) -> str:
    return f"{_TICKER_PREFIX}{ticker}"


def parse_focus(threshold: str) -> Category | str | None:
    """A stored threshold → the Category or bare ticker it names; None when
    malformed (an unknown category name, an empty ticker, no prefix)."""
    if threshold.startswith(_CATEGORY_PREFIX):
        try:
            return Category(threshold.removeprefix(_CATEGORY_PREFIX))
        except ValueError:
            return None
    if threshold.startswith(_TICKER_PREFIX):
        return threshold.removeprefix(_TICKER_PREFIX) or None
    return None


def focus_label(threshold: str) -> str:
    """What a focus filter reads as in the builder and summaries:
    "Focus market: Metals" / "Focus market: SILVER"."""
    target = parse_focus(threshold)
    named = target.label if isinstance(target, Category) else target
    return f"Focus market: {named}"


_TICKER_PATTERN = re.compile(r"^[A-Z0-9]{1,20}$")


def normalize_ticker(text: str) -> str | None:
    """A typed ticker → its canonical bare form (silver → SILVER,
    xyz:sp500 → SP500), or None when it doesn't read as one."""
    bare = text.strip().rsplit(":", 1)[-1].upper()
    return bare if _TICKER_PATTERN.fullmatch(bare) else None


# --- the screener condition ----------------------------------------------------

# A stored coin's bare ticker, venue prefix stripped, for case-insensitive
# matching — the SQL twin of the display-side rsplit(":", 1)[-1].
_BARE_COIN_SQL = (
    "upper(CASE WHEN strpos(coin, ':') > 0 THEN split_part(coin, ':', 2) ELSE coin END)"
)


def focus_condition(threshold: object, params: list[object]) -> str:
    """The WHERE fragment for one focus-market filter, appending its
    parameters to `params`. Raises KeyError on a malformed threshold, like an
    unknown metric key elsewhere in the screener."""
    target = parse_focus(str(threshold))
    if isinstance(target, Category):
        return _category_condition(target, params)
    if target is None:
        raise KeyError(f"malformed focus-market threshold: {threshold!r}")
    return _ticker_condition(target, params)


def _category_condition(category: Category, params: list[object]) -> str:
    """Majority share: more than half of the wallet's completed round-trips in
    the category's tickers. Strict — a 50/50 split does not qualify — and a
    wallet with no round-trips aggregates to 0 > 0, so it never qualifies.
    Uncategorized coins inflate only the denominator."""
    params.append(category_coins(category))
    member = f"upper(coin) = ANY(${len(params)}::text[])"
    if category is Category.CRYPTO:
        member = f"(strpos(coin, ':') = 0 OR {member})"
    return (
        " AND t.address IN (SELECT address FROM fine_trades GROUP BY address"
        f" HAVING count(*) FILTER (WHERE {member}) * 2 > count(*))"
    )


def _ticker_condition(ticker: str, params: list[object]) -> str:
    """Top-2 membership per the shared #80 ranking — round-trips per coin plus
    the open-episode bonus, coin-name tiebreak (epigone.plays)."""
    params.append(ticker.upper())
    return (
        f" AND t.address IN (SELECT address FROM ({RANKED_PLAYS_SQL}) plays"
        f" WHERE play_rank <= {TOP_PLAYED_RANK} AND {_BARE_COIN_SQL} = ${len(params)})"
    )


async def ticker_seen(pool: asyncpg.Pool, ticker: str) -> bool:
    """Whether any analyzed wallet has ever played this bare ticker — the
    builder answers an unknown ticker helpfully instead of saving a filter
    that silently matches nobody."""
    seen = await pool.fetchval(
        f"""
        SELECT EXISTS (
            SELECT 1 FROM (
                SELECT coin FROM fine_trades
                UNION ALL
                SELECT coin FROM fine_open_episodes
            ) plays
            WHERE {_BARE_COIN_SQL} = $1
        )
        """,
        ticker.upper(),
    )
    return bool(seen)

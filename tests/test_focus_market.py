"""Focus-market classification and threshold plumbing (issue #108): every
ticker in Hyperliquid's live universe maps to CRYPTO / STOCKS / METALS /
ENERGY or stays deliberately uncategorized; thresholds round-trip through the
cat:/tick: string encoding; typed tickers normalize prefix- and
case-insensitively."""

from epigone.focus_market import (
    DEX_TICKER_CATEGORIES,
    Category,
    categorize,
    category_threshold,
    normalize_ticker,
    parse_focus,
    ticker_threshold,
)

# --- categorize: core coins ---------------------------------------------------


def test_core_coins_are_crypto_automatically() -> None:
    # Any unprefixed coin is a core perp — CRYPTO without needing a map entry.
    assert categorize("BTC") is Category.CRYPTO
    assert categorize("kPEPE") is Category.CRYPTO
    assert categorize("FARTCOIN") is Category.CRYPTO
    assert categorize("0G") is Category.CRYPTO


# --- categorize: curated dex tickers -------------------------------------------


def test_dex_tickers_classify_by_the_curated_map() -> None:
    assert categorize("xyz:SILVER") is Category.METALS
    assert categorize("xyz:GOLD") is Category.METALS
    assert categorize("xyz:CL") is Category.ENERGY
    assert categorize("xyz:BRENTOIL") is Category.ENERGY
    assert categorize("xyz:NATGAS") is Category.ENERGY
    assert categorize("xyz:AAPL") is Category.STOCKS
    assert categorize("xyz:SP500") is Category.STOCKS
    assert categorize("hyna:BTC") is Category.CRYPTO
    assert categorize("mkts:US500") is Category.STOCKS


def test_the_same_bare_ticker_can_mean_different_markets_per_venue() -> None:
    # flx:GAS was Felix's natural-gas market; core GAS is Neo Gas, a crypto
    # coin. Keying the map on the full prefixed name keeps them apart.
    assert categorize("GAS") is Category.CRYPTO
    assert categorize("flx:GAS") is Category.ENERGY


def test_crypto_index_products_count_as_crypto() -> None:
    assert categorize("para:BTCD") is Category.CRYPTO
    assert categorize("para:TOTAL2") is Category.CRYPTO


def test_equity_products_are_stocks_even_when_themed_on_a_commodity() -> None:
    # Instrument over theme: an energy-sector ETF or a gold-miners index is an
    # equity product, not the commodity itself.
    assert categorize("xyz:XLE") is Category.STOCKS
    assert categorize("xyz:URNM") is Category.STOCKS
    assert categorize("km:GLDMINE") is Category.STOCKS


def test_delisted_dex_tickers_stay_classified_for_wallet_history() -> None:
    # Fully-delisted dexs (flx, vntl, km, cash…) still appear in stored
    # round-trips, so their tickers keep their categories.
    assert categorize("flx:SILVER") is Category.METALS
    assert categorize("cash:WTI") is Category.ENERGY
    assert categorize("vntl:OPENAI") is Category.STOCKS
    assert categorize("hyna:GOLD") is Category.METALS


def test_unmappable_markets_are_uncategorized() -> None:
    # FX, rates, agri, vol and unknown dex tickers count toward no category —
    # never silently miscounted (issue #108).
    assert categorize("xyz:EUR") is None
    assert categorize("xyz:DXY") is None
    assert categorize("km:USBOND") is None
    assert categorize("xyz:CORN") is None
    assert categorize("xyz:NEVERHEARDOFIT") is None
    assert categorize("newdex:ANYTHING") is None


def test_categorize_ignores_case() -> None:
    assert categorize("XYZ:silver") is Category.METALS
    assert categorize("Hyna:btc") is Category.CRYPTO


def test_every_curated_entry_is_dex_prefixed() -> None:
    # Core coins are CRYPTO by rule, never by map entry — a bare key would be
    # dead weight that suggests otherwise.
    assert all(":" in name for name in DEX_TICKER_CATEGORIES)


# --- threshold encoding ---------------------------------------------------------


def test_category_threshold_round_trips() -> None:
    assert category_threshold(Category.METALS) == "cat:METALS"
    assert parse_focus("cat:METALS") is Category.METALS


def test_ticker_threshold_round_trips() -> None:
    assert ticker_threshold("SILVER") == "tick:SILVER"
    assert parse_focus("tick:SILVER") == "SILVER"


def test_malformed_thresholds_parse_to_none() -> None:
    assert parse_focus("cat:BOGUS") is None
    assert parse_focus("tick:") is None
    assert parse_focus("42") is None


# --- ticker input normalization --------------------------------------------------


def test_typed_tickers_trim_upcase_and_drop_venue_prefixes() -> None:
    assert normalize_ticker(" silver ") == "SILVER"
    assert normalize_ticker("xyz:sp500") == "SP500"
    assert normalize_ticker("1000pepe") == "1000PEPE"


def test_unreadable_tickers_normalize_to_none() -> None:
    assert normalize_ticker("") is None
    assert normalize_ticker("   ") is None
    assert normalize_ticker("not a ticker") is None
    assert normalize_ticker("btc!") is None
    assert normalize_ticker("xyz:") is None

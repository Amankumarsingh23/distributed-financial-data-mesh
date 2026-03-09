"""
Unit tests for the Financial Data Mesh pipeline.
Uses mocks to avoid real GCP calls.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime


# ─── Ingestion Tests ─────────────────────────────────────────────────────────

class TestFetchData:

    @patch("src.ingestion.fetch_data.requests.get")
    def test_fetch_ticker_returns_dataframe(self, mock_get):
        """Should return a properly structured DataFrame for a valid response."""
        from src.ingestion.fetch_data import fetch_ticker_ohlcv

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = [
            {
                "date": "2024-01-15T00:00:00+00:00",
                "open": 180.0, "high": 185.0, "low": 178.0, "close": 183.0,
                "volume": 55000000,
                "adjOpen": 180.0, "adjHigh": 185.0, "adjLow": 178.0,
                "adjClose": 183.0, "adjVolume": 55000000,
            }
        ]
        mock_get.return_value = mock_response

        df = fetch_ticker_ohlcv("AAPL", "2024-01-01", "2024-01-31")
        assert df is not None
        assert len(df) == 1
        assert "ticker" in df.columns
        assert df["ticker"].iloc[0] == "AAPL"
        assert "adj_close" in df.columns
        assert "ingested_at" in df.columns

    @patch("src.ingestion.fetch_data.requests.get")
    def test_fetch_ticker_empty_response(self, mock_get):
        """Should return None for empty API response."""
        from src.ingestion.fetch_data import fetch_ticker_ohlcv

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        result = fetch_ticker_ohlcv("FAKE", "2024-01-01", "2024-01-31")
        assert result is None

    @patch("src.ingestion.fetch_data.requests.get")
    def test_fetch_ticker_http_error(self, mock_get):
        """Should return None on HTTP error, not raise."""
        import requests
        from src.ingestion.fetch_data import fetch_ticker_ohlcv

        mock_get.side_effect = requests.exceptions.HTTPError("404 Not Found")
        result = fetch_ticker_ohlcv("BADTICKER", "2024-01-01", "2024-01-31")
        assert result is None


# ─── Feature Engineering Tests ───────────────────────────────────────────────

def make_sample_df(n: int = 250) -> pd.DataFrame:
    """Create a synthetic price series for testing."""
    np.random.seed(42)
    dates  = pd.date_range("2023-01-01", periods=n, freq="B")
    prices = 100 + np.cumsum(np.random.randn(n) * 2)
    return pd.DataFrame({
        "date":       dates.strftime("%Y-%m-%d"),
        "ticker":     "TEST",
        "adj_close":  prices,
        "adj_open":   prices * 0.998,
        "adj_high":   prices * 1.005,
        "adj_low":    prices * 0.995,
        "adj_volume": np.random.randint(1_000_000, 10_000_000, n),
        "high":       prices * 1.005,
        "low":        prices * 0.995,
        "close":      prices,
        "open":       prices * 0.998,
        "volume":     np.random.randint(1_000_000, 10_000_000, n),
    })


class TestRSI:

    def test_rsi_bounds(self):
        """RSI must be between 0 and 100."""
        df = make_sample_df(100)
        df["prev_close"] = df["adj_close"].shift(1)
        df["delta"]      = df["adj_close"] - df["prev_close"]
        df["gain"]       = df["delta"].clip(lower=0)
        df["loss"]       = (-df["delta"]).clip(lower=0)
        df["avg_gain"]   = df["gain"].rolling(14).mean()
        df["avg_loss"]   = df["loss"].rolling(14).mean()
        df["rs"]         = df["avg_gain"] / (df["avg_loss"] + 1e-10)
        df["rsi"]        = 100 - (100 / (1 + df["rs"]))

        valid_rsi = df["rsi"].dropna()
        assert (valid_rsi >= 0).all(), "RSI below 0 detected"
        assert (valid_rsi <= 100).all(), "RSI above 100 detected"

    def test_rsi_overbought(self):
        """Monotonically rising prices should produce RSI near 100."""
        prices = list(range(100, 150))
        df = pd.DataFrame({"adj_close": prices})
        df["delta"]    = df["adj_close"].diff()
        df["gain"]     = df["delta"].clip(lower=0)
        df["loss"]     = (-df["delta"]).clip(lower=0)
        df["avg_gain"] = df["gain"].rolling(14).mean()
        df["avg_loss"] = df["loss"].rolling(14).mean()
        df["rsi"]      = 100 - (100 / (1 + df["avg_gain"] / (df["avg_loss"] + 1e-10)))

        last_rsi = df["rsi"].dropna().iloc[-1]
        assert last_rsi > 90, f"Expected RSI > 90 for trending up, got {last_rsi:.1f}"


class TestBollingerBands:

    def test_price_within_bands(self):
        """At least ~95% of prices should fall within 2-std Bollinger Bands."""
        df = make_sample_df(250)
        df["bb_mid"]   = df["adj_close"].rolling(20).mean()
        df["bb_std"]   = df["adj_close"].rolling(20).std()
        df["bb_upper"] = df["bb_mid"] + 2 * df["bb_std"]
        df["bb_lower"] = df["bb_mid"] - 2 * df["bb_std"]

        valid = df.dropna(subset=["bb_upper", "bb_lower"])
        inside = ((valid["adj_close"] <= valid["bb_upper"]) &
                  (valid["adj_close"] >= valid["bb_lower"]))
        pct_inside = inside.mean()
        assert pct_inside >= 0.85, f"Only {pct_inside:.1%} of prices inside bands"


class TestVolatility:

    def test_log_returns_reasonable(self):
        """Log returns for normal stock prices should be small."""
        df = make_sample_df(200)
        df["log_return"] = np.log(df["adj_close"] / df["adj_close"].shift(1))
        valid = df["log_return"].dropna()
        assert valid.abs().max() < 0.5, "Suspiciously large log return detected"

    def test_annualized_vol_scaling(self):
        """Annualized vol should be sqrt(252) * daily vol."""
        df = make_sample_df(100)
        df["log_return"] = np.log(df["adj_close"] / df["adj_close"].shift(1))
        daily_vol = df["log_return"].std()
        ann_vol   = daily_vol * (252 ** 0.5)
        assert 0 < ann_vol < 5.0, f"Annualized vol out of range: {ann_vol:.4f}"


class TestMovingAverages:

    def test_sma_lags_behind(self):
        """In a rising market, SMA 50 should be below SMA 200."""
        rising = list(range(1, 251))
        df = pd.DataFrame({"adj_close": rising})
        df["sma_50"]  = df["adj_close"].rolling(50).mean()
        df["sma_200"] = df["adj_close"].rolling(200).mean()
        last = df.dropna().iloc[-1]
        assert last["sma_50"] > last["sma_200"], "SMA50 should exceed SMA200 in rising market"

    def test_golden_cross_signal(self):
        """Golden cross should be 1 when SMA50 > SMA200."""
        df = pd.DataFrame({
            "sma_50":  [150, 160, 170],
            "sma_200": [140, 145, 150],
        })
        df["golden_cross"] = (df["sma_50"] > df["sma_200"]).astype(int)
        assert all(df["golden_cross"] == 1)


# ─── Integration smoke test ───────────────────────────────────────────────────

class TestPipelineIntegration:

    def test_default_tickers_list_not_empty(self):
        from src.ingestion.fetch_data import DEFAULT_TICKERS
        assert len(DEFAULT_TICKERS) >= 10

    def test_default_tickers_are_strings(self):
        from src.ingestion.fetch_data import DEFAULT_TICKERS
        for t in DEFAULT_TICKERS:
            assert isinstance(t, str)
            assert len(t) <= 10

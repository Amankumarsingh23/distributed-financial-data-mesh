"""
Distributed Financial Data Mesh — Ingestion Layer
Fetches multi-ticker stock/crypto OHLCV data from Tiingo API
and stages it to GCS for downstream PySpark processing.
"""

import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage
from typing import List, Dict, Optional

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("ingestion")

# ─── Config ──────────────────────────────────────────────────────────────────
TIINGO_API_KEY   = os.environ["TIINGO_API_KEY"]
GCS_BUCKET       = os.environ["GCS_BUCKET_NAME"]
GCS_RAW_PREFIX   = "raw/stocks"
TIINGO_BASE_URL  = "https://api.tiingo.com/tiingo/daily"

# Tickers to ingest — equity + crypto mix for recruiter wow-factor
DEFAULT_TICKERS: List[str] = [
    # Large-cap equities
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META",
    # Financial sector
    "JPM", "GS", "BAC",
    # Index ETFs
    "SPY", "QQQ", "IWM",
    # Tech growth
    "TSLA", "NFLX", "AMD",
    # Nasdaq composite itself
    "NDAQ",
]


def fetch_ticker_ohlcv(
    ticker: str,
    start_date: str,
    end_date: str,
) -> Optional[pd.DataFrame]:
    """
    Pull daily OHLCV + adjusted price data for a single ticker.
    Returns a DataFrame or None if the request fails.
    """
    url = f"{TIINGO_BASE_URL}/{ticker}/prices"
    params = {
        "startDate":  start_date,
        "endDate":    end_date,
        "token":      TIINGO_API_KEY,
        "resampleFreq": "daily",
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            logger.warning(f"[{ticker}] Empty response — skipping.")
            return None

        df = pd.DataFrame(data)
        df["ticker"]        = ticker.upper()
        df["ingested_at"]   = datetime.utcnow().isoformat()
        df["date"]          = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        # Rename to snake_case for BigQuery friendliness
        df = df.rename(columns={
            "adjClose":  "adj_close",
            "adjHigh":   "adj_high",
            "adjLow":    "adj_low",
            "adjOpen":   "adj_open",
            "adjVolume": "adj_volume",
        })

        logger.info(f"[{ticker}] Fetched {len(df)} rows  ({start_date} → {end_date})")
        return df

    except requests.exceptions.HTTPError as e:
        logger.error(f"[{ticker}] HTTP error: {e}")
    except Exception as e:
        logger.error(f"[{ticker}] Unexpected error: {e}")
    return None


def upload_to_gcs(df: pd.DataFrame, ticker: str, run_date: str) -> str:
    """
    Write a ticker's DataFrame to GCS as newline-delimited JSON.
    Path: gs://<bucket>/raw/stocks/<run_date>/<ticker>.json
    """
    client  = storage.Client()
    bucket  = client.bucket(GCS_BUCKET)
    blob_path = f"{GCS_RAW_PREFIX}/{run_date}/{ticker.upper()}.json"
    blob    = bucket.blob(blob_path)

    ndjson  = df.to_json(orient="records", lines=True)
    blob.upload_from_string(ndjson, content_type="application/json")

    gcs_uri = f"gs://{GCS_BUCKET}/{blob_path}"
    logger.info(f"[{ticker}] Uploaded → {gcs_uri}")
    return gcs_uri


def run_ingestion(
    tickers: List[str] = None,
    lookback_days: int = 365,
) -> Dict[str, str]:
    """
    Main ingestion entrypoint.
    Fetches data for all tickers and stages to GCS.
    Returns mapping of ticker → GCS URI.
    """
    tickers    = tickers or DEFAULT_TICKERS
    end_date   = datetime.utcnow().strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    run_date   = datetime.utcnow().strftime("%Y%m%d")

    logger.info(f"Starting ingestion | {len(tickers)} tickers | {start_date} → {end_date}")

    results: Dict[str, str] = {}
    failed:  List[str]      = []

    for ticker in tickers:
        df = fetch_ticker_ohlcv(ticker, start_date, end_date)
        if df is not None and not df.empty:
            uri = upload_to_gcs(df, ticker, run_date)
            results[ticker] = uri
        else:
            failed.append(ticker)

    logger.info(f"Ingestion complete — {len(results)} succeeded, {len(failed)} failed")
    if failed:
        logger.warning(f"Failed tickers: {failed}")

    # Write manifest for downstream steps
    manifest = {
        "run_date":    run_date,
        "start_date":  start_date,
        "end_date":    end_date,
        "succeeded":   list(results.keys()),
        "failed":      failed,
        "gcs_uris":    results,
    }
    manifest_path = f"/tmp/ingestion_manifest_{run_date}.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    logger.info(f"Manifest written → {manifest_path}")

    return results


if __name__ == "__main__":
    run_ingestion()

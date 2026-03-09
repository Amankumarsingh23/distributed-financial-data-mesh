<div align="center">

# 📊 Distributed Financial Data Mesh

### Cloud-Native ETL Pipeline for Quantitative Research & ML

[![Pipeline](https://github.com/Amankumarsingh23/distributed-financial-data-mesh/actions/workflows/pipeline.yml/badge.svg)](https://github.com/Amankumarsingh23/distributed-financial-data-mesh/actions/workflows/pipeline.yml)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://python.org)
[![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?logo=google-cloud&logoColor=white)](https://cloud.google.com)
[![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?logo=google-bigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apache-spark&logoColor=white)](https://spark.apache.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

*Ingests multi-ticker financial time series from Tiingo API · Processes with distributed PySpark on Dataproc · Computes 30+ ML features · Loads to BigQuery — fully automated via GitHub Actions, zero human intervention.*

</div>

---

## 🗺️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         GITHUB ACTIONS CI/CD                            │
│  Schedule: weekdays 6PM UTC (post market-close) + manual dispatch       │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │                  │
         ▼                    ▼                    ▼                  ▼
  ┌─────────────┐    ┌──────────────┐    ┌──────────────┐   ┌──────────────┐
  │  Quality    │    │Infrastructure│    │   Ingest     │   │  Transform   │
  │   Gate      │───▶│  Provision   │───▶│  Raw Data    │──▶│  & Load      │
  │ (lint+test) │    │              │    │              │   │              │
  └─────────────┘    └──────────────┘    └──────────────┘   └──────────────┘
                             │                  │                   │
                             ▼                  ▼                   ▼
                    ┌────────────────┐  ┌──────────────┐  ┌───────────────┐
                    │  GCS Bucket    │  │  Tiingo API  │  │   Dataproc    │
                    │                │  │  (16 tickers)│  │ PySpark Clust.│
                    │  raw/          │◀─│  OHLCV + Adj │  │  2+2 workers  │
                    │  processed/    │  │  daily data  │  │  preemptible  │
                    │  scripts/      │  └──────────────┘  └───────┬───────┘
                    └───────┬────────┘                            │
                            │           ┌─────────────────────────┘
                            │           │  30+ ML Features computed:
                            │           │  • SMA/EMA (5/10/20/50/200d)
                            │           │  • Bollinger Bands + %B
                            │           │  • RSI (14d), MACD, Signal
                            │           │  • Log Returns, ATR, Vol regimes
                            │           │  • OBV, Volume Z-Score
                            │           │  • 52w High/Low proximity
                            │           │  • Momentum (20d, 60d)
                            │           │  • Golden/Death Cross signal
                            ▼           ▼
                    ┌────────────────────────────────────┐
                    │           BigQuery                  │
                    │                                     │
                    │  stock_features (partitioned+        │
                    │    clustered, 30+ columns)           │
                    │                                     │
                    │  Analytics Views:                   │
                    │  • vw_latest_snapshot               │
                    │  • vw_cross_sectional_momentum      │
                    │  • vw_rsi_signals                   │
                    │  • vw_volatility_regime             │
                    │  • vw_ml_feature_set  ◀── ML Ready  │
                    │  • vw_pipeline_audit                │
                    └────────────────────────────────────┘
```

---

## ✨ Key Features

| Feature | Detail |
|---|---|
| **Multi-Ticker** | 16 tickers: AAPL, MSFT, GOOGL, NVDA, META, JPM, SPY, QQQ, NDAQ + more |
| **Distributed Processing** | PySpark on Dataproc with auto-scaling + preemptible workers |
| **30+ ML Features** | Technical indicators, volatility metrics, momentum signals |
| **Zero Human Intervention** | Fully automated: infra provisioning → ingest → transform → load → teardown |
| **Cost-Optimized** | Cluster spun up per-run, deleted after job (idle TTL = 1hr fallback) |
| **Data Quality** | Row-count validation gates, null-check monitoring, pipeline audit view |
| **ML-Ready Output** | BigQuery view with label columns for next-day return prediction |

---

## 🏗️ Project Structure

```
financial-data-mesh/
├── .github/
│   └── workflows/
│       └── pipeline.yml          # Full 6-job CI/CD pipeline
├── src/
│   ├── ingestion/
│   │   └── fetch_data.py         # Tiingo API → GCS (multi-ticker)
│   ├── processing/
│   │   └── spark_transform.py    # PySpark feature engineering (Dataproc)
│   ├── analytics/
│   │   └── deploy_views.py       # BigQuery SQL view deployment
│   └── infrastructure/
│       └── automate.py           # GCP resource provisioning (GCS+BQ+Dataproc)
├── sql/
│   └── views/
│       └── analytics_views.sql   # 6 BigQuery analytics views
├── configs/
│   └── settings.py               # Pydantic-based config management
├── tests/
│   └── test_pipeline.py          # Unit tests (RSI, Bollinger, vol, integration)
├── .env.example                  # Secret template
├── requirements.txt
└── README.md
```

---

## ⚡ Quick Start

### 1. Prerequisites

- GCP project with billing enabled
- Service account with roles: `BigQuery Admin`, `Dataproc Admin`, `Storage Admin`
- [Tiingo API account](https://api.tiingo.com) (free tier works)
- GitHub repository (public or private)

### 2. GCP Setup (one-time)

```bash
# Create a service account
gcloud iam service-accounts create financial-mesh-sa \
  --display-name="Financial Data Mesh SA"

# Grant required roles
for ROLE in bigquery.admin dataproc.admin storage.admin; do
  gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:financial-mesh-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/${ROLE}"
done

# Download key
gcloud iam service-accounts keys create service_account.json \
  --iam-account=financial-mesh-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### 3. GitHub Secrets

Navigate to **Settings → Secrets → Actions** and add:

| Secret | Value |
|---|---|
| `GCP_PROJECT_ID` | Your GCP project ID |
| `GCP_SERVICE_ACCOUNT_KEY` | Full contents of `service_account.json` |
| `GCS_BUCKET_NAME` | e.g. `my-financial-mesh-bucket` |
| `TIINGO_API_KEY` | From [api.tiingo.com](https://api.tiingo.com) → Account → Token |

### 4. Run the Pipeline

```bash
# Trigger manually from GitHub Actions UI, or push to main
# Automatic schedule: weekdays at 18:00 UTC
```

That's it. The pipeline handles everything else.

---

## 🔬 Feature Engineering Reference

### Technical Indicators

| Feature | Description | Window |
|---|---|---|
| `sma_5/10/20/50/200` | Simple Moving Average | 5, 10, 20, 50, 200 days |
| `ema_12/26` | Exponential Moving Average | 12, 26 days |
| `bb_upper/mid/lower` | Bollinger Bands | 20d, ±2σ |
| `bb_pct_b` | Price position within bands | 0=lower, 1=upper |
| `rsi` | Relative Strength Index | 14 days |
| `macd_line/signal/hist` | MACD + Signal + Histogram | 12/26/9 |

### Volatility Metrics

| Feature | Description |
|---|---|
| `log_return` | Daily log return |
| `vol_5/10/20/60d` | Rolling log-return std dev |
| `annualized_vol_20d` | Daily vol × √252 |
| `atr_14` | Average True Range (14d) |

### Volume & Momentum

| Feature | Description |
|---|---|
| `obv` | On-Balance Volume (cumulative) |
| `vol_zscore` | Volume z-score (20d basis) |
| `momentum_20d/60d` | Price return over N days |
| `golden_cross` | 1 if SMA50 > SMA200, else 0 |
| `pct_from_52w_high/low` | Distance from 52-week extremes |

---

## 📊 BigQuery Analytics Views

```sql
-- Cross-sectional momentum ranking + buy/sell signals
SELECT ticker, momentum_signal, composite_momentum_score
FROM `your-project.financial_data_mesh.vw_cross_sectional_momentum`
ORDER BY composite_momentum_score DESC;

-- RSI overbought / oversold board
SELECT ticker, rsi, rsi_signal, macd_signal
FROM `your-project.financial_data_mesh.vw_rsi_signals`
WHERE rsi_signal IN ('OVERBOUGHT', 'OVERSOLD');

-- ML feature matrix with next-day return label
SELECT *
FROM `your-project.financial_data_mesh.vw_ml_feature_set`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY);

-- Pipeline health audit
SELECT pipeline_run, tickers_loaded, total_rows, invalid_price_count
FROM `your-project.financial_data_mesh.vw_pipeline_audit`
ORDER BY pipeline_run DESC LIMIT 10;
```

---

## 💰 Cost Estimate

| Resource | Approximate Cost |
|---|---|
| Dataproc cluster (n1-standard-4 × 4 nodes, ~30 min/day) | ~$1.50/day |
| GCS storage (raw + processed, ~100MB/day) | ~$0.02/day |
| BigQuery storage + queries | ~$0.05/day |
| **Total** | **~$1.57/day, ~$47/month** |

*Using preemptible secondary workers reduces Dataproc cost by ~60%.*

---

## 🧪 Running Tests Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/ -v --cov=src

# Lint
flake8 src/ --max-line-length=120
```

---

## 🛠️ Tech Stack

- **Cloud**: Google Cloud Platform (GCS, BigQuery, Dataproc, IAM)
- **Processing**: Apache Spark 3.x (PySpark) on Dataproc
- **Ingestion**: Python, Tiingo REST API, `google-cloud-storage`
- **Analytics**: BigQuery SQL, partitioned + clustered tables
- **Orchestration**: GitHub Actions (6-job DAG, scheduled + manual dispatch)
- **Config**: Pydantic Settings, `.env` management
- **Testing**: pytest, unittest.mock, pytest-cov

---

## 📄 License

MIT — see [LICENSE](LICENSE)

---

<div align="center">
Built for production. Designed for recruiters. Ready for scale.
</div>

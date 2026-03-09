"""
Centralized configuration using Pydantic Settings.
All values are resolved from environment variables.
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List


class PipelineConfig(BaseSettings):
    # GCP
    gcp_project_id:    str  = Field(..., env="GCP_PROJECT_ID")
    gcp_region:        str  = Field("us-central1", env="GCP_REGION")
    gcs_bucket_name:   str  = Field(..., env="GCS_BUCKET_NAME")

    # BigQuery
    bq_dataset:        str  = Field("financial_data_mesh", env="BQ_DATASET")
    bq_table_features: str  = Field("stock_features", env="BQ_TABLE_FEATURES")

    # Dataproc
    dataproc_cluster:  str  = Field("financial-mesh-cluster", env="DATAPROC_CLUSTER")

    # Tiingo
    tiingo_api_key:    str  = Field(..., env="TIINGO_API_KEY")
    tiingo_base_url:   str  = Field("https://api.tiingo.com/tiingo/daily")

    # Ingestion
    lookback_days:     int  = Field(365, env="LOOKBACK_DAYS")
    default_tickers: List[str] = Field(
        default=[
            "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META",
            "JPM", "GS", "BAC",
            "SPY", "QQQ", "IWM",
            "TSLA", "NFLX", "AMD", "NDAQ",
        ],
        env="DEFAULT_TICKERS",
    )

    # GCS paths
    gcs_raw_prefix:       str = "raw/stocks"
    gcs_processed_prefix: str = "processed/stocks"
    gcs_scripts_prefix:   str = "scripts"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Singleton
settings = PipelineConfig()

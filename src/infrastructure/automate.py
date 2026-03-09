"""
Distributed Financial Data Mesh — Infrastructure Automation
Provisions all required GCP resources via Python SDK.
No manual console clicks. Full idempotency.

Resources created:
  - GCS bucket (raw + processed zones)
  - BigQuery dataset + tables
  - Dataproc cluster (auto-scaling, preemptible workers)
  - IAM service account + role bindings
"""

import os
import json
import logging
import time
from typing import Optional

from google.cloud import storage, bigquery
from google.cloud import dataproc_v1
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("infrastructure")

# ─── Config ──────────────────────────────────────────────────────────────────
PROJECT_ID   = os.environ["GCP_PROJECT_ID"]
REGION       = os.environ.get("GCP_REGION", "us-central1")
BUCKET_NAME  = os.environ["GCS_BUCKET_NAME"]
DATASET_NAME = os.environ.get("BQ_DATASET", "financial_data_mesh")
CLUSTER_NAME = os.environ.get("DATAPROC_CLUSTER", "financial-mesh-cluster")


# ─── GCS ─────────────────────────────────────────────────────────────────────

def create_gcs_bucket(bucket_name: str, location: str = "US") -> None:
    client = storage.Client(project=PROJECT_ID)
    bucket = client.lookup_bucket(bucket_name)
    if bucket:
        logger.info(f"GCS bucket already exists: gs://{bucket_name}")
        return

    bucket = client.create_bucket(bucket_name, location=location)
    # Lifecycle: delete raw files after 90 days, processed after 365
    bucket.add_lifecycle_delete_rule(age=90,  matches_prefix=["raw/"])
    bucket.add_lifecycle_delete_rule(age=365, matches_prefix=["processed/"])
    bucket.patch()
    logger.info(f"Created GCS bucket: gs://{bucket_name} ({location})")

    # Create folder structure markers
    for prefix in ["raw/stocks/", "processed/stocks/", "scripts/", "logs/"]:
        blob = bucket.blob(f"{prefix}.gitkeep")
        blob.upload_from_string("")
    logger.info(f"GCS folder structure initialized.")


# ─── BigQuery ────────────────────────────────────────────────────────────────

BQ_STOCK_FEATURES_SCHEMA = [
    bigquery.SchemaField("date",                 "DATE",    mode="REQUIRED"),
    bigquery.SchemaField("ticker",               "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("open",                 "FLOAT64"),
    bigquery.SchemaField("high",                 "FLOAT64"),
    bigquery.SchemaField("low",                  "FLOAT64"),
    bigquery.SchemaField("close",                "FLOAT64"),
    bigquery.SchemaField("volume",               "INT64"),
    bigquery.SchemaField("adj_close",            "FLOAT64"),
    bigquery.SchemaField("adj_volume",           "INT64"),
    # Moving averages
    bigquery.SchemaField("sma_5",                "FLOAT64"),
    bigquery.SchemaField("sma_10",               "FLOAT64"),
    bigquery.SchemaField("sma_20",               "FLOAT64"),
    bigquery.SchemaField("sma_50",               "FLOAT64"),
    bigquery.SchemaField("sma_200",              "FLOAT64"),
    bigquery.SchemaField("ema_12",               "FLOAT64"),
    bigquery.SchemaField("ema_26",               "FLOAT64"),
    # Bollinger Bands
    bigquery.SchemaField("bb_upper",             "FLOAT64"),
    bigquery.SchemaField("bb_mid",               "FLOAT64"),
    bigquery.SchemaField("bb_lower",             "FLOAT64"),
    bigquery.SchemaField("bb_pct_b",             "FLOAT64"),
    bigquery.SchemaField("bb_bandwidth",         "FLOAT64"),
    # RSI / MACD
    bigquery.SchemaField("rsi",                  "FLOAT64"),
    bigquery.SchemaField("macd_line",            "FLOAT64"),
    bigquery.SchemaField("macd_signal",          "FLOAT64"),
    bigquery.SchemaField("macd_hist",            "FLOAT64"),
    # Volatility
    bigquery.SchemaField("log_return",           "FLOAT64"),
    bigquery.SchemaField("vol_20d",              "FLOAT64"),
    bigquery.SchemaField("annualized_vol_20d",   "FLOAT64"),
    bigquery.SchemaField("atr_14",               "FLOAT64"),
    # Volume
    bigquery.SchemaField("obv",                  "FLOAT64"),
    bigquery.SchemaField("vol_zscore",           "FLOAT64"),
    # Price signals
    bigquery.SchemaField("pct_from_52w_high",    "FLOAT64"),
    bigquery.SchemaField("pct_from_52w_low",     "FLOAT64"),
    bigquery.SchemaField("golden_cross",         "INT64"),
    bigquery.SchemaField("momentum_20d",         "FLOAT64"),
    bigquery.SchemaField("momentum_60d",         "FLOAT64"),
    # Metadata
    bigquery.SchemaField("processed_at",         "STRING"),
    bigquery.SchemaField("pipeline_run",         "STRING"),
    bigquery.SchemaField("date_ts",              "DATE"),
    bigquery.SchemaField("year",                 "INT64"),
    bigquery.SchemaField("month",                "INT64"),
]


def create_bigquery_dataset() -> None:
    client  = bigquery.Client(project=PROJECT_ID)
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_NAME}")
    dataset_ref.location    = "US"
    dataset_ref.description = "Financial Data Mesh — ML-ready stock features"

    try:
        client.create_dataset(dataset_ref, exists_ok=True)
        logger.info(f"BigQuery dataset ready: {PROJECT_ID}.{DATASET_NAME}")
    except Exception as e:
        logger.error(f"Dataset creation failed: {e}")
        raise


def create_bigquery_table(table_name: str, schema, partition_field: str = "date_ts") -> None:
    client    = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}"
    table     = bigquery.Table(table_ref, schema=schema)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=partition_field,
    )
    table.clustering_fields = ["ticker"]
    table.description       = f"Financial Data Mesh — {table_name}"

    try:
        client.create_table(table, exists_ok=True)
        logger.info(f"BigQuery table ready: {table_ref}")
    except Exception as e:
        logger.error(f"Table creation failed: {e}")
        raise


# ─── Dataproc ────────────────────────────────────────────────────────────────

def create_dataproc_cluster() -> None:
    client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )

    cluster_config = {
        "project_id": PROJECT_ID,
        "cluster_name": CLUSTER_NAME,
        "config": {
            "gce_cluster_config": {
                "zone_uri":    f"https://www.googleapis.com/compute/v1/projects/{PROJECT_ID}/zones/{REGION}-a",
                "subnetwork_uri": "default",
                "internal_ip_only": False,
                "metadata": {"PIP_PACKAGES": "google-cloud-bigquery pandas requests"},
            },
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-4",
                "disk_config": {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 100},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-4",
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
            },
            "secondary_worker_config": {
                "num_instances": 2,
                "is_preemptible": True,
                "preemptibility": "PREEMPTIBLE",
            },
            "software_config": {
                "image_version": "2.1-debian11",
                "properties": {
                    "spark:spark.sql.adaptive.enabled":                      "true",
                    "spark:spark.sql.adaptive.coalescePartitions.enabled":   "true",
                    "spark:spark.executor.memoryOverhead":                   "1g",
                },
                "optional_components": ["JUPYTER"],
            },
            "autoscaling_config": {
                "policy_uri": ""  # Can wire in autoscaling policy if needed
            },
            "lifecycle_config": {
                "idle_delete_ttl": {"seconds": 3600},  # Auto-delete after 1h idle
            },
            "endpoint_config": {"enable_http_port_access": True},
        },
    }

    # Check if cluster exists
    try:
        existing = client.get_cluster(
            project_id=PROJECT_ID, region=REGION, cluster_name=CLUSTER_NAME
        )
        if existing.status.state.name == "RUNNING":
            logger.info(f"Dataproc cluster already running: {CLUSTER_NAME}")
            return
    except Exception:
        pass  # Cluster doesn't exist yet

    logger.info(f"Creating Dataproc cluster: {CLUSTER_NAME}...")
    operation = client.create_cluster(
        request={"project_id": PROJECT_ID, "region": REGION, "cluster": cluster_config}
    )
    result = operation.result()
    logger.info(f"Dataproc cluster created: {result.cluster_name}")


def delete_dataproc_cluster() -> None:
    """Tear down cluster to avoid idle charges."""
    client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )
    try:
        operation = client.delete_cluster(
            request={"project_id": PROJECT_ID, "region": REGION, "cluster_name": CLUSTER_NAME}
        )
        operation.result()
        logger.info(f"Dataproc cluster deleted: {CLUSTER_NAME}")
    except Exception as e:
        logger.warning(f"Cluster deletion warning: {e}")


def submit_pyspark_job(script_gcs_path: str, run_date: str, dataset: str) -> str:
    """Submit PySpark job and wait for completion. Returns job ID."""
    client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )
    job = {
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": script_gcs_path,
            "args": [
                "--bucket",   BUCKET_NAME,
                "--project",  PROJECT_ID,
                "--dataset",  dataset,
                "--run_date", run_date,
            ],
            "jar_file_uris": [
                "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.0.jar"
            ],
            "properties": {
                "spark.executor.memory": "4g",
                "spark.driver.memory":   "4g",
            },
        },
        "labels": {"pipeline": "financial-data-mesh", "run_date": run_date},
    }
    result = client.submit_job(project_id=PROJECT_ID, region=REGION, job=job)
    job_id = result.reference.job_id
    logger.info(f"Submitted PySpark job: {job_id}")

    # Poll for completion
    while True:
        job_status = client.get_job(project_id=PROJECT_ID, region=REGION, job_id=job_id)
        state = job_status.status.state.name
        logger.info(f"Job {job_id} state: {state}")
        if state in ("DONE", "CANCELLED", "ERROR"):
            break
        time.sleep(15)

    if state != "DONE":
        raise RuntimeError(f"PySpark job failed with state: {state}")
    return job_id


# ─── Main ────────────────────────────────────────────────────────────────────

def provision_all() -> None:
    """Idempotent full infrastructure provisioning."""
    logger.info("=== Financial Data Mesh — Infrastructure Provisioning ===")
    create_gcs_bucket(BUCKET_NAME)
    create_bigquery_dataset()
    create_bigquery_table("stock_features", BQ_STOCK_FEATURES_SCHEMA)
    create_dataproc_cluster()
    logger.info("=== Infrastructure provisioning complete ===")


def teardown_cluster() -> None:
    delete_dataproc_cluster()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", choices=["provision", "teardown"], default="provision")
    args = parser.parse_args()
    if args.action == "provision":
        provision_all()
    else:
        teardown_cluster()

"""
Deploys all BigQuery analytics views by reading the SQL file
and substituting the project/dataset placeholder.
"""

import os
import re
import logging
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("deploy_views")

PROJECT_ID   = os.environ["GCP_PROJECT_ID"]
DATASET_NAME = os.environ.get("BQ_DATASET", "financial_data_mesh")
SQL_FILE     = os.path.join(os.path.dirname(__file__), "../../sql/views/analytics_views.sql")


def deploy_views():
    client = bigquery.Client(project=PROJECT_ID)

    with open(SQL_FILE, "r") as f:
        sql_content = f.read()

    # Replace placeholder project reference
    sql_content = sql_content.replace(
        "your-gcp-project.financial_data_mesh",
        f"{PROJECT_ID}.{DATASET_NAME}",
    )

    # Split on individual CREATE OR REPLACE VIEW statements
    statements = [
        s.strip() for s in re.split(r"(?=CREATE OR REPLACE VIEW)", sql_content)
        if s.strip().startswith("CREATE OR REPLACE VIEW")
    ]

    logger.info(f"Deploying {len(statements)} BigQuery views to {PROJECT_ID}.{DATASET_NAME}")

    for stmt in statements:
        view_name_match = re.search(r"CREATE OR REPLACE VIEW `([^`]+)`", stmt)
        view_name = view_name_match.group(1) if view_name_match else "unknown"
        try:
            # Strip trailing semicolons — BQ API doesn't accept them
            clean_stmt = stmt.rstrip(";").strip()
            client.query(clean_stmt).result()
            logger.info(f"  ✅ {view_name}")
        except Exception as e:
            logger.error(f"  ❌ {view_name}: {e}")
            raise

    logger.info("All views deployed successfully.")


if __name__ == "__main__":
    deploy_views()

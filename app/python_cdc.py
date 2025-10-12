"""
Custom Change Data Capture (CDC) Script for Azure Clinical Data Warehouse

This script implements a hash-based CDC mechanism to detect and capture changes in SQL Server tables
for the Maplewood General Hospital Azure data warehouse pipeline. It uses row-level hashing to identify
inserts, updates, and deletes, storing change logs and baselines in Azure Blob Storage for downstream
processing by Azure Data Factory (ADF).

Key Features:
- Secure authentication via Azure Key Vault and Active Directory.
- Hash-based change detection (MD5 on non-volatile columns).
- Blob Storage for persistent logs and baselines.
- Error handling with retries and logging.
- ADF integration via JSON summary output.
- Environment variable support for development (.env files).

Tables Monitored:
- sus_episodes (primary key: episode_id)
- social_care (primary key: package_id)
- prescriptions (primary key: prescription_id)
- Add more in the TABLES dict as needed.

Prerequisites:
- Azure Key Vault with secrets: 'sqlserver-connstr' and 'blob-connstr'.
- Blob Storage container: 'cdc-logs' (or set via BLOB_CONTAINER env var).
- ODBC drivers for SQL Server.
- Python environment with required packages (managed via Poetry).

Usage:
1. Update .env.development with real Azure credentials and connection strings.
2. Run: poetry run python app/python_cdc.py
3. Check Blob Storage for logs (e.g., {table}_log_{run_id}.json) and baselines.
4. Integrate with ADF for automated execution.

Environment Variables (.env.development):
- KEY_VAULT_URL: Azure Key Vault URL.
- BLOB_CONTAINER: Blob container name (default: 'cdc-logs').
- AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID: For local auth (optional if using managed identity).

Output:
- Console logs for monitoring.
- JSON summary printed for ADF capture.
- Blob files: Change logs and updated baselines.

Error Handling:
- Retries for SQL connections.
- Graceful skips for empty tables.
- Detailed logging for troubleshooting.

Author: Chibueze Igwe (scientistigwe@gmail.com)
Version: 0.1.0
"""

import pandas as pd
import pyodbc, hashlib, os, json, time, logging
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env.development
load_dotenv('.env.development')

# Set up logging for ADF integration and error handling
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------------
# 🔐 1. Retrieve connections from Key Vault
# -------------------------------
key_vault_url = os.getenv('KEY_VAULT_URL', "https://<your-key-vault-name>.vault.azure.net/")
secret_name_sql = "sqlserver-connstr"
secret_name_blob = "blob-connstr"

credential = DefaultAzureCredential()
client = SecretClient(vault_url=key_vault_url, credential=credential)

try:
    secret_sql = client.get_secret(secret_name_sql)
    conn_str = secret_sql.value
    logger.info("Retrieved SQL connection string from Key Vault.")
except Exception as e:
    logger.warning(f"Key Vault failed for SQL: {e}. Falling back to .env.")
    conn_str = os.getenv('SQL_SERVER_CONN_STR', 'Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=master;Trusted_Connection=yes;')

try:
    secret_blob = client.get_secret(secret_name_blob)
    blob_conn_str = secret_blob.value
    blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
    container_name = os.getenv('BLOB_CONTAINER', 'cdc-logs')  # Ensure this container exists
    logger.info("Retrieved Blob connection and initialized client from Key Vault.")
except Exception as e:
    logger.warning(f"Key Vault failed for Blob: {e}. Falling back to .env.")
    blob_conn_str = os.getenv('BLOB_CONN_STR', 'UseDevelopmentStorage=true;')
    blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
    container_name = os.getenv('BLOB_CONTAINER', 'cdc-logs')

# -------------------------------
# ⚙️ 2. Table config (name : primary key)
# -------------------------------
TABLES = {
    "sus_episodes": "episode_id",
    "social_care": "package_id",
    "prescriptions": "prescription_id",
    # ... add your remaining 7 tables
}

# -------------------------------
# 🗄️ 3. Blob Storage helpers for change logs and baselines
# -------------------------------
def upload_to_blob(blob_name, data):
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        logger.info(f"Uploaded {blob_name} to Blob Storage.")
    except Exception as e:
        logger.error(f"Failed to upload {blob_name}: {e}")
        raise

def download_from_blob(blob_name):
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        data = blob_client.download_blob().readall()
        return json.loads(data)
    except Exception as e:
        logger.warning(f"Blob {blob_name} not found or failed to download: {e}")
        return {}

# -------------------------------
# 🔁 4. CDC processing loop with error handling
# -------------------------------
run_summary = {"run_id": str(int(time.time())), "changes": {}}

for tbl, pk in TABLES.items():
    logger.info(f"🔄 Checking table: {tbl}")
    start = time.strftime("%Y-%m-%d %H:%M:%S")
    changes_logged = 0

    try:
        # Step 1: Load current data with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                df = pd.read_sql(f"SELECT * FROM dbo.{tbl}", pyodbc.connect(conn_str))
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {tbl}: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2)

        if df.empty:
            logger.info(f"{tbl}: no rows found, skipping.")
            continue

        # Step 2: Compute hash per row (excluding volatile timestamp)
        cols_to_hash = [c for c in df.columns if c.lower() not in ["created_timestamp"]]
        df["row_hash"] = df[cols_to_hash].astype(str).sum(axis=1).apply(lambda x: hashlib.md5(x.encode()).hexdigest())

        # Step 3: Load previous baseline from Blob
        baseline_blob = f"{tbl}_baseline.json"
        prev_data = download_from_blob(baseline_blob)
        prev = pd.DataFrame(prev_data) if prev_data else pd.DataFrame(columns=["primary_key", "row_hash"])

        df.rename(columns={pk: "primary_key"}, inplace=True)

        # Step 4: Compare old vs new
        merged = df.merge(prev, on="primary_key", how="outer", suffixes=("_new", "_old"))

        inserted = merged[merged["row_hash_old"].isna()]
        deleted = merged[merged["row_hash_new"].isna()]
        changed = merged[
            (~merged["row_hash_old"].isna()) &
            (merged["row_hash_new"] != merged["row_hash_old"])
        ]

        # Step 5: Log changes to Blob
        log_entries = []
        for change_type, subset in {
            "INSERT": inserted,
            "DELETE": deleted,
            "UPDATE": changed
        }.items():
            if not subset.empty:
                count = len(subset)
                changes_logged += count
                logger.info(f"  → {change_type}: {count} rows")
                for _, row in subset.iterrows():
                    log_entries.append({
                        "run_id": run_summary["run_id"],
                        "change_type": change_type,
                        "primary_key": row["primary_key"],
                        "row_hash": row["row_hash_new"] if change_type != "DELETE" else row["row_hash_old"],
                        "change_time": start
                    })

        if log_entries:
            log_blob = f"{tbl}_log_{run_summary['run_id']}.json"
            upload_to_blob(log_blob, log_entries)

        # Step 6: Update baseline in Blob
        baseline_data = df[["primary_key", "row_hash"]].to_dict(orient="records")
        upload_to_blob(baseline_blob, baseline_data)

        run_summary["changes"][tbl] = changes_logged

    except Exception as e:
        logger.error(f"Error processing table {tbl}: {e}")
        run_summary["changes"][tbl] = f"Error: {str(e)}"
        continue

# Output summary for ADF integration
summary_blob = f"cdc_summary_{run_summary['run_id']}.json"
upload_to_blob(summary_blob, run_summary)
logger.info(f"CDC process complete. Summary uploaded to {summary_blob}.")
print(json.dumps(run_summary))  # For ADF to capture output

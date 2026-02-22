"""
Ingest FHIR R4 resources from the public HAPI FHIR server and upload to ADLS Gen2.

This script:
1. Queries the HAPI FHIR R4 API for Patient, Condition, and Encounter resources.
2. Writes each resource as a newline-delimited JSON (NDJSON) file.
3. Uploads the files to the "bronze" container in ADLS Gen2.

Authentication: Uses DefaultAzureCredential, which picks up your `az login` session.
AWS analogy: This is like a Glue Python Shell job that calls an external API and writes to S3.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

import requests


# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# --- Configuration ---
FHIR_BASE_URL = "https://hapi.fhir.org/baseR4"
RESOURCE_TYPES = ["Patient", "Condition", "Encounter"]
PAGE_SIZE = 100  # Number of resources to fetch per type
CONTAINER_NAME = "bronze"


def _running_on_databricks() -> bool:
    """Detect whether we are running inside a Databricks environment."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _get_dbutils():
    """Get the dbutils object when running on Databricks."""
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils

    spark = SparkSession.builder.getOrCreate()
    return DBUtils(spark)


def _get_storage_account_name() -> str:
    """
    Resolve the storage account name.

    On Databricks: read from a job widget parameter.
    Locally: read from the STORAGE_ACCOUNT_NAME environment variable.
    """
    if _running_on_databricks():
        dbutils = _get_dbutils()
        dbutils.widgets.text("storage_account_name", "")
        return dbutils.widgets.get("storage_account_name")
    return os.environ["STORAGE_ACCOUNT_NAME"]


def fetch_fhir_resources(resource_type: str, count: int = PAGE_SIZE) -> list[dict]:
    """
    Fetch FHIR resources from the public HAPI FHIR server.

    FHIR APIs return a 'Bundle' — a wrapper object containing an array of resources
    under the 'entry' key. Each entry has a 'resource' field with the actual clinical data.

    AWS analogy: This is like calling an external REST API from a Glue job.
    """
    url = f"{FHIR_BASE_URL}/{resource_type}"
    params = {"_count": count, "_format": "json"}

    logger.info(f"Fetching up to {count} {resource_type} resources from {url}")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    bundle = response.json()
    entries = bundle.get("entry", [])
    resources = [entry["resource"] for entry in entries]

    logger.info(f"Successfully retrieved {len(resources)} {resource_type} resources")
    return resources


def write_ndjson(resources: list[dict], filepath: str) -> None:
    """
    Write resources as newline-delimited JSON (NDJSON).

    NDJSON is one JSON object per line. This is the standard format for bulk data loading
    into data lakes because each line can be parsed independently (unlike a JSON array
    which must be read entirely into memory).

    AWS analogy: This is the same NDJSON format that Athena and Glue expect when reading JSON from S3.
    """
    logger.debug(f"Writing {len(resources)} records to {filepath}")
    with open(filepath, "w") as f:
        for resource in resources:
            f.write(json.dumps(resource) + "\n")
    logger.info(f"Wrote {len(resources)} records to local file: {filepath}")


def upload_to_adls(local_path: str, remote_dir: str, filename: str, storage_account_name: str) -> None:
    """
    Upload a local file to ADLS Gen2.

    On Databricks: uses dbutils.fs.cp to copy from the driver's local filesystem to ABFSS.
                   Auth is handled transparently by Unity Catalog credential vending.
    Locally:       uses DefaultAzureCredential (picks up `az login` token).

    AWS analogy: This is like boto3's s3.upload_file() using credentials from ~/.aws/credentials.
    """
    abfss_path = f"abfss://{CONTAINER_NAME}@{storage_account_name}.dfs.core.windows.net/{remote_dir}/{filename}"

    logger.info(f"Uploading {local_path} to {abfss_path}")

    if _running_on_databricks():
        dbutils = _get_dbutils()
        dbutils.fs.cp(f"file:{local_path}", abfss_path)
        logger.info(f"Successfully uploaded via dbutils to {abfss_path}")
    else:
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient

        logger.debug(f"Authenticating with DefaultAzureCredential for storage account: {storage_account_name}")
        credential = DefaultAzureCredential()
        service_client = DataLakeServiceClient(
            account_url=f"https://{storage_account_name}.dfs.core.windows.net",
            credential=credential,
        )
        file_system_client = service_client.get_file_system_client(CONTAINER_NAME)
        directory_client = file_system_client.get_directory_client(remote_dir)
        file_client = directory_client.get_file_client(filename)

        with open(local_path, "rb") as f:
            file_client.upload_data(f, overwrite=True)

        logger.info(f"Successfully uploaded via Azure SDK to {abfss_path}")


def main():
    logger.info("Starting FHIR ingestion pipeline")
    
    storage_account_name = _get_storage_account_name()
    logger.info(f"Using storage account: {storage_account_name}")
    
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logger.info(f"Ingestion timestamp: {timestamp}")

    total_resources_ingested = 0

    for resource_type in RESOURCE_TYPES:
        logger.info(f"Processing resource type: {resource_type}")
        
        # 1. Fetch from FHIR API
        try:
            resources = fetch_fhir_resources(resource_type)
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {resource_type} resources: {e}")
            continue

        if not resources:
            logger.warning(f"No {resource_type} resources found, skipping")
            continue

        # 2. Write to local NDJSON file
        local_filename = f"{resource_type.lower()}_{timestamp}.ndjson"
        local_path = f"/tmp/{local_filename}"
        write_ndjson(resources, local_path)

        # 3. Upload to ADLS Gen2 Bronze container
        # Directory structure: bronze/{resource_type}/filename.ndjson
        remote_dir = resource_type.lower()
        try:
            upload_to_adls(local_path, remote_dir, local_filename, storage_account_name)
            total_resources_ingested += len(resources)
        except Exception as e:
            logger.error(f"Failed to upload {resource_type} to ADLS: {e}")
            continue
        finally:
            # 4. Cleanup local file
            if os.path.exists(local_path):
                os.remove(local_path)
                logger.debug(f"Cleaned up local file: {local_path}")

    logger.info(f"Ingestion complete. Total resources ingested: {total_resources_ingested}")


if __name__ == "__main__":
    main()
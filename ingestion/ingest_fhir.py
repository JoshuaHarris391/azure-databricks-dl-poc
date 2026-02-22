"""
Ingest FHIR R4 resources from the public HAPI FHIR server into Delta tables.

This script (Databricks-only):
1. Queries the HAPI FHIR R4 API for Patient, Condition, and Encounter resources.
2. Extracts key fields from the raw JSON dicts into flat rows.
3. Appends the rows to Delta tables in healthcare_poc.bronze via Spark.

Each batch is tagged with a unique batch_id (UUID) and ingested_at timestamp.
"""

import logging
import sys
import uuid
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


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
PAGE_SIZE = 100
CATALOG = "healthcare_poc"
SCHEMA = "bronze"


def _g(d, *keys):
    """Safely traverse nested dicts/lists. Returns None on any miss."""
    current = d
    for k in keys:
        if current is None:
            return None
        if isinstance(k, int):
            if isinstance(current, list) and len(current) > k:
                current = current[k]
            else:
                return None
        elif isinstance(current, dict):
            current = current.get(k)
        else:
            return None
    return current


def _str(val):
    """Coerce a value to str or None."""
    return str(val) if val is not None else None


# ------------------------------------------------------------------
# Explicit Spark schemas (all StringType to avoid inference issues)
# ------------------------------------------------------------------

_META_FIELDS = [
    StructField("batch_id", StringType(), True),
    StructField("ingested_at", StringType(), True),
]

PATIENT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("active", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("deceased", StringType(), True),
    StructField("family_name", StringType(), True),
    StructField("given_names", StringType(), True),
    StructField("telecom_phone", StringType(), True),
    StructField("telecom_email", StringType(), True),
    StructField("address_line", StringType(), True),
    StructField("address_city", StringType(), True),
    StructField("address_state", StringType(), True),
    StructField("address_postal_code", StringType(), True),
    StructField("address_country", StringType(), True),
    StructField("marital_status_code", StringType(), True),
] + _META_FIELDS)

CONDITION_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("clinical_status", StringType(), True),
    StructField("verification_status", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("code", StringType(), True),
    StructField("code_display", StringType(), True),
    StructField("subject_reference", StringType(), True),
    StructField("encounter_reference", StringType(), True),
    StructField("onset_datetime", StringType(), True),
    StructField("recorded_date", StringType(), True),
] + _META_FIELDS)

ENCOUNTER_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("class_code", StringType(), True),
    StructField("type_code", StringType(), True),
    StructField("type_display", StringType(), True),
    StructField("subject_reference", StringType(), True),
    StructField("period_start", StringType(), True),
    StructField("period_end", StringType(), True),
    StructField("reason_code", StringType(), True),
    StructField("reason_display", StringType(), True),
    StructField("service_provider_reference", StringType(), True),
] + _META_FIELDS)

SCHEMAS = {
    "Patient": PATIENT_SCHEMA,
    "Condition": CONDITION_SCHEMA,
    "Encounter": ENCOUNTER_SCHEMA,
}


# ------------------------------------------------------------------
# Fetch
# ------------------------------------------------------------------

def fetch_fhir_resources(
    resource_type: str, count: int = PAGE_SIZE
) -> list[dict]:
    """
    Fetch FHIR resources from the public HAPI FHIR server.

    FHIR APIs return a 'Bundle' — a wrapper object containing an array
    of resources under the 'entry' key. Each entry has a 'resource'
    field with the actual clinical data.
    """
    url = f"{FHIR_BASE_URL}/{resource_type}"
    params = {"_count": count, "_format": "json"}

    logger.info(
        "Fetching up to %d %s resources from %s",
        count, resource_type, url,
    )
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    bundle = response.json()
    entries = bundle.get("entry", [])
    resources = [entry["resource"] for entry in entries]

    logger.info(
        "Successfully retrieved %d %s resources",
        len(resources), resource_type,
    )
    return resources


# ------------------------------------------------------------------
# Flatten raw dicts (no pydantic — tolerant of messy public data)
# ------------------------------------------------------------------

def _flatten_patient(raw: dict) -> dict:
    """Extract key fields from a raw Patient dict."""
    name = _g(raw, "name", 0) or {}
    addr = _g(raw, "address", 0) or {}
    telecoms = raw.get("telecom") or []
    phones = [t["value"] for t in telecoms
              if t.get("system") == "phone" and t.get("value")]
    emails = [t["value"] for t in telecoms
              if t.get("system") == "email" and t.get("value")]
    given = name.get("given")
    line = addr.get("line")
    deceased = raw.get("deceasedBoolean")
    if deceased is None:
        deceased = raw.get("deceasedDateTime")
    return {
        "id": _str(raw.get("id")),
        "active": _str(raw.get("active")),
        "gender": _str(raw.get("gender")),
        "birth_date": _str(raw.get("birthDate")),
        "deceased": _str(deceased),
        "family_name": _str(name.get("family")),
        "given_names": (
            ", ".join(given) if given else None
        ),
        "telecom_phone": phones[0] if phones else None,
        "telecom_email": emails[0] if emails else None,
        "address_line": (
            ", ".join(line) if line else None
        ),
        "address_city": _str(addr.get("city")),
        "address_state": _str(addr.get("state")),
        "address_postal_code": _str(addr.get("postalCode")),
        "address_country": _str(addr.get("country")),
        "marital_status_code": _str(
            _g(raw, "maritalStatus", "coding", 0, "code")
        ),
    }


def _flatten_condition(raw: dict) -> dict:
    """Extract key fields from a raw Condition dict."""
    return {
        "id": _str(raw.get("id")),
        "clinical_status": _str(
            _g(raw, "clinicalStatus", "coding", 0, "code")
        ),
        "verification_status": _str(
            _g(raw, "verificationStatus", "coding", 0, "code")
        ),
        "category_code": _str(
            _g(raw, "category", 0, "coding", 0, "code")
        ),
        "code": _str(_g(raw, "code", "coding", 0, "code")),
        "code_display": _str(
            _g(raw, "code", "coding", 0, "display")
        ),
        "subject_reference": _str(
            _g(raw, "subject", "reference")
        ),
        "encounter_reference": _str(
            _g(raw, "encounter", "reference")
        ),
        "onset_datetime": _str(raw.get("onsetDateTime")),
        "recorded_date": _str(raw.get("recordedDate")),
    }


def _flatten_encounter(raw: dict) -> dict:
    """Extract key fields from a raw Encounter dict."""
    return {
        "id": _str(raw.get("id")),
        "status": _str(raw.get("status")),
        "class_code": _str(_g(raw, "class", "code")),
        "type_code": _str(
            _g(raw, "type", 0, "coding", 0, "code")
        ),
        "type_display": _str(
            _g(raw, "type", 0, "coding", 0, "display")
        ),
        "subject_reference": _str(
            _g(raw, "subject", "reference")
        ),
        "period_start": _str(_g(raw, "period", "start")),
        "period_end": _str(_g(raw, "period", "end")),
        "reason_code": _str(
            _g(raw, "reasonCode", 0, "coding", 0, "code")
        ),
        "reason_display": _str(
            _g(raw, "reasonCode", 0, "coding", 0, "display")
        ),
        "service_provider_reference": _str(
            _g(raw, "serviceProvider", "reference")
        ),
    }


FLATTEN_FN = {
    "Patient": _flatten_patient,
    "Condition": _flatten_condition,
    "Encounter": _flatten_encounter,
}


def parse_fhir_resources(
    resource_type: str, raw_dicts: list[dict]
) -> list[dict]:
    """
    Extract fields from each raw FHIR dict and flatten
    into a list of simple dicts ready for a Spark DataFrame.
    """
    flatten = FLATTEN_FN[resource_type]
    rows = []
    for raw in raw_dicts:
        try:
            rows.append(flatten(raw))
        except Exception:
            logger.warning(
                "Skipping invalid %s resource id=%s",
                resource_type, raw.get("id"),
                exc_info=True,
            )
    logger.info(
        "Parsed %d / %d %s resources",
        len(rows), len(raw_dicts), resource_type,
    )
    return rows


def write_to_delta(
    spark: SparkSession,
    rows: list[dict],
    table_name: str,
    resource_type: str,
    batch_id: str,
    ingested_at: str,
) -> None:
    """
    Write parsed rows to a Unity Catalog Delta table in append mode.

    Adds batch_id and ingested_at columns for lineage tracking.
    Uses an explicit schema to avoid Spark inference failures.
    """
    for row in rows:
        row["batch_id"] = batch_id
        row["ingested_at"] = ingested_at

    schema = SCHEMAS[resource_type]
    df = spark.createDataFrame(rows, schema=schema)

    full_table = f"{CATALOG}.{SCHEMA}.{table_name}"
    logger.info(
        "Writing %d rows to %s", len(rows), full_table,
    )
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(full_table)
    )
    logger.info("Successfully wrote to %s", full_table)


def main():
    logger.info("Starting FHIR ingestion pipeline")

    spark = SparkSession.builder.getOrCreate()
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()
    logger.info("Batch ID: %s", batch_id)

    total = 0

    for resource_type in RESOURCE_TYPES:
        logger.info("Processing resource type: %s", resource_type)

        # 1. Fetch from FHIR API
        try:
            raw_resources = fetch_fhir_resources(resource_type)
        except requests.RequestException:
            logger.error(
                "Failed to fetch %s resources",
                resource_type, exc_info=True,
            )
            continue

        if not raw_resources:
            logger.warning(
                "No %s resources found, skipping", resource_type,
            )
            continue

        # 2. Parse and flatten
        rows = parse_fhir_resources(resource_type, raw_resources)
        if not rows:
            continue

        # 3. Write to Delta table
        table_name = resource_type.lower()
        try:
            write_to_delta(
                spark, rows, table_name,
                resource_type, batch_id, ingested_at,
            )
            total += len(rows)
        except Exception:
            logger.error(
                "Failed to write %s to Delta",
                resource_type, exc_info=True,
            )
            continue

    logger.info(
        "Ingestion complete. Total resources ingested: %d", total,
    )


if __name__ == "__main__":
    main()
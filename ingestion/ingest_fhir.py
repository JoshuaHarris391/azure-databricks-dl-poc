"""
Ingest FHIR R4 resources from the public HAPI FHIR server into Delta tables.

This script (Databricks-only):
1. Queries the HAPI FHIR R4 API for Patient, Condition, and Encounter resources.
2. Validates and parses each resource using fhir.resources (pydantic).
3. Flattens the parsed resources into tabular rows.
4. Appends the rows to Delta tables in healthcare_poc.bronze via Spark.

Each batch is tagged with a unique batch_id (UUID) and ingested_at timestamp.
"""

import logging
import sys
import uuid
from datetime import datetime, timezone

import requests
from fhir.resources.R4B.condition import Condition
from fhir.resources.R4B.encounter import Encounter
from fhir.resources.R4B.patient import Patient
from pyspark.sql import SparkSession


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


# --- FHIR model class lookup ---
FHIR_MODELS = {
    "Patient": Patient,
    "Condition": Condition,
    "Encounter": Encounter,
}


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
# Parsers: validate with fhir.resources then flatten to dicts
# ------------------------------------------------------------------

def _safe(fn):
    """Return None instead of raising on missing nested attributes."""
    try:
        return fn()
    except (AttributeError, IndexError, TypeError):
        return None


def _flatten_patient(raw: dict) -> dict:
    """Parse and flatten a Patient resource."""
    pat = Patient.model_validate(raw)
    name = _safe(lambda: pat.name[0])
    addr = _safe(lambda: pat.address[0])
    phones = [
        t.value for t in (pat.telecom or [])
        if t.system == "phone"
    ]
    emails = [
        t.value for t in (pat.telecom or [])
        if t.system == "email"
    ]
    return {
        "id": pat.id,
        "active": pat.active,
        "gender": pat.gender,
        "birth_date": str(pat.birthDate) if pat.birthDate else None,
        "deceased": (
            pat.deceasedBoolean
            if pat.deceasedBoolean is not None
            else str(pat.deceasedDateTime) if pat.deceasedDateTime else None
        ),
        "family_name": _safe(lambda: name.family),
        "given_names": (
            ", ".join(name.given)
            if _safe(lambda: name.given) else None
        ),
        "telecom_phone": phones[0] if phones else None,
        "telecom_email": emails[0] if emails else None,
        "address_line": (
            ", ".join(addr.line)
            if _safe(lambda: addr.line) else None
        ),
        "address_city": _safe(lambda: addr.city),
        "address_state": _safe(lambda: addr.state),
        "address_postal_code": _safe(lambda: addr.postalCode),
        "address_country": _safe(lambda: addr.country),
        "marital_status_code": _safe(
            lambda: pat.maritalStatus.coding[0].code
        ),
    }


def _flatten_condition(raw: dict) -> dict:
    """Parse and flatten a Condition resource."""
    cond = Condition.model_validate(raw)
    return {
        "id": cond.id,
        "clinical_status": _safe(
            lambda: cond.clinicalStatus.coding[0].code
        ),
        "verification_status": _safe(
            lambda: cond.verificationStatus.coding[0].code
        ),
        "category_code": _safe(
            lambda: cond.category[0].coding[0].code
        ),
        "code": _safe(lambda: cond.code.coding[0].code),
        "code_display": _safe(
            lambda: cond.code.coding[0].display
        ),
        "subject_reference": _safe(
            lambda: cond.subject.reference
        ),
        "encounter_reference": _safe(
            lambda: cond.encounter.reference
        ),
        "onset_datetime": (
            str(cond.onsetDateTime) if cond.onsetDateTime else None
        ),
        "recorded_date": (
            str(cond.recordedDate) if cond.recordedDate else None
        ),
    }


def _flatten_encounter(raw: dict) -> dict:
    """Parse and flatten an Encounter resource."""
    enc = Encounter.model_validate(raw)
    return {
        "id": enc.id,
        "status": enc.status,
        "class_code": _safe(lambda: enc.class_fhir.code),
        "type_code": _safe(
            lambda: enc.type[0].coding[0].code
        ),
        "type_display": _safe(
            lambda: enc.type[0].coding[0].display
        ),
        "subject_reference": _safe(
            lambda: enc.subject.reference
        ),
        "period_start": (
            str(enc.period.start) if _safe(lambda: enc.period.start) else None
        ),
        "period_end": (
            str(enc.period.end) if _safe(lambda: enc.period.end) else None
        ),
        "reason_code": _safe(
            lambda: enc.reasonCode[0].coding[0].code
        ),
        "reason_display": _safe(
            lambda: enc.reasonCode[0].coding[0].display
        ),
        "service_provider_reference": _safe(
            lambda: enc.serviceProvider.reference
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
    Validate each raw FHIR dict with fhir.resources and flatten
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
    batch_id: str,
    ingested_at: str,
) -> None:
    """
    Write parsed rows to a Unity Catalog Delta table in append mode.

    Adds batch_id and ingested_at columns for lineage tracking.
    """
    for row in rows:
        row["batch_id"] = batch_id
        row["ingested_at"] = ingested_at

    df = spark.createDataFrame(rows)

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

        # 2. Parse and flatten with fhir.resources
        rows = parse_fhir_resources(resource_type, raw_resources)
        if not rows:
            continue

        # 3. Write to Delta table
        table_name = resource_type.lower()
        try:
            write_to_delta(
                spark, rows, table_name, batch_id, ingested_at,
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
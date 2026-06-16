import os

import psycopg2


EXPECTED_SCHEMAS = {
    "landing",
    "staging",
    "rejected",
    "bronze",
    "silver",
    "gold",
    "audit",
}

EXPECTED_TABLES = {
    ("landing", "crypto_market_raw"),
    ("staging", "crypto_market_valid"),
    ("rejected", "crypto_market_rejected"),
    ("bronze", "crypto_market"),
    ("silver", "crypto_market"),
    ("gold", "crypto_market_summary"),
    ("gold", "crypto_top_performers"),
    ("gold", "crypto_market_dominance"),
    ("audit", "watermarks"),
    ("audit", "pipeline_runs"),
    ("audit", "data_quality_metrics"),
}

EXPECTED_WATERMARKS = {
    "crypto_market_pipeline",
    "bronze_upsert",
    "silver_upsert",
    "gold_upsert",
}


def get_connection():
    return psycopg2.connect(
        host=os.getenv("TEST_DB_HOST", "postgres"),
        port=os.getenv("TEST_DB_PORT", "5432"),
        dbname=os.getenv("TEST_DB_NAME", "airflow"),
        user=os.getenv("TEST_DB_USER", "airflow"),
        password=os.getenv("TEST_DB_PASSWORD", "airflow"),
    )


def test_required_schemas_exist():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                """
            )
            existing_schemas = {row[0] for row in cursor.fetchall()}

    missing_schemas = EXPECTED_SCHEMAS - existing_schemas
    assert missing_schemas == set()


def test_required_tables_exist():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                """
            )
            existing_tables = {(row[0], row[1]) for row in cursor.fetchall()}

    missing_tables = EXPECTED_TABLES - existing_tables
    assert missing_tables == set()


def test_watermark_seed_records_exist():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT pipeline_name, last_ingestion_id
                FROM audit.watermarks
                """
            )
            rows = cursor.fetchall()

    watermark_values = {pipeline_name: last_id for pipeline_name, last_id in rows}

    missing_watermarks = EXPECTED_WATERMARKS - set(watermark_values)
    assert missing_watermarks == set()

    for pipeline_name in EXPECTED_WATERMARKS:
        assert watermark_values[pipeline_name] is not None
        assert watermark_values[pipeline_name] >= 0
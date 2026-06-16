from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from contextlib import contextmanager
from datetime import datetime, timedelta

import json
import logging
import requests


# =====================================================
# CONFIGURACION
# =====================================================

POSTGRES_CONN_ID   = "postgres_ecommerce"
PIPELINE_NAME      = "crypto_market_pipeline"
QUALITY_MIN_SCORE  = float(Variable.get("quality_min_score",  default_var=95.0))
COINGECKO_PER_PAGE = int(Variable.get("coingecko_per_page",   default_var=250))
COINGECKO_PAGE     = int(Variable.get("coingecko_page",       default_var=1))

logger = logging.getLogger(__name__)

default_args = {
    "owner":            "data_engineering",
    "depends_on_past":  False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}


# =====================================================
# HELPERS
# =====================================================

@contextmanager
def get_postgres_conn():
    hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        yield conn, cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def _get_watermark(cursor, pipeline_name: str) -> int:
    cursor.execute(
        """
        SELECT last_ingestion_id
        FROM   audit.watermarks
        WHERE  pipeline_name = %s
        """,
        (pipeline_name,)
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"Watermark no encontrado para: {pipeline_name}")
    return row[0]


def _update_watermark(cursor, pipeline_name: str, new_id: int) -> None:
    cursor.execute(
        """
        UPDATE audit.watermarks
        SET    last_ingestion_id = %s,
               updated_at        = CURRENT_TIMESTAMP
        WHERE  pipeline_name     = %s
        """,
        (new_id, pipeline_name)
    )


def _validate_record(payload: dict) -> list[str]:
    errors = []

    if not payload.get("id"):
        errors.append("coin_id is null or empty")
    if not payload.get("symbol"):
        errors.append("symbol is null or empty")
    if not payload.get("name"):
        errors.append("name is null or empty")

    price = payload.get("current_price")
    if price is None or price <= 0:
        errors.append(f"invalid current_price: {price}")

    cap = payload.get("market_cap")
    if cap is None or cap <= 0:
        errors.append(f"invalid market_cap: {cap}")

    rank = payload.get("market_cap_rank")
    if rank is None or rank <= 0:
        errors.append(f"invalid market_cap_rank: {rank}")

    return errors


# =====================================================
# AUDITORIA — HELPERS
# =====================================================

def _get_run_id(context) -> int | None:
    """
    Recupera el run_id guardado por start_pipeline_run via XCom.
    Retorna None si no existe (proteccion defensiva).
    """
    run_id = context["ti"].xcom_pull(
        task_ids="start_pipeline_run",
        key="pipeline_run_id",
    )
    if run_id is None:
        logger.warning("No se encontro run_id en XCom. Auditoria omitida.")
    return run_id


def _get_quality_metrics(cursor) -> tuple[int, int, int]:
    """
    Lee el ultimo registro de audit.data_quality_metrics
    para obtener los conteos del run actual.
    Retorna (total, validos, invalidos).
    """
    cursor.execute(
        """
        SELECT total_records, valid_records, invalid_records
        FROM   audit.data_quality_metrics
        ORDER  BY execution_timestamp DESC
        LIMIT  1
        """
    )
    row = cursor.fetchone()
    if row:
        return row[0], row[1], row[2]
    return 0, 0, 0


# =====================================================
# AUDITORIA — CALLBACKS DEL DAG
# =====================================================

def _on_dag_success(context) -> None:
    """
    Callback ejecutado por Airflow cuando el DAG completa
    todas sus tasks exitosamente.
    Actualiza pipeline_runs con status='success' y las
    metricas de calidad leidas desde data_quality_metrics.
    """
    run_id = _get_run_id(context)
    if run_id is None:
        return

    logger.info("=== AUDITORIA: DAG exitoso. Actualizando run_id=%d ===", run_id)

    try:
        with get_postgres_conn() as (conn, cursor):

            total, validos, invalidos = _get_quality_metrics(cursor)

            cursor.execute(
                """
                UPDATE audit.pipeline_runs
                SET    end_time          = CURRENT_TIMESTAMP,
                       duration_seconds  = EXTRACT(
                                               EPOCH FROM (
                                                   CURRENT_TIMESTAMP - start_time
                                               )
                                           )::int,
                       records_extracted = %s,
                       records_valid     = %s,
                       records_invalid   = %s,
                       status            = 'success'
                WHERE  run_id = %s
                """,
                (total, validos, invalidos, run_id)
            )

        logger.info(
            "pipeline_runs actualizado: run_id=%d | status=success | "
            "extracted=%d | valid=%d | invalid=%d",
            run_id, total, validos, invalidos,
        )

    except Exception as exc:
        # El callback nunca debe romper el estado del DAG
        logger.error("Error actualizando pipeline_runs en success: %s", exc)


def _on_dag_failure(context) -> None:
    """
    Callback ejecutado por Airflow cuando cualquier task
    del DAG falla y agota sus reintentos.
    Actualiza pipeline_runs con status='failed' y el
    mensaje de error capturado del contexto de Airflow.
    """
    run_id = _get_run_id(context)
    if run_id is None:
        return

    # Airflow expone la excepcion en context["exception"]
    exception     = context.get("exception")
    error_message = str(exception) if exception else "Unknown error"
    task_id       = context["task_instance"].task_id

    # Incluir el task que fallo para facilitar el debugging
    full_error = f"[task: {task_id}] {error_message}"

    logger.warning(
        "=== AUDITORIA: DAG fallido. run_id=%d | task=%s ===",
        run_id, task_id,
    )

    try:
        with get_postgres_conn() as (conn, cursor):

            # Intentamos leer metricas parciales si quality_check
            # ya habia ejecutado antes del fallo
            total, validos, invalidos = _get_quality_metrics(cursor)

            cursor.execute(
                """
                UPDATE audit.pipeline_runs
                SET    end_time          = CURRENT_TIMESTAMP,
                       duration_seconds  = EXTRACT(
                                               EPOCH FROM (
                                                   CURRENT_TIMESTAMP - start_time
                                               )
                                           )::int,
                       records_extracted = %s,
                       records_valid     = %s,
                       records_invalid   = %s,
                       status            = 'failed',
                       error_message     = %s
                WHERE  run_id = %s
                """,
                (total or None, validos or None, invalidos or None,
                 full_error, run_id)
            )

        logger.warning(
            "pipeline_runs actualizado: run_id=%d | status=failed | error=%s",
            run_id, full_error,
        )

    except Exception as exc:
        logger.error("Error actualizando pipeline_runs en failure: %s", exc)


# =====================================================
# TASK 0 — START PIPELINE RUN (AUDITORIA)
# Inserta la fila inicial en pipeline_runs y propaga
# el run_id via XCom para que los callbacks lo usen.
# =====================================================

def start_pipeline_run(**context) -> None:
    """
    Abre el registro de auditoria al inicio del DAG.
    Guarda el run_id en XCom bajo la key 'pipeline_run_id'.
    """
    dag_id = context["dag"].dag_id

    logger.info("=== AUDITORIA: Iniciando pipeline_run para dag_id=%s ===", dag_id)

    with get_postgres_conn() as (conn, cursor):
        cursor.execute(
            """
            INSERT INTO audit.pipeline_runs
                (dag_id, start_time, status)
            VALUES
                (%s, CURRENT_TIMESTAMP, 'running')
            RETURNING run_id
            """,
            (dag_id,)
        )
        run_id = cursor.fetchone()[0]

    # Publicar en XCom para que los callbacks lo lean
    context["ti"].xcom_push(key="pipeline_run_id", value=run_id)

    logger.info("=== AUDITORIA: pipeline_run abierto. run_id=%d ===", run_id)


# =====================================================
# TASK 1 — EXTRACT + LANDING
# =====================================================

def extract_and_load_landing() -> None:

    logger.info("=== INICIANDO EXTRACCION COINGECKO ===")

    url    = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
    "vs_currency": "usd",
    "order":       "market_cap_desc",
    "per_page":    COINGECKO_PER_PAGE,
    "page":        COINGECKO_PAGE,
    "sparkline":   "false",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    records = response.json()

    if not records:
        raise ValueError("CoinGecko devolvio 0 registros")

    logger.info("Registros recibidos de API: %d", len(records))

    with get_postgres_conn() as (conn, cursor):
        cursor.executemany(
            """
            INSERT INTO landing.crypto_market_raw
                (source_name, extraction_timestamp, payload)
            VALUES
                (%s, CURRENT_TIMESTAMP, %s::jsonb)
            """,
            [("coingecko", json.dumps(r)) for r in records]
        )

    logger.info("Landing cargada. Total insertados: %d", len(records))
    logger.info("=== FIN EXTRACCION COINGECKO ===")


# =====================================================
# TASK 2 — DATA QUALITY CHECK
# =====================================================

def quality_check() -> None:

    logger.info("=== INICIANDO DATA QUALITY CHECK ===")

    with get_postgres_conn() as (conn, cursor):

        watermark = _get_watermark(cursor, PIPELINE_NAME)
        logger.info("Watermark actual: %d", watermark)

        cursor.execute(
            """
            SELECT ingestion_id, payload
            FROM   landing.crypto_market_raw
            WHERE  ingestion_id > %s
            ORDER  BY ingestion_id
            """,
            (watermark,)
        )
        records = cursor.fetchall()
        total   = len(records)

        if total == 0:
            logger.info("No existen registros nuevos. Fin.")
            return

        valid_rows    = []
        rejected_rows = []
        max_id        = watermark

        for ingestion_id, payload in records:

            if ingestion_id > max_id:
                max_id = ingestion_id

            errors = _validate_record(payload)

            if errors:
                rejected_rows.append((
                    json.dumps(payload),
                    " | ".join(errors)
                ))
            else:
                valid_rows.append((
                    ingestion_id,
                    json.dumps(payload),
                ))

        if valid_rows:
            cursor.executemany(
                """
                INSERT INTO staging.crypto_market_valid
                    (ingestion_id, payload, created_at)
                VALUES
                    (%s, %s::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (ingestion_id) DO NOTHING
                """,
                valid_rows
            )
            logger.info("Validos insertados en staging: %d", len(valid_rows))

        if rejected_rows:
            cursor.executemany(
                """
                INSERT INTO rejected.crypto_market_rejected
                    (source_payload, rejection_reason)
                VALUES
                    (%s::jsonb, %s)
                """,
                rejected_rows
            )
            logger.warning("Registros rechazados: %d", len(rejected_rows))

        quality_score = round((len(valid_rows) / total) * 100, 2)

        cursor.execute(
            """
            INSERT INTO audit.data_quality_metrics
                (execution_timestamp, total_records,
                 valid_records, invalid_records, quality_score)
            VALUES
                (CURRENT_TIMESTAMP, %s, %s, %s, %s)
            """,
            (total, len(valid_rows), len(rejected_rows), quality_score)
        )

        _update_watermark(cursor, PIPELINE_NAME, max_id)

    logger.info("Total nuevos   : %d", total)
    logger.info("Validos        : %d", len(valid_rows))
    logger.info("Invalidos      : %d", len(rejected_rows))
    logger.info("Quality Score  : %.2f%%", quality_score)
    logger.info("Nuevo watermark: %d", max_id)

    if quality_score < QUALITY_MIN_SCORE:
        raise ValueError(
            f"Quality Score demasiado bajo: {quality_score}% "
            f"(minimo requerido: {QUALITY_MIN_SCORE}%)"
        )

    logger.info("=== DATA QUALITY CHECK EXITOSO ===")


# =====================================================
# TASK 3 — BRONZE UPSERT
# =====================================================

def bronze_upsert() -> None:

    logger.info("=== INICIANDO BRONZE UPSERT ===")

    BRONZE_PIPELINE = "bronze_upsert"

    with get_postgres_conn() as (conn, cursor):

        watermark = _get_watermark(cursor, BRONZE_PIPELINE)
        logger.info("Watermark Bronze: %d", watermark)

        cursor.execute(
            """
            SELECT ingestion_id, payload
            FROM   staging.crypto_market_valid
            WHERE  ingestion_id > %s
            ORDER  BY ingestion_id
            """,
            (watermark,)
        )
        records = cursor.fetchall()
        total   = len(records)

        if total == 0:
            logger.info("No existen registros nuevos para Bronze.")
            return

        max_id = watermark
        rows   = []

        for ingestion_id, payload in records:

            if ingestion_id > max_id:
                max_id = ingestion_id

            rows.append((
                payload.get("id"),
                payload.get("symbol"),
                payload.get("name"),
                payload.get("current_price"),
                payload.get("market_cap"),
                payload.get("market_cap_rank"),
                ingestion_id,
            ))

        cursor.executemany(
            """
            INSERT INTO bronze.crypto_market
                (coin_id, symbol, coin_name,
                 current_price, market_cap, market_cap_rank,
                 last_updated, ingestion_id)
            VALUES
                (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (coin_id) DO UPDATE SET
                symbol          = EXCLUDED.symbol,
                coin_name       = EXCLUDED.coin_name,
                current_price   = EXCLUDED.current_price,
                market_cap      = EXCLUDED.market_cap,
                market_cap_rank = EXCLUDED.market_cap_rank,
                ingestion_id    = EXCLUDED.ingestion_id,
                updated_at      = CURRENT_TIMESTAMP
            """,
            rows
        )

        _update_watermark(cursor, BRONZE_PIPELINE, max_id)

    logger.info("Registros procesados  : %d", total)
    logger.info("Nuevo watermark Bronze: %d", max_id)
    logger.info("=== BRONZE UPSERT EXITOSO ===")


# =====================================================
# TASK 4 — SILVER UPSERT
# =====================================================

def silver_upsert() -> None:

    logger.info("=== INICIANDO SILVER UPSERT ===")

    SILVER_PIPELINE = "silver_upsert"

    with get_postgres_conn() as (conn, cursor):

        watermark = _get_watermark(cursor, SILVER_PIPELINE)
        logger.info("Watermark Silver: %d", watermark)

        cursor.execute(
            """
            SELECT ingestion_id, coin_id, symbol, coin_name,
                   current_price, market_cap, market_cap_rank
            FROM   bronze.crypto_market
            WHERE  ingestion_id > %s
            ORDER  BY ingestion_id
            """,
            (watermark,)
        )
        records = cursor.fetchall()
        total   = len(records)

        if total == 0:
            logger.info("No existen registros nuevos para Silver.")
            return

        max_id = watermark
        rows   = []

        for ingestion_id, coin_id, symbol, coin_name, \
                current_price, market_cap, market_cap_rank in records:

            if ingestion_id > max_id:
                max_id = ingestion_id

            rows.append((
                coin_id,
                symbol,
                coin_name,
                current_price,
                market_cap,
                market_cap_rank,
                ingestion_id,
            ))

        cursor.executemany(
            """
            INSERT INTO silver.crypto_market
                (coin_id, symbol, coin_name,
                 current_price, market_cap, market_cap_rank,
                 snapshot_date, processed_at, ingestion_id)
            VALUES
                (%s, %s, %s, %s, %s, %s,
                 CURRENT_DATE, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (coin_id, snapshot_date) DO UPDATE SET
                current_price   = EXCLUDED.current_price,
                market_cap      = EXCLUDED.market_cap,
                market_cap_rank = EXCLUDED.market_cap_rank,
                processed_at    = CURRENT_TIMESTAMP
            """,
            rows
        )

        _update_watermark(cursor, SILVER_PIPELINE, max_id)

    logger.info("Registros procesados   : %d", total)
    logger.info("Nuevo watermark Silver : %d", max_id)
    logger.info("=== SILVER UPSERT EXITOSO ===")


# =====================================================
# TASK 5 — GOLD UPSERT
# =====================================================

# =====================================================
# TASK 5 — GOLD UPSERT
# =====================================================

def gold_upsert() -> None:

    logger.info("=== INICIANDO GOLD UPSERT ===")

    GOLD_PIPELINE = "gold_upsert"

    with get_postgres_conn() as (conn, cursor):

        watermark = _get_watermark(cursor, GOLD_PIPELINE)
        logger.info("Watermark Gold: %d", watermark)

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM   silver.crypto_market
            WHERE  ingestion_id > %s
            """,
            (watermark,)
        )
        total = cursor.fetchone()[0]

        if total == 0:
            logger.info("No existen registros nuevos para Gold.")
            return

        logger.info("Calculando market summary...")
        cursor.execute(
            """
            INSERT INTO gold.crypto_market_summary
            (
                snapshot_date, total_coins, total_market_cap,
                avg_price, highest_price, lowest_price,
                top_coin_id, top_coin_market_cap, generated_at
            )
            SELECT
                s.snapshot_date,
                COUNT(*),
                SUM(s.market_cap),
                AVG(s.current_price),
                MAX(s.current_price),
                MIN(s.current_price),
                (
                    SELECT s2.coin_id FROM silver.crypto_market s2
                    WHERE  s2.snapshot_date = s.snapshot_date
                    AND    s2.market_cap_rank = 1 LIMIT 1
                ),
                (
                    SELECT s3.market_cap FROM silver.crypto_market s3
                    WHERE  s3.snapshot_date = s.snapshot_date
                    AND    s3.market_cap_rank = 1 LIMIT 1
                ),
                CURRENT_TIMESTAMP
            FROM   silver.crypto_market s
            WHERE  s.ingestion_id > %s
            GROUP  BY s.snapshot_date
            ON CONFLICT (snapshot_date) DO UPDATE SET
                total_coins         = EXCLUDED.total_coins,
                total_market_cap    = EXCLUDED.total_market_cap,
                avg_price           = EXCLUDED.avg_price,
                highest_price       = EXCLUDED.highest_price,
                lowest_price        = EXCLUDED.lowest_price,
                top_coin_id         = EXCLUDED.top_coin_id,
                top_coin_market_cap = EXCLUDED.top_coin_market_cap,
                generated_at        = CURRENT_TIMESTAMP
            """,
            (watermark,)
        )
        logger.info("Market summary calculado.")

        logger.info("Calculando top performers...")
        cursor.execute(
            """
            WITH con_lag AS (
                SELECT
                    snapshot_date, coin_id, symbol, coin_name,
                    current_price, market_cap_rank,
                    ingestion_id,
                    LAG(current_price) OVER (
                        PARTITION BY coin_id ORDER BY snapshot_date
                    ) AS previous_price
                FROM silver.crypto_market
            )
            INSERT INTO gold.crypto_top_performers
            (
                snapshot_date, coin_id, symbol, coin_name,
                current_price, previous_price, price_change,
                price_change_pct, market_cap_rank,
                performance_label, generated_at
            )
            SELECT
                snapshot_date, coin_id, symbol, coin_name,
                current_price, previous_price,
                current_price - previous_price,
                CASE
                    WHEN previous_price IS NULL OR previous_price = 0 THEN NULL
                    ELSE ROUND(
                        ((current_price - previous_price) / previous_price) * 100, 4
                    )
                END,
                market_cap_rank,
                CASE
                    WHEN previous_price IS NULL         THEN 'neutral'
                    WHEN current_price > previous_price THEN 'gainer'
                    WHEN current_price < previous_price THEN 'loser'
                    ELSE                                     'neutral'
                END,
                CURRENT_TIMESTAMP
            FROM con_lag
            WHERE ingestion_id > %s
            ON CONFLICT (coin_id, snapshot_date) DO UPDATE SET
                current_price     = EXCLUDED.current_price,
                previous_price    = EXCLUDED.previous_price,
                price_change      = EXCLUDED.price_change,
                price_change_pct  = EXCLUDED.price_change_pct,
                performance_label = EXCLUDED.performance_label,
                generated_at      = CURRENT_TIMESTAMP
            """,
            (watermark,)
        )
        logger.info("Top performers calculado.")

        logger.info("Calculando market dominance...")
        cursor.execute(
            """
            WITH con_totales AS (
                SELECT
                    snapshot_date, coin_id, symbol, coin_name,
                    market_cap, market_cap_rank,
                    SUM(market_cap) OVER (
                        PARTITION BY snapshot_date
                    ) AS total_market_cap
                FROM silver.crypto_market
                WHERE ingestion_id > %s
            )
            INSERT INTO gold.crypto_market_dominance
            (
                snapshot_date, coin_id, symbol, coin_name,
                market_cap, total_market_cap, dominance_pct,
                market_cap_rank, market_cap_tier, generated_at
            )
            SELECT
                snapshot_date, coin_id, symbol, coin_name,
                market_cap, total_market_cap,
                ROUND(
                    (market_cap / NULLIF(total_market_cap, 0)) * 100, 4
                ),
                market_cap_rank,
                CASE
                    WHEN market_cap_rank <= 10 THEN 'large'
                    WHEN market_cap_rank <= 50 THEN 'mid'
                    ELSE                            'small'
                END,
                CURRENT_TIMESTAMP
            FROM con_totales
            ON CONFLICT (coin_id, snapshot_date) DO UPDATE SET
                market_cap       = EXCLUDED.market_cap,
                total_market_cap = EXCLUDED.total_market_cap,
                dominance_pct    = EXCLUDED.dominance_pct,
                market_cap_rank  = EXCLUDED.market_cap_rank,
                market_cap_tier  = EXCLUDED.market_cap_tier,
                generated_at     = CURRENT_TIMESTAMP
            """,
            (watermark,)
        )
        logger.info("Market dominance calculado.")

        cursor.execute(
            """
            SELECT MAX(ingestion_id)
            FROM   silver.crypto_market
            WHERE  ingestion_id > %s
            """,
            (watermark,)
        )
        max_id = cursor.fetchone()[0]
        _update_watermark(cursor, GOLD_PIPELINE, max_id)

    logger.info("Nuevo watermark Gold: %d", max_id)
    logger.info("=== GOLD UPSERT EXITOSO ===")


# =====================================================
# DAG
# =====================================================

with DAG(
    dag_id              = "crypto_market_pipeline",
    start_date          = datetime(2025, 1, 1),
    schedule            = "0 */6 * * *",
    catchup             = False,
    default_args        = default_args,
    on_success_callback = _on_dag_success,
    on_failure_callback = _on_dag_failure,
    description         = "Pipeline medallion: CoinGecko → Landing → Staging → Bronze → Silver → Gold",
    tags                = ["crypto", "coingecko", "medallion", "quality"],
) as dag:

    start_run_task = PythonOperator(
        task_id         = "start_pipeline_run",
        python_callable = start_pipeline_run,
    )

    landing_task = PythonOperator(
        task_id         = "extract_and_load_landing",
        python_callable = extract_and_load_landing,
    )

    quality_task = PythonOperator(
        task_id         = "quality_check",
        python_callable = quality_check,
    )

    bronze_task = PythonOperator(
        task_id         = "bronze_upsert",
        python_callable = bronze_upsert,
    )

    silver_task = PythonOperator(
        task_id         = "silver_upsert",
        python_callable = silver_upsert,
    )

    gold_task = PythonOperator(
        task_id         = "gold_upsert",
        python_callable = gold_upsert,
    )

    start_run_task >> landing_task >> quality_task >> bronze_task >> silver_task >> gold_task
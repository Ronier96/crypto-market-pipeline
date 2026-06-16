INSERT INTO audit.watermarks
    (pipeline_name, last_ingestion_id, updated_at)
VALUES
    ('crypto_market_pipeline', 0, CURRENT_TIMESTAMP),
    ('bronze_upsert',          0, CURRENT_TIMESTAMP),
    ('silver_upsert',          0, CURRENT_TIMESTAMP),
    ('gold_upsert',            0, CURRENT_TIMESTAMP)
ON CONFLICT (pipeline_name) DO NOTHING;
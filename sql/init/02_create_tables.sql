--
-- PostgreSQL database dump
--

\restrict ma5kbJfQljaclgHS8B71jTjmvQPvCehR0fDbc1YS8VUoEz4aXkwfIa0h1THnM31

-- Dumped from database version 13.23 (Debian 13.23-1.pgdg13+1)
-- Dumped by pg_dump version 13.23 (Debian 13.23-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: data_quality_metrics; Type: TABLE; Schema: audit; Owner: -
--

CREATE TABLE audit.data_quality_metrics (
    metric_id bigint NOT NULL,
    execution_timestamp timestamp without time zone,
    total_records integer,
    valid_records integer,
    invalid_records integer,
    quality_score numeric(5,2),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: data_quality_metrics_metric_id_seq; Type: SEQUENCE; Schema: audit; Owner: -
--

CREATE SEQUENCE audit.data_quality_metrics_metric_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: data_quality_metrics_metric_id_seq; Type: SEQUENCE OWNED BY; Schema: audit; Owner: -
--

ALTER SEQUENCE audit.data_quality_metrics_metric_id_seq OWNED BY audit.data_quality_metrics.metric_id;


--
-- Name: pipeline_runs; Type: TABLE; Schema: audit; Owner: -
--

CREATE TABLE audit.pipeline_runs (
    run_id bigint NOT NULL,
    dag_id character varying(200),
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    duration_seconds integer,
    records_extracted integer,
    records_valid integer,
    records_invalid integer,
    status character varying(50),
    error_message text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: pipeline_runs_run_id_seq; Type: SEQUENCE; Schema: audit; Owner: -
--

CREATE SEQUENCE audit.pipeline_runs_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pipeline_runs_run_id_seq; Type: SEQUENCE OWNED BY; Schema: audit; Owner: -
--

ALTER SEQUENCE audit.pipeline_runs_run_id_seq OWNED BY audit.pipeline_runs.run_id;


--
-- Name: watermarks; Type: TABLE; Schema: audit; Owner: -
--

CREATE TABLE audit.watermarks (
    pipeline_name character varying(100) NOT NULL,
    last_ingestion_id bigint,
    updated_at timestamp without time zone
);


--
-- Name: crypto_market; Type: TABLE; Schema: bronze; Owner: -
--

CREATE TABLE bronze.crypto_market (
    coin_id character varying(100) NOT NULL,
    symbol character varying(50),
    coin_name character varying(200),
    current_price numeric(20,8),
    market_cap numeric(30,2),
    market_cap_rank integer,
    last_updated timestamp without time zone,
    ingestion_id bigint,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: crypto_market_dominance; Type: TABLE; Schema: gold; Owner: -
--

CREATE TABLE gold.crypto_market_dominance (
    id bigint NOT NULL,
    snapshot_date date NOT NULL,
    coin_id character varying(100) NOT NULL,
    symbol character varying(50) NOT NULL,
    coin_name character varying(200) NOT NULL,
    market_cap numeric(30,2) NOT NULL,
    total_market_cap numeric(30,2) NOT NULL,
    dominance_pct numeric(10,4) NOT NULL,
    market_cap_rank integer NOT NULL,
    market_cap_tier character varying(20) NOT NULL,
    generated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: crypto_market_dominance_id_seq; Type: SEQUENCE; Schema: gold; Owner: -
--

CREATE SEQUENCE gold.crypto_market_dominance_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: crypto_market_dominance_id_seq; Type: SEQUENCE OWNED BY; Schema: gold; Owner: -
--

ALTER SEQUENCE gold.crypto_market_dominance_id_seq OWNED BY gold.crypto_market_dominance.id;


--
-- Name: crypto_market_summary; Type: TABLE; Schema: gold; Owner: -
--

CREATE TABLE gold.crypto_market_summary (
    id bigint NOT NULL,
    snapshot_date date NOT NULL,
    total_coins integer NOT NULL,
    total_market_cap numeric(30,2) NOT NULL,
    avg_price numeric(20,8) NOT NULL,
    highest_price numeric(20,8) NOT NULL,
    lowest_price numeric(20,8) NOT NULL,
    top_coin_id character varying(100) NOT NULL,
    top_coin_market_cap numeric(30,2) NOT NULL,
    generated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: crypto_market_summary_id_seq; Type: SEQUENCE; Schema: gold; Owner: -
--

CREATE SEQUENCE gold.crypto_market_summary_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: crypto_market_summary_id_seq; Type: SEQUENCE OWNED BY; Schema: gold; Owner: -
--

ALTER SEQUENCE gold.crypto_market_summary_id_seq OWNED BY gold.crypto_market_summary.id;


--
-- Name: crypto_top_performers; Type: TABLE; Schema: gold; Owner: -
--

CREATE TABLE gold.crypto_top_performers (
    id bigint NOT NULL,
    snapshot_date date NOT NULL,
    coin_id character varying(100) NOT NULL,
    symbol character varying(50) NOT NULL,
    coin_name character varying(200) NOT NULL,
    current_price numeric(20,8) NOT NULL,
    previous_price numeric(20,8),
    price_change numeric(20,8),
    price_change_pct numeric(10,4),
    market_cap_rank integer NOT NULL,
    performance_label character varying(20) NOT NULL,
    generated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: crypto_top_performers_id_seq; Type: SEQUENCE; Schema: gold; Owner: -
--

CREATE SEQUENCE gold.crypto_top_performers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: crypto_top_performers_id_seq; Type: SEQUENCE OWNED BY; Schema: gold; Owner: -
--

ALTER SEQUENCE gold.crypto_top_performers_id_seq OWNED BY gold.crypto_top_performers.id;


--
-- Name: crypto_market_raw; Type: TABLE; Schema: landing; Owner: -
--

CREATE TABLE landing.crypto_market_raw (
    ingestion_id bigint NOT NULL,
    source_name character varying(100),
    extraction_timestamp timestamp without time zone,
    payload jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: crypto_market_raw_ingestion_id_seq; Type: SEQUENCE; Schema: landing; Owner: -
--

CREATE SEQUENCE landing.crypto_market_raw_ingestion_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: crypto_market_raw_ingestion_id_seq; Type: SEQUENCE OWNED BY; Schema: landing; Owner: -
--

ALTER SEQUENCE landing.crypto_market_raw_ingestion_id_seq OWNED BY landing.crypto_market_raw.ingestion_id;


--
-- Name: crypto_market_rejected; Type: TABLE; Schema: rejected; Owner: -
--

CREATE TABLE rejected.crypto_market_rejected (
    rejected_id bigint NOT NULL,
    source_payload jsonb,
    rejection_reason text,
    rejected_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: crypto_market_rejected_rejected_id_seq; Type: SEQUENCE; Schema: rejected; Owner: -
--

CREATE SEQUENCE rejected.crypto_market_rejected_rejected_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: crypto_market_rejected_rejected_id_seq; Type: SEQUENCE OWNED BY; Schema: rejected; Owner: -
--

ALTER SEQUENCE rejected.crypto_market_rejected_rejected_id_seq OWNED BY rejected.crypto_market_rejected.rejected_id;


--
-- Name: crypto_market; Type: TABLE; Schema: silver; Owner: -
--

CREATE TABLE silver.crypto_market (
    id bigint NOT NULL,
    coin_id character varying(100) NOT NULL,
    symbol character varying(50) NOT NULL,
    coin_name character varying(200) NOT NULL,
    current_price numeric(20,8) NOT NULL,
    market_cap numeric(30,2) NOT NULL,
    market_cap_rank integer NOT NULL,
    snapshot_date date DEFAULT CURRENT_DATE NOT NULL,
    processed_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ingestion_id bigint NOT NULL
);


--
-- Name: crypto_market_id_seq; Type: SEQUENCE; Schema: silver; Owner: -
--

CREATE SEQUENCE silver.crypto_market_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: crypto_market_id_seq; Type: SEQUENCE OWNED BY; Schema: silver; Owner: -
--

ALTER SEQUENCE silver.crypto_market_id_seq OWNED BY silver.crypto_market.id;


--
-- Name: crypto_market_valid; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.crypto_market_valid (
    id bigint NOT NULL,
    ingestion_id bigint NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: crypto_market_valid_id_seq; Type: SEQUENCE; Schema: staging; Owner: -
--

CREATE SEQUENCE staging.crypto_market_valid_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: crypto_market_valid_id_seq; Type: SEQUENCE OWNED BY; Schema: staging; Owner: -
--

ALTER SEQUENCE staging.crypto_market_valid_id_seq OWNED BY staging.crypto_market_valid.id;


--
-- Name: data_quality_metrics metric_id; Type: DEFAULT; Schema: audit; Owner: -
--

ALTER TABLE ONLY audit.data_quality_metrics ALTER COLUMN metric_id SET DEFAULT nextval('audit.data_quality_metrics_metric_id_seq'::regclass);


--
-- Name: pipeline_runs run_id; Type: DEFAULT; Schema: audit; Owner: -
--

ALTER TABLE ONLY audit.pipeline_runs ALTER COLUMN run_id SET DEFAULT nextval('audit.pipeline_runs_run_id_seq'::regclass);


--
-- Name: crypto_market_dominance id; Type: DEFAULT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.crypto_market_dominance ALTER COLUMN id SET DEFAULT nextval('gold.crypto_market_dominance_id_seq'::regclass);


--
-- Name: crypto_market_summary id; Type: DEFAULT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.crypto_market_summary ALTER COLUMN id SET DEFAULT nextval('gold.crypto_market_summary_id_seq'::regclass);


--
-- Name: crypto_top_performers id; Type: DEFAULT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.crypto_top_performers ALTER COLUMN id SET DEFAULT nextval('gold.crypto_top_performers_id_seq'::regclass);


--
-- Name: crypto_market_raw ingestion_id; Type: DEFAULT; Schema: landing; Owner: -
--

ALTER TABLE ONLY landing.crypto_market_raw ALTER COLUMN ingestion_id SET DEFAULT nextval('landing.crypto_market_raw_ingestion_id_seq'::regclass);


--
-- Name: crypto_market_rejected rejected_id; Type: DEFAULT; Schema: rejected; Owner: -
--

ALTER TABLE ONLY rejected.crypto_market_rejected ALTER COLUMN rejected_id SET DEFAULT nextval('rejected.crypto_market_rejected_rejected_id_seq'::regclass);


--
-- Name: crypto_market id; Type: DEFAULT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.crypto_market ALTER COLUMN id SET DEFAULT nextval('silver.crypto_market_id_seq'::regclass);


--
-- Name: crypto_market_valid id; Type: DEFAULT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.crypto_market_valid ALTER COLUMN id SET DEFAULT nextval('staging.crypto_market_valid_id_seq'::regclass);


--
-- Name: data_quality_metrics data_quality_metrics_pkey; Type: CONSTRAINT; Schema: audit; Owner: -
--

ALTER TABLE ONLY audit.data_quality_metrics
    ADD CONSTRAINT data_quality_metrics_pkey PRIMARY KEY (metric_id);


--
-- Name: pipeline_runs pipeline_runs_pkey; Type: CONSTRAINT; Schema: audit; Owner: -
--

ALTER TABLE ONLY audit.pipeline_runs
    ADD CONSTRAINT pipeline_runs_pkey PRIMARY KEY (run_id);


--
-- Name: watermarks watermarks_pkey; Type: CONSTRAINT; Schema: audit; Owner: -
--

ALTER TABLE ONLY audit.watermarks
    ADD CONSTRAINT watermarks_pkey PRIMARY KEY (pipeline_name);


--
-- Name: crypto_market crypto_market_pkey; Type: CONSTRAINT; Schema: bronze; Owner: -
--

ALTER TABLE ONLY bronze.crypto_market
    ADD CONSTRAINT crypto_market_pkey PRIMARY KEY (coin_id);


--
-- Name: crypto_market_dominance crypto_market_dominance_pkey; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.crypto_market_dominance
    ADD CONSTRAINT crypto_market_dominance_pkey PRIMARY KEY (id);


--
-- Name: crypto_market_summary crypto_market_summary_pkey; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.crypto_market_summary
    ADD CONSTRAINT crypto_market_summary_pkey PRIMARY KEY (id);


--
-- Name: crypto_market_summary crypto_market_summary_snapshot_date_key; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.crypto_market_summary
    ADD CONSTRAINT crypto_market_summary_snapshot_date_key UNIQUE (snapshot_date);


--
-- Name: crypto_top_performers crypto_top_performers_pkey; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.crypto_top_performers
    ADD CONSTRAINT crypto_top_performers_pkey PRIMARY KEY (id);


--
-- Name: crypto_market_dominance uq_dominance_coin_date; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.crypto_market_dominance
    ADD CONSTRAINT uq_dominance_coin_date UNIQUE (coin_id, snapshot_date);


--
-- Name: crypto_top_performers uq_performers_coin_date; Type: CONSTRAINT; Schema: gold; Owner: -
--

ALTER TABLE ONLY gold.crypto_top_performers
    ADD CONSTRAINT uq_performers_coin_date UNIQUE (coin_id, snapshot_date);


--
-- Name: crypto_market_raw crypto_market_raw_pkey; Type: CONSTRAINT; Schema: landing; Owner: -
--

ALTER TABLE ONLY landing.crypto_market_raw
    ADD CONSTRAINT crypto_market_raw_pkey PRIMARY KEY (ingestion_id);


--
-- Name: crypto_market_rejected crypto_market_rejected_pkey; Type: CONSTRAINT; Schema: rejected; Owner: -
--

ALTER TABLE ONLY rejected.crypto_market_rejected
    ADD CONSTRAINT crypto_market_rejected_pkey PRIMARY KEY (rejected_id);


--
-- Name: crypto_market crypto_market_pkey; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.crypto_market
    ADD CONSTRAINT crypto_market_pkey PRIMARY KEY (id);


--
-- Name: crypto_market uq_silver_coin_date; Type: CONSTRAINT; Schema: silver; Owner: -
--

ALTER TABLE ONLY silver.crypto_market
    ADD CONSTRAINT uq_silver_coin_date UNIQUE (coin_id, snapshot_date);


--
-- Name: crypto_market_valid crypto_market_valid_ingestion_id_key; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.crypto_market_valid
    ADD CONSTRAINT crypto_market_valid_ingestion_id_key UNIQUE (ingestion_id);


--
-- Name: crypto_market_valid crypto_market_valid_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.crypto_market_valid
    ADD CONSTRAINT crypto_market_valid_pkey PRIMARY KEY (id);


--
-- Name: idx_dominance_dominance_pct; Type: INDEX; Schema: gold; Owner: -
--

CREATE INDEX idx_dominance_dominance_pct ON gold.crypto_market_dominance USING btree (dominance_pct DESC);


--
-- Name: idx_dominance_snapshot_date; Type: INDEX; Schema: gold; Owner: -
--

CREATE INDEX idx_dominance_snapshot_date ON gold.crypto_market_dominance USING btree (snapshot_date);


--
-- Name: idx_performers_price_change_pct; Type: INDEX; Schema: gold; Owner: -
--

CREATE INDEX idx_performers_price_change_pct ON gold.crypto_top_performers USING btree (price_change_pct);


--
-- Name: idx_performers_snapshot_date; Type: INDEX; Schema: gold; Owner: -
--

CREATE INDEX idx_performers_snapshot_date ON gold.crypto_top_performers USING btree (snapshot_date);


--
-- Name: idx_silver_coin_id; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_coin_id ON silver.crypto_market USING btree (coin_id);


--
-- Name: idx_silver_market_cap_rank; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_market_cap_rank ON silver.crypto_market USING btree (market_cap_rank);


--
-- Name: idx_silver_snapshot_date; Type: INDEX; Schema: silver; Owner: -
--

CREATE INDEX idx_silver_snapshot_date ON silver.crypto_market USING btree (snapshot_date);


--
-- Name: idx_staging_valid_ingestion_id; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX idx_staging_valid_ingestion_id ON staging.crypto_market_valid USING btree (ingestion_id);


--
-- PostgreSQL database dump complete
--

\unrestrict ma5kbJfQljaclgHS8B71jTjmvQPvCehR0fDbc1YS8VUoEz4aXkwfIa0h1THnM31


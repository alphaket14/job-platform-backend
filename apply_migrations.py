import psycopg2

# Put your connection string here
CONN_STR = "postgresql://postgres.yavisbyihuuwnwmhucne:postgresadmin@aws-0-us-east-1.pooler.supabase.com:6543/postgres"

# Put your migration SQL here (can be multi-line)
MIGRATION_SQL = """
-- Migration: Create open_jobs table
CREATE TABLE IF NOT EXISTS open_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_hash TEXT,
    external_job_id TEXT,
    job_source TEXT,
    feed_id INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    posted_at TIMESTAMP WITHOUT TIME ZONE,
    expires_at TIMESTAMP WITHOUT TIME ZONE,
    status TEXT,
    company_name TEXT,
    title TEXT,
    description TEXT,
    application_url TEXT,
    employment_type TEXT,
    is_remote BOOLEAN,
    is_multi_location BOOLEAN,
    is_international BOOLEAN,
    locations JSONB,
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_period TEXT,
    currency TEXT,
    ai_title TEXT,
    ai_description TEXT,
    ai_job_tasks JSONB,
    ai_search_terms JSONB,
    ai_top_tags JSONB,
    ai_job_function_id INTEGER,
    ai_skills JSONB,
    ai_confidence_score NUMERIC,
    sector TEXT,
    industry_group TEXT,
    industry TEXT,
    industry_id INTEGER,
    cpc DOUBLE PRECISION,
    cpa DOUBLE PRECISION,
    sector_id INTEGER,
    industry_group_id INTEGER,
    job_function_id INTEGER,
    ai_enrichment_status TEXT,
    ai_enrichment_error TEXT,
    ih_reasoning TEXT,
    jf_reasoning TEXT,
    execution_id INTEGER
);

-- Create indexes for open_jobs table
CREATE INDEX IF NOT EXISTS idx_open_jobs_job_hash ON open_jobs(job_hash);
CREATE INDEX IF NOT EXISTS idx_open_jobs_external_job_id ON open_jobs(external_job_id);
CREATE INDEX IF NOT EXISTS idx_open_jobs_feed_id ON open_jobs(feed_id);
CREATE INDEX IF NOT EXISTS idx_open_jobs_status ON open_jobs(status);
CREATE INDEX IF NOT EXISTS idx_open_jobs_created_at ON open_jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_open_jobs_execution_id ON open_jobs(execution_id);

-- Add unique constraint on job_hash for upsert operations
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'unique_job_hash' 
        AND table_name = 'open_jobs'
    ) THEN
        ALTER TABLE open_jobs ADD CONSTRAINT unique_job_hash UNIQUE (job_hash);
    END IF;
END $$;

-- Migration: Create manual_review_jobs table (same schema as open_jobs)
CREATE TABLE IF NOT EXISTS manual_review_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_hash TEXT,
    external_job_id TEXT,
    job_source TEXT,
    feed_id INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    posted_at TIMESTAMP WITHOUT TIME ZONE,
    expires_at TIMESTAMP WITHOUT TIME ZONE,
    status TEXT,
    company_name TEXT,
    title TEXT,
    description TEXT,
    application_url TEXT,
    employment_type TEXT,
    is_remote BOOLEAN,
    is_multi_location BOOLEAN,
    is_international BOOLEAN,
    locations JSONB,
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_period TEXT,
    currency TEXT,
    ai_title TEXT,
    ai_description TEXT,
    ai_job_tasks JSONB,
    ai_search_terms JSONB,
    ai_top_tags JSONB,
    ai_job_function_id INTEGER,
    ai_skills JSONB,
    ai_confidence_score NUMERIC,
    sector TEXT,
    industry_group TEXT,
    industry TEXT,
    industry_id INTEGER,
    cpc DOUBLE PRECISION,
    cpa DOUBLE PRECISION,
    sector_id INTEGER,
    industry_group_id INTEGER,
    job_function_id INTEGER,
    ai_enrichment_status TEXT,
    ai_enrichment_error TEXT,
    ih_reasoning TEXT,
    jf_reasoning TEXT,
    execution_id INTEGER
);

-- Create indexes for manual_review_jobs table
CREATE INDEX IF NOT EXISTS idx_manual_review_jobs_job_hash ON manual_review_jobs(job_hash);
CREATE INDEX IF NOT EXISTS idx_manual_review_jobs_external_job_id ON manual_review_jobs(external_job_id);
CREATE INDEX IF NOT EXISTS idx_manual_review_jobs_feed_id ON manual_review_jobs(feed_id);
CREATE INDEX IF NOT EXISTS idx_manual_review_jobs_status ON manual_review_jobs(status);
CREATE INDEX IF NOT EXISTS idx_manual_review_jobs_created_at ON manual_review_jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_manual_review_jobs_execution_id ON manual_review_jobs(execution_id);

-- Add unique constraint on job_hash for upsert operations
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'unique_job_hash_manual_review' 
        AND table_name = 'manual_review_jobs'
    ) THEN
        ALTER TABLE manual_review_jobs ADD CONSTRAINT unique_job_hash_manual_review UNIQUE (job_hash);
    END IF;
END $$;

-- Migration: Create archive_jobs table (same schema as open_jobs)
CREATE TABLE IF NOT EXISTS archive_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_hash TEXT,
    external_job_id TEXT,
    job_source TEXT,
    feed_id INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    posted_at TIMESTAMP WITHOUT TIME ZONE,
    expires_at TIMESTAMP WITHOUT TIME ZONE,
    status TEXT,
    company_name TEXT,
    title TEXT,
    description TEXT,
    application_url TEXT,
    employment_type TEXT,
    is_remote BOOLEAN,
    is_multi_location BOOLEAN,
    is_international BOOLEAN,
    locations JSONB,
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_period TEXT,
    currency TEXT,
    ai_title TEXT,
    ai_description TEXT,
    ai_job_tasks JSONB,
    ai_search_terms JSONB,
    ai_top_tags JSONB,
    ai_job_function_id INTEGER,
    ai_skills JSONB,
    ai_confidence_score NUMERIC,
    sector TEXT,
    industry_group TEXT,
    industry TEXT,
    industry_id INTEGER,
    cpc DOUBLE PRECISION,
    cpa DOUBLE PRECISION,
    sector_id INTEGER,
    industry_group_id INTEGER,
    job_function_id INTEGER,
    ai_enrichment_status TEXT,
    ai_enrichment_error TEXT,
    ih_reasoning TEXT,
    jf_reasoning TEXT,
    execution_id INTEGER
);

-- Create indexes for archive_jobs table
CREATE INDEX IF NOT EXISTS idx_archive_jobs_job_hash ON archive_jobs(job_hash);
CREATE INDEX IF NOT EXISTS idx_archive_jobs_external_job_id ON archive_jobs(external_job_id);
CREATE INDEX IF NOT EXISTS idx_archive_jobs_feed_id ON archive_jobs(feed_id);
CREATE INDEX IF NOT EXISTS idx_archive_jobs_status ON archive_jobs(status);
CREATE INDEX IF NOT EXISTS idx_archive_jobs_created_at ON archive_jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_archive_jobs_execution_id ON archive_jobs(execution_id);

-- Add unique constraint on job_hash for upsert operations
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'unique_job_hash_archive' 
        AND table_name = 'archive_jobs'
    ) THEN
        ALTER TABLE archive_jobs ADD CONSTRAINT unique_job_hash_archive UNIQUE (job_hash);
    END IF;
END $$;


-- Migration: Create execution_reports table
CREATE TABLE IF NOT EXISTS execution_reports (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE,
    feed_id INTEGER,
    feed_name TEXT,
    run_status TEXT,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    duration_seconds DOUBLE PRECISION,
    total_in_feed INTEGER,
    processed_successfully INTEGER,
    skipped_closed INTEGER,
    skipped_duplicate INTEGER,
    records_failed INTEGER,
    jobs_closed INTEGER,
    jobs_auto_approved INTEGER,
    jobs_manual_review INTEGER,
    api_calls_gemini INTEGER,
    api_calls_custom INTEGER,
    cache_hits INTEGER,
    error_summary TEXT,
    gemini_input_tokens INTEGER,
    gemini_output_tokens INTEGER,
    gemini_total_tokens INTEGER,
    feed_file_size_bytes BIGINT,
    jobs_rejected INTEGER,
    
    -- --- ADD THESE NEW COLUMNS ---
    skipped_duplicate_initial_aggregation INTEGER,
    skipped_duplicate_ai_aggregation INTEGER,
    xano_jobs_added INTEGER,
    xano_jobs_deleted INTEGER
);

-- Create indexes for execution_reports table
CREATE INDEX IF NOT EXISTS idx_execution_reports_feed_id ON execution_reports(feed_id);
CREATE INDEX IF NOT EXISTS idx_execution_reports_created_at ON execution_reports(created_at);
CREATE INDEX IF NOT EXISTS idx_execution_reports_run_status ON execution_reports(run_status);

-- Migration: Create rejected_jobs table
CREATE TABLE IF NOT EXISTS rejected_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_hash TEXT,
    external_job_id TEXT,
    job_source TEXT,
    feed_id INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    posted_at TIMESTAMP WITHOUT TIME ZONE,
    expires_at TIMESTAMP WITHOUT TIME ZONE,
    status TEXT,
    company_name TEXT,
    title TEXT,
    description TEXT,
    application_url TEXT,
    employment_type TEXT,
    is_remote BOOLEAN,
    is_multi_location BOOLEAN,
    is_international BOOLEAN,
    locations JSONB,
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_period TEXT,
    currency TEXT,
    ai_title TEXT,
    ai_description TEXT,
    ai_job_tasks JSONB,
    ai_search_terms JSONB,
    ai_top_tags JSONB,
    ai_job_function_id INTEGER,
    ai_skills JSONB,
    ai_confidence_score NUMERIC,
    sector TEXT,
    industry_group TEXT,
    industry TEXT,
    industry_id INTEGER,
    cpc DOUBLE PRECISION,
    cpa DOUBLE PRECISION,
    sector_id INTEGER,
    industry_group_id INTEGER,
    job_function_id INTEGER,
    ai_enrichment_status TEXT,
    ai_enrichment_error TEXT,
    ih_reasoning TEXT,
    jf_reasoning TEXT,
    execution_id INTEGER
);

-- Create indexes for rejected_jobs table
CREATE INDEX IF NOT EXISTS idx_rejected_jobs_job_hash ON rejected_jobs(job_hash);
CREATE INDEX IF NOT EXISTS idx_rejected_jobs_external_job_id ON rejected_jobs(external_job_id);
CREATE INDEX IF NOT EXISTS idx_rejected_jobs_feed_id ON rejected_jobs(feed_id);
CREATE INDEX IF NOT EXISTS idx_rejected_jobs_status ON rejected_jobs(status);
CREATE INDEX IF NOT EXISTS idx_rejected_jobs_created_at ON rejected_jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_rejected_jobs_execution_id ON rejected_jobs(execution_id);



-- Migration: Create feeds table
CREATE TABLE IF NOT EXISTS feeds (
    id INTEGER PRIMARY KEY,
    name TEXT,
    source_url TEXT,
    feed_type TEXT,
    parser_type TEXT,
    company_name TEXT,
    is_active BOOLEAN,
    check_frequency_minutes INTEGER,
    last_checked_at TIMESTAMP WITH TIME ZONE,
    next_check_at TIMESTAMP WITH TIME ZONE,
    last_run_status TEXT,
    last_succeeded_at TIMESTAMP WITH TIME ZONE,
    consecutive_failure_count INTEGER,
    last_error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    unique_jobs_processed INTEGER,
    cw_auto_approved_jobs INTEGER,
    avg_processing_time INTEGER,
    error_count INTEGER,
    avg_feed_size DOUBLE PRECISION
);

-- Create indexes for feeds table
CREATE INDEX IF NOT EXISTS idx_feeds_is_active ON feeds(is_active);
CREATE INDEX IF NOT EXISTS idx_feeds_last_checked_at ON feeds(last_checked_at);
CREATE INDEX IF NOT EXISTS idx_feeds_next_check_at ON feeds(next_check_at);
CREATE INDEX IF NOT EXISTS idx_feeds_last_run_status ON feeds(last_run_status);


"""

def run_migration(conn_str, migration_sql):
    try:
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        cur.execute(migration_sql)
        conn.commit()
        print("Migration applied successfully.")
    except Exception as e:
        print(f"Migration failed: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    run_migration(CONN_STR, MIGRATION_SQL)
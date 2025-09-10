-- Clinical Data ETL Pipeline Database Schema
-- TODO: Candidate to design and implement optimal schema

-- Basic schema provided for bootstrapping
-- Candidate should enhance with proper indexes, constraints, and optimization

-- ETL Jobs tracking table
CREATE TABLE IF NOT EXISTS etl_jobs (
    id UUID PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    study_id VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);

-- Sample basic table structure (candidate should enhance)
CREATE TABLE IF NOT EXISTS clinical_measurements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    study_id VARCHAR(50) NOT NULL,
    participant_id VARCHAR(50) NOT NULL,
    measurement_type VARCHAR(50) NOT NULL,
    value TEXT NOT NULL,
    unit VARCHAR(20),
    timestamp TIMESTAMP NOT NULL,
    site_id VARCHAR(50) NOT NULL,
    quality_score DECIMAL(3,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- track the ETL job id that generated this table
    etl_job_id UUID,
    FOREIGN KEY (etl_job_id) REFERENCES etl_jobs(id)
);

-- TODO: Candidate to implement
-- Expected tables to be designed by candidate:
-- - clinical_measurements (raw data)
-- - processed_measurements (transformed data)
-- - participants
-- - studies
-- - data_quality_reports
-- - measurement_aggregations

-- Tables deriving information from the raw clinical measurements
-- design for demo purposes:
-- using data derived from the raw clinical data we can calculate further datapoints

-- from clinical measurements concatenate the participant_id and study_id to make a composite primary key
CREATE TABLE IF NOT EXISTS participants (
    participant_id VARCHAR(50) NOT NULL,
    study_id VARCHAR(50) NOT NULL,
    primary_site_id VARCHAR(50) NOT NULL,
    first_measurement_date TIMESTAMP,
    last_measurement_date TIMESTAMP,
    total_measurements INTEGER DEFAULT 0,
    avg_quality_score DECIMAL(5,4),
    measurement_types_count INTEGER DEFAULT 0,
    sites_count INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (participant_id, study_id)
);

CREATE TABLE IF NOT EXISTS studies (
    study_id VARCHAR(50) PRIMARY KEY,
    first_measurement_date TIMESTAMP,
    last_measurement_date TIMESTAMP,
    total_measurements INTEGER DEFAULT 0,
    total_participants INTEGER DEFAULT 0,
    total_sites INTEGER DEFAULT 0,
    avg_quality_score DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_measurements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_measurement_id UUID NOT NULL,
    study_id VARCHAR(50) NOT NULL,
    participant_id VARCHAR(50) NOT NULL,
    measurement_type VARCHAR(50) NOT NULL,
    processed_value DECIMAL(15,6),
    original_value TEXT NOT NULL,
    standardized_unit VARCHAR(20),
    timestamp TIMESTAMP NOT NULL,
    site_id VARCHAR(50) NOT NULL,
    quality_score DECIMAL(3,2),
    is_numeric BOOLEAN DEFAULT FALSE,
    is_outlier BOOLEAN DEFAULT FALSE,
    processing_notes TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_quality_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    study_id VARCHAR(50) NOT NULL,
    site_id VARCHAR(50),
    measurement_type VARCHAR(50),
    report_date DATE NOT NULL,
    total_measurements INTEGER NOT NULL DEFAULT 0,
    quality_score_avg DECIMAL(5,4),
    quality_score_min DECIMAL(3,2),
    quality_score_max DECIMAL(3,2),
    numeric_value_count INTEGER DEFAULT 0,
    non_numeric_value_count INTEGER DEFAULT 0,
    unique_participants INTEGER DEFAULT 0,
    date_range_start TIMESTAMP,
    date_range_end TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS measurement_aggregations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    study_id VARCHAR(50) NOT NULL,
    participant_id VARCHAR(50),
    site_id VARCHAR(50),
    measurement_type VARCHAR(50) NOT NULL,
    aggregation_period VARCHAR(20) NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    measurement_count INTEGER NOT NULL DEFAULT 0,
    avg_value DECIMAL(15,6),
    min_value DECIMAL(15,6),
    max_value DECIMAL(15,6),
    numeric_measurement_count INTEGER DEFAULT 0,
    avg_quality_score DECIMAL(5,4),
    unique_participants INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ETL Jobs indexes
CREATE INDEX IF NOT EXISTS idx_etl_jobs_status ON etl_jobs(status);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_created_at ON etl_jobs(created_at);

-- Clinical measurements indexes
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_study_id ON clinical_measurements(study_id);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_participant_id ON clinical_measurements(participant_id);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_timestamp ON clinical_measurements(timestamp);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_site_id ON clinical_measurements(site_id);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_quality_score ON clinical_measurements(quality_score);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_created_at ON clinical_measurements(created_at);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_measurement_type ON clinical_measurements(measurement_type);

-- Composite indexes for specific business questions
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_study_quality ON clinical_measurements(study_id, quality_score);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_participant_type_time ON clinical_measurements(participant_id, measurement_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_site_created ON clinical_measurements(site_id, created_at);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_study_participant ON clinical_measurements(study_id, participant_id);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_type_quality ON clinical_measurements(measurement_type, quality_score);

-- Specialized partial indexes for common queries
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_low_quality ON clinical_measurements(quality_score, study_id, measurement_type) 
    WHERE quality_score < 0.8;

-- Studies indexes
CREATE INDEX IF NOT EXISTS idx_studies_quality ON studies(avg_quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_studies_measurement_dates ON studies(first_measurement_date, last_measurement_date);

-- Participants indexes
CREATE INDEX IF NOT EXISTS idx_participants_study_id ON participants(study_id);
CREATE INDEX IF NOT EXISTS idx_participants_site_id ON participants(primary_site_id);
CREATE INDEX IF NOT EXISTS idx_participants_quality ON participants(avg_quality_score);
CREATE INDEX IF NOT EXISTS idx_participants_study_quality ON participants(study_id, avg_quality_score);

-- Processed measurements indexes
CREATE INDEX IF NOT EXISTS idx_processed_measurements_raw_id ON processed_measurements(raw_measurement_id);
CREATE INDEX IF NOT EXISTS idx_processed_measurements_participant_type_time ON processed_measurements(participant_id, measurement_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_processed_measurements_numeric ON processed_measurements(is_numeric) WHERE is_numeric = true;
CREATE INDEX IF NOT EXISTS idx_processed_measurements_outliers ON processed_measurements(is_outlier) WHERE is_outlier = true;

-- Data quality reports indexes
CREATE INDEX IF NOT EXISTS idx_data_quality_reports_study_date ON data_quality_reports(study_id, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_data_quality_reports_site_date ON data_quality_reports(site_id, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_data_quality_reports_type_quality ON data_quality_reports(measurement_type, quality_score_avg);

-- Measurement aggregations indexes
CREATE INDEX IF NOT EXISTS idx_aggregations_study_type_period ON measurement_aggregations(study_id, measurement_type, aggregation_period, period_start);
CREATE INDEX IF NOT EXISTS idx_aggregations_participant_type ON measurement_aggregations(participant_id, measurement_type, period_start);
CREATE INDEX IF NOT EXISTS idx_aggregations_site_period ON measurement_aggregations(site_id, aggregation_period, period_start);

-- Sample queries to address business questions:
-- 1. Which studies have the highest data quality scores?
/*
SELECT 
    s.study_id,
    s.avg_quality_score,
    s.total_measurements,
    s.total_participants
FROM studies s
WHERE s.avg_quality_score IS NOT NULL
ORDER BY s.avg_quality_score DESC;

*/

-- 2. What are the glucose trends for a specific participant over time?
/*
SELECT 
    timestamp,
    processed_value,
    standardized_unit,
    quality_score,
    is_outlier
FROM processed_measurements 
WHERE participant_id = 'PARTICIPANT_123' 
    AND measurement_type = 'glucose'
    AND is_numeric = true
ORDER BY timestamp;
*/

-- 3. How do measurement counts compare across different research sites?
/*
SELECT 
    site_id,
    SUM(measurement_count) as total_measurements,
    AVG(avg_quality_score) as avg_quality_score,
    COUNT(DISTINCT study_id) as studies_count
FROM measurement_aggregations
WHERE aggregation_period = 'monthly'
GROUP BY site_id
ORDER BY total_measurements DESC;
*/

-- 4. Which measurements have quality scores below our threshold?
/*
SELECT 
    cm.id,
    cm.study_id,
    cm.participant_id,
    cm.measurement_type,
    cm.quality_score,
    cm.timestamp,
    cm.site_id
FROM clinical_measurements cm
WHERE cm.quality_score < 0.8
ORDER BY cm.quality_score ASC, cm.timestamp DESC;
*/

-- 5. What clinical data was collected in the last 30 days?
/*
SELECT 
    study_id,
    site_id,
    measurement_type,
    COUNT(*) as measurement_count,
    COUNT(DISTINCT participant_id) as unique_participants,
    AVG(quality_score) as avg_quality_score
FROM clinical_measurements 
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY study_id, site_id, measurement_type
ORDER BY measurement_count DESC;
*/

-- 6. How many participants are enrolled in each study?
/*
SELECT 
    s.study_id,
    s.total_participants,
    s.total_measurements,
    s.avg_quality_score
FROM studies s
ORDER BY s.total_participants DESC;
*/

-- 7. What's the average BMI for participants in a specific study?
/*
SELECT 
    pm.study_id,
    AVG(pm.processed_value) as avg_bmi,
    MIN(pm.processed_value) as min_bmi,
    MAX(pm.processed_value) as max_bmi,
    COUNT(pm.*) as bmi_measurements,
    COUNT(DISTINCT pm.participant_id) as participants_with_bmi
FROM processed_measurements pm
WHERE pm.study_id = 'STUDY_ABC' 
    AND pm.measurement_type = 'bmi'
    AND pm.is_numeric = true
    AND pm.processed_value BETWEEN 10 AND 60  -- Reasonable BMI range
GROUP BY pm.study_id;
*/
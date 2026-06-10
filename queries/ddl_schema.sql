PRAGMA foreign_keys = ON;

-- Dimension tables keep provider, procedure, and geography lookups compact so the
-- fact table can stay narrow and analytics-friendly.

CREATE TABLE IF NOT EXISTS dim_providers (
    Rndrg_Npi TEXT PRIMARY KEY,
    Rndrg_Prvdr_Last_Org_Name TEXT,
    Rndrg_Prvdr_First_Name TEXT,
    Rndrg_Prvdr_Crdntl TEXT,
    Rndrg_Prvdr_Type TEXT
);

CREATE TABLE IF NOT EXISTS dim_procedures (
    Hcpcs_Cd TEXT PRIMARY KEY,
    Hcpcs_Desc TEXT
);

CREATE TABLE IF NOT EXISTS dim_geography (
    Rndrg_Prvdr_Zip5 TEXT PRIMARY KEY,
    Rndrg_Prvdr_State_Abrvtn TEXT
);

-- The fact table stores the measurable CMS service metrics and references the
-- dimensions through explicit foreign keys to preserve relational integrity.

CREATE TABLE IF NOT EXISTS fact_provider_services (
    Fact_Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Rndrg_Npi TEXT NOT NULL,
    Hcpcs_Cd TEXT NOT NULL,
    Rndrg_Prvdr_Zip5 TEXT NOT NULL,
    Place_Of_Srvc TEXT,
    Tot_Benes INTEGER,
    Tot_Srvcs REAL,
    Avg_Srvc_Smtd_Chrg NUMERIC,
    Avg_Medcr_Alwd_Amt NUMERIC,
    Avg_Medcr_Pymt_Amt NUMERIC,
    FOREIGN KEY (Rndrg_Npi) REFERENCES dim_providers (Rndrg_Npi)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    FOREIGN KEY (Hcpcs_Cd) REFERENCES dim_procedures (Hcpcs_Cd)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    FOREIGN KEY (Rndrg_Prvdr_Zip5) REFERENCES dim_geography (Rndrg_Prvdr_Zip5)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CHECK (Tot_Benes IS NULL OR typeof(Tot_Benes) = 'integer'),
    CHECK (Tot_Srvcs IS NULL OR typeof(Tot_Srvcs) IN ('integer', 'real')),
    CHECK (Avg_Srvc_Smtd_Chrg IS NULL OR typeof(Avg_Srvc_Smtd_Chrg) IN ('integer', 'real')),
    CHECK (Avg_Medcr_Alwd_Amt IS NULL OR typeof(Avg_Medcr_Alwd_Amt) IN ('integer', 'real')),
    CHECK (Avg_Medcr_Pymt_Amt IS NULL OR typeof(Avg_Medcr_Pymt_Amt) IN ('integer', 'real'))
);

-- Explicit indexes on the foreign-key columns keep provider and procedure filters
-- responsive during downstream compliance and anomaly investigations.

CREATE INDEX IF NOT EXISTS idx_fact_provider_services_hcpcs_cd
    ON fact_provider_services (Hcpcs_Cd);

CREATE INDEX IF NOT EXISTS idx_fact_provider_services_rndrg_npi
    ON fact_provider_services (Rndrg_Npi);

CREATE INDEX IF NOT EXISTS idx_fact_provider_services_rndrg_prvdr_zip5
    ON fact_provider_services (Rndrg_Prvdr_Zip5);

-- This multi-column composite index dramatically accelerates peer-group
-- benchmarking and specialty-controlled utilization/upcoding calculations.
CREATE INDEX IF NOT EXISTS idx_fact_provider_services_npi_hcpcs
    ON fact_provider_services (Rndrg_Npi, Hcpcs_Cd);

-- Pre-computed peer-group benchmark table. Populated by populate_dim_benchmarks.sql.
-- Storing computed stats here decouples analytical views from expensive inline aggregations.
CREATE TABLE IF NOT EXISTS dim_benchmarks (
    Benchmark_Id         INTEGER PRIMARY KEY AUTOINCREMENT,
    Rndrg_Prvdr_Type     TEXT    NOT NULL,
    Hcpcs_Cd             TEXT    NOT NULL,
    Peer_Avg_Submitted_Charge NUMERIC,
    Peer_Avg_Allowed_Amt NUMERIC,
    Peer_Avg_Payment_Amt NUMERIC,
    Peer_Avg_Markup_Ratio NUMERIC,
    Peer_Row_Count       INTEGER,
    Computed_At          TEXT    DEFAULT (datetime('now')),
    UNIQUE (Rndrg_Prvdr_Type, Hcpcs_Cd)
);

CREATE INDEX IF NOT EXISTS idx_dim_benchmarks_type_hcpcs
    ON dim_benchmarks (Rndrg_Prvdr_Type, Hcpcs_Cd);

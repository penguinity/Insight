DROP VIEW IF EXISTS v_billing_elasticity_anomalies;

-- Requires dim_benchmarks to be populated first via populate_dim_benchmarks.sql.
-- All column names match the actual SQLite schema (Rndrg_* prefix, not Rndrng_*).
CREATE VIEW v_billing_elasticity_anomalies AS
WITH scored_rows AS (
    SELECT
        f.Rndrg_Npi,
        prov.Rndrg_Prvdr_Last_Org_Name,
        prov.Rndrg_Prvdr_First_Name,
        prov.Rndrg_Prvdr_Type,
        f.Hcpcs_Cd,
        f.Avg_Srvc_Smtd_Chrg,
        f.Avg_Medcr_Alwd_Amt,
        ROUND(f.Avg_Srvc_Smtd_Chrg / NULLIF(f.Avg_Medcr_Alwd_Amt, 0), 2)
            AS provider_markup_ratio,
        ROUND(b.Peer_Avg_Markup_Ratio, 2)
            AS peer_group_markup_baseline,
        b.Peer_Row_Count                  AS peer_group_row_count,
        CASE
            WHEN b.Peer_Row_Count >= 10
                 AND (f.Avg_Srvc_Smtd_Chrg / NULLIF(f.Avg_Medcr_Alwd_Amt, 0))
                     > (b.Peer_Avg_Markup_Ratio * 1.20)
            THEN 35
            ELSE 0
        END AS elasticity_risk_points
    FROM fact_provider_services f
    -- INNER JOIN to dim_providers: both tables share Rndrg_Npi as the join key.
    JOIN dim_providers prov
        ON f.Rndrg_Npi = prov.Rndrg_Npi
    -- JOIN to pre-computed benchmark table for stable, fast peer-group lookups.
    JOIN dim_benchmarks b
        ON prov.Rndrg_Prvdr_Type = b.Rndrg_Prvdr_Type
        AND f.Hcpcs_Cd = b.Hcpcs_Cd
    WHERE
        f.Avg_Srvc_Smtd_Chrg IS NOT NULL
        AND f.Avg_Medcr_Alwd_Amt IS NOT NULL
        AND f.Avg_Medcr_Alwd_Amt > 0
        AND b.Peer_Avg_Markup_Ratio IS NOT NULL
)
SELECT
    Rndrg_Npi,
    Rndrg_Prvdr_Last_Org_Name,
    Rndrg_Prvdr_First_Name,
    Rndrg_Prvdr_Type,
    Hcpcs_Cd,
    Avg_Srvc_Smtd_Chrg,
    Avg_Medcr_Alwd_Amt,
    provider_markup_ratio,
    peer_group_markup_baseline,
    peer_group_row_count,
    elasticity_risk_points
FROM scored_rows
WHERE elasticity_risk_points > 0
ORDER BY provider_markup_ratio DESC;

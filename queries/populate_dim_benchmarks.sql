-- Populate dim_benchmarks with per-specialty/CPT peer group statistics.
-- Run this script after each full ETL reload to refresh the benchmark table.
-- The billing elasticity view depends on this table being current.

-- Step 1: Wipe and rebuild to avoid stale benchmark values after a reload.
DELETE FROM dim_benchmarks;

-- Step 2: Insert fresh peer group aggregates.
-- Avg_Markup_Ratio is computed as AVG(submitted / allowed) per peer group.
-- Rows where allowed amount is zero or null are excluded to protect against
-- division-by-zero in downstream view calculations.
INSERT INTO dim_benchmarks (
    Rndrg_Prvdr_Type,
    Hcpcs_Cd,
    Peer_Avg_Submitted_Charge,
    Peer_Avg_Allowed_Amt,
    Peer_Avg_Payment_Amt,
    Peer_Avg_Markup_Ratio,
    Peer_Row_Count
)
SELECT
    p.Rndrg_Prvdr_Type,
    f.Hcpcs_Cd,
    ROUND(AVG(f.Avg_Srvc_Smtd_Chrg), 4),
    ROUND(AVG(f.Avg_Medcr_Alwd_Amt), 4),
    ROUND(AVG(f.Avg_Medcr_Pymt_Amt), 4),
    ROUND(AVG(f.Avg_Srvc_Smtd_Chrg / NULLIF(f.Avg_Medcr_Alwd_Amt, 0)), 6),
    COUNT(*)
FROM fact_provider_services f
JOIN dim_providers p
    ON f.Rndrg_Npi = p.Rndrg_Npi
WHERE
    f.Avg_Srvc_Smtd_Chrg  IS NOT NULL
    AND f.Avg_Medcr_Alwd_Amt IS NOT NULL
    AND f.Avg_Medcr_Alwd_Amt > 0
    AND p.Rndrg_Prvdr_Type  IS NOT NULL
GROUP BY
    p.Rndrg_Prvdr_Type,
    f.Hcpcs_Cd;

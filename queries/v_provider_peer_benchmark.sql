DROP VIEW IF EXISTS v_provider_peer_benchmark;

CREATE VIEW v_provider_peer_benchmark AS
WITH peer_stats AS (
    SELECT
        p.Rndrg_Prvdr_Type,
        f.Hcpcs_Cd,
        AVG(f.Avg_Medcr_Alwd_Amt) AS peer_avg_allowed_amt,
        AVG(f.Avg_Medcr_Pymt_Amt) AS peer_avg_payment_amt,
        AVG(f.Tot_Srvcs) AS peer_avg_services,
        COUNT(*) AS peer_group_row_count
    FROM fact_provider_services f
    JOIN dim_providers p
        ON f.Rndrg_Npi = p.Rndrg_Npi
    WHERE
        f.Avg_Medcr_Alwd_Amt IS NOT NULL
        AND f.Avg_Medcr_Pymt_Amt IS NOT NULL
        AND f.Tot_Srvcs IS NOT NULL
    GROUP BY
        p.Rndrg_Prvdr_Type,
        f.Hcpcs_Cd
),
provider_scored AS (
    SELECT
        f.Rndrg_Npi,
        p.Rndrg_Prvdr_Last_Org_Name,
        p.Rndrg_Prvdr_First_Name,
        p.Rndrg_Prvdr_Type,
        f.Hcpcs_Cd,
        d.Hcpcs_Desc,
        f.Place_Of_Srvc,
        f.Tot_Srvcs,
        f.Avg_Medcr_Alwd_Amt,
        f.Avg_Medcr_Pymt_Amt,
        s.peer_avg_allowed_amt,
        s.peer_avg_payment_amt,
        s.peer_avg_services,
        s.peer_group_row_count,
        ROUND(f.Avg_Medcr_Alwd_Amt - s.peer_avg_allowed_amt, 2) AS allowed_amt_delta_vs_peer,
        ROUND(f.Avg_Medcr_Pymt_Amt - s.peer_avg_payment_amt, 2) AS payment_amt_delta_vs_peer,
        ROUND(
            CASE
                WHEN s.peer_avg_allowed_amt = 0 THEN NULL
                ELSE ((f.Avg_Medcr_Alwd_Amt - s.peer_avg_allowed_amt) / s.peer_avg_allowed_amt) * 100
            END,
            2
        ) AS allowed_amt_pct_vs_peer,
        ROUND(
            CASE
                WHEN s.peer_avg_payment_amt = 0 THEN NULL
                ELSE ((f.Avg_Medcr_Pymt_Amt - s.peer_avg_payment_amt) / s.peer_avg_payment_amt) * 100
            END,
            2
        ) AS payment_amt_pct_vs_peer,
        CASE
            WHEN s.peer_group_row_count >= 20
                 AND s.peer_avg_allowed_amt > 0
                 AND ((f.Avg_Medcr_Alwd_Amt - s.peer_avg_allowed_amt) / s.peer_avg_allowed_amt) >= 0.40
            THEN 40
            WHEN s.peer_group_row_count >= 20
                 AND s.peer_avg_allowed_amt > 0
                 AND ((f.Avg_Medcr_Alwd_Amt - s.peer_avg_allowed_amt) / s.peer_avg_allowed_amt) >= 0.25
            THEN 20
            ELSE 0
        END AS benchmark_variance_risk_points
    FROM fact_provider_services f
    JOIN dim_providers p
        ON f.Rndrg_Npi = p.Rndrg_Npi
    JOIN dim_procedures d
        ON f.Hcpcs_Cd = d.Hcpcs_Cd
    JOIN peer_stats s
        ON p.Rndrg_Prvdr_Type = s.Rndrg_Prvdr_Type
        AND f.Hcpcs_Cd = s.Hcpcs_Cd
)
SELECT
    Rndrg_Npi,
    Rndrg_Prvdr_Last_Org_Name,
    Rndrg_Prvdr_First_Name,
    Rndrg_Prvdr_Type,
    Hcpcs_Cd,
    Hcpcs_Desc,
    Place_Of_Srvc,
    Tot_Srvcs,
    Avg_Medcr_Alwd_Amt,
    Avg_Medcr_Pymt_Amt,
    peer_avg_allowed_amt,
    peer_avg_payment_amt,
    peer_avg_services,
    peer_group_row_count,
    allowed_amt_delta_vs_peer,
    payment_amt_delta_vs_peer,
    allowed_amt_pct_vs_peer,
    payment_amt_pct_vs_peer,
    benchmark_variance_risk_points
FROM provider_scored;

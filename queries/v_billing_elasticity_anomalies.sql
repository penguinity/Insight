CREATE VIEW IF NOT EXISTS v_billing_elasticity_anomalies AS
WITH peer_group_baselines AS (
    -- Calculate the average submitted charge and allowed baseline for every specialty/CPT combination across North Carolina
    SELECT 
        p.Rndrg_Prvdr_Type,
        f.Hcpcs_Cd,
        AVG(f.Avg_Srvc_Smtd_Chrg) AS avg_state_submitted_charge,
        AVG(f.Avg_Medcr_Alwd_Amt) AS avg_state_allowed_amount,
        -- Establish the target benchmark: the regional markup coefficient
        AVG(f.Avg_Srvc_Smtd_Chrg / f.Avg_Medcr_Alwd_Amt) AS median_peer_markup_ratio
    FROM 
        fact_provider_services f
    JOIN 
        dim_providers p ON f.Rndrg_Npi = p.Rndrg_Npi
    GROUP BY 
        p.Rndrg_Prvdr_Type, f.Hcpcs_Cd
)
SELECT 
    f.Rndrg_Npi,
    prov.Rndrg_Prvdr_Last_Org_Name,
    prov.Rndrg_Prvdr_First_Name,
    prov.Rndrg_Prvdr_Type,
    f.Hcpcs_Cd,
    f.Avg_Srvc_Smtd_Chrg,
    f.Avg_Medcr_Alwd_Amt,
    -- Individual Markup Ratio
    ROUND(f.Avg_Srvc_Smtd_Chrg / f.Avg_Medcr_Alwd_Amt, 2) AS provider_markup_ratio,
    ROUND(base.median_peer_markup_ratio, 2) AS peer_group_markup_baseline,
    -- Risk Flag Vector 1 Condition: Individual markup is more than 2.5x the peer specialty benchmark
    CASE 
        WHEN (f.Avg_Srvc_Smtd_Chrg / f.Avg_Medcr_Alwd_Amt) > (base.median_peer_markup_ratio * 2.5) THEN 35
        ELSE 0
    END AS elasticity_risk_points
FROM 
    fact_provider_services f
JOIN 
    dim_providers prov ON f.Rndrg_Npi = prov.Rndrg_Npi
JOIN 
    peer_group_baselines base ON prov.Rndrg_Prvdr_Type = base.Rndrg_Prvdr_Type 
    AND f.Hcpcs_Cd = base.Hcpcs_Cd;

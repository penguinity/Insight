CREATE VIEW IF NOT EXISTS v_em_upcoding_anomalies AS
WITH provider_em_distributions AS (
    -- Aggregate total Evaluation and Management service volume and isolate the Level 5 tier
    SELECT 
        Rndrg_Npi,
        SUM(CASE WHEN Hcpcs_Cd = '99215' THEN Tot_Srvcs ELSE 0 END) AS volume_level_5,
        SUM(Tot_Srvcs) AS total_em_volume
    FROM 
        fact_provider_services
    WHERE 
        Hcpcs_Cd IN ('99211', '99212', '99213', '99214', '99215')
    GROUP BY 
        Rndrg_Npi
)
SELECT 
    d.Rndrg_Npi,
    p.Rndrg_Prvdr_Last_Org_Name,
    p.Rndrg_Prvdr_First_Name,
    p.Rndrg_Prvdr_Type,
    d.volume_level_5,
    d.total_em_volume,
    -- Determine the percentage share of high-complexity code selections
    ROUND((d.volume_level_5 / d.total_em_volume) * 100, 2) AS level_5_utilization_share,
    -- Risk Flag Vector 3 Condition: Level 5 share exceeds 50% with an audit-viable sample size
    CASE 
        WHEN d.total_em_volume >= 50 AND (d.volume_level_5 / d.total_em_volume) > 0.50 THEN 30
        ELSE 0
    END AS upcoding_risk_points
FROM 
    provider_em_distributions d
JOIN 
    dim_providers p ON d.Rndrg_Npi = p.Rndrg_Npi;

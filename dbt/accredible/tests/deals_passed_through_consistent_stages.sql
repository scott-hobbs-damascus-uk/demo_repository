WITH import_deal AS (
    SELECT
        *
    FROM
        {{ ref('stg_deal_pipeline_stages')}}
),

deals_and_latest_stages AS (
    SELECT
        deal_id,
        MAX(stage_display_order) AS latest_stage
    FROM
        import_deal
    GROUP BY
        deal_id
)
SELECT
    *
FROM
    deals_and_latest_stages dls
    INNER JOIN import_deal imd
        ON dls.deal_id = imd.deal_id
    LEFT JOIN import_deal imd2
        ON imd2.deal_id = imd.deal_id
        AND imd2.stage_display_order = imd.stage_display_order - 1
WHERE
    dls.latest_stage <> 0
    AND imd2.stage_display_order IS NULL
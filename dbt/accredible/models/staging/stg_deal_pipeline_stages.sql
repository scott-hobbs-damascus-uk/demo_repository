WITH cleaning AS (
    SELECT
        CAST(deal_id AS BIGINT) AS deal_id,
        TO_TIMESTAMP(stage_created_at, 'YYYY-MM-DD\THH24:MI:ss') AS stage_created_at,
        pipeline_id,
        pipeline_name,
        deal_stage_id,
        stage_name,
        stage_display_order,
        /* there's a bad duplicate for id 14554829628 - i've elected to remove it in this code by using a row number function.
        There's an implicit assumption here that we only care about when deals first enter a stage and that is the most valid choice
        for when it considers moved between the stages.
        We could do some complex logic to "fallback" to current stages, but that's too much for right now */
        ROW_NUMBER() OVER (PARTITION BY deal_id, stage_name ORDER BY stage_created_at ASC) AS row_id
    FROM
        {{ source('deals', 'raw_deal_pipeline_stages') }}
    
    )
SELECT 
    *
FROM 
    cleaning
WHERE
    row_id = 1

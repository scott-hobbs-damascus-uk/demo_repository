SELECT
    CAST(deal_id AS BIGINT) AS deal_id,
    /* pipeline_id is only "default" */
    pipeline_id,
    pipeline_name,
    stage_name,
    /* remove empty strings to nulls because that's how they've loaded to postgres. 
    For this exercise, the assumption is that I don't need to convert the currency here to a single consistent one
    */
    COALESCE(CAST(NULLIF(amount_in_home_currency,'') AS DECIMAL), 0) AS amount_in_home_currency,
    /* assuming we don't need microsecond precision on these columns for now */
    TO_TIMESTAMP(created_at, 'YYYY-MM-DD\THH24:MI:ss') AS created_at,
    TO_TIMESTAMP(close_date, 'YYYY-MM-DD\THH24:MI:ss') AS close_date,
    deal_source_type
FROM
    {{ source('deals','raw_deals') }}
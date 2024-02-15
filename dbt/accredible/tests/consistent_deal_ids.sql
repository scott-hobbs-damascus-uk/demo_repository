/* this test ensures all deal_ids have at least one stage in their pipelines */

SELECT
    deal_id
FROM
    {{ ref('stg_deals') }}
WHERE
    deal_id NOT IN (SELECT deal_id FROM {{ ref('stg_deal_pipeline_stages')}})
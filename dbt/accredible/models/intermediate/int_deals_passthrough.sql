
/* normally, intermediate models would join together any required concepts.
As we only have the two tables, I've stuck a passthrough here so its easier for future use
- that way, this model could be changed without affecting the mart layer where the bi tool is connecting to.
*/
SELECT
    *
FROM
    {{ ref('stg_deals') }}
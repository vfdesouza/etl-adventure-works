{{ config(
    materialized = 'incremental',
    unique_key = '"contactTypeId"',
) }}

WITH raw_data AS (

    SELECT
        "contactTypeId",
        "name",
        "modifiedDate" :: DATE AS "modifiedDate"
    FROM
        {{ ref('stg_contactTypes_dataform') }}
)
SELECT
    *
FROM
    raw_data

{% if is_incremental() %}
WHERE
    "modifiedDate" >= (
        SELECT
            MAX("modifiedDate")
        FROM
            {{ this }}
    )
{% endif %}

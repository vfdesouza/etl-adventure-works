WITH raw_data AS (

    SELECT
        *
    FROM
        {{ source(
            'public',
            'contactTypes_raw'
        ) }}
)
SELECT
    *
FROM
    raw_data

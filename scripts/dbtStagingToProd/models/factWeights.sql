{{ config(
    materialized='table'
) }}

with stagingFactWeights as (
        select 
            USERID,
            DATE,
            WEIGHT
        from
        {{ source('SF_LIFT', 'STAGING_FACTWEIGHTS') }}
        where DATE is not null 
        and WEIGHT > 0
    )

select USERID,
DATE,
WEIGHT
from stagingFactWeights
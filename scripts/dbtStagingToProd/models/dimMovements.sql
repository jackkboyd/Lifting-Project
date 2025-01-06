{{ config(
    materialized='table'
) }}

with stagingDimMovements as (
    select 
        MOVEMENTID,
        MOVEMENTNAME,
        PRIMARYMUSCLE,
        SECONDARYMUSCLE,
        TERTIARYMUSCLE,
        MOVEMENTCODE
    from {{ source('SF_LIFT', 'STAGING_DIMMOVEMENTS') }}
    where MOVEMENTID is not null
)

select *
from stagingDimMovements

{{ config(
    materialized='table'
) }}

with stagingFactLifts as (
    select 
        LIFTID,
        USERID,
        ROUTINEID,
        WORKOUTID,
        MOVEMENTID,
        REPS1,
        WEIGHT1,
        REPS2,
        WEIGHT2,
        REPS3,
        WEIGHT3,
        REPS4,
        WEIGHT4,
        ISSUPERSET,
        ISSKIPPED,
        APPLYDATE,
        SEQUENCE,
        ISSUB
    from {{ source('SF_LIFT', 'STAGING_FACTLIFTS') }}
    where LIFTID is not null
)

select *
from stagingFactLifts

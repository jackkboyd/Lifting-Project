with stagingDimWorkouts as (
    select 
        WORKOUTID,
        WORKOUTNAME,
        MOVEMENTSEQUENCE,
        ISSUPERSET,
        STARTDATE,
        ENDDATE,
        WORKOUTCODE,
        MOVEMENTNAME
    from {{ source('SF_LIFT', 'STAGING_DIMWORKOUTS') }}
    where WORKOUTID is not null 
)

select *
from stagingDimWorkouts

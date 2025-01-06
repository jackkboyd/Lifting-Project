{{ config(
    materialized='table'
) }}

with stagingDimRoutines as (
    select 
        RoutineID,
        RoutineCode,
        Workout1ID,
        Workout2ID,
        Workout3ID,
        Workout4ID,
        Workout5ID,
        Workout6ID,
        Workout7ID,
        StartDate,
        EndDate,
        RoutineName
    from
    {{ source('SF_LIFT', 'STAGING_DIMROUTINES') }}
    where RoutineID is not null
)

select *
from stagingDimRoutines

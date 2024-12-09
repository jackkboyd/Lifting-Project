with stagingDimUsers as (
    select 
        USERID,
        FIRSTNAME,
        LASTNAME,
        CURRENTROUTINEID,
        CREATEDATE,
        TOTALDAYSLIFTED,
        TOTALTONNAGELIFTED,
        AGE,
        USERCODE
    from {{ source('SF_LIFT', 'STAGING_DIMUSERS') }}
    where USERID is not null
)

select *
from stagingDimUsers

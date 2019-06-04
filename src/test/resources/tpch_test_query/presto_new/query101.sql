-- This is an additional query used for testing count-distinct aggregation.
select
    cast(count(distinct l.orderkey) as int) as distinct_key
from  SCRAMBLE_SCHEMA.lineitem l
where
    l.shipdate <= date '1998-12-01'
    
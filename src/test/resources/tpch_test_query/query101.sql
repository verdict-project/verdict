-- This is an additional query used for testing count-distinct aggregation.
select
    cast(count(distinct l_orderkey) as int) as distinct_key
from  lineitem
where
    l_shipdate <= date '1998-12-01'
    
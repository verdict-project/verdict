-- This is an additional query used for testing count-distinct aggregation.
select
    cast(count(distinct l_orderkey) * 10.1 as int) as distinct_key
from  lineitem
where
    l_shipdate <= '1998-12-01'

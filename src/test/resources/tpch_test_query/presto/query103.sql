-- This is an additional query used for testing count-distinct aggregation.
-- MySQL use SIGNED/UNSIGNED instead of INT
select
    cast(approx_distinct(l_orderkey) * 10.1 as int) as distinct_key
from  lineitem
where
    l_shipdate <= date '1998-12-01'

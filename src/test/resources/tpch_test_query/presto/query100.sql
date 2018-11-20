-- This is an additional query used for testing count-distinct aggregation.
select
    l_returnflag,
    l_linestatus,
    count(distinct l_orderkey) as distinct_key
from  lineitem
where
    l_shipdate <= date '1998-12-01'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
    
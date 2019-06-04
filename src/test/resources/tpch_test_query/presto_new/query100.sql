-- This is an additional query used for testing count-distinct aggregation.
select
    l.returnflag,
    l.linestatus,
    count(distinct l.orderkey) as distinct_key
from  SCRAMBLE_SCHEMA.lineitem l
where
    l.shipdate <= date '1998-12-01'
group by
    l.returnflag,
    l.linestatus
order by
    l.returnflag,
    l.linestatus
    
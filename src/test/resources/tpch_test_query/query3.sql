select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate, o_shippriority
from
    customer,
    orders,
    lineitem
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1998-12-01'
    and l_shipdate > date '1996-12-01'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    l_orderkey desc,
    o_orderdate,
    o_shippriority
limit 10

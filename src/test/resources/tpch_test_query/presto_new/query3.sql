select
    o.orderkey,
    sum(extendedprice * (1 - discount)) as revenue,
    orderdate, shippriority
from
    TPCH_SCHEMA.customer c,
    SCRAMBLE_SCHEMA.orders o,
    SCRAMBLE_SCHEMA.lineitem l
where
    c.custkey = o.custkey
    and o.orderkey = l.orderkey
    and orderdate < date '1998-12-01'
    and shipdate > date '1996-12-01'
group by
    o.orderkey,
    o.orderdate,
    shippriority
order by
    revenue desc,
    orderdate
limit 10

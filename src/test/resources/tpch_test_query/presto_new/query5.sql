select
    n."name",
    sum(l.extendedprice * (1 - l.discount)) as revenue
from
    TPCH_SCHEMA.customer c,
    SCRAMBLE_SCHEMA.orders o,
    SCRAMBLE_SCHEMA.lineitem l,
    TPCH_SCHEMA.supplier s,
    TPCH_SCHEMA.nation n,
    TPCH_SCHEMA.region r
where
    c.custkey = o.custkey
    and l.orderkey = o.orderkey
    and l.suppkey = s.suppkey
    and c.nationkey = s.nationkey
    and s.nationkey = n.nationkey
    and n.regionkey = r.regionkey
    and o.orderdate >= date '1992-12-01'
    and o.orderdate < date '1998-12-01'
group by
    n."name"
order by
    revenue desc

select
    c.custkey,
    c."name",
    sum(l.extendedprice * (1 - l.discount)) as revenue,
    c.acctbal,
--    n."name",
    c.address,
    c.phone,
    c."comment"
from
    TPCH_SCHEMA.customer c,
    SCRAMBLE_SCHEMA.orders o,
    SCRAMBLE_SCHEMA.lineitem l,
    TPCH_SCHEMA.nation n
where
    c.custkey = o.custkey
    and l.orderkey = o.orderkey
    and o.orderdate >= date '1992-01-01'
    and o.orderdate < date '1998-01-01'
    and l.returnflag = 'R'
    and c.nationkey = n.nationkey
group by
    c.custkey,
    c."name",
    c.acctbal,
    c.phone,
--    n."name", commented out until #375 is fixed
    c.address,
    c."comment"
order by
    revenue desc

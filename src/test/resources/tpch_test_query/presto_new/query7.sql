select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from (
    select
        n1."name" as supp_nation,
        n2."name" as cust_nation,
        date_format(l.shipdate, '%Y') as l_year,
        l.extendedprice * (1 - l.discount) as volume
    from
        TPCH_SCHEMA.supplier s,
        SCRAMBLE_SCHEMA.lineitem l,
        SCRAMBLE_SCHEMA.orders o,
        TPCH_SCHEMA.customer c,
        TPCH_SCHEMA.nation n1,
        TPCH_SCHEMA.nation n2
    where
        s.suppkey = l.suppkey
        and o.orderkey = l.orderkey
        and c.custkey = o.custkey
        and s.nationkey = n1.nationkey
        and c.nationkey = n2.nationkey
        and ( (n1."name" = 'CHINA' and n2."name" = 'RUSSIA')
            or (n1."name" = 'RUSSIA' and n2."name" = 'CHINA') )
        and l.shipdate between (timestamp '1992-01-01') and (timestamp '1996-12-31') )
    as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year

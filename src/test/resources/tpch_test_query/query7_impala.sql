select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from (
    select
        n1.n_name as supp_nation,
        n2.n_name as cust_nation,
        substring(cast(l_shipdate as STRING),0,4) as l_year,
        l_extendedprice * (1 - l_discount) as volume
    from
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
    where
        s_suppkey = l_suppkey
        and o_orderkey = l_orderkey
        and c_custkey = o_custkey
        and s_nationkey = n1.n_nationkey
        and c_nationkey = n2.n_nationkey
        and ( (n1.n_name = 'CHINA' and n2.n_name = 'RUSSIA')
            or (n1.n_name = 'RUSSIA' and n2.n_name = 'CHINA') )
        and l_shipdate between '1992-01-01' and '1996-12-31' )
    as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year

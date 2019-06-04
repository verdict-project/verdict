select
    c.custkey,
    count(o.orderkey) as c_count
from
    TPCH_SCHEMA.customer c inner join SCRAMBLE_SCHEMA.orders o
        on c.custkey = o.custkey
        and o."comment" not like '%unusual%'
group by
    c.custkey
order by
    c.custkey

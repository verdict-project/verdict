select
    o_orderpriority,
    count(*) as order_count
from
    orders join lineitem
        on l_orderkey = o_orderkey
where
    o_orderdate >=  '1992-12-01'
    and o_orderdate <  '1998-12-01'
    and l_commitdate < l_receiptdate
group by
    o_orderpriority
order by
    o_orderpriority

select
    orderpriority,
    count(*) as order_count
from
    SCRAMBLE_SCHEMA.orders o join SCRAMBLE_SCHEMA.lineitem l
        on l.orderkey = o.orderkey
where
    o.orderdate >= date '1992-12-01'
    and o.orderdate < date '1998-12-01'
    and l.commitdate < l.receiptdate
group by
    orderpriority
order by
    orderpriority

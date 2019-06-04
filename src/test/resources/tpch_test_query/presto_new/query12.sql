select
    l.shipmode,
    sum(case when o.orderpriority = '1-URGENT' or o.orderpriority = '2-HIGH'
        then 1 else 0 end) as high_line_count,
    sum(case when o.orderpriority <> '1-URGENT' and o.orderpriority <> '2-HIGH'
        then 1 else 0 end) as low_line_count
from
    SCRAMBLE_SCHEMA.orders o,
    SCRAMBLE_SCHEMA.lineitem l
where
    o.orderkey = l.orderkey
    and l.commitdate < l.receiptdate
    and l.shipdate < l.commitdate
    and l.receiptdate >= date '1992-01-01'
    and l.receiptdate < date '1998-01-01'
group by
    l.shipmode
order by
    l.shipmode

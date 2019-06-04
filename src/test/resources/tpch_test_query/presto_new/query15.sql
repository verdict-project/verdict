select
    l.suppkey,
    sum(l.extendedprice * (1 - l.discount))
from
    SCRAMBLE_SCHEMA.lineitem l
where
    l.shipdate >= date '1992-01-01'
    and l.shipdate < date '1999-01-01'
group by
    l.suppkey
order by
    l.suppkey

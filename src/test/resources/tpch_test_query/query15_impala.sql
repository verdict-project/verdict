select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem_scrambled
where
    l_shipdate >= '1992-01-01'
    and l_shipdate < '1999-01-01'
group by
    l_suppkey
order by
    l_suppkey
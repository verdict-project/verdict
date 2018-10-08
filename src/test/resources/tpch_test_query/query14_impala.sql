select
    100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount)
        else 0 end) as numerator,
    sum(l_extendedprice * (1 - l_discount)) as denominator
from
    lineitem,
    part
where
    l_partkey = p_partkey
    and l_shipdate >= '1992-01-01'
    and l_shipdate < '1998-01-01'

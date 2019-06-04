select
    100.00 * sum(case when p."type" like 'PROMO%' then l.extendedprice * (1 - l.discount)
        else 0 end) as numerator,
    sum(l.extendedprice * (1 - l.discount)) as denominator
from
    SCRAMBLE_SCHEMA.lineitem l,
    TPCH_SCHEMA.part p
where
    l.partkey = p.partkey
    and l.shipdate >= date '1992-01-01'
    and l.shipdate < date '1998-01-01'

select
  s."name",
  count(s.address)
from
  TPCH_SCHEMA.supplier s,
  TPCH_SCHEMA.nation n,
  TPCH_SCHEMA.partsupp ps,
  (select
    l.partkey,
    l.suppkey,
    0.5 * sum(l.quantity) as sum_quantity
  from
    SCRAMBLE_SCHEMA.lineitem l
where
  l.shipdate >= date '1994-01-01'
  and l.shipdate < date '1998-01-01'
group by l.partkey, l.suppkey) as q20_tmp2_cached
where
  s.nationkey = n.nationkey
  and n."name" = 'CANADA'
  and s.suppkey = ps.suppkey
  group by s."name"
order by s."name"

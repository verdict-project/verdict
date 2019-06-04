select
  c."name",
  c.custkey,
  o.orderkey,
  o.orderdate,
  o.totalprice,
  sum(l.quantity)
from
  TPCH_SCHEMA.customer c,
  SCRAMBLE_SCHEMA.orders o,
  (select
  l1.orderkey,
  sum(l1.quantity) as t_sum_quantity
  from
    SCRAMBLE_SCHEMA.lineitem l1
  where
    l1.orderkey is not null
  group by
    l1.orderkey) as t,
  SCRAMBLE_SCHEMA.lineitem l
where
  c.custkey = o.custkey
  and o.orderkey = t.orderkey
  and o.orderkey is not null
  and t.t_sum_quantity > 150
group by
  c."name",
  c.custkey,
  o.orderkey,
  o.orderdate,
  o.totalprice
order by
  o.totalprice desc,
  o.orderdate

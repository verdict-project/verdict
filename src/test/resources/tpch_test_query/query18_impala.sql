select
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
from
  customer,
  orders,
  (select
  l_orderkey,
  sum(l_quantity) as t_sum_quantity
  from
    lineitem
  where
    l_orderkey is not null
  group by
    l_orderkey) as t,
  lineitem l
where
  c_custkey = o_custkey
  and o_orderkey = t.l_orderkey
  and o_orderkey is not null
  and t.t_sum_quantity > 150
group by
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by
  o_totalprice desc,
  o_orderdate

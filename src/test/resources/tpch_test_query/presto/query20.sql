select
  s_name,
  count(s_address)
from
  supplier,
  nation,
  partsupp,
  (select
    l_partkey,
    l_suppkey,
    0.5 * sum(l_quantity) as sum_quantity
  from
    lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1998-01-01'
group by l_partkey, l_suppkey) as q20_tmp2_cached
where
  s_nationkey = n_nationkey
  and n_name = 'CANADA'
  and s_suppkey = ps_suppkey
  group by s_name
order by s_name

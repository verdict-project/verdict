select
  nation,
  o_year,
  sum(amount) as sum_profit
from
  (
    select
      n_name as nation,
      date_format(o_orderdate, '%Y') as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
      lineitem join orders on o_orderkey = l_orderkey
      join partsupp on ps_suppkey = l_suppkey and ps_partkey = l_partkey
      join supplier on s_suppkey = l_suppkey
      join part on p_partkey = l_partkey
      join nation on s_nationkey = n_nationkey
    where
      p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc

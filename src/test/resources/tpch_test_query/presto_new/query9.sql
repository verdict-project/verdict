select
  nation,
  o_year,
  sum(amount) as sum_profit
from
  (
    select
      n."name" as nation,
      date_format(o.orderdate, '%Y') as o_year,
      l.extendedprice * (1 - l.discount) - ps.supplycost * l.quantity as amount
    from
      SCRAMBLE_SCHEMA.lineitem l join SCRAMBLE_SCHEMA.orders o on o.orderkey = l.orderkey
      join TPCH_SCHEMA.partsupp ps on ps.suppkey = l.suppkey and ps.partkey = l.partkey
      join TPCH_SCHEMA.supplier s on s.suppkey = l.suppkey
      join TPCH_SCHEMA.part p on p.partkey = l.partkey
      join TPCH_SCHEMA.nation n on s.nationkey = n.nationkey
    where
      p."name" like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc

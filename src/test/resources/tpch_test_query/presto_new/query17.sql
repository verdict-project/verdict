select
  sum(extendedprice) / 7.0 as avg_yearly
from (
  select
    quantity as quantity,
    extendedprice as extendedprice,
    t_avg_quantity
  from
    (select
  l.partkey as t_partkey,
  0.2 * avg(l.quantity) as t_avg_quantity
from
  SCRAMBLE_SCHEMA.lineitem l
group by l.partkey) as q17_lineitem_tmp_cached Inner Join
    (select
      l.quantity,
      l.partkey,
      l.extendedprice
    from
      TPCH_SCHEMA.part p,
      SCRAMBLE_SCHEMA.lineitem l
    where
      p.partkey = l.partkey
    ) as l1 on l1.partkey = t_partkey
) a
where quantity > t_avg_quantity

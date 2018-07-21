select
  sum(extendedprice) / 7.0 as avg_yearly
from (
  select
    l_quantity as quantity,
    l_extendedprice as extendedprice,
    t_avg_quantity
  from
    (select
  l_partkey as t_partkey,
  0.2 * avg(l_quantity) as t_avg_quantity
from
  lineitem_scrambled
group by l_partkey) as q17_lineitem_tmp_cached Inner Join
    (select
      l_quantity,
      l_partkey,
      l_extendedprice
    from
      part,
      lineitem_scrambled
    where
      p_partkey = l_partkey
    ) as l1 on l1.l_partkey = t_partkey
) a
where quantity > t_avg_quantity
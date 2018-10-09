select
  o_year,
  sum(case
    when nation = 'PERU' then volume
    else 0
  end) as numerator, sum(volume) as demoninator
from
  (
    select
      substr(cast(o_orderdate AS TEXT),1,4) as o_year,
      l_extendedprice * (1 - l_discount) as volume,
      n2.n_name as nation
    from
      lineitem join orders on l_orderkey = o_orderkey
      join supplier on s_suppkey = l_suppkey
      join part on p_partkey = l_partkey
      join customer on o_custkey = c_custkey
      join nation n1 on c_nationkey = n1.n_nationkey
      join region on n1.n_regionkey = r_regionkey
      join nation n2 on s_nationkey = n2.n_nationkey
    where
      r_name = 'AMERICA'
      and o_orderdate between '1995-01-01' and '1996-12-31'
      and p_type = 'ECONOMY ANODIZED STEEL'  ) as all_nations
group by
  o_year
order by
  o_year

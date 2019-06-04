select
  o_year,
  sum(case
    when nation = 'PERU' then volume
    else 0
  end) as numerator, sum(volume) as demoninator
from
  (
    select
      date_format(o.orderdate, '%Y') as o_year,
      l.extendedprice * (1 - l.discount) as volume,
      n2."name" as nation
    from
      SCRAMBLE_SCHEMA.lineitem l join SCRAMBLE_SCHEMA.orders o on l.orderkey = o.orderkey
      join TPCH_SCHEMA.supplier s on s.suppkey = l.suppkey
      join TPCH_SCHEMA.part p on p.partkey = l.partkey
      join TPCH_SCHEMA.customer c on o.custkey = c.custkey
      join TPCH_SCHEMA.nation n1 on c.nationkey = n1.nationkey
      join TPCH_SCHEMA.region r on n1.regionkey = r.regionkey
      join TPCH_SCHEMA.nation n2 on s.nationkey = n2.nationkey
    where
      r."name" = 'AMERICA'
      and o.orderdate between date '1995-01-01' and date '1996-12-31'
      and p."type" = 'ECONOMY ANODIZED STEEL'  ) as all_nations
group by
  o_year
order by
  o_year

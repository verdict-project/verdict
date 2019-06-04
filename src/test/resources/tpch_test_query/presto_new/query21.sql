select s_name, count(1) as numwait
from (  select s_name   from (    select s_name, t2.orderkey, l_suppkey, countsuppkey, maxsuppkey
    from (      select l.orderkey, count(l.suppkey) countsuppkey, max(l.suppkey) as maxsuppkey
      from SCRAMBLE_SCHEMA.lineitem l
      where l.receiptdate > l.commitdate and l.orderkey is not null
      group by l.orderkey) as t2    right outer join (select  s_name, l_orderkey, l_suppkey  from
      (select s_name, t1.orderkey as l_orderkey, l2.suppkey as l_suppkey, countsuppkey, maxsuppkey
       from
         (select l.orderkey, count(l.suppkey) as countsuppkey, max(l.suppkey) as maxsuppkey
          from SCRAMBLE_SCHEMA.lineitem l
          where l.orderkey is not null
          group by l.orderkey) as t1
          join
          (select s_name, l1.orderkey, l1.suppkey
           from SCRAMBLE_SCHEMA.orders o
           join (select s."name" as s_name, l.orderkey, l.suppkey
                 from TPCH_SCHEMA.nation n join TPCH_SCHEMA.supplier s on s.nationkey = n.nationkey
                                           join SCRAMBLE_SCHEMA.lineitem l on s.suppkey = l.suppkey
                 where l.receiptdate > l.commitdate
                 and l.orderkey is not null) l1 on o.orderkey = l1.orderkey
          ) l2 on l2.orderkey = t1.orderkey
        ) a
      where (countsuppkey > 1) or ((countsuppkey=1) and (l_suppkey <> maxsuppkey))
    ) l3 on l_orderkey = t2.orderkey
  ) b
  where (countsuppkey is null) or ((countsuppkey=1) and (l_suppkey = maxsuppkey))
) c group by s_name order by numwait desc, s_name

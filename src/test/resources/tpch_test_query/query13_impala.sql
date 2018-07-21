select
    c_custkey,
    count(o_orderkey) as c_count
from
    customer inner join orders_scrambled
        on c_custkey = o_custkey
        and o_comment not like '%unusual%'
group by
    c_custkey
order by
    c_custkey
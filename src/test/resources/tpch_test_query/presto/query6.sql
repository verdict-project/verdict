select
    sum(l_extendedprice * l_discount) as revenue
    from lineitem
where
    date(l_shipdate) >= date '1992-12-01'
    and date(l_shipdate) < date '1998-12-01'
    and l_discount between 0.04 - 0.02
    and 0.04 + 0.02
    and l_quantity < 15

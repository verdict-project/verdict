select
    sum(l.extendedprice * l.discount) as revenue
    from SCRAMBLE_SCHEMA.lineitem l
where
    l.shipdate >= date '1992-12-01'
    and l.shipdate < date '1998-12-01'
    and l.discount between 0.04 - 0.02
    and 0.04 + 0.02
    and l.quantity < 15

SELECT `lineitem`.`l_suppkey` AS `l_suppkey`,
  `supplier`.`s_address` AS `s_address`,
  `supplier`.`s_name` AS `s_name`,
  `supplier`.`s_phone` AS `s_phone`,
  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`
FROM `lineitem`
  INNER JOIN `supplier` ON (`lineitem`.`l_suppkey` = `supplier`.`s_suppkey`)
  INNER JOIN (
  SELECT `lineitem`.`l_suppkey` AS `l_suppkey`,
    SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `__measure__0`
  FROM `lineitem`
  WHERE ((`lineitem`.`l_shipdate` >= TIMESTAMP('1996-01-01 00:00:00')) AND (`lineitem`.`l_shipdate` < TIMESTAMP('1996-04-01 00:00:00')))
  GROUP BY 1
) `t0` ON (`lineitem`.`l_suppkey` = `t0`.`l_suppkey`)
  CROSS JOIN (
  SELECT MAX(`t1`.`__measure__1`) AS `__measure__2`
  FROM (
    SELECT `lineitem`.`l_suppkey` AS `l_suppkey`,
      SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `__measure__1`
    FROM `lineitem`
    WHERE ((`lineitem`.`l_shipdate` >= TIMESTAMP('1996-01-01 00:00:00')) AND (`lineitem`.`l_shipdate` < TIMESTAMP('1996-04-01 00:00:00')))
    GROUP BY 1
  ) `t1`
  HAVING (COUNT(1) > 0)
) `t2`
WHERE (((`lineitem`.`l_shipdate` >= TIMESTAMP('1996-01-01 00:00:00')) AND (`lineitem`.`l_shipdate` < TIMESTAMP('1996-04-01 00:00:00'))) AND (`t0`.`__measure__0` = `t2`.`__measure__2`))
GROUP BY 1, 2, 3, 4;

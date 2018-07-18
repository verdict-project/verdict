SELECT `customer`.`c_acctbal` AS `c_acctbal`,
  `customer`.`c_address` AS `c_address`,
  `customer`.`c_comment` AS `c_comment`,
  `customer`.`c_custkey` AS `c_custkey`,
  `customer`.`c_name` AS `c_name`,
  `customer`.`c_phone` AS `c_phone`,
  `N1`.`n_name` AS `n_NAME (nation1)`,
  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`
FROM `lineitem`
  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)
  INNER JOIN `nation` `N1` ON (`customer`.`c_nationkey` = `N1`.`n_nationkey`)
  INNER JOIN (
  SELECT `orders`.`o_custkey` AS `c_custkey`,
    SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `$__alias__0`
  FROM `lineitem`
    INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
  WHERE ((`lineitem`.`l_returnflag` = 'R') AND (`orders`.`o_orderdate` >= TIMESTAMP('1993-10-01 00:00:00')) AND (`orders`.`o_orderdate` < TIMESTAMP('1994-01-01 00:00:00')))
  GROUP BY 1
  ORDER BY `$__alias__0` DESC
  LIMIT 20
) `t0` ON (`customer`.`c_custkey` = `t0`.`c_custkey`)
WHERE ((`lineitem`.`l_returnflag` = 'R') AND (`orders`.`o_orderdate` >= TIMESTAMP('1993-10-01 00:00:00')) AND (`orders`.`o_orderdate` < TIMESTAMP('1994-01-01 00:00:00')))
GROUP BY 1, 2, 3, 4, 5, 6, 7;

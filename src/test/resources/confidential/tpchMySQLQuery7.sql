SELECT `N1`.`n_name` AS `n_NAME (nation1)`,
  `N2`.`n_name` AS `n_name`,
  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`,
  YEAR(`lineitem`.`l_shipdate`) AS `yr_l_shipdate_ok`
FROM `lineitem`
  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)
  INNER JOIN `nation` `N1` ON (`customer`.`c_nationkey` = `N1`.`n_nationkey`)
  INNER JOIN `supplier` ON (`lineitem`.`l_suppkey` = `supplier`.`s_suppkey`)
  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)
WHERE ((NOT (`customer`.`c_nationkey` = `supplier`.`s_nationkey`)) AND (`lineitem`.`l_shipdate` >= DATE('1995-01-01')) AND (`lineitem`.`l_shipdate` <= DATE('1996-12-31')) AND (`N1`.`n_name` IN ('FRANCE', 'GERMANY')) AND (`N2`.`n_name` IN ('FRANCE', 'GERMANY')))
GROUP BY 1, 2, 4;

SELECT `N2`.`n_name` AS `n_name`,
  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`
FROM `lineitem`
  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)
  INNER JOIN `nation` `N1` ON (`customer`.`c_nationkey` = `N1`.`n_nationkey`)
  INNER JOIN `region` ON (`N1`.`n_regionkey` = `region`.`r_regionkey`)
  INNER JOIN `supplier` ON (`lineitem`.`l_suppkey` = `supplier`.`s_suppkey`)
  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)
WHERE ((`customer`.`c_nationkey` = `supplier`.`s_nationkey`) AND (`orders`.`o_orderdate` >= TIMESTAMP('1994-01-01 00:00:00')) AND (`orders`.`o_orderdate` < TIMESTAMP('1995-01-01 00:00:00')) AND (`region`.`r_name` = 'ASIA'))
GROUP BY 1;

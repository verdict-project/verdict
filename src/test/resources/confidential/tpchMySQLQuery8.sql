SELECT `N2`.`n_name` AS `n_name`,
  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`,
  YEAR(`orders`.`o_orderdate`) AS `yr_o_orderdate_ok`
FROM `lineitem`
  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)
  INNER JOIN `nation` `N1` ON (`customer`.`c_nationkey` = `N1`.`n_nationkey`)
  INNER JOIN `region` ON (`N1`.`n_regionkey` = `region`.`r_regionkey`)
  INNER JOIN `supplier` ON (`lineitem`.`l_suppkey` = `supplier`.`s_suppkey`)
  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)
  INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)
WHERE ((`orders`.`o_orderdate` >= DATE('1995-01-01')) AND (`orders`.`o_orderdate` <= DATE('1996-12-31')) AND (`part`.`p_type` = 'ECONOMY ANODIZED STEEL') AND (`region`.`r_name` = 'AMERICA'))
GROUP BY 1, 3;

SELECT `supplier`.`s_name` AS `s_name`,
  SUM(1) AS `sum_Number of Records_ok`,
  SUM(1) AS `$__alias__0`
FROM `lineitem`
  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
  INNER JOIN `supplier` ON (`lineitem`.`l_suppkey` = `supplier`.`s_suppkey`)
  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)
  INNER JOIN (
  SELECT `lineitem`.`l_orderkey` AS `none_L Orderkey (copy)_ok`
  FROM `lineitem`
  GROUP BY 1
  HAVING (COUNT(DISTINCT (CASE WHEN (`lineitem`.`l_commitdate` < `lineitem`.`l_receiptdate`) THEN `lineitem`.`l_suppkey` ELSE NULL END)) = 1)
) `t0` ON (`lineitem`.`l_orderkey` = `t0`.`none_L Orderkey (copy)_ok`)
  INNER JOIN (
  SELECT `lineitem`.`l_orderkey` AS `none_l_orderkey_ok`
  FROM `lineitem`
  GROUP BY 1
  HAVING (COUNT(DISTINCT `lineitem`.`l_suppkey`) > 1)
) `t1` ON (`lineitem`.`l_orderkey` = `t1`.`none_l_orderkey_ok`)
WHERE ((`lineitem`.`l_commitdate` < `lineitem`.`l_receiptdate`) AND (`N2`.`n_name` = 'SAUDI ARABIA') AND (`orders`.`o_orderstatus` = 'F'))
GROUP BY 1
ORDER BY `$__alias__0` DESC
LIMIT 100;

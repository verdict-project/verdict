SELECT `orders`.`o_orderdate` AS `o_orderdate`,
  `orders`.`o_orderkey` AS `o_orderkey`,
  `orders`.`o_shippriority` AS `o_shippriority`,
  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`
FROM `lineitem`
  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)
  INNER JOIN (
  SELECT `orders`.`o_orderkey` AS `o_orderkey`,
    SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `$__alias__0`
  FROM `lineitem`
    INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
    INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)
  WHERE ((`lineitem`.`l_shipdate` > DATE('1995-03-15')) AND (`orders`.`o_orderdate` < DATE('1995-03-15')) AND (`customer`.`c_mktsegment` = 'BUILDING'))
  GROUP BY 1
  ORDER BY `$__alias__0` DESC
  LIMIT 10
) `t0` ON (`orders`.`o_orderkey` = `t0`.`o_orderkey`)
WHERE ((`lineitem`.`l_shipdate` > DATE('1995-03-15')) AND (`orders`.`o_orderdate` < DATE('1995-03-15')) AND (`customer`.`c_mktsegment` = 'BUILDING'))
GROUP BY 1, 2, 3;

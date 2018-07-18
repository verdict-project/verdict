SELECT (CASE WHEN 2 >= 0 THEN SUBSTR(CAST(`customer`.`c_phone` AS CHAR), 1, FLOOR(2)) ELSE NULL END) AS `Calculation_5131031153053149`,
  COUNT(`customer`.`c_custkey`) AS `cnt:c_custkey:ok`,
  SUM(`customer`.`c_acctbal`) AS `sum:c_acctbal:ok`
FROM `tpch_flat_orc_2`.`orders` `orders`
  RIGHT JOIN `tpch_flat_orc_2`.`customer` `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)
  CROSS JOIN (
  SELECT AVG((CASE WHEN (`customer`.`c_acctbal` > 0) THEN `customer`.`c_acctbal` ELSE NULL END)) AS `__measure__1`
  FROM `tpch_flat_orc_2`.`customer` `customer`
  WHERE ((CASE WHEN 2 >= 0 THEN SUBSTR(CAST(`customer`.`c_phone` AS CHAR), 1, FLOOR(2)) ELSE NULL END) IN ('13', '17', '18', '23', '29', '30', '31'))
  HAVING (COUNT(1) > 0)
) `t0`
WHERE (((CASE WHEN 2 >= 0 THEN SUBSTR(CAST(`customer`.`c_phone` AS CHAR), 1, FLOOR(2)) ELSE NULL END) IN ('13', '17', '18', '23', '29', '30', '31')) AND ((`customer`.`c_acctbal` > `t0`.`__measure__1`) AND (`orders`.`o_orderkey` IS NULL)))
GROUP BY `Calculation_5131031153053149`;
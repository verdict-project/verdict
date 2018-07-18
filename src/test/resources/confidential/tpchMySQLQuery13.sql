SELECT `t0`.`__measure__0` AS `Calculation_1361031105746580`,
  COUNT(DISTINCT `customer`.`c_custkey`) AS `ctd_c_custkey_ok`
FROM `customer`
  INNER JOIN (
  SELECT `customer`.`c_custkey` AS `c_custkey`,
    COUNT((CASE WHEN (NOT ((0 < LOCATE('special',`orders`.`o_comment`)) AND (0 < IF(ISNULL(LOCATE('special',`orders`.`o_comment`)), NULL, LOCATE('requests',`orders`.`o_comment`,GREATEST(1,FLOOR(LOCATE('special',`orders`.`o_comment`)))))))) THEN `orders`.`o_orderkey` ELSE NULL END)) AS `__measure__0`
  FROM `orders`
    RIGHT JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)
  GROUP BY 1
) `t0` ON (`customer`.`c_custkey` = `t0`.`c_custkey`)
GROUP BY 1;

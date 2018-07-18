SELECT SUM(`lineitem`.`l_extendedprice`) AS `TEMP(Calculation_7921031152254526)(1602391293)(0)`
FROM `lineitem`
  INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)
  INNER JOIN (
  SELECT `lineitem`.`l_partkey` AS `l_partkey`,
    AVG(`lineitem`.`l_quantity`) AS `__measure__0`
  FROM `lineitem`
  GROUP BY 1
) `t0` ON (`lineitem`.`l_partkey` = `t0`.`l_partkey`)
WHERE ((((0.20000000000000001 * `t0`.`__measure__0`) - `lineitem`.`l_quantity`) >= -1.0000000000000001E-17) AND (`part`.`p_brand` = 'Brand#23') AND (`part`.`p_container` = 'MED BOX'))
HAVING (COUNT(1) > 0);

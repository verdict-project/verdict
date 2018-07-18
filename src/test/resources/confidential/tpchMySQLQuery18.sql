SELECT `customer`.`c_custkey` AS `c_custkey`,
  `customer`.`c_name` AS `c_name`,
  MIN(`orders`.`o_totalprice`) AS `min_o_totalprice_ok`,
  `orders`.`o_orderdate` AS `o_orderdate`,
  `orders`.`o_orderkey` AS `o_orderkey`,
  `orders`.`o_totalprice` AS `o_totalprice`,
  SUM(`lineitem`.`l_quantity`) AS `sum_l_quantity_ok`
FROM `lineitem`
  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)
GROUP BY 1,
  2,
  4,
  5,
  6
HAVING (SUM(`lineitem`.`l_quantity`) >= 300.99999999999699);

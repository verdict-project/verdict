SELECT AVG(`lineitem`.`l_discount`) AS `avg_l_discount_ok`,
  AVG(`lineitem`.`l_extendedprice`) AS `avg_l_extendedprice_ok`,
  AVG(`lineitem`.`l_quantity`) AS `avg_l_quantity_ok`,
  `lineitem`.`l_linestatus` AS `l_linestatus`,
  `lineitem`.`l_returnflag` AS `l_returnflag`,
  SUM(((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`)) * (1 + `lineitem`.`l_tax`))) AS `sum_Calculation_4310711085325373_ok`,
  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`,
  SUM(1) AS `sum_Number of Records_ok`,
  SUM(`lineitem`.`l_extendedprice`) AS `sum_l_extendedprice_ok`,
  SUM(`lineitem`.`l_quantity`) AS `sum_l_quantity_ok`
FROM `lineitem`
WHERE (`lineitem`.`l_shipdate` <= TIMESTAMP('1998-09-02 00:00:00'))
GROUP BY 4, 5;
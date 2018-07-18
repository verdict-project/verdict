SELECT SUM((`lineitem`.`l_extendedprice` * `lineitem`.`l_discount`)) AS `sum_Calculation_8100805085146752_ok`
FROM `lineitem`
WHERE ((`lineitem`.`l_discount` >= 0.05) AND (`lineitem`.`l_discount` <= 0.07) AND (`lineitem`.`l_quantity` <= 23.00) AND (`lineitem`.`l_shipdate` >= TIMESTAMP('1994-01-01 00:00:00')) AND (`lineitem`.`l_shipdate` < TIMESTAMP('1995-01-01 00:00:00')))
HAVING (COUNT(1) > 0);
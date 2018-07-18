SELECT (CASE WHEN (`orders`.`o_orderpriority` IN ('1-URGENT', '2-HIGH')) THEN '1-URGENT' WHEN (`orders`.`o_orderpriority` IN ('3-MEDIUM', '4-NOT SPECIFIED', '5-LOW')) THEN '3-MEDIUM' ELSE `orders`.`o_orderpriority` END) AS `O Orderpriority (group)`,
  `lineitem`.`l_shipmode` AS `l_shipmode`,
  SUM(1) AS `sum_Number of Records_ok`
FROM `lineitem`
  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
WHERE ((`lineitem`.`l_commitdate` < `lineitem`.`l_receiptdate`) AND (`lineitem`.`l_shipdate` < `lineitem`.`l_commitdate`) AND (`lineitem`.`l_receiptdate` >= TIMESTAMP('1994-01-01 00:00:00')) AND (`lineitem`.`l_receiptdate` < TIMESTAMP('1995-01-01 00:00:00')) AND (`lineitem`.`l_shipmode` IN ('MAIL', 'SHIP')))
GROUP BY 1, 2;

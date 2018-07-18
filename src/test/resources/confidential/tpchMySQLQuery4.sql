SELECT COUNT(DISTINCT `orders`.`o_orderkey`) AS `ctd_o_orderkey_ok`,
  `orders`.`o_orderpriority` AS `o_orderpriority`
FROM `lineitem`
  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
WHERE ((`lineitem`.`l_commitdate` < `lineitem`.`l_receiptdate`) AND (`orders`.`o_orderdate` >= TIMESTAMP('1993-07-01 00:00:00')) AND (`orders`.`o_orderdate` < TIMESTAMP('1993-10-01 00:00:00')))
GROUP BY 2;

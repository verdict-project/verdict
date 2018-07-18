SELECT (NOT ISNULL(`t0`.`_Tableau_join_flag`)) AS `io_Promo Tpe_nk`,
  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`
FROM `lineitem`
  INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)
  LEFT JOIN (
  SELECT `part`.`p_type` AS `p_type`,
    MIN(1) AS `_Tableau_join_flag`
  FROM `lineitem`
    INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)
  WHERE (LEFT(`part`.`p_type`, LENGTH('PROMO')) = 'PROMO')
  GROUP BY 1
) `t0` ON (`part`.`p_type` = `t0`.`p_type`)
WHERE ((`lineitem`.`l_shipdate` >= TIMESTAMP('1995-09-01 00:00:00')) AND (`lineitem`.`l_shipdate` < TIMESTAMP('1995-10-01 00:00:00')))
GROUP BY 1;

SELECT SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`
FROM `lineitem`
  INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)
  LEFT JOIN (
  SELECT `part`.`p_container` AS `p_container`,
    MIN(1) AS `_Tableau_join_flag`
  FROM `lineitem`
    INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)
  WHERE (`part`.`p_container` IN ('SM BOX', 'SM CASE', 'SM PACK', 'SM PKG'))
  GROUP BY 1
) `t0` ON (`part`.`p_container` = `t0`.`p_container`)
  LEFT JOIN (
  SELECT `part`.`p_container` AS `p_container`,
    MIN(1) AS `_Tableau_join_flag`
  FROM `lineitem`
    INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)
  WHERE (`part`.`p_container` IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG'))
  GROUP BY 1
) `t1` ON (`part`.`p_container` = `t1`.`p_container`)
  LEFT JOIN (
  SELECT `part`.`p_container` AS `p_container`,
    MIN(1) AS `_Tableau_join_flag`
  FROM `lineitem`
    INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)
  WHERE (`part`.`p_container` IN ('LG BOX', 'LG CASE', 'LG PACK', 'LG PKG'))
  GROUP BY 1
) `t2` ON (`part`.`p_container` = `t2`.`p_container`)
WHERE ((((NOT ISNULL(`t0`.`_Tableau_join_flag`)) AND (`part`.`p_brand` = 'Brand#12') AND (`lineitem`.`l_quantity` >= 1) AND (`lineitem`.`l_quantity` <= 11) AND (`part`.`p_size` >= 1) AND (`part`.`p_size` <= 5)) OR ((NOT ISNULL(`t1`.`_Tableau_join_flag`)) AND (`part`.`p_brand` = 'Brand#23') AND (`lineitem`.`l_quantity` >= 10) AND (`lineitem`.`l_quantity` <= 20) AND (`part`.`p_size` >= 1) AND (`part`.`p_size` <= 10)) OR ((NOT ISNULL(`t2`.`_Tableau_join_flag`)) AND (`part`.`p_brand` = 'Brand#34') AND (`lineitem`.`l_quantity` >= 20) AND (`lineitem`.`l_quantity` <= 30) AND (`part`.`p_size` >= 1) AND (`part`.`p_size` <= 15))) AND (`lineitem`.`l_shipinstruct` = 'DELIVER IN PERSON') AND (`lineitem`.`l_shipmode` = 'AIR'))
HAVING (COUNT(1) > 0);

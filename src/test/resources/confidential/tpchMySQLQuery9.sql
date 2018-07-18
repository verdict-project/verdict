SELECT `N2`.`n_name` AS `n_name`,
  SUM(((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`)) - (`partsupp`.`ps_supplycost` * `lineitem`.`l_quantity`))) AS `sum_Calculation_1690805085450945_ok`,
  YEAR(`orders`.`o_orderdate`) AS `yr_o_orderdate_ok`
FROM `lineitem`
  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)
  INNER JOIN `partsupp` ON ((`lineitem`.`l_partkey` = `partsupp`.`ps_partkey`) AND (`lineitem`.`l_suppkey` = `partsupp`.`ps_suppkey`))
  INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)
  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)
  INNER JOIN `part` ON (`partsupp`.`ps_partkey` = `part`.`p_partkey`)
WHERE (LOCATE('green',`part`.`p_name`) > 0)
GROUP BY 1, 3;

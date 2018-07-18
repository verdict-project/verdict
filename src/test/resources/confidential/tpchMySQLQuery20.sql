SELECT `supplier`.`s_address` AS `s_address`,
  `supplier`.`s_name` AS `s_name`
FROM `partsupp`
  INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)
  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)
  INNER JOIN `part` ON (`partsupp`.`ps_partkey` = `part`.`p_partkey`)
  INNER JOIN (
  SELECT `partsupp`.`ps_partkey` AS `ps_partkey`,
    `partsupp`.`ps_suppkey` AS `ps_suppkey`
  FROM `lineitem`
    INNER JOIN `partsupp` ON ((`lineitem`.`l_partkey` = `partsupp`.`ps_partkey`) AND (`lineitem`.`l_suppkey` = `partsupp`.`ps_suppkey`))
  GROUP BY 1,
    2
  HAVING (MIN(`partsupp`.`ps_availqty`) > (0.5 * SUM((CASE WHEN (YEAR(`lineitem`.`l_shipdate`) = 1994) THEN `lineitem`.`l_quantity` ELSE NULL END))))
) `t0` ON ((`partsupp`.`ps_partkey` = `t0`.`ps_partkey`) AND (`partsupp`.`ps_suppkey` = `t0`.`ps_suppkey`))
WHERE ((`N2`.`n_name` = 'CANADA') AND (LEFT(`part`.`p_name`, LENGTH('forest')) = 'forest'))
GROUP BY 1, 2;

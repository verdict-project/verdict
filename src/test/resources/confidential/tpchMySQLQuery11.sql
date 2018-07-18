SELECT `partsupp`.`ps_partkey` AS `ps_partkey`,
  SUM((`partsupp`.`ps_supplycost` * `partsupp`.`ps_availqty`)) AS `sum_Calculation_6140711155912621_ok`
FROM `partsupp`
  INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)
  INNER JOIN `nation` ON (`supplier`.`s_nationkey` = `nation`.`n_nationkey`)
WHERE (`nation`.`n_name` = 'GERMANY')
GROUP BY 1;

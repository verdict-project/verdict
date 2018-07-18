SELECT `nation`.`n_name` AS `n_name`,
  `part`.`p_mfgr` AS `p_mfgr`,
  `partsupp`.`ps_partkey` AS `ps_partkey`,
  `partsupp`.`ps_suppkey` AS `ps_suppkey`,
  `supplier`.`s_address` AS `s_address`,
  `supplier`.`s_comment` AS `s_comment`,
  `supplier`.`s_name` AS `s_name`,
  `supplier`.`s_phone` AS `s_phone`,
  `supplier`.`s_acctbal` AS `sum_s_acctbal_ok`
FROM `partsupp`
  INNER JOIN `part` ON (`partsupp`.`ps_partkey` = `part`.`p_partkey`)
  INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)
  INNER JOIN `nation` ON (`supplier`.`s_nationkey` = `nation`.`n_nationkey`)
  INNER JOIN `region` ON (`nation`.`n_regionkey` = `region`.`r_regionkey`)
  INNER JOIN (
  SELECT `partsupp`.`ps_partkey` AS `p_partkey`,
    MIN(`partsupp`.`ps_supplycost`) AS `__measure__0`
  FROM `partsupp`
    INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)
    INNER JOIN `nation` ON (`supplier`.`s_nationkey` = `nation`.`n_nationkey`)
    INNER JOIN `region` ON (`nation`.`n_regionkey` = `region`.`r_regionkey`)
  WHERE (`region`.`r_name` = 'EUROPE')
  GROUP BY 1
) `t0` ON (`part`.`p_partkey` = `t0`.`p_partkey`)
WHERE ((`region`.`r_name` = 'EUROPE') AND ((`partsupp`.`ps_supplycost` = `t0`.`__measure__0`) AND (`part`.`p_size` = 15) AND (RIGHT(RTRIM(`part`.`p_type`), LENGTH('BRASS')) = 'BRASS')));

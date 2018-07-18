SELECT COUNT(DISTINCT `partsupp`.`ps_suppkey`) AS `ctd_ps_suppkey_ok`,
  `part`.`p_brand` AS `p_brand`,
  `part`.`p_size` AS `p_size`,
  `part`.`p_type` AS `p_type`
FROM `partsupp`
  INNER JOIN `part` ON (`partsupp`.`ps_partkey` = `part`.`p_partkey`)
  INNER JOIN (
  SELECT `supplier`.`s_suppkey` AS `s_suppkey`
  FROM `partsupp`
    INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)
  GROUP BY 1
  HAVING (MIN((CASE WHEN ((0 < LOCATE('Customer',`supplier`.`s_comment`)) AND (0 < IF(ISNULL(LOCATE('Customer',`supplier`.`s_comment`)), NULL, LOCATE('Complaints',`supplier`.`s_comment`,GREATEST(1,FLOOR(LOCATE('Customer',`supplier`.`s_comment`))))))) THEN 1 ELSE 0 END)) = 0)
) `t0` ON (`partsupp`.`ps_suppkey` = `t0`.`s_suppkey`)
WHERE ((NOT (`part`.`p_brand` = 'Brand#45')) AND (`part`.`p_size` IN (3, 9, 14, 19, 23, 36, 45, 49)) AND (NOT (LEFT(`part`.`p_type`, LENGTH('MEDIUM POLISHED')) = 'MEDIUM POLISHED')))
GROUP BY 2, 3, 4;

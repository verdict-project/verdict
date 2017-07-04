package edu.umich.verdict.relation;

import static org.junit.Assert.*;

import org.junit.Test;

public class PrettyPrintTest {

	@Test
	public void selectAllTest() {
		String sql = "SELECT * FROM instacart1g.verdict_meta_size";
		System.out.println(Relation.prettyfySql(sql));
	}
	
	@Test
	public void partitionByTest() {
		String sql = "SELECT sum(price * (1 - discount)) OVER (partition by ship_method) FROM instacart1g.verdict_meta_size";
		System.out.println(Relation.prettyfySql(sql));
	}
	
	@Test
	public void stratifiedSampleTest() {
		String sql = "SELECT *, (count(*) OVER (partition by order_dow) / grp_size) AS verdict_sampling_prob "
				+ "FROM (SELECT *, count(*) OVER (partition by order_dow) AS grp_size FROM orders) AS vt8 "
				+ "WHERE rand(unix_timestamp()) < (3421082 * (0.01 / (grp_size / (SELECT count(distinct order_dow) AS expr1 FROM orders))))";
		System.out.println(Relation.prettyfySql(sql));
	}

}

package edu.umich.verdict.relation;

import static org.junit.Assert.*;

import org.junit.Test;

public class PrettyPrintTest {

	@Test
	public void selectAllTest() {
		
		String sql = "SELECT * FROM instacart1g.verdict_meta_size";
		System.out.println(Relation.prettyfySql(sql));
		
	}

}

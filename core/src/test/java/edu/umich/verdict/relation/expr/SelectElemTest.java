package edu.umich.verdict.relation.expr;

import org.junit.Test;

public class SelectElemTest {

	@Test
	public void starTest() {
		SelectElem.from("*");
	}
	
	@Test
	public void randExprTest() {
		SelectElem e = SelectElem.from("mod(rand(unix_timestamp()), 100) * 100 AS __vpart");
		System.out.println(e.toString());
		
		e = SelectElem.from("(rand(unix_timestamp()) * 100) % 100 AS __vpart");
		System.out.println(e.toString());
	}
	
}

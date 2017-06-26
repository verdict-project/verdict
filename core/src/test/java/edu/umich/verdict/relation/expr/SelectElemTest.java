package edu.umich.verdict.relation.expr;

import static org.junit.Assert.*;

import org.junit.Test;

public class SelectElemTest {

	@Test
	public void starTest() {
		SelectElem.from("*");
	}

}

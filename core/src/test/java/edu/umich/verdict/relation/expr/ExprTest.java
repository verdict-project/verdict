package edu.umich.verdict.relation.expr;

import static org.junit.Assert.*;

import org.junit.Test;

public class ExprTest {

	@Test
	public void caseExpr() {
		Expr a = Expr.from("CASE WHEN a < 5 THEN 1 WHEN a < 10 THEN 2 ELSE 3 END");
		System.out.println(a.toString());
	}

}

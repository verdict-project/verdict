package edu.umich.verdict.relation.expr;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.umich.verdict.relation.Relation;

public class ExprTest {

	@Test
	public void caseExprTest() {
		Expr a = Expr.from("CASE WHEN a < 5 THEN 1 WHEN a < 10 THEN 2 ELSE 3 END");
		assertEquals(a.toString(), "(CASE WHEN a < 5 THEN 1 WHEN a < 10 THEN 2 ELSE 3 END)");
	}
	
	@Test
	public void partitonExprTest() {
		Expr a = Expr.from("count(*) over (partition by order_dow)");
		assertEquals(a.toString(), "count(*) OVER (partition by order_dow)");
		
		Expr b = Expr.from("count(*) over ()");
		assertEquals(b.toString(), "count(*) OVER ()");
		
		Expr c = Expr.from("sum(prices * (1 - discount)) over (partition by ship_method)");
		assertEquals(c.toString(), "sum((prices * (1 - discount))) OVER (partition by ship_method)");
		
		Expr d = Expr.from("count(*) over (partition by order_dow, __vpart)");
		System.out.println(d);
//		assertEquals(b.toString(), "count(*) OVER ()");
	}
	
	@Test
	public void mathFuncTest() {
		Expr a = Expr.from("round(rand(unix_timestamp())*100)%100");
		assertEquals(a.toString(), "(round((rand(unix_timestamp()) * 100)) % 100)");
		
		a = Expr.from("ndv(user_id)");
		assertEquals(a.toString(), "ndv(user_id)");
	}
	
	@Test
	public void castExprTest() {
		Expr a = Expr.from("abs(fnv_hash(cast(user_id as string)))%1000000");
		System.out.println(a.toString());
	}

}

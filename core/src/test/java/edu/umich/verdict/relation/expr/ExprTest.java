package edu.umich.verdict.relation.expr;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;

public class ExprTest {

    private static VerdictContext dummyContext = null;
    
    @Test
    public void redshiftModStr() {
    	Expr a = Expr.from(dummyContext, "mod(strtol(crc32(cast('hello world' as text)),16),30)");
    	System.out.println(a.toSql());
    }
    
    @Test
    public void redshiftExtractStr() {
    	Expr a = Expr.from(dummyContext, "extract (year from o_orderdate)");
    	System.out.println(a.toSql());
    }
    
    @Test
    public void redshiftRandPartition() {
    	Expr a = Expr.from(dummyContext, "mod(cast(round(random()) as integer), 100)");
    	System.out.println(a.toSql());
    }

    @Test
    public void caseExprTest() {
        Expr a = Expr.from(dummyContext, "CASE WHEN a < 5 THEN 1 WHEN a < 10 THEN 2 ELSE 3 END");
        assertEquals(a.toString(), "(CASE WHEN `a` < 5 THEN 1 WHEN `a` < 10 THEN 2 ELSE 3 END)");
    }

    @Test
    public void partitonExprTest() {
        Expr a = Expr.from(dummyContext, "count(*) over (partition by order_dow)");
        assertEquals(a.toString(), "count(*) OVER (partition by `order_dow`)");

        Expr b = Expr.from(dummyContext, "count(*) over ()");
        assertEquals(b.toString(), "count(*) OVER ()");

        Expr c = Expr.from(dummyContext, "sum(prices * (1 - discount)) over (partition by ship_method)");
        assertEquals(c.toString(), "sum((`prices` * (1 - `discount`))) OVER (partition by `ship_method`)");

        Expr d = Expr.from(dummyContext, "count(*) over (partition by order_dow, __vpart)");
        assertEquals(d.toString(), "count(*) OVER (partition by `order_dow`, `__vpart`)");
    }

    @Test
    public void mathFuncTest() {
        Expr a = Expr.from(dummyContext, "round(rand(unix_timestamp())*100)%100");
        assertEquals(a.toString(), "(round((rand(unix_timestamp()) * 100)) % 100)");

        a = Expr.from(dummyContext, "ndv(user_id)");
        assertEquals(a.toString(), "ndv(`user_id`)");
    }

    @Test
    public void castExprTest() {
        Expr a = Expr.from(dummyContext, "abs(fnv_hash(cast(user_id as string)))%1000000");
        // changed `user_id` to 'user_id'
        assertEquals(a.toString(), "(abs(fnv_hash(cast('user_id' as string))) % 1000000)");
    }

    @Test
    public void hiveHashTest() {
        Expr a = Expr.from(dummyContext, "pmod(conv(substr(md5(cast(user_id AS string)),17,16),16,10), 100)");
        assertEquals(a.toString(), "pmod(conv(substr(md5(cast(`user_id` as string)), 17, 16), 16, 10), 100)");

        Expr b = Expr.from(dummyContext, "pmod(crc32(user_id), 100)");
        assertEquals(b.toString(), "pmod(crc32(`user_id`), 100)");
    }

}

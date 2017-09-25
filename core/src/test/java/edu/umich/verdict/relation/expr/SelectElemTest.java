package edu.umich.verdict.relation.expr;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.umich.verdict.VerdictContext;

public class SelectElemTest {

    VerdictContext dummyContext = null;

    @Test
    public void starTest() {
        SelectElem.from(dummyContext, "*");
    }

    @Test
    public void aliasTest() {
        SelectElem e = SelectElem.from(dummyContext, "count(*) AS __group_size");
        assertEquals("count(*) AS `__group_size`", e.toString());
//        System.out.println(e.toString());
    }

    @Test
    public void randExprTest() {
        SelectElem e = SelectElem.from(dummyContext, "mod(rand(unix_timestamp()), 100) * 100 AS __vpart");
        assertEquals("(mod(rand(unix_timestamp()), 100) * 100) AS `__vpart`", e.toString());
//        System.out.println(e.toString());

        e = SelectElem.from(dummyContext, "(rand(unix_timestamp()) * 100) % 100 AS __vpart");
        assertEquals("((rand(unix_timestamp()) * 100) % 100) AS `__vpart`", e.toString());
//        System.out.println(e.toString());
    }

}

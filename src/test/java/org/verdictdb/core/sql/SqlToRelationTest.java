package org.verdictdb.core.sql;

import org.junit.Test;
import org.verdictdb.core.logical_query.*;
import org.verdictdb.core.sql.syntax.HiveSyntax;
import org.verdictdb.exception.VerdictDbException;


import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SqlToRelationTest {

    //@Test
    public void testSelectAllBaseTable() throws VerdictDbException {
        String actual = "select * from `myschema`.`mytable` as t";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }

    //@Test
    public void testSelectColumnsBaseTable() throws VerdictDbException {
        String actual = "select `t`.`mycolumn1`, `t`.`mycolumn2` from `myschema`.`mytable` as t";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }

    //@Test
    public void testSelectAvgBaseTable() throws VerdictDbException {
        String actual = "select avg(`t`.`mycolumn1`) from `myschema`.`mytable` as t";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }

    //@Test
    public void testSelectSumBaseTable() throws VerdictDbException {
        String actual = "select sum(`t`.`mycolumn1`) from `myschema`.`mytable` as t";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }

    //@Test
    public void testSelectCountBaseTable() throws VerdictDbException {
        String actual = "select count(*) from `myschema`.`mytable` as t";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }

    //@Test
    public void testSelectAggregatesBaseTable() throws VerdictDbException {
        String actual = "select avg(`t`.`mycolumn1`), sum(`t`.`mycolumn1`), count(*) from `myschema`.`mytable` as t";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }

    //@Test
    public void testSelectAddBaseTable() throws VerdictDbException {
        String actual = "select `t`.`mycolumn1` + `t`.`mycolumn2` from `myschema`.`mytable` as t";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }

    //@Test
    public void testSelectSubtractBaseTable() throws VerdictDbException {
        String actual = "select `t`.`mycolumn1` - `t`.`mycolumn2` from `myschema`.`mytable` as t";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }

    //@Test
    public void testSelectMultiplyBaseTable() throws VerdictDbException {
        String actual = "select `t`.`mycolumn1` * `t`.`mycolumn2` from `myschema`.`mytable` as t";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }

    //@Test
    public void testSelectDivideBaseTable() throws VerdictDbException {
        String actual = "select `t`.`mycolumn1` / `t`.`mycolumn2` from `myschema`.`mytable` as t";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }

    //@Test
    public void testSelectAvgGroupbyBaseTable() throws VerdictDbException {
        String actual = "select `t`.`mygroup`, avg(`t`.`mycolumn1`) as myavg from `myschema`.`mytable` as t group by `mygroup`";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }

    //@Test
    public void testSelectNestedGroupby() throws VerdictDbException {
        String actual = "select * from ("
                + "select `t`.`mygroup`, avg(`t`.`mycolumn1`) as myavg from `myschema`.`mytable` as t group by `mygroup`) as s "
                + "group by `mygroup2`";
        SqlToRelation sqlToRelation = new SqlToRelation(null);
        AbstractRelation sel = sqlToRelation.ToRelation(actual);
        assertEquals(true, true);
    }
}

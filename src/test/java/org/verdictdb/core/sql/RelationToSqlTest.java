package org.verdictdb.core.sql;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;
import org.verdictdb.core.logical_query.BaseColumn;
import org.verdictdb.core.logical_query.BaseTable;
import org.verdictdb.core.logical_query.ColumnOp;
import org.verdictdb.core.logical_query.RelationalOp;
import org.verdictdb.core.sql.syntax.HiveSyntax;
import org.verdictdb.exception.VerdictDbException;

public class RelationToSqlTest {

    @Test
    public void testSelectAllBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        Object parameters = Arrays.asList(new ColumnOp("*", null));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select * from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testSelectColumnsBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        Object parameters = Arrays.asList(new BaseColumn("t", "mycolumn1"), new BaseColumn("t", "mycolumn2"));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select `t`.`mycolumn1`, `t`.`mycolumn2` from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testSelectAvgBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        Object parameters = Arrays.asList(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select avg(`t`.`mycolumn1`) from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testSelectSumBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        Object parameters = Arrays.asList(new ColumnOp("sum", new BaseColumn("t", "mycolumn1")));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select sum(`t`.`mycolumn1`) from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testSelectCountBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        Object parameters = Arrays.asList(new ColumnOp("count", new BaseColumn("t", "mycolumn1")));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select count(`t`.`mycolumn1`) from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testSelectCountStarBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        Object parameters = Arrays.asList(new ColumnOp("count", new ColumnOp("*", null)));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select count(*) from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }

}

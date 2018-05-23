package org.verdictdb.core.sql;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.verdictdb.core.logical_query.AbstractColumn;
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
        Object parameters = Arrays.asList(new ColumnOp("*"));
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
        Object parameters = Arrays.asList(new ColumnOp("count", new ColumnOp("*")));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select count(*) from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }

    @Test
    public void testSelectAggregatesBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        Object parameters = Arrays.asList(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")),
                                          new ColumnOp("sum", new BaseColumn("t", "mycolumn1")),
                                          new ColumnOp("count", new ColumnOp("*")));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select avg(`t`.`mycolumn1`), sum(`t`.`mycolumn1`), count(*) from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testSelectAddBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        List<AbstractColumn> operands = Arrays.<AbstractColumn>asList(
                                                new BaseColumn("t", "mycolumn1"),
                                                new BaseColumn("t", "mycolumn2"));
        Object parameters = Arrays.asList(new ColumnOp("add", operands));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select (`t`.`mycolumn1` + `t`.`mycolumn2`) from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testSelectSubtractBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        List<AbstractColumn> operands = Arrays.<AbstractColumn>asList(
                                                new BaseColumn("t", "mycolumn1"),
                                                new BaseColumn("t", "mycolumn2"));
        Object parameters = Arrays.asList(new ColumnOp("subtract", operands));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select (`t`.`mycolumn1` - `t`.`mycolumn2`) from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }

    @Test
    public void testSelectMultiplyBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        List<AbstractColumn> operands = Arrays.<AbstractColumn>asList(
                                                new BaseColumn("t", "mycolumn1"),
                                                new BaseColumn("t", "mycolumn2"));
        Object parameters = Arrays.asList(new ColumnOp("multiply", operands));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select (`t`.`mycolumn1` * `t`.`mycolumn2`) from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testSelectDivideBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        List<AbstractColumn> operands = Arrays.<AbstractColumn>asList(
                                                new BaseColumn("t", "mycolumn1"),
                                                new BaseColumn("t", "mycolumn2"));
        Object parameters = Arrays.asList(new ColumnOp("divide", operands));
        RelationalOp relation = new RelationalOp(base, "select", parameters);
        String expected = "select (`t`.`mycolumn1` / `t`.`mycolumn2`) from `myschema`.`mytable`";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(relation);
        assertEquals(expected, actual);
    }
    
}

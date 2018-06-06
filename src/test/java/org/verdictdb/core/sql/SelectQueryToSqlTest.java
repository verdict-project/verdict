package org.verdictdb.core.sql;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.verdictdb.core.query.AliasReference;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.core.query.UnnamedColumn;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.HiveSyntax;

public class SelectQueryToSqlTest {

  @Test
  public void testSelectAllBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    String expected = "select * from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectAllFilterBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    relation.addFilterByAnd(ColumnOp.less(new BaseColumn("t", "mycolumn"), ColumnOp.rand()));
    String expected = "select * from `myschema`.`mytable` as t where `t`.`mycolumn` < rand()";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectColumnsBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new BaseColumn("t", "mycolumn1"),
            new BaseColumn("t", "mycolumn2")),
        base);
    String expected = "select `t`.`mycolumn1`, `t`.`mycolumn2` from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectAvgBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new ColumnOp("avg", new BaseColumn("t", "mycolumn1"))),
        base);
    String expected = "select avg(`t`.`mycolumn1`) from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectSumBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new ColumnOp("sum", new BaseColumn("t", "mycolumn1"))),
        base);
    String expected = "select sum(`t`.`mycolumn1`) from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectCountBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new ColumnOp("count", new BaseColumn("t", "mycolumn1"))),
        base);
    String expected = "select count(*) from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectCountStarBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new ColumnOp("count", new AsteriskColumn())),
        base);
    String expected = "select count(*) from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectAggregatesBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new ColumnOp("avg", new BaseColumn("t", "mycolumn1")),
            new ColumnOp("sum", new BaseColumn("t", "mycolumn1")),
            new ColumnOp("count", new AsteriskColumn())),
        base);
    String expected = "select avg(`t`.`mycolumn1`), sum(`t`.`mycolumn1`), count(*) from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectAddBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    List<UnnamedColumn> operands = Arrays.<UnnamedColumn>asList(
        new BaseColumn("t", "mycolumn1"),
        new BaseColumn("t", "mycolumn2"));
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new ColumnOp("add", operands)),
        base);
    String expected = "select `t`.`mycolumn1` + `t`.`mycolumn2` from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectSubtractBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    List<UnnamedColumn> operands = Arrays.<UnnamedColumn>asList(
        new BaseColumn("t", "mycolumn1"),
        new BaseColumn("t", "mycolumn2"));
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new ColumnOp("subtract", operands)),
        base);
    String expected = "select `t`.`mycolumn1` - `t`.`mycolumn2` from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectMultiplyBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    List<UnnamedColumn> operands = Arrays.<UnnamedColumn>asList(
        new BaseColumn("t", "mycolumn1"),
        new BaseColumn("t", "mycolumn2"));
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new ColumnOp("multiply", operands)),
        base);
    String expected = "select `t`.`mycolumn1` * `t`.`mycolumn2` from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectDivideBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    List<UnnamedColumn> operands = Arrays.<UnnamedColumn>asList(
        new BaseColumn("t", "mycolumn1"),
        new BaseColumn("t", "mycolumn2"));
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new ColumnOp("divide", operands)),
        base);
    String expected = "select `t`.`mycolumn1` / `t`.`mycolumn2` from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectAvgGroupbyBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new BaseColumn("t", "mygroup"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), "myavg")),
        base);
    relation.addGroupby(new AliasReference("mygroup"));
    String expected = "select `t`.`mygroup`, avg(`t`.`mycolumn1`) as myavg from `myschema`.`mytable` as t group by `mygroup`";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectNestedGroupby() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp innerRelation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new BaseColumn("t", "mygroup"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), "myavg")),
        base);
    innerRelation.addGroupby(new AliasReference("mygroup"));
    innerRelation.setAliasName("s");
    SelectQueryOp outerRelation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        innerRelation);
    outerRelation.addGroupby(new AliasReference("mygroup2"));

    String expected = "select * from ("
        + "select `t`.`mygroup`, avg(`t`.`mycolumn1`) as myavg from `myschema`.`mytable` as t group by `mygroup`) as s "
        + "group by `mygroup2`";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(outerRelation);
    assertEquals(expected, actual);
  }
}

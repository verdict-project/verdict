package org.verdictdb.sqlwriter;

import org.junit.Test;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.HiveSyntax;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SelectQueryToSqlTest {

  @Test
  public void testSelectAllBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    String expected = "select * from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectAllFilterBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    relation.addFilterByAnd(ColumnOp.less(new BaseColumn("t", "mycolumn"), ColumnOp.rand()));
    String expected = "select * from `myschema`.`mytable` as t where t.`mycolumn` < rand()";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectColumnsBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("t", "mycolumn1"),
            new BaseColumn("t", "mycolumn2")),
        base);
    String expected = "select t.`mycolumn1`, t.`mycolumn2` from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectAvgBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new ColumnOp("avg", new BaseColumn("t", "mycolumn1"))),
        base);
    String expected = "select avg(t.`mycolumn1`) from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectSumBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new ColumnOp("sum", new BaseColumn("t", "mycolumn1"))),
        base);
    String expected = "select sum(t.`mycolumn1`) from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectCountBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new ColumnOp("count", new BaseColumn("t", "mycolumn1"))),
        base);
    String expected = "select count(t.`mycolumn1`) from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectCountStarBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new ColumnOp("count", new AsteriskColumn())),
        base);
    String expected = "select count(*) from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectAggregatesBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new ColumnOp("avg", new BaseColumn("t", "mycolumn1")),
            new ColumnOp("sum", new BaseColumn("t", "mycolumn1")),
            new ColumnOp("count", new AsteriskColumn())),
        base);
    String expected = "select avg(t.`mycolumn1`), sum(t.`mycolumn1`), count(*) from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectAddBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    List<UnnamedColumn> operands = Arrays.<UnnamedColumn>asList(
        new BaseColumn("t", "mycolumn1"),
        new BaseColumn("t", "mycolumn2"));
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new ColumnOp("add", operands)),
        base);
    String expected = "select t.`mycolumn1` + t.`mycolumn2` from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectSubtractBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    List<UnnamedColumn> operands = Arrays.<UnnamedColumn>asList(
        new BaseColumn("t", "mycolumn1"),
        new BaseColumn("t", "mycolumn2"));
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new ColumnOp("subtract", operands)),
        base);
    String expected = "select t.`mycolumn1` - t.`mycolumn2` from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectMultiplyBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    List<UnnamedColumn> operands = Arrays.<UnnamedColumn>asList(
        new BaseColumn("t", "mycolumn1"),
        new BaseColumn("t", "mycolumn2"));
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new ColumnOp("multiply", operands)),
        base);
    String expected = "select t.`mycolumn1` * t.`mycolumn2` from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectDivideBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    List<UnnamedColumn> operands = Arrays.<UnnamedColumn>asList(
        new BaseColumn("t", "mycolumn1"),
        new BaseColumn("t", "mycolumn2"));
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new ColumnOp("divide", operands)),
        base);
    String expected = "select t.`mycolumn1` / t.`mycolumn2` from `myschema`.`mytable` as t";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectAvgGroupbyBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("t", "mygroup"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), "myavg")),
        base);
    relation.addGroupby(new AliasReference("mygroup"));
    String expected = "select t.`mygroup`, avg(t.`mycolumn1`) as `myavg` from `myschema`.`mytable` as t group by `mygroup`";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(relation);
    assertEquals(expected, actual);
  }

  @Test
  public void testSelectNestedGroupby() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery innerRelation = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("t", "mygroup"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), "myavg")),
        base);
    innerRelation.addGroupby(new AliasReference("mygroup"));
    innerRelation.setAliasName("s");
    SelectQuery outerRelation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        innerRelation);
    outerRelation.addGroupby(new AliasReference("mygroup2"));

    String expected = "select * from ("
        + "select t.`mygroup`, avg(t.`mycolumn1`) as `myavg` from `myschema`.`mytable` as t group by `mygroup`) as s "
        + "group by `mygroup2`";
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(outerRelation);
    assertEquals(expected, actual);
  }
  
  @Test
  public void testEmptyFrom() throws VerdictDBException {
    SelectQuery query = SelectQuery.create(ConstantColumn.valueOf("abc"));
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(query);
    
    String expected = "select abc";
    assertEquals(expected, actual);
  }
  
}

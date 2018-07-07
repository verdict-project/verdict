package org.verdictdb.sqlreader;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;

public class SqlToRelationTest {

  @Test
  public void testSelectAllBaseTable() throws VerdictDBException {
    String actual = "select * from myschema.mytable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new AsteriskColumn()
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    assertEquals(expected, sel);
  }
  
  @Test
  public void testQuotedQuery() throws VerdictDBException {
    String actual = "select \"t\".* from \"myschema\".\"mytable\" as \"t\"";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new AsteriskColumn("t")
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    assertEquals(expected, sel);
  }
  
  @Test
  public void testQuotedQuery2() throws VerdictDBException {
    String actual = "select \"t\".*, \"a.b\".\"AbC\" "
        + "from \"myschema\".\"mytable\" as \"t\", \"Mytable\" \"s.S\"";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AsteriskColumn("t"),
            new BaseColumn("a.b", "AbC")), 
        Arrays.<AbstractRelation>asList(
            new BaseTable("myschema", "mytable", "t"),
            BaseTable.getBaseTableWithoutSchema("Mytable", "s.S")));
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    assertEquals(expected, sel);
  }

  @Test
  public void testSelectColumnsBaseTable() throws VerdictDBException {
    String actual = "select t.mycolumn1, t.mycolumn2 from myschema.mytable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new BaseColumn("t", "mycolumn1"),
        new BaseColumn("t", "mycolumn2")
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    assertEquals(expected, sel);
  }

  @Test
  public void testSelectAvgBaseTable() throws VerdictDBException {
    String actual = "select avg(t.mycolumn1) from myschema.mytable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new ColumnOp("avg", new BaseColumn("t", "mycolumn1"))
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    assertEquals(expected, sel);
  }

  @Test
  public void testSelectSumBaseTable() throws VerdictDBException {
    String actual = "select sum(t.mycolumn1) from myschema.mytable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new ColumnOp("sum", new BaseColumn("t", "mycolumn1"))
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    assertEquals(expected, sel);
  }

  @Test
  public void testSelectCountBaseTable() throws VerdictDBException {
    String actual = "select count(*) from myschema.mytable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new ColumnOp("count", new AsteriskColumn())
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    assertEquals(expected, sel);
  }

  @Test
  public void testSelectAggregatesBaseTable() throws VerdictDBException {
    String actual = "select avg(t.mycolumn1), sum(t.mycolumn1), count(*) from myschema.mytable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new ColumnOp("avg", new BaseColumn("t", "mycolumn1")),
        new ColumnOp("sum", new BaseColumn("t", "mycolumn1")),
        new ColumnOp("count", new AsteriskColumn())
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    assertEquals(expected, sel);
  }

  @Test
  public void testSelectAddBaseTable() throws VerdictDBException {
    String actual = "select t.mycolumn1 + t.mycolumn2 from myschema.mytable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new BaseColumn("t", "mycolumn1"),
            new BaseColumn("t", "mycolumn2")
        ))
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    assertEquals(expected, sel);
  }

  @Test
  public void testSelectSubtractBaseTable() throws VerdictDBException {
    String actual = "select t.mycolumn1 - t.mycolumn2 from myschema.mytable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
            new BaseColumn("t", "mycolumn1"),
            new BaseColumn("t", "mycolumn2")
        ))
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    assertEquals(expected, sel);
  }

  @Test
  public void testSelectMultiplyBaseTable() throws VerdictDBException {
    String actual = "select t.mycolumn1 * t.mycolumn2 from myschema.mytable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
            new BaseColumn("t", "mycolumn1"),
            new BaseColumn("t", "mycolumn2")
        ))
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    assertEquals(expected, sel);
  }

  @Test
  public void testSelectDivideBaseTable() throws VerdictDBException {
    String actual = "select t.mycolumn1 / t.mycolumn2 from myschema.mytable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new ColumnOp("divide", Arrays.<UnnamedColumn>asList(
            new BaseColumn("t", "mycolumn1"),
            new BaseColumn("t", "mycolumn2")
        ))
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    assertEquals(expected, sel);
  }

  @Test
  public void testSelectAvgGroupbyBaseTable() throws VerdictDBException {
    String actual = "select t.mygroup, avg(t.mycolumn1) as myavg from myschema.mytable as t group by mygroup";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new BaseColumn("t", "mygroup"),
        new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), "myavg")
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    expected.addGroupby(new AliasReference("mygroup"));
    assertEquals(expected, sel);
  }

  @Test
  public void testSelectNestedGroupby() throws VerdictDBException {
    String actual = "select * from ("
        + "select t.mygroup, avg(t.mycolumn1) as myavg from myschema.mytable as t group by mygroup) as s "
        + "group by mygroup2";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    SelectQuery subquery = SelectQuery.create(Arrays.<SelectItem>asList(
        new BaseColumn("t", "mygroup"),
        new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), "myavg")
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    subquery.addGroupby(new AliasReference("mygroup"));
    subquery.setAliasName("s");
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(new AsteriskColumn()),
        Arrays.<AbstractRelation>asList(subquery));
    expected.addGroupby(new AliasReference("mygroup2"));
    assertEquals(expected, sel);
  }
}

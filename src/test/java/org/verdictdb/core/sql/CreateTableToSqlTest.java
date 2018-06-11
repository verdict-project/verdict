package org.verdictdb.core.sql;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.CreateTableAsSelect;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.HiveSyntax;

public class CreateTableToSqlTest {

  @Test
  public void selectAllTest() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelect create = new CreateTableAsSelect("newschema", "newtable", relation);
    String expected = "create table `newschema`.`newtable` as select * from `myschema`.`mytable` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new HiveSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);
  }

  @Test
  public void selectAllWithPartition1Test() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelect create = new CreateTableAsSelect("newschema", "newtable", relation);
    create.addPartitionColumn("part1");
    String expected = "create table `newschema`.`newtable` partitioned by (`part1`) as select * from `myschema`.`mytable` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new HiveSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);
  }

  @Test
  public void selectAllWithPartition2Test() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelect create = new CreateTableAsSelect("newschema", "newtable", relation);
    create.addPartitionColumn("part1");
    create.addPartitionColumn("part2");
    String expected = "create table `newschema`.`newtable` partitioned by (`part1`, `part2`) as select * from `myschema`.`mytable` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new HiveSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);
  }

}
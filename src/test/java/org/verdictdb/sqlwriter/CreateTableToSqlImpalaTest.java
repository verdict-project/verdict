package org.verdictdb.sqlwriter;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.ImpalaSyntax;


public class CreateTableToSqlImpalaTest {

  @Test
  public void createTableSelectAllWithSignlePartitionTest() throws VerdictDBException {
    BaseTable base = new BaseTable("tpch", "nation", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelectQuery create = new CreateTableAsSelectQuery("tpch", "newtable", relation);
    create.addPartitionColumn("n_nationkey");
    String expected = "create table `tpch`.`newtable` partitioned by (`n_nationkey`) as select * from `tpch`.`nation` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new ImpalaSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);
  }

  @Test
  public void createTableSelectAllWithMultiPartitionTest() throws VerdictDBException {
    BaseTable base = new BaseTable("tpch", "nation", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelectQuery create = new CreateTableAsSelectQuery("tpch", "newtable", relation);
    create.addPartitionColumn("n_nationkey");
    create.addPartitionColumn("n_name");
    String expected = "create table `tpch`.`newtable` partitioned by (`n_nationkey`, `n_name`) as select * from `tpch`.`nation` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new ImpalaSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);
  }
}

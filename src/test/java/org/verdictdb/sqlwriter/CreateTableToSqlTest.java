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
import org.verdictdb.sqlsyntax.HiveSyntax;

public class CreateTableToSqlTest {
  
  
  @Test
  public void createTableSelectAllTest() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelectQuery create = new CreateTableAsSelectQuery("newschema", "newtable", relation);
    String expected = "create table `newschema`.`newtable` using parquet as select * from `myschema`.`mytable` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new HiveSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);
  }

  @Test
  public void createTableSelectAllWithSignlePartitionHiveTest() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelectQuery create = new CreateTableAsSelectQuery("newschema", "newtable", relation);
    create.addPartitionColumn("part1");
    String expected = "create table `newschema`.`newtable` partitioned by (`part1`) as select * from `myschema`.`mytable` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new HiveSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);
  }

  @Test
  public void createTableSelectAllWithMultiPartitionHiveTest() throws VerdictDBException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelectQuery create = new CreateTableAsSelectQuery("newschema", "newtable", relation);
    create.addPartitionColumn("part1");
    create.addPartitionColumn("part2");
    String expected = "create table `newschema`.`newtable` partitioned by (`part1`, `part2`) as select * from `myschema`.`mytable` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new HiveSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);
  }
  
//  @Test
//  public void createPartitionedTableOnMySqlTest() throws VerdictDBException {
//    BaseTable base = new BaseTable("myschema", "mytable", "t");
//    SelectQuery relation = SelectQuery.create(
//        Arrays.<SelectItem>asList(new AsteriskColumn()),
//        base);
//    CreateTableAsSelectQuery create = new CreateTableAsSelectQuery("newschema", "newtable", relation);
//    create.addPartitionColumn("part1");
//    
//    String sql = QueryToSql.convert(new PostgresqlSyntax(), create);
//    DbmsConnection con = new JdbcConnection(conn, new PostgresqlSyntax());
//    con.execute(sql);
//  }

}

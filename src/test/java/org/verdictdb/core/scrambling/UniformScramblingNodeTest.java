package org.verdictdb.core.scrambling;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.QueryToSql;

public class UniformScramblingNodeTest {

  private static Connection mysqlConn;

  private static Statement mysqlStmt;

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_UESR;

  private static final String MYSQL_PASSWORD = "";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
      MYSQL_UESR = "root";
    } else {
      MYSQL_HOST = "localhost";
      MYSQL_UESR = "root";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    mysqlConn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);

    mysqlConn.createStatement().execute("create schema if not exists oldschema");
    mysqlConn.createStatement().execute("create schema if not exists newschema");
    mysqlConn.createStatement().execute("drop table if exists oldschema.oldtable");
    mysqlConn.createStatement().execute("create table if not exists oldschema.oldtable (id smallint)");
    for (int i = 0; i < 6; i++) {
      mysqlConn.createStatement().execute(String.format("insert into oldschema.oldtable values (%d)", i));
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    mysqlConn.createStatement().execute("drop table if exists oldschema.oldtable");
    mysqlConn.createStatement().execute("drop schema if exists oldschema");
    mysqlConn.createStatement().execute("drop table if exists newschema.newtable");
    mysqlConn.createStatement().execute("drop schema if exists newschema");
  }

  @Test
  public void testScramblingNodeCreation() throws VerdictDBException, SQLException {
    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    int blockSize = 2;    // 3 blocks will be created
    ScramblingMethod method = new UniformScramblingMethod(blockSize);
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "tiercolumn");
    options.put("blockColumnName", "blockcolumn");
//    options.put("blockCount", "3");

    // query result
    String sql = "select count(*) as `verdictdbtotalcount` from `oldschema`.`oldtable` as t";
    DbmsConnection conn = new JdbcConnection(mysqlConn);
    DbmsQueryResult queryResult = conn.execute(sql);

    ScramblingNode node = ScramblingNode.create(
        newSchemaName, newTableName,
        oldSchemaName, oldTableName,
        method, options);

    // set tokens
    List<ExecutionInfoToken> tokens = new ArrayList<>();
    ExecutionInfoToken e = new ExecutionInfoToken();
    e.setKeyValue(TableSizeCountNode.class.getSimpleName(), queryResult);
    tokens.add(e);

    e = new ExecutionInfoToken();
    e.setKeyValue("schemaName", newSchemaName);
    e.setKeyValue("tableName", newTableName);
    tokens.add(e);

    e = new ExecutionInfoToken();
    List<Pair<String, String>> columnNamesAndTypes = new ArrayList<>();
    columnNamesAndTypes.add(Pair.of("id", "smallint"));
    e.setKeyValue(ScramblingPlan.COLUMN_METADATA_KEY, columnNamesAndTypes);
    tokens.add(e);

    SqlConvertible query = node.createQuery(tokens);
    sql = QueryToSql.convert(new MysqlSyntax(), query);
    String expected = "create table `newschema`.`newtable` "
        + "partition by key (`blockcolumn`) "
        + "select t.`id`, 0 as `tiercolumn`, "
        + "case when (rand() <= 0.3333333333333333) then 0 "
        + "when (rand() <= 0.49999999999999994) then 1 "
        + "when (rand() <= 1.0) then 2 else 2 end as `blockcolumn` "
        + "from `oldschema`.`oldtable` as t";
    assertEquals(expected, sql);
    mysqlConn.createStatement().execute("drop table if exists newschema.newtable");
    mysqlConn.createStatement().execute(sql);
  }

}

package org.verdictdb.core.scrambling;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.exception.VerdictDBException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class FastConvergeScramblingPlanTest {

  private static Connection mysqlConn;

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
    mysqlConn.createStatement().execute("create table if not exists oldschema.oldtable (orderdate smallint, id smallint)");
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    mysqlConn.createStatement().execute("drop table if exists oldschema.oldtable");
    mysqlConn.createStatement().execute("drop schema if exists oldschema");
    mysqlConn.createStatement().execute("drop table if exists newschema.newtable");
    mysqlConn.createStatement().execute("drop schema if exists newschema");
  }

  @Test
  public void testFastConvergeScramblingPlanEmptyTable() throws VerdictDBException, SQLException {
    mysqlConn.createStatement().execute("delete from oldschema.oldtable");

    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    String scratchpadSchemaName = "oldschema";
    String primaryColumn = "orderdate";
    int blockSize = 2;
    ScramblingMethod method = new FastConvergeScramblingMethod(blockSize, scratchpadSchemaName, primaryColumn);
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "tiercolumn");
    options.put("blockColumnName", "blockcolumn");
    options.put("blockCount", "3");

    mysqlConn.createStatement().execute(
        String.format("drop table if exists %s.%s", newSchemaName, newTableName));
    ScramblingPlan plan = ScramblingPlan.create(
        newSchemaName, newTableName,
        oldSchemaName, oldTableName,
        method, options);
//    System.out.println(plan.getReportingNode());

    DbmsConnection conn = JdbcConnection.create(mysqlConn);
    ExecutablePlanRunner.runTillEnd(conn, plan);
  }

  @Test
  public void testFastConvergeScramblingPlanEmptyTableNoPrimaryGroup() throws VerdictDBException, SQLException {
    mysqlConn.createStatement().execute("delete from oldschema.oldtable");

    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    String scratchpadSchemaName = "oldschema";
//    String primaryColumn = "orderdate";
    int blockSize = 2;
    ScramblingMethod method = new FastConvergeScramblingMethod(blockSize, scratchpadSchemaName);
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "tiercolumn");
    options.put("blockColumnName", "blockcolumn");
    options.put("blockCount", "3");

    mysqlConn.createStatement().execute(
        String.format("drop table if exists %s.%s", newSchemaName, newTableName));
    ScramblingPlan plan = ScramblingPlan.create(
        newSchemaName, newTableName,
        oldSchemaName, oldTableName,
        method, options);
//    System.out.println(plan.getReportingNode());

    DbmsConnection conn = JdbcConnection.create(mysqlConn);
    ExecutablePlanRunner.runTillEnd(conn, plan);
  }

  @Test
  public void testFastConvergeScramblingPlanNonEmptyTable() throws VerdictDBException, SQLException {
    mysqlConn.createStatement().execute("delete from oldschema.oldtable");
    for (int i = 0; i < 10; i++) {
      mysqlConn.createStatement().execute(
          String.format("insert into oldschema.oldtable values (%d, %d)", i%2, i));
    }

    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    String scratchpadSchemaName = "oldschema";
    String primaryColumn = "orderdate";
    int blockSize = 2;
    ScramblingMethod method = new FastConvergeScramblingMethod(blockSize, scratchpadSchemaName, primaryColumn);
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "tiercolumn");
    options.put("blockColumnName", "blockcolumn");
    options.put("blockCount", "3");

    mysqlConn.createStatement().execute(
        String.format("drop table if exists %s.%s", newSchemaName, newTableName));
    ScramblingPlan plan = ScramblingPlan.create(
        newSchemaName, newTableName,
        oldSchemaName, oldTableName,
        method, options);
//    System.out.println(plan.getReportingNode());

    DbmsConnection conn = JdbcConnection.create(mysqlConn);
    ExecutablePlanRunner.runTillEnd(conn, plan);

    DbmsQueryResult result = conn.execute(String.format("select * from %s.%s", newSchemaName, newTableName));
    result.printContent();
  }

  @Test
  public void testFastConvergeScramblingPlanNonEmptyTableNoPrimaryGroup() throws VerdictDBException, SQLException {
    mysqlConn.createStatement().execute("delete from oldschema.oldtable");
    for (int i = 0; i < 10; i++) {
      mysqlConn.createStatement().execute(
          String.format("insert into oldschema.oldtable values (%d, %d)", i%2, i));
    }

    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    String scratchpadSchemaName = "oldschema";
//    String primaryColumn = "orderdate";
    int blockSize = 2;
    ScramblingMethod method = new FastConvergeScramblingMethod(blockSize, scratchpadSchemaName);
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "tiercolumn");
    options.put("blockColumnName", "blockcolumn");

    mysqlConn.createStatement().execute(
        String.format("drop table if exists %s.%s", newSchemaName, newTableName));
    ScramblingPlan plan = ScramblingPlan.create(
        newSchemaName, newTableName,
        oldSchemaName, oldTableName,
        method, options);
//    System.out.println(plan.getReportingNode());

    DbmsConnection conn = JdbcConnection.create(mysqlConn);
    ExecutablePlanRunner.runTillEnd(conn, plan);

    DbmsQueryResult result = conn.execute(String.format("select * from %s.%s", newSchemaName, newTableName));
    result.printContent();
  }
}

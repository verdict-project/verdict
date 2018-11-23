package org.verdictdb.core.scrambling;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.exception.VerdictDBException;

public class PostgresFastConvergeScramblingPlanTest {

  static Connection psqlConn;
  private static Statement stmt;
  private static final String POSTGRES_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      POSTGRES_HOST = "postgres";
    } else {
      POSTGRES_HOST = "localhost";
    }
  }

  private static final String POSTGRES_DATABASE = "test";

  private static final String POSTGRES_USER = "postgres";

  private static final String POSTGRES_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException {
    String postgresConnectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    psqlConn = DriverManager.getConnection(postgresConnectionString, POSTGRES_USER, POSTGRES_PASSWORD);

    psqlConn.createStatement().execute("create schema if not exists oldschema");
    psqlConn.createStatement().execute("create schema if not exists newschema");
    psqlConn.createStatement().execute("drop table if exists oldschema.oldtable");
    psqlConn.createStatement().execute("create table if not exists oldschema.oldtable (orderdate smallint, id smallint)");
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    psqlConn.createStatement().execute("drop table if exists oldschema.oldtable");
    psqlConn.createStatement().execute("drop schema if exists oldschema cascade");
    psqlConn.createStatement().execute("drop table if exists newschema.newtable");
    psqlConn.createStatement().execute("drop schema if exists newschema cascade");
  }

  @Test
  public void testFastConvergeScramblingPlanEmptyTable() throws VerdictDBException, SQLException {
    psqlConn.createStatement().execute("delete from oldschema.oldtable");

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

    psqlConn.createStatement().execute(
        String.format("drop table if exists %s.%s", newSchemaName, newTableName));
    ScramblingPlan plan = ScramblingPlan.create(
        newSchemaName, newTableName,
        oldSchemaName, oldTableName,
        method, null, options);
//    System.out.println(plan.getReportingNode());

    DbmsConnection conn = JdbcConnection.create(psqlConn);
    ExecutablePlanRunner.runTillEnd(conn, plan);
  }

  @Test
  public void testFastConvergeScramblingPlanEmptyTableNoPrimaryGroup() throws VerdictDBException, SQLException {
    psqlConn.createStatement().execute("delete from oldschema.oldtable");

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

    psqlConn.createStatement().execute(
        String.format("drop table if exists %s.%s", newSchemaName, newTableName));
    ScramblingPlan plan = ScramblingPlan.create(
        newSchemaName, newTableName,
        oldSchemaName, oldTableName,
        method, null, options);
//    System.out.println(plan.getReportingNode());

    DbmsConnection conn = JdbcConnection.create(psqlConn);
    ExecutablePlanRunner.runTillEnd(conn, plan);
  }

  @Test
  public void testFastConvergeScramblingPlanNonEmptyTable() throws VerdictDBException, SQLException {
    psqlConn.createStatement().execute("delete from oldschema.oldtable");
    for (int i = 0; i < 10; i++) {
      psqlConn.createStatement().execute(
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

    psqlConn.createStatement().execute(
        String.format("drop table if exists %s.%s", newSchemaName, newTableName));
    ScramblingPlan plan = ScramblingPlan.create(
        newSchemaName, newTableName,
        oldSchemaName, oldTableName,
        method, null, options);
//    System.out.println(plan.getReportingNode());

    DbmsConnection conn = JdbcConnection.create(psqlConn);
    ExecutablePlanRunner.runTillEnd(conn, plan);

    DbmsQueryResult result = conn.execute(String.format("select * from %s.%s", newSchemaName, newTableName));
    result.printContent();
  }

  @Test
  public void testFastConvergeScramblingPlanNonEmptyTableNoPrimaryGroup() throws VerdictDBException, SQLException {
    psqlConn.createStatement().execute("delete from oldschema.oldtable");
    for (int i = 0; i < 10; i++) {
      psqlConn.createStatement().execute(
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

    psqlConn.createStatement().execute(
        String.format("drop table if exists %s.%s", newSchemaName, newTableName));
    ScramblingPlan plan = ScramblingPlan.create(
        newSchemaName, newTableName,
        oldSchemaName, oldTableName,
        method, null, options);
//    System.out.println(plan.getReportingNode());

    DbmsConnection conn = JdbcConnection.create(psqlConn);
    ExecutablePlanRunner.runTillEnd(conn, plan);

    DbmsQueryResult result = conn.execute(String.format("select * from %s.%s", newSchemaName, newTableName));
    result.printContent();
  }
}

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
import org.verdictdb.connection.JdbcDbmsConnection;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.exception.VerdictDBException;

public class UniformScramblingPlanTest {

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
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    mysqlConn.createStatement().execute("drop table if exists oldschema.oldtable");
    mysqlConn.createStatement().execute("drop schema if exists oldschema");
    mysqlConn.createStatement().execute("drop table if exists newschema.newtable");
    mysqlConn.createStatement().execute("drop schema if exists newschema");
  }

  @Test
  public void testUniformScramblingPlanEmptyTable() throws VerdictDBException, SQLException {
    mysqlConn.createStatement().execute("delete from oldschema.oldtable");

    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    int blockSize = 3;
    ScramblingMethod method = new UniformScramblingMethod(blockSize);
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

    DbmsConnection conn = JdbcDbmsConnection.create(mysqlConn);
    ExecutablePlanRunner.runTillEnd(conn, plan);
  }

  @Test
  public void testUniformScramblingPlanNonEmptyTable() throws VerdictDBException, SQLException {
    for (int i = 0; i < 10; i++) {
      mysqlConn.createStatement().execute(String.format("insert into oldschema.oldtable values (%d)", i));
    }

    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    int blockSize = 2;
    ScramblingMethod method = new UniformScramblingMethod(blockSize);
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

    DbmsConnection conn = JdbcDbmsConnection.create(mysqlConn);
    ExecutablePlanRunner.runTillEnd(conn, plan);

    DbmsQueryResult result = conn.execute(String.format("select * from %s.%s", newSchemaName, newTableName));
    result.printContent();

    mysqlConn.createStatement().execute("delete from oldschema.oldtable");
  }

}

package org.verdictdb.core.scrambling;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.exception.VerdictDBDbmsException;
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
    if (env != null && env.equals("GitLab")) {
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
  public void testUniformScramblingPlan() throws VerdictDBException, SQLException {
    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    int blockCount = 10;
    ScramblingMethod method = new UniformScramblingMethod(blockCount);
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
    
    DbmsConnection conn = new JdbcConnection(mysqlConn);
    ExecutablePlanRunner.runTillEnd(conn, plan);
  }

}

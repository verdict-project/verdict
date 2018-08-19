package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;

public class JdbcMultipleQueriesInConnectionTest {

  static Connection conn;

  static DbmsConnection dbmsConn;

  private static Statement stmt;

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE =
      "multiple_queries_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  private static final String TABLE_NAME = "mytable";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
    dbmsConn = JdbcConnection.create(conn);
    dbmsConn.execute("create database if not exists " + MYSQL_DATABASE);
    dbmsConn.execute(String.format("create table if not exists %s.%s (myvalue int)", MYSQL_DATABASE, TABLE_NAME));
    for (int i = 0; i < 1; i++) {
      dbmsConn.execute(String.format("insert into %s.%s values (1)", MYSQL_DATABASE, TABLE_NAME));
    }
  }
  
  @AfterClass
  public static void tearDown() throws VerdictDBDbmsException {
    dbmsConn.execute("drop database if exists " + MYSQL_DATABASE);
  }
  
//  @Test
  public void test() throws SQLException {
    String url = String.format("jdbc:verdict:mysql://%s", MYSQL_HOST);
    Connection conn = DriverManager.getConnection(url, MYSQL_UESR, MYSQL_PASSWORD);
    
    // scrambling
    String scrambleName = "myscramble";
    String sql = String.format("create scramble %s.%s from %s.%s",
        MYSQL_DATABASE, scrambleName,
        MYSQL_DATABASE, TABLE_NAME);
    Statement stmt0 = conn.createStatement();
    stmt0.execute(sql);
    stmt0.close();
    
    // query
    sql = String.format("select count(*) from %s.%s", MYSQL_DATABASE, scrambleName);
    
    Statement stmt1 = conn.createStatement();
    ResultSet rs = stmt1.executeQuery(sql);
    rs.next();
    assertEquals(1, rs.getInt(1));
    rs.close();
    stmt1.close();
    
    Statement stmt2 = conn.createStatement();
    rs = stmt2.executeQuery(sql);
    rs.next();
    assertEquals(1, rs.getInt(1));
    rs.close();
    stmt2.close();
  }
  
}

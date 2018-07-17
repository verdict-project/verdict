package org.verdictdb.sqlwriter;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.sqlobject.InsertValuesQuery;
import org.verdictdb.exception.VerdictDBException;

public class InsertValuesQueryToSqlTest {
  
  private static Connection mysqlConn;

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "insert_test";

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
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    mysqlConn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
    mysqlConn.createStatement().execute(String.format("drop schema if exists %s", MYSQL_DATABASE));
    mysqlConn.createStatement().execute(String.format("create schema %s", MYSQL_DATABASE));
  }
  
  @AfterClass
  public static void tearDown() throws SQLException {
    mysqlConn.createStatement().execute(String.format("drop table if exists %s.mytable", MYSQL_DATABASE));
    mysqlConn.createStatement().execute(String.format("drop schema if exists %s", MYSQL_DATABASE));
  }

  @Test
  public void testSimpleInsert() throws VerdictDBException {
    DbmsConnection dbmsConn = JdbcConnection.create(mysqlConn);
    String createSql = String.format("create table %s.mytable (varcharCol VARCHAR(10), textCol TEXT)", MYSQL_DATABASE);
    dbmsConn.execute(String.format("drop table if exists %s.mytable", MYSQL_DATABASE));
    dbmsConn.execute(createSql);
    
    InsertValuesQuery insertQuery = new InsertValuesQuery();
    insertQuery.setSchemaName(MYSQL_DATABASE);
    insertQuery.setTableName("mytable");
    insertQuery.setValues(Arrays.<Object>asList("abc", "abcd"));
    String sql = QueryToSql.convert(dbmsConn.getSyntax(), insertQuery);
    dbmsConn.execute(sql);
    
    // tests
    DbmsQueryResult result = dbmsConn.execute(String.format("select * from %s.mytable", MYSQL_DATABASE));
    assertEquals(1, result.getRowCount());
  }

}

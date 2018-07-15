package org.verdictdb.sqlsyntax;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Test;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;

public class SqlSyntaxTest {


  @Test
  public void testH2SyntaxInference() throws SQLException, VerdictDBDbmsException {
    final String DB_CONNECTION = "jdbc:h2:mem:sqlsyntax;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    Connection conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    SqlSyntax expected = new H2Syntax();
    SqlSyntax actual = JdbcConnection.create(conn).getSyntax();
    assertEquals(expected, actual);
  }

  @Test
  public void testMySqlSyntaxInference() throws SQLException, VerdictDBDbmsException {
    String MYSQL_HOST = null;
    final String MYSQL_DATABASE = "test";
    final String MYSQL_UESR = "root";
    final String MYSQL_PASSWORD = "";
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }

    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    Connection conn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);

    SqlSyntax expected = new MysqlSyntax();
    SqlSyntax actual = JdbcConnection.create(conn).getSyntax();
    assertEquals(expected, actual);
  }

  @Test
  public void testPostgresSyntaxInference() throws SQLException, VerdictDBDbmsException {
    String POSTGRES_HOST = null;
    String POSTGRES_DATABASE = "test";
    String POSTGRES_USER = "postgres";
    String POSTGRES_PASSWORD = "";

    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      POSTGRES_HOST = "postgres";
    } else {
      POSTGRES_HOST = "localhost";
    }

    String postgresConnectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn = DriverManager.getConnection(postgresConnectionString, POSTGRES_USER, POSTGRES_PASSWORD);

    SqlSyntax expected = new PostgresqlSyntax();
    SqlSyntax actual = JdbcConnection.create(conn).getSyntax();
    assertEquals(expected, actual);
  }

  @Test
  public void testSqliteSyntaxInference() throws SQLException, VerdictDBDbmsException {
    final String DB_CONNECTION = "jdbc:sqlite:";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    Connection conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    SqlSyntax expected = new SqliteSyntax();
    SqlSyntax actual = JdbcConnection.create(conn).getSyntax();
    assertEquals(expected, actual);
  }

}

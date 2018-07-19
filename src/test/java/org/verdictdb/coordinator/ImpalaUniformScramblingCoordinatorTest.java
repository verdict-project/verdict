package org.verdictdb.coordinator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ImpalaUniformScramblingCoordinatorTest {

  static Connection conn;

  static DbmsConnection dbmsConn;

  private static Statement stmt;

  private static final String IMPALA_HOST;

  private static final String IMPALA_DATABASE = "tpch";

  private static final String IMPALA_UESR = "";

  private static final String IMPALA_PASSWORD = "";


  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      IMPALA_HOST = "impala";
    } else {
      IMPALA_HOST = "localhost";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBDbmsException, IOException {
    String connectionString =
        String.format("jdbc:impala://%s:21050/%s", IMPALA_HOST, IMPALA_DATABASE);
    conn =
        DatabaseConnectionHelpers.setupImpala(
            connectionString, IMPALA_UESR, IMPALA_PASSWORD, IMPALA_DATABASE);
    stmt = conn.createStatement();
  }

  @Test
  public void test() {

  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("drop schema if exists `%s`", IMPALA_DATABASE));
  }
}

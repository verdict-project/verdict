package org.verdictdb.jdbc41;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcMetaDataForRedshiftTest {

  static Connection conn;

  static DbmsConnection dbmsConn;

  private static Statement stmt;

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_SCHEMA = "public";

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  private static final String SCHEMA_NAME =
      "verdict_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String TABLE_NAME = "mytable";

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
    //    System.out.println(REDSHIFT_HOST);
    //    System.out.println(REDSHIFT_USER);
    //    System.out.println(REDSHIFT_PASSWORD);
  }

  @BeforeClass
  public static void setupRedshiftDatabase() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    conn = DriverManager.getConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    dbmsConn = JdbcConnection.create(conn);

    stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS \"%s\"", SCHEMA_NAME));
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", SCHEMA_NAME));
    stmt.execute(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));

    // create a test table
    stmt.execute(
        String.format(
            "CREATE TABLE \"%s\".\"%s\" ("
                + "smallintCol   SMALLINT, "
                + "intCol        INT, "
                + "bigintCol     BIGINT, "
                + "decimalCol    DECIMAL, "
                + "realCol       REAL, "
                + "doubleCol     DOUBLE PRECISION, "
                + "boolCol       BOOLEAN, "
                + "charCol       CHAR(4), "
                + "varcharCol    VARCHAR(10), "
                + "dateCol       DATE, "
                + "timestampCol  TIMESTAMP, "
                + "timestamptzCol TIMESTAMPTZ"
                + ")",
            SCHEMA_NAME, TABLE_NAME));
  }

  @AfterClass
  public static void tearDown() throws VerdictDBDbmsException {
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));
    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", SCHEMA_NAME));
    dbmsConn.close();
  }

  @Test
  public void test() {}
}

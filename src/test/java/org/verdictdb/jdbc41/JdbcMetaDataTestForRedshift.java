package org.verdictdb.jdbc41;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;

public class JdbcMetaDataTestForRedshift {

  static Connection conn;

  static DbmsConnection dbmsConn;

  private static Statement stmt;

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";
  
  private static final String REDSHIFT_SCHEMA = "public";

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

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
    stmt.execute(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));
    
    // create a test table
    stmt.execute(String.format(
        "CREATE TABLE \"%s\" ("
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
            + ")"
        , TABLE_NAME));
  }
  
  @AfterClass
  public static void tearDown() throws VerdictDBDbmsException {
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));
    dbmsConn.close();
  }
  
  @Test
  public void test() {
    
  }

}

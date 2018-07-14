package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;

/**
 * 
 * @author Yongjoo Park
 *
 */
public class JdbcMetaDataTestForImpala {
  
  static Connection conn;

  static DbmsConnection dbmsConn;

  private static Statement stmt;

  private static final String IMPALA_HOST;

  private static final String IMPALA_DATABASE = "default";

  private static final String IMPALA_UESR = "";

  private static final String IMPALA_PASSWORD = "";

  private static final String TABLE_NAME = "mytable";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      IMPALA_HOST = "impala";
    } else {
      IMPALA_HOST = "localhost";
    }
  }

  @BeforeClass
  public static void setupImpalaDatabase() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:impala://%s:21050/%s", IMPALA_HOST, IMPALA_DATABASE);
    conn = DriverManager.getConnection(connectionString, IMPALA_UESR, IMPALA_PASSWORD);
    dbmsConn = JdbcConnection.create(conn);

    stmt = conn.createStatement();
    stmt.execute(String.format("DROP TABLE IF EXISTS `%s`", TABLE_NAME));
    stmt.execute(String.format(
        "CREATE TABLE `%s` ("
            + "tinyintCol    TINYINT, "
            + "boolCol       BOOLEAN, "
            + "smallintCol   SMALLINT, "
            + "intCol        INT, "
            + "bigintCol     BIGINT, "
            + "decimalCol    DECIMAL, "
            + "floatCol      FLOAT, "
            + "doubleCol     DOUBLE, "
            + "timestampCol  TIMESTAMP, "
            + "charCol       CHAR(4), "
            + "stringCol     STRING)"
        , TABLE_NAME));
  }
  
  @AfterClass
  public static void tearDown() throws VerdictDBDbmsException {
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    dbmsConn.close();
  }
  
  @Test
  public void testColumnTypes() throws VerdictDBDbmsException, SQLException {
    List<Pair<String, String>> columns = dbmsConn.getColumns("default", TABLE_NAME);
    assertEquals(11, columns.size());

    ResultSet expected = stmt.executeQuery(String.format("describe %s", TABLE_NAME));
    int idx = 0;
    while (expected.next()) {
      assertEquals(expected.getString(1), columns.get(idx).getLeft());
      assertEquals(expected.getString(2), columns.get(idx).getRight());
      idx++;
    }

    // column name is case-sensitive for mysql
    assertEquals("tinyintcol", columns.get(0).getLeft());

    // column type name is lower case and includes parentheses for mysql
    assertEquals("char(4)", columns.get(9).getRight());
  }

}

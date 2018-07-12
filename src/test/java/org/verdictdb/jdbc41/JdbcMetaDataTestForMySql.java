package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;

public class JdbcMetaDataTestForMySql {

  static Connection conn;

  static DbmsConnection dbmsConn;

  private static Statement stmt;

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "test";

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
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    conn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
    dbmsConn = JdbcConnection.create(conn);

    stmt = conn.createStatement();
    stmt.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    stmt.execute(String.format(
        "CREATE TABLE %s ("
        + "bitCol        BIT(1), "
        + "tinyintCol    TINYINT(2), "
        + "boolCol       BOOL, "
        + "smallintCol   SMALLINT(3), "
        + "mediumintCol  MEDIUMINT(4), "
        + "intCol        INT(4), "
        + "integerCol    INTEGER(4), "
        + "bigintCol     BIGINT(8), "
        + "decimalCol    DECIMAL(4,2), "
        + "decCol        DEC(4,2), "
        + "floatCol      FLOAT(4,2), "
        + "doubleCol     DOUBLE(8,2), "
        + "doubleprecisionCol DOUBLE PRECISION(8,2), "
        + "dateCol       DATE, "
        + "datetimeCol   DATETIME, "
        + "timestampCol  TIMESTAMP, "
        + "timeCol       TIME, "
        + "yearCol       YEAR(2), "
        + "yearCol2      YEAR(4), "
        + "charCol       CHAR(4), "
        + "varcharCol    VARCHAR(4), "
        + "binaryCol     BINARY(4), "
        + "varbinaryCol  VARBINARY(4), "
        + "tinyblobCol   TINYBLOB, "
        + "tinytextCol   TINYTEXT, "
        + "blobCol       BLOB(4), "
        + "textCol       TEXT(100), "
        + "medimumblobCol MEDIUMBLOB, "
        + "medimumtextCol MEDIUMTEXT, "
        + "longblobCol   LONGBLOB, "
        + "longtextCol   LONGTEXT, "
        + "enumCol       ENUM('1', '2'), "
        + "setCol        SET('1', '2'))"
        , TABLE_NAME));
  }

  public static void tearDown() throws VerdictDBDbmsException {
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    dbmsConn.close();
  }

  @Test
  public void testGetColumns() throws VerdictDBDbmsException, SQLException {
    List<Pair<String, String>> columns = dbmsConn.getColumns("test", TABLE_NAME);
    assertEquals(33, columns.size());

    ResultSet expected = stmt.executeQuery(String.format("describe %s", TABLE_NAME));
    int idx = 0;
    while (expected.next()) {
      assertEquals(expected.getString(1), columns.get(idx).getLeft());
      assertEquals(expected.getString(2), columns.get(idx).getRight());
      idx++;
    }

    // column name is case-sensitive for mysql
    assertEquals("bitCol", columns.get(0).getLeft());

    // column type name is lower case and includes parentheses for mysql
    assertEquals("bit(1)", columns.get(0).getRight());
  }

}

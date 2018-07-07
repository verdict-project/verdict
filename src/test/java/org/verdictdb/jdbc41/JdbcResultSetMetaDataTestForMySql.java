package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;

public class JdbcResultSetMetaDataTestForMySql {

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
    if (env != null && env.equals("GitLab")) {
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
    dbmsConn = new JdbcConnection(conn);

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

    stmt.execute(String.format("INSERT INTO %s VALUES ( "
        + "1, 2, 1, 1, 1, 1, 1, 1, "
        + "1.0, 1.0, 1.0, 1.0, 1.0, "
        + "'2018-12-31', '2018-12-31 01:00:00', '2018-12-31 00:00:01', '10:59:59', "
        + "18, 2019, 'abc', 'abc', '1010', '1010', "
        + "'1110', 'abc', '1110', 'abc', '1110', 'abc', '1110', 'abc', '1', '2')",
        TABLE_NAME));
  }

  public static void tearDown() throws VerdictDBDbmsException {
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    dbmsConn.close();
  }

  @Test
  public void testColumnTypes() throws VerdictDBDbmsException, SQLException {
    String sql = String.format("select * from %s", TABLE_NAME);
    ResultSet ourResult = new JdbcResultSet(dbmsConn.execute(sql));
    ResultSetMetaData ourMetaData = ourResult.getMetaData();
    assertEquals(33, ourMetaData.getColumnCount());

    ResultSetMetaData expected = stmt.executeQuery(sql).getMetaData();
    assertEquals(expected.getColumnCount(), ourMetaData.getColumnCount());

    for (int i = 1; i <= ourMetaData.getColumnCount(); i++) {
      assertEquals(expected.getColumnType(i), ourMetaData.getColumnType(i));
    }

    ourResult.next();
    assertEquals(true, ourResult.getBoolean(1));  // bit
    assertEquals(1, ourResult.getByte(1));        // bit
    assertEquals(2, ourResult.getInt(2));         // tinyint
    assertEquals(2, ourResult.getLong(2));        // tinyint
    assertEquals(2, ourResult.getByte(2));        // tinyint
    assertNotEquals(1, ourResult.getInt(2));      // tinyint
    assertNotEquals(1, ourResult.getLong(2));     // tinyint
    assertNotEquals(1, ourResult.getByte(2));     // tinyint
    assertEquals(true, ourResult.getBoolean(3));  // bool
    assertEquals(1, ourResult.getInt(3));         // bool
    assertEquals(1, ourResult.getLong(3));        // bool
    assertEquals(1, ourResult.getByte(3));        // bool
    assertEquals(true, ourResult.getBoolean(4));  // smallint
    assertEquals(1, ourResult.getInt(4));         // smallint
    assertEquals(1, ourResult.getLong(4));        // smallint
    assertEquals(1, ourResult.getByte(4));        // smallint
    assertEquals(true, ourResult.getBoolean(5));  // mediumint
    assertEquals(1, ourResult.getInt(5));         // mediumint
    assertEquals(1, ourResult.getLong(5));        // mediumint
    assertEquals(1, ourResult.getByte(5));        // mediumint
    assertEquals(true, ourResult.getBoolean(6));  // int
    assertEquals(1, ourResult.getInt(6));         // int
    assertEquals(1, ourResult.getLong(6));        // int
    assertEquals(1, ourResult.getByte(6));        // int
    assertEquals(true, ourResult.getBoolean(7));  // integer
    assertEquals(1, ourResult.getInt(7));         // integer
    assertEquals(1, ourResult.getLong(7));        // integer
    assertEquals(1, ourResult.getByte(7));        // integer
    assertEquals(true, ourResult.getBoolean(8));  // bigint
    assertEquals(1, ourResult.getInt(8));         // bigint
    assertEquals(1, ourResult.getLong(8));        // bigint
    assertEquals(1, ourResult.getByte(8));        // bigint
    
    try {
      assertEquals(true, ourResult.getBoolean(9));          // decimal
      fail();
    } catch (java.sql.SQLException e) {}
    assertEquals(1.0, ourResult.getFloat(9), 1e-6);         // decimal
    assertEquals(1.0, ourResult.getDouble(9), 1e-6);        // decimal
    assertEquals(1.0, ourResult.getByte(9), 1e-6);          // decimal
    assertEquals(1.0, ourResult.getInt(9), 1e-6);           // decimal
    assertEquals(1.0, ourResult.getLong(9), 1e-6);          // decimal
    try {
      assertEquals(true, ourResult.getBoolean(10));          // dec
      fail();
    } catch (java.sql.SQLException e) {}
    assertEquals(1.0, ourResult.getFloat(10), 1e-6);         // dec
    assertEquals(1.0, ourResult.getDouble(10), 1e-6);        // dec
    assertEquals(1.0, ourResult.getByte(10), 1e-6);          // dec
    assertEquals(1.0, ourResult.getInt(10), 1e-6);           // dec
    assertEquals(1.0, ourResult.getLong(10), 1e-6);          // dec
    assertEquals(1.0, ourResult.getFloat(11), 1e-6);         // float
    assertEquals(1.0, ourResult.getDouble(11), 1e-6);        // float
    assertEquals(1.0, ourResult.getByte(11), 1e-6);          // float
    assertEquals(1.0, ourResult.getInt(11), 1e-6);           // float
    assertEquals(1.0, ourResult.getLong(11), 1e-6);          // float
    assertEquals(1.0, ourResult.getFloat(12), 1e-6);         // double
    assertEquals(1.0, ourResult.getDouble(12), 1e-6);        // double
    assertEquals(1.0, ourResult.getByte(12), 1e-6);          // double
    assertEquals(1.0, ourResult.getInt(12), 1e-6);           // double
    assertEquals(1.0, ourResult.getLong(12), 1e-6);          // double
    assertEquals(1.0, ourResult.getFloat(13), 1e-6);         // double precision
    assertEquals(1.0, ourResult.getDouble(13), 1e-6);        // double precision
    assertEquals(1.0, ourResult.getByte(13), 1e-6);          // double precision
    assertEquals(1.0, ourResult.getInt(13), 1e-6);           // double precision
    assertEquals(1.0, ourResult.getLong(13), 1e-6);          // double precision
    
//    assertEquals(Timestamp.valueOf(LocalDateTime.of(2018, 12, 31, 0, 0, 0)), ourResult.getTimestamp(14));  // date
//    assertEquals(Timestamp.valueOf(LocalDateTime.of(2018, 12, 31, 1, 0, 0)), ourResult.getTimestamp(15));  // datetime
//    assertEquals(Timestamp.valueOf(LocalDateTime.of(2018, 12, 31, 0, 0, 1)), ourResult.getTimestamp(16));  // timestamp
//    assertEquals(Time.valueOf(LocalTime.of(10, 59, 59)), ourResult.getTime(17));  // time
//    assertEquals(Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 10, 59, 59)), ourResult.getTimestamp(17));  // time
    assertEquals(Timestamp.valueOf("2018-12-31"), ourResult.getTimestamp(14));  // date
    assertEquals(Timestamp.valueOf("2018-12-31 00:00:01"), ourResult.getTimestamp(15));  // datetime
    assertEquals(Timestamp.valueOf("2018-12-31 00:00:01"), ourResult.getTimestamp(16));  // timestamp
    assertEquals(Time.valueOf(LocalTime.of(10, 59, 59)), ourResult.getTime(17));  // time
    assertEquals(Timestamp.valueOf("1970-01-01 10:59:59"), ourResult.getTimestamp(17));  // time

    ourResult.close();
  }

}

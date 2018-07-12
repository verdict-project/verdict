package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;



public class JdbcResultSetMetaDataTestForPostgreSQL {

  static Connection conn;

  static DbmsConnection dbmsConn;

  private static Statement stmt;

  private static final String POSTGRESQL_HOST;

  private static final String POSTGRESQL_DATABASE = "test";

  private static final String POSTGRESQL_USER = "postgres";

  private static final String POSTGRESQL_PASSWORD = "";

  private static final String TABLE_NAME = "mytable";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      POSTGRESQL_HOST = "postgres";
    } else {
      POSTGRESQL_HOST = "localhost";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException {
    String mysqlConnectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRESQL_HOST, POSTGRESQL_DATABASE);
    conn = DriverManager.getConnection(mysqlConnectionString, POSTGRESQL_USER, POSTGRESQL_PASSWORD);
    dbmsConn = new JdbcConnection(conn, new PostgresqlSyntax());

    stmt = conn.createStatement();
    stmt.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    stmt.execute(String.format(
        "CREATE TABLE %s ("
            + "bigintCol      bigint, "
            + "bigserialCol   bigserial, "
            + "bitCol         bit(1), "
            + "varbitCol      varbit(4), "
            + "booleanCol     boolean, "
            + "boxCol         box, "
            + "byteaCol       bytea, "
            + "charCol        char(4), "
            + "varcharCol     varchar(4), "
            + "cidrCol        cidr, "
            + "circleCol      circle, "
            + "dateCol        date, "
            + "float8Col      float8, "
            + "inetCol        inet, "
            + "integerCol     integer, "
            + "jsonCol        json, "
            + "lineCol        line, "
            + "lsegCol        lseg, "
            + "macaddrCol     macaddr, "
            + "macaddr8Col    macaddr8, "
            + "moneyCol       money, "
            + "numericCol     numeric(4,2), "
            + "pathCol        path, "
            + "pointCol       point, "
            + "polygonCol     polygon, "
            + "realCol        real, "
            + "smallintCol    smallint, "
            + "smallserialCol smallserial, "
            + "serialCol      serial, "
            + "textCol        text, "
            + "timeCol        time, "
            + "timestampCol   timestamp, "
            + "uuidCol        uuid, "
            + "xmlCol         xml)"
        , TABLE_NAME));
    stmt.execute(String.format("INSERT INTO %s VALUES ( "
            + "1, 1, '1', '1011', true, '((1,1), (2,2))', '1', '1234', '1234', "
            + "'10', '((1,1),2)', '2018-12-31', 1.0, '88.99.0.0/16', 1, "
            + "'{\"2\":1}', '{1,2,3}', '((1,1),(2,2))', "
            + "'08002b:010203', '08002b:0102030405', '12.34', 1.0, '((1,1))', '(1,1)', "
            + "'((1,1))', 1.0, 1, 1, 1, '1110', '2018-12-31 00:00:01', '2018-12-31 00:00:01', "
            + "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11','<foo>bar</foo>')",
        TABLE_NAME));
    stmt.execute(String.format("INSERT INTO %s VALUES ( "
            + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
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
    assertEquals(34, ourMetaData.getColumnCount());

    ResultSetMetaData expected = stmt.executeQuery(sql).getMetaData();
    assertEquals(expected.getColumnCount(), ourMetaData.getColumnCount());

    for (int i = 1; i <= ourMetaData.getColumnCount(); i++) {
      assertEquals(expected.getColumnType(i), ourMetaData.getColumnType(i));
    }

    ourResult.next();
    assertEquals(true, ourResult.getBoolean(1));  // bit
    assertEquals(1, ourResult.getByte(1));        // bit
    assertEquals(1, ourResult.getInt(27));         // smallint
    assertEquals(1, ourResult.getLong(27));        // smallint
    assertEquals(1, ourResult.getByte(27));        // smallint
    assertNotEquals(2, ourResult.getInt(27));      // smallint
    assertNotEquals(2, ourResult.getLong(27));     // smallint
    assertNotEquals(2, ourResult.getByte(27));     // smallint
    assertEquals(true, ourResult.getBoolean(5));  // bool
    assertEquals(1, ourResult.getInt(5));         // bool
    assertEquals(1, ourResult.getLong(5));        // bool
    assertEquals(1, ourResult.getByte(5));        // bool
    assertEquals(true, ourResult.getBoolean(15));  // integer
    assertEquals(1, ourResult.getInt(15));         // integer
    assertEquals(1, ourResult.getLong(15));        // integer
    assertEquals(1, ourResult.getByte(15));        // integer
    assertEquals(true, ourResult.getBoolean(8));  // bigint
    assertEquals(1, ourResult.getInt(1));         // bigint
    assertEquals(1, ourResult.getLong(1));        // bigint
    assertEquals(1, ourResult.getByte(1));        // bigint

    try {
      assertEquals(true, ourResult.getBoolean(9));          // numeric
      fail();
    } catch (java.sql.SQLException e) {}
    assertEquals(1.0, ourResult.getFloat(22), 1e-6);         // numeric
    assertEquals(1.0, ourResult.getDouble(22), 1e-6);        // numeric
    assertEquals(1.0, ourResult.getByte(22), 1e-6);          // numeric
    assertEquals(1.0, ourResult.getInt(22), 1e-6);           // numeric
    assertEquals(1.0, ourResult.getLong(22), 1e-6);          // numeric
    assertEquals(1.0, ourResult.getFloat(26), 1e-6);         // real
    assertEquals(1.0, ourResult.getDouble(26), 1e-6);        // real
    assertEquals(1.0, ourResult.getByte(26), 1e-6);          // real
    assertEquals(1.0, ourResult.getInt(26), 1e-6);           // real
    assertEquals(1.0, ourResult.getLong(26), 1e-6);          // real
    assertEquals(1.0, ourResult.getFloat(13), 1e-6);         // float8
    assertEquals(1.0, ourResult.getDouble(13), 1e-6);        // float8
    assertEquals(1.0, ourResult.getByte(13), 1e-6);          // float8
    assertEquals(1.0, ourResult.getInt(13), 1e-6);           // float8
    assertEquals(1.0, ourResult.getLong(13), 1e-6);          // float8
  }


}

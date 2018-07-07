package org.verdictdb.jdbc41;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.connection.SparkConnection;
import org.verdictdb.core.connection.SparkQueryResult;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SparkSyntax;

import java.sql.*;

import static java.sql.Types.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class JdbcResultSetMetaDataTestForSpark {

  private static SparkSession spark;

  private static SparkConnection sparkConnection;

  private static final String TABLE_NAME = "mytable";

  @BeforeClass
  public static void setupSpark() throws VerdictDBDbmsException {
    spark = SparkSession.builder().appName("test")
        .master("local")
        .config("spark.sql.catalogImplementation", "hive")
        .getOrCreate();
    sparkConnection = new SparkConnection(spark, new SparkSyntax());
    sparkConnection.execute(String.format(
        "CREATE TABLE %s ("
            + "tinyintCol    TINYINT, "
            + "boolCol       BOOLEAN, "
            + "smallintCol   SMALLINT, "
            + "intCol        INT, "
            + "bigintCol     BIGINT, "
            + "decimalCol    DECIMAL(4,2), "
            + "floatCol      FLOAT, "
            + "doubleCol     DOUBLE, "
            + "dateCol       DATE, "
            + "timestampCol  TIMESTAMP, "
            + "charCol       CHAR(4), "
            + "varcharCol    VARCHAR(4), "
            + "stringCol     STRING, "
            + "binaryCol     BINARY, "
            + "arrayCol      array<int>, "
            + "mapCol        map<string,int>, "
            + "structCol     struct<a:int,b:string>)"
        , TABLE_NAME));
    sparkConnection.execute(String.format("INSERT INTO %s VALUES ( "
            + "1, 1, 1, 1, 1, 1.0, 1.0, 1.0, "
            + "'2018-12-31', '2018-12-31 01:00:00', "
            + "'abc', 'abc', '1010', '1', "
            + "array(1,2), map('1', 1), NAMED_STRUCT('a',1,'b','12'))",
        TABLE_NAME));

  }

  @AfterClass
  public static void tearDown() throws VerdictDBDbmsException {
    sparkConnection.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    sparkConnection.close();
  }

  @Test
  public void testColumnTypes() throws VerdictDBDbmsException, SQLException {
    String sql = String.format("select * from %s", TABLE_NAME);
    ResultSet ourResult = new JdbcResultSet(sparkConnection.execute(sql));
    ResultSetMetaData ourMetaData = ourResult.getMetaData();
    assertEquals(17, ourMetaData.getColumnCount());

    assertEquals(SMALLINT, ourMetaData.getColumnType(1));
    assertEquals(BOOLEAN, ourMetaData.getColumnType(2));
    assertEquals(SMALLINT, ourMetaData.getColumnType(3));
    assertEquals(INTEGER, ourMetaData.getColumnType(4));
    assertEquals(BIGINT, ourMetaData.getColumnType(5));
    assertEquals(DECIMAL, ourMetaData.getColumnType(6));
    assertEquals(FLOAT, ourMetaData.getColumnType(7));
    assertEquals(DOUBLE, ourMetaData.getColumnType(8));
    assertEquals(DATE, ourMetaData.getColumnType(9));
    assertEquals(TIMESTAMP, ourMetaData.getColumnType(10));
    assertEquals(VARCHAR, ourMetaData.getColumnType(11));
    assertEquals(VARCHAR, ourMetaData.getColumnType(12));
    assertEquals(VARCHAR, ourMetaData.getColumnType(13));
    assertEquals(BIT, ourMetaData.getColumnType(14));
    assertEquals(ARRAY, ourMetaData.getColumnType(15));
    assertEquals(OTHER, ourMetaData.getColumnType(16));
    assertEquals(STRUCT, ourMetaData.getColumnType(17));

    ourResult.next();
    assertEquals(1, ourResult.getInt(1));         // tinyint
    assertEquals(1, ourResult.getLong(1));        // tinyint
    assertEquals(1, ourResult.getByte(1));        // tinyint
    assertNotEquals(2, ourResult.getInt(1));      // tinyint
    assertNotEquals(2, ourResult.getLong(1));     // tinyint
    assertNotEquals(2, ourResult.getByte(1));     // tinyint
    //assertEquals(true, ourResult.getBoolean(2));  // bool
    assertEquals(1, ourResult.getInt(2));         // bool
    assertEquals(1, ourResult.getLong(2));        // bool
    assertEquals(1, ourResult.getByte(2));        // bool
    //assertEquals(true, ourResult.getBoolean(3));  // smallint
    assertEquals(1, ourResult.getInt(3));         // smallint
    assertEquals(1, ourResult.getLong(3));        // smallint
    assertEquals(1, ourResult.getByte(3));        // smallint
    //assertEquals(true, ourResult.getBoolean(4));  // int
    assertEquals(1, ourResult.getInt(4));         // int
    assertEquals(1, ourResult.getLong(4));        // int
    assertEquals(1, ourResult.getByte(4));        // int
    //assertEquals(true, ourResult.getBoolean(5));  // bigint
    assertEquals(1, ourResult.getInt(5));         // bigint
    assertEquals(1, ourResult.getLong(5));        // bigint
    assertEquals(1, ourResult.getByte(5));        // bigint

    assertEquals(1.0, ourResult.getFloat(6), 1e-6);         // decimal
    assertEquals(1.0, ourResult.getDouble(6), 1e-6);        // decimal
    assertEquals(1.0, ourResult.getByte(6), 1e-6);          // decimal
    assertEquals(1.0, ourResult.getInt(6), 1e-6);           // decimal
    assertEquals(1.0, ourResult.getLong(6), 1e-6);          // decimal
    assertEquals(1.0, ourResult.getFloat(7), 1e-6);         // float
    assertEquals(1.0, ourResult.getDouble(7), 1e-6);        // float
    assertEquals(1.0, ourResult.getByte(7), 1e-6);          // float
    assertEquals(1.0, ourResult.getInt(7), 1e-6);           // float
    assertEquals(1.0, ourResult.getLong(7), 1e-6);          // float
    assertEquals(1.0, ourResult.getFloat(8), 1e-6);         // double
    assertEquals(1.0, ourResult.getDouble(8), 1e-6);        // double
    assertEquals(1.0, ourResult.getByte(8), 1e-6);          // double
    assertEquals(1.0, ourResult.getInt(8), 1e-6);           // double
    assertEquals(1.0, ourResult.getLong(8), 1e-6);          // double

    assertEquals(Date.valueOf("2018-12-31"), ourResult.getDate(9));  // date
    assertEquals(Timestamp.valueOf("2018-12-31 01:00:00.0"), ourResult.getTimestamp(10));  // timestamp



  }
}

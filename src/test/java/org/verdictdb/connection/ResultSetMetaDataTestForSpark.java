package org.verdictdb.connection;

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BIT;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.STRUCT;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SparkSyntax;

public class ResultSetMetaDataTestForSpark {

  private static SparkSession spark;

  private static SparkConnection sparkConnection;

  private static final String TABLE_NAME = "mytable";

  @BeforeClass
  public static void setupSpark() throws VerdictDBDbmsException {
    spark = SparkSession.builder().appName("test")
        .master("local")
        .enableHiveSupport()
//        .config("spark.sql.catalogImplementation", "hive")
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
    DbmsQueryResult ourResult = sparkConnection.execute(sql);
//    ResultSetMetaData ourMetaData = ourResult.getMetaData();
    assertEquals(17, ourResult.getColumnCount());

    assertEquals(SMALLINT, ourResult.getColumnType(0));
    assertEquals(BOOLEAN, ourResult.getColumnType(1));
    assertEquals(SMALLINT, ourResult.getColumnType(2));
    assertEquals(INTEGER, ourResult.getColumnType(3));
    assertEquals(BIGINT, ourResult.getColumnType(4));
    assertEquals(DECIMAL, ourResult.getColumnType(5));
    assertEquals(FLOAT, ourResult.getColumnType(6));
    assertEquals(DOUBLE, ourResult.getColumnType(7));
    assertEquals(DATE, ourResult.getColumnType(8));
    assertEquals(TIMESTAMP, ourResult.getColumnType(9));
    assertEquals(VARCHAR, ourResult.getColumnType(10));
    assertEquals(VARCHAR, ourResult.getColumnType(11));
    assertEquals(VARCHAR, ourResult.getColumnType(12));
    assertEquals(BIT, ourResult.getColumnType(13));
    assertEquals(ARRAY, ourResult.getColumnType(14));
    assertEquals(OTHER, ourResult.getColumnType(15));
    assertEquals(STRUCT, ourResult.getColumnType(16));

    ourResult.next();
    assertEquals(1, ourResult.getInt(0));         // tinyint
    assertEquals(1, ourResult.getLong(0));        // tinyint
    assertEquals(1, ourResult.getByte(0));        // tinyint
    assertNotEquals(2, ourResult.getInt(0));      // tinyint
    assertNotEquals(2, ourResult.getLong(0));     // tinyint
    assertNotEquals(2, ourResult.getByte(0));     // tinyint
    //assertEquals(true, ourResult.getBoolean(2));  // bool
    assertEquals(1, ourResult.getInt(1));         // bool
    assertEquals(1, ourResult.getLong(1));        // bool
    assertEquals(1, ourResult.getByte(1));        // bool
    //assertEquals(true, ourResult.getBoolean(3));  // smallint
    assertEquals(1, ourResult.getInt(2));         // smallint
    assertEquals(1, ourResult.getLong(2));        // smallint
    assertEquals(1, ourResult.getByte(2));        // smallint
    //assertEquals(true, ourResult.getBoolean(4));  // int
    assertEquals(1, ourResult.getInt(3));         // int
    assertEquals(1, ourResult.getLong(3));        // int
    assertEquals(1, ourResult.getByte(3));        // int
    //assertEquals(true, ourResult.getBoolean(5));  // bigint
    assertEquals(1, ourResult.getInt(4));         // bigint
    assertEquals(1, ourResult.getLong(4));        // bigint
    assertEquals(1, ourResult.getByte(4));        // bigint

    assertEquals(1.0, ourResult.getFloat(5), 1e-6);         // decimal
    assertEquals(1.0, ourResult.getDouble(5), 1e-6);        // decimal
    assertEquals(1.0, ourResult.getByte(5), 1e-6);          // decimal
    assertEquals(1.0, ourResult.getInt(5), 1e-6);           // decimal
    assertEquals(1.0, ourResult.getLong(5), 1e-6);          // decimal
    assertEquals(1.0, ourResult.getFloat(6), 1e-6);         // float
    assertEquals(1.0, ourResult.getDouble(6), 1e-6);        // float
    assertEquals(1.0, ourResult.getByte(6), 1e-6);          // float
    assertEquals(1.0, ourResult.getInt(6), 1e-6);           // float
    assertEquals(1.0, ourResult.getLong(6), 1e-6);          // float
    assertEquals(1.0, ourResult.getFloat(7), 1e-6);         // double
    assertEquals(1.0, ourResult.getDouble(7), 1e-6);        // double
    assertEquals(1.0, ourResult.getByte(7), 1e-6);          // double
    assertEquals(1.0, ourResult.getInt(7), 1e-6);           // double
    assertEquals(1.0, ourResult.getLong(7), 1e-6);          // double

    assertEquals(Date.valueOf("2018-12-31"), ourResult.getDate(8));  // date
    assertEquals(Timestamp.valueOf("2018-12-31 01:00:00.0"), ourResult.getTimestamp(9));  // timestamp
  }
}

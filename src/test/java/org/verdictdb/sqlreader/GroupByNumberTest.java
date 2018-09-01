package org.verdictdb.sqlreader;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

//<<<<<<< HEAD
//import static org.junit.Assert.fail;
//

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.SparkConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SparkSyntax;

//
//=======
//>>>>>>> origin/master

public class GroupByNumberTest {

  private static SparkSession spark;

  private static SparkConnection sparkConnection;

  private static final String SPARK_TABLE_NAME = "mysparktable";

  private static final String IMPALA_HOST;

  private static final String IMPALA_DATABASE =
      "groupbynumbertest_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String IMPALA_UESR = "";

  private static final String IMPALA_PASSWORD = "";

  private static final String IMPALA_TABLE_NAME = "myimpalatable";

  private static Connection impalaConn;

  private static DbmsConnection impalaConnection;

  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  @BeforeClass
  public static void setupDatabases() throws VerdictDBDbmsException, SQLException {
    // Impala
    String connectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    impalaConn = DriverManager.getConnection(connectionString, IMPALA_UESR, IMPALA_PASSWORD);
    impalaConnection = JdbcConnection.create(impalaConn);

    impalaConnection.execute(String.format("drop schema if exists %s cascade", IMPALA_DATABASE));
    impalaConnection.execute(String.format("create schema if not exists %s", IMPALA_DATABASE));
    impalaConnection.execute(
        String.format(
            "CREATE TABLE %s.%s (" + "tinyintCol    TINYINT, " + "boolCol       BOOLEAN)",
            IMPALA_DATABASE, IMPALA_TABLE_NAME));

    // Spark
    spark =
        SparkSession.builder()
            .appName("groupbyNumberTest")
            .master("local")
            .config("hive.groupby.orderby.position.alias", "true")
            .config("hive.groupby.position.alias", "true")
            .enableHiveSupport()
            .getOrCreate();
    sparkConnection = new SparkConnection(spark, new SparkSyntax());

    sparkConnection.execute(String.format("drop table if exists %s", SPARK_TABLE_NAME));
    sparkConnection.execute(
        String.format(
            "CREATE TABLE %s (" + "tinyintCol    TINYINT, " + "boolCol       BOOLEAN)",
            SPARK_TABLE_NAME));
  }

  @Test
  public void testSpark() throws VerdictDBDbmsException {
    String sql = String.format("select avg(tinyintCol) from %s group by 1", SPARK_TABLE_NAME);
    try {
      sparkConnection.execute(sql);
      fail();
    } catch (VerdictDBDbmsException e) {
      if (e.getMessage().contains("GROUP BY position 1 is an aggregate function")) {
        // expected
      } else {
        throw e;
      }
    }
  }

  @Test
  public void testImpala() throws VerdictDBDbmsException {
    String sql =
        String.format(
            "select avg(tinyintCol) from %s.%s group by 1", IMPALA_DATABASE, IMPALA_TABLE_NAME);
    try {
      impalaConnection.execute(sql);
      fail();
    } catch (VerdictDBDbmsException e) {
      if (e.getMessage().contains("GROUP BY expression must not contain aggregate functions: 1")) {
        // expected
      } else {
        throw e;
      }
    }
  }
}

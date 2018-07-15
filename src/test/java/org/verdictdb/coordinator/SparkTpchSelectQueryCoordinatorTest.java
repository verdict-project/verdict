package org.verdictdb.coordinator;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.SparkConnection;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;

import java.sql.SQLException;

public class SparkTpchSelectQueryCoordinatorTest {

  final static int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static final String TEST_SCHEMA = "scrambling_coordinator_test";

  static DbmsConnection conn;

  static SparkSession spark;

  @BeforeClass
  public static void setupSpark() throws VerdictDBException {
    String appname = "scramblingCoordinatorTest";
    spark = DatabaseConnectionHelpers.setupSpark(appname, TEST_SCHEMA);
    conn = new SparkConnection(spark);

    // Create Scramble table
    conn.execute(String.format("DROP TABLE IF EXISTS `%s`.`lineitem_scrambled`", TEST_SCHEMA));
    conn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders_scrambled`", TEST_SCHEMA));

    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(conn, TEST_SCHEMA, TEST_SCHEMA, (long) 100);
    ScrambleMeta meta1 =
        scrambler.scramble(TEST_SCHEMA, "lineitem", TEST_SCHEMA, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(TEST_SCHEMA, "orders", TEST_SCHEMA, "orders_scrambled", "uniform");
    meta.insertScrambleMetaEntry(meta1);
    meta.insertScrambleMetaEntry(meta2);
  }

  @Test
  public void test() {

  }

  @AfterClass
  public static void tearDown() {
    spark.sql(String.format("DROP SCHEMA IF EXISTS %s CASCADE", TEST_SCHEMA));
  }
}

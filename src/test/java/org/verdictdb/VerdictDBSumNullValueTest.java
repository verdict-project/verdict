package org.verdictdb;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.SparkConnection;
import org.verdictdb.coordinator.*;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * This test is to check NULL value is returned when no row is selected by sum().
 */

public class VerdictDBSumNullValueTest {

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static final String TEST_SCHEMA =
      "spark_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  static DbmsConnection conn;

  static SparkSession spark;

  @BeforeClass
  public static void setupSpark() throws VerdictDBException {
    String appname = "sparkTest";
    spark = DatabaseConnectionHelpers.setupSpark(appname, TEST_SCHEMA);
    conn = new SparkConnection(spark);

    // Create Scramble table
    conn.execute(String.format("DROP TABLE IF EXISTS `%s`.`lineitem_scrambled`", TEST_SCHEMA));
    conn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders_scrambled`", TEST_SCHEMA));

    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(conn, TEST_SCHEMA, TEST_SCHEMA, (long) 200);
    ScrambleMeta meta1 =
        scrambler.scramble(TEST_SCHEMA, "lineitem", TEST_SCHEMA, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(TEST_SCHEMA, "orders", TEST_SCHEMA, "orders_scrambled", "uniform");
    meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);


  }

  @Before
  public void setupSchema() {
    spark.sql(
        String.format(
            "drop schema if exists `%s` cascade", VerdictOption.getDefaultTempSchemaName()));
    spark.sql(
        String.format(
            "create schema if not exists `%s`", VerdictOption.getDefaultTempSchemaName()));
  }

  @Test
  public void test() throws VerdictDBException {
    // This query doesn't select any rows.
    String sql = String.format(
        "select sum(l_extendedprice) from " +
            "%s.lineitem, %s.customer, %s.orders " +
            "where c_mktsegment='AAAAAA' and c_custkey=o_custkey and o_orderkey=l_orderkey",
        TEST_SCHEMA, TEST_SCHEMA, TEST_SCHEMA);

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    ExecutionResultReader reader = coordinator.process(sql);
    VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader);

    try {
      while (stream.hasNext()) {
        VerdictSingleResult rs = stream.next();
        rs.next();
        assertNull(rs.getValue(0));
        assertEquals(0, rs.getDouble(0), 0);
        assertEquals(0, rs.getInt(0));
      }
    } catch (RuntimeException e) {
      throw e;
    }

  }
}

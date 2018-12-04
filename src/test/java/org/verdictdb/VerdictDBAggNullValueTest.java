package org.verdictdb;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.*;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * This test is to check NULL value is returned when no row is selected by sum() or avg().
 */

public class VerdictDBAggNullValueTest {
  // lineitem has 10 blocks, orders has 3 blocks;
  // lineitem join orders has 12 blocks
  static final int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static VerdictOption options = new VerdictOption();

  static Connection conn;

  private static Statement stmt;

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE =
      "mysql_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    conn.setCatalog(MYSQL_DATABASE);
    stmt = conn.createStatement();
    stmt.execute(String.format("use `%s`", MYSQL_DATABASE));
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    // Create Scramble table
    dbmsConn.execute(
        String.format("DROP TABLE IF EXISTS `%s`.`lineitem_scrambled`", MYSQL_DATABASE));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders_scrambled`", MYSQL_DATABASE));
    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(dbmsConn, MYSQL_DATABASE, MYSQL_DATABASE, (long) 100);
    ScrambleMeta meta1 =
        scrambler.scramble(
            MYSQL_DATABASE, "lineitem", MYSQL_DATABASE, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(MYSQL_DATABASE, "orders", MYSQL_DATABASE, "orders_scrambled", "uniform");
     meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);
    stmt.execute(String.format("drop schema if exists `%s`", options.getVerdictTempSchemaName()));
    stmt.execute(
        String.format("create schema if not exists `%s`", options.getVerdictTempSchemaName()));
  }

  @Test
  public void testAvg() throws VerdictDBException {
    // This query doesn't select any rows.
    String sql = String.format(
        "select avg(l_extendedprice) from " +
            "%s.lineitem, %s.customer, %s.orders " +
            "where c_mktsegment='AAAAAA' and c_custkey=o_custkey and o_orderkey=l_orderkey",
        MYSQL_DATABASE, MYSQL_DATABASE, MYSQL_DATABASE);

    JdbcConnection jdbcConn = new JdbcConnection(conn, new MysqlSyntax());
    jdbcConn.setOutputDebugMessage(true);
    DbmsConnection dbmsconn = new CachedDbmsConnection(jdbcConn);
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
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

  @Test
  public void testSum() throws VerdictDBException {
    // This query doesn't select any rows.
    String sql = String.format(
        "select sum(l_extendedprice) from " +
            "%s.lineitem, %s.customer, %s.orders " +
            "where c_mktsegment='AAAAAA' and c_custkey=o_custkey and o_orderkey=l_orderkey",
        MYSQL_DATABASE, MYSQL_DATABASE, MYSQL_DATABASE);

    JdbcConnection jdbcConn = new JdbcConnection(conn, new MysqlSyntax());
    jdbcConn.setOutputDebugMessage(true);
    DbmsConnection dbmsconn = new CachedDbmsConnection(jdbcConn);
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
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

  @Test
  public void testSumAvg() throws VerdictDBException {
    // This query doesn't select any rows.
    String sql = String.format(
        "select sum(l_extendedprice), avg(l_extendedprice) from " +
            "%s.lineitem, %s.customer, %s.orders " +
            "where c_mktsegment='AAAAAA' and c_custkey=o_custkey and o_orderkey=l_orderkey",
        MYSQL_DATABASE, MYSQL_DATABASE, MYSQL_DATABASE);

    JdbcConnection jdbcConn = new JdbcConnection(conn, new MysqlSyntax());
    jdbcConn.setOutputDebugMessage(true);
    DbmsConnection dbmsconn = new CachedDbmsConnection(jdbcConn);
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
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
        assertNull(rs.getValue(1));
        assertEquals(0, rs.getDouble(1), 0);
        assertEquals(0, rs.getInt(1));
      }
    } catch (RuntimeException e) {
      throw e;
    }
  }

  @Test
  public void testCount() throws VerdictDBException {
    // This query doesn't select any rows.
    String sql = String.format(
        "select count(l_orderkey) from " +
            "%s.lineitem, %s.customer, %s.orders " +
            "where c_mktsegment='AAAAAA' and c_custkey=o_custkey and o_orderkey=l_orderkey",
        MYSQL_DATABASE, MYSQL_DATABASE, MYSQL_DATABASE);

    JdbcConnection jdbcConn = new JdbcConnection(conn, new MysqlSyntax());
    jdbcConn.setOutputDebugMessage(true);
    DbmsConnection dbmsconn = new CachedDbmsConnection(jdbcConn);
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    ExecutionResultReader reader = coordinator.process(sql);
    VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader);

    try {
      while (stream.hasNext()) {
        VerdictSingleResult rs = stream.next();
        rs.next();
        assertEquals(0, rs.getDouble(0), 0);
        assertEquals(0, rs.getInt(0));
      }
    } catch (RuntimeException e) {
      throw e;
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", MYSQL_DATABASE));
  }
}

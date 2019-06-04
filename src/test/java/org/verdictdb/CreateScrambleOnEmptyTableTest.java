package org.verdictdb;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.ScramblingCoordinator;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.H2Syntax;

public class CreateScrambleOnEmptyTableTest {

  static Connection conn;

  static Statement stmt;

  static final String schema = "tpch_emptydataset_test";

  @BeforeClass
  public static void setupH2Database() throws SQLException {
    final String DB_CONNECTION = "jdbc:h2:mem:aggexecnodetest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    stmt = conn.createStatement();
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schema));
    stmt.execute(
        "CREATE TABLE  IF NOT EXISTS \"tpch_emptydataset_test\".\"lineitem\" ( \"l_orderkey\"    INT , "
            + "                             \"l_partkey\"     INT , "
            + "                             \"l_suppkey\"     INT , "
            + "                             \"l_linenumber\"  INT , "
            + "                             \"l_quantity\"    DECIMAL(15,2) , "
            + "                             \"l_extendedprice\"  DECIMAL(15,2) , "
            + "                             \"l_discount\"    DECIMAL(15,2) , "
            + "                             \"l_tax\"         DECIMAL(15,2) , "
            + "                             \"l_returnflag\"  CHAR(1) , "
            + "                             \"l_linestatus\"  CHAR(1) , "
            + "                             \"l_shipdate\"    DATE , "
            + "                             \"l_commitdate\"  DATE , "
            + "                             \"l_receiptdate\" DATE , "
            + "                             \"l_shipinstruct\" CHAR(25) , "
            + "                             \"l_shipmode\"     CHAR(10) , "
            + "                             \"l_comment\"      VARCHAR(44), "
            + "                             \"l_dummy\" varchar(10))");
  }

  @Test
  public void test1() throws SQLException, VerdictDBException, IOException {
    // Create Scramble table
    stmt.execute(
        String.format("DROP TABLE IF EXISTS \"%s\".\"lineitem_scrambled\"", schema));
    JdbcConnection h2conn = new JdbcConnection(conn, new H2Syntax());
    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(h2conn, schema, schema, (long) 100);
    // uniform scrambling
    try {
      scrambler.scramble(
          schema, "lineitem", schema, "lineitem_scrambled", "uniform");
      fail();
    } catch (RuntimeException e) {
      if (!(e.getCause() instanceof VerdictDBException)) {
        fail();
      }
    }

    // hash scrambling
    try {
      scrambler.scramble(
          schema, "lineitem", schema, "lineitem_hash_scrambled", "hash", "l_orderkey");
      fail();
    } catch (RuntimeException e) {
      if (!(e.getCause() instanceof VerdictDBException)) {
        fail();
      }
    }
  }


}

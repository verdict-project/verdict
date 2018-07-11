package org.verdictdb.core.scrambling;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.coordinator.ScramblingCoordinator;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;

public class FastConvergeScramblingCoordinatorTest {
  private static Connection mysqlConn;

  private static Statement mysqlStmt;

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_UESR;

  private static final String MYSQL_PASSWORD = "";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
      MYSQL_UESR = "root";
    } else {
      MYSQL_HOST = "localhost";
      MYSQL_UESR = "root";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    mysqlConn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
    mysqlStmt = mysqlConn.createStatement();

    mysqlStmt.execute("create schema if not exists tpch");
    mysqlStmt.execute("drop table if exists tpch.lineitem");
    mysqlStmt.execute(
        "create table if not exists tpch.lineitem (" +
        "  l_orderkey       INT ,\n" +
        "  l_partkey        INT ,\n" +
        "  l_suppkey        INT ,\n" +
        "  l_linenumber     INT ,\n" +
        "  l_quantity       DECIMAL(15,2) ,\n" +
        "  l_extendedprice  DECIMAL(15,2) ,\n" +
        "  l_discount       DECIMAL(15,2) ,\n" +
        "  l_tax            DECIMAL(15,2) ,\n" +
        "  l_returnflag     CHAR(1) ,\n" +
        "  l_linestatus     CHAR(1) ,\n" +
        "  l_shipdate       DATE ,\n" +
        "  l_commitdate     DATE ,\n" +
        "  l_receiptdate    DATE ,\n" +
        "  l_shipinstruct   CHAR(25) ,\n" +
        "  l_shipmode       CHAR(10) ,\n" +
        "  l_comment        VARCHAR(44),\n" +
        "  l_dummy          VARCHAR(10))");

    mysqlStmt.execute("drop table if exists tpch.orders");
    mysqlStmt.execute(
        "create table if not exists tpch.orders (" +
        "  o_orderkey       INT ,\n" +
        "  o_custkey        INT ,\n" +
        "  o_orderstatus    CHAR(1) ,\n" +
        "  o_totalprice     DECIMAL(15,2) ,\n" +
        "  o_orderdate      DATE ,\n" +
        "  o_orderpriority  CHAR(15) ,\n" +
        "  o_clerk          CHAR(15) ,\n" +
        "  o_shippriority   INT ,\n" +
        "  o_comment        VARCHAR(79),\n" +
        "  o_dummy          VARCHAR(10))");

    // import data
    File dataFile = new File("src/test/resources/tpch_test_data/lineitem.tbl");
    String dataFilePath = dataFile.getAbsolutePath();
    mysqlStmt.execute(String.format("LOAD DATA INFILE '%s' INTO TABLE tpch.lineitem COLUMNS TERMINATED BY '|'", dataFilePath));
    
    dataFile = new File("src/test/resources/tpch_test_data/orders.tbl");
    dataFilePath = dataFile.getAbsolutePath();
    mysqlStmt.execute(String.format("LOAD DATA INFILE '%s' INTO TABLE tpch.orders COLUMNS TERMINATED BY '|'", dataFilePath));

    //    File schemaFile = new File("src/test/resources/tpch_test_data/tpch-schema-data.sql");
    //  String schemas = Files.toString(schemaFile, Charsets.UTF_8);
    //  for (String schema : schemas.split(";")) {
    //    schema += ";"; // add semicolon at the end
    //    schema = schema.trim();
    //    shell.execute(schema);
    //  }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    mysqlStmt.execute("drop table if exists tpch.lineitem");
    mysqlStmt.execute("drop table if exists tpch.lineitem_scrambled");
    mysqlStmt.execute("drop table if exists tpch.orders");
    mysqlStmt.execute("drop table if exists tpch.orders_scrambled");
    mysqlStmt.execute("drop schema if exists tpch");
  }
  
  @Test
  public void sanityCheck() throws VerdictDBDbmsException {
    DbmsConnection conn = new JdbcConnection(mysqlConn);
    DbmsQueryResult result = conn.execute("select * from tpch.lineitem");
    int rowCount = 0;
    while (result.next()) {
      rowCount++;
    }
    assertEquals(1000, rowCount);
  }

  @Test
  public void testScramblingCoordinatorLineitem() throws VerdictDBException {
    testScramblingCoordinator("lineitem");
  }

  @Test
  public void testScramblingCoordinatorOrders() throws VerdictDBException {
    testScramblingCoordinator("orders");
  }
  
  @Test
  public void testScramblingCoordinatorLineitemWithPrimaryColumn() throws VerdictDBException {
    testScramblingCoordinatorWithPrimaryColumn("lineitem", "l_shipdate");
  }

  @Test
  public void testScramblingCoordinatorOrdersWithPrimaryColumn() throws VerdictDBException {
    testScramblingCoordinatorWithPrimaryColumn("orders", "o_orderdate");
  }

  public void testScramblingCoordinator(String tablename) throws VerdictDBException {
    DbmsConnection conn = new JdbcConnection(mysqlConn);

    String scrambleSchema = "tpch";
    String scratchpadSchema = "tpch";
    long blockSize = 100;
    ScramblingCoordinator scrambler = 
        new ScramblingCoordinator(conn, scrambleSchema, scratchpadSchema, blockSize);

    // perform scrambling
    String originalSchema = "tpch";
    String originalTable = tablename;
    String scrambledTable = tablename + "_scrambled";
    conn.execute(String.format("drop table if exists tpch.%s", scrambledTable));
    ScrambleMeta meta = scrambler.scramble(originalSchema, originalTable, "fastconverge");

    // tests
    List<Pair<String, String>> originalColumns = conn.getColumns("tpch", originalTable);
    List<Pair<String, String>> columns = conn.getColumns("tpch", scrambledTable);
    for (int i = 0; i < originalColumns.size(); i++) {
      assertEquals(originalColumns.get(i).getLeft(), columns.get(i).getLeft());
      assertEquals(originalColumns.get(i).getRight(), columns.get(i).getRight());
    }
    assertEquals(originalColumns.size()+2, columns.size());

    List<String> partitions = conn.getPartitionColumns("tpch", scrambledTable);
    assertEquals(Arrays.asList("verdictdbblock"), partitions);

    DbmsQueryResult result1 = conn.execute(String.format("select count(*) from tpch.%s", originalTable));
    DbmsQueryResult result2 = conn.execute(String.format("select count(*) from tpch.%s", scrambledTable));
    result1.next();
    result2.next();
    assertEquals(result1.getInt(0), result2.getInt(0));

    DbmsQueryResult result = conn.execute(String.format("select min(verdictdbblock), max(verdictdbblock) from tpch.%s", scrambledTable));
    result.next();
    assertEquals(0, result.getInt(0));
    assertEquals((int) Math.ceil(result2.getInt(0) / (float) blockSize) - 1, result.getInt(1));
  }
  
  public void testScramblingCoordinatorWithPrimaryColumn(
      String tablename, String primaryColumn) 
          throws VerdictDBException {
    DbmsConnection conn = new JdbcConnection(mysqlConn);

    String scrambleSchema = "tpch";
    String scratchpadSchema = "tpch";
    long blockSize = 100;
    ScramblingCoordinator scrambler = 
        new ScramblingCoordinator(conn, scrambleSchema, scratchpadSchema, blockSize);

    // perform scrambling
    String originalSchema = "tpch";
    String originalTable = tablename;
    String scrambledTable = tablename + "_scrambled";
    conn.execute(String.format("drop table if exists tpch.%s", scrambledTable));
    ScrambleMeta meta = scrambler.scramble(originalSchema, originalTable, "fastconverge", primaryColumn);

    // tests
    List<Pair<String, String>> originalColumns = conn.getColumns("tpch", originalTable);
    List<Pair<String, String>> columns = conn.getColumns("tpch", scrambledTable);
    for (int i = 0; i < originalColumns.size(); i++) {
      assertEquals(originalColumns.get(i).getLeft(), columns.get(i).getLeft());
      assertEquals(originalColumns.get(i).getRight(), columns.get(i).getRight());
    }
    assertEquals(originalColumns.size()+2, columns.size());

    List<String> partitions = conn.getPartitionColumns("tpch", scrambledTable);
    assertEquals(Arrays.asList("verdictdbblock"), partitions);

    DbmsQueryResult result1 = conn.execute(String.format("select count(*) from tpch.%s", originalTable));
    DbmsQueryResult result2 = conn.execute(String.format("select count(*) from tpch.%s", scrambledTable));
    result1.next();
    result2.next();
    assertEquals(result1.getInt(0), result2.getInt(0));

    DbmsQueryResult result = conn.execute(String.format("select min(verdictdbblock), max(verdictdbblock) from tpch.%s", scrambledTable));
    result.next();
    assertEquals(0, result.getInt(0));
    assertEquals((int) Math.ceil(result2.getInt(0) / (float) blockSize) - 1, result.getInt(1));
  }

}

package org.verdictdb.coordinator;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ImpalaUniformScramblingCoordinatorTest {

  private static Connection impalaConn;

  private static Statement impalaStmt;

  private static final String IMPALA_HOST;

  // to avoid possible conflicts among concurrent tests
  private static final String IMPALA_DATABASE =
      "scrambling_coordinator_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String IMPALA_USER = "";

  private static final String IMPALA_PASSWORD = "";

  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  @BeforeClass
  public static void setupImpalaDatabase()
      throws SQLException, VerdictDBDbmsException, IOException {
    String impalaConnectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    impalaConn =
        DatabaseConnectionHelpers.setupImpala(
            impalaConnectionString, IMPALA_USER, IMPALA_PASSWORD, IMPALA_DATABASE);
    //    impalaStmt = impalaConn.createStatement();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    impalaConn
        .createStatement()
        .execute(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", IMPALA_DATABASE));
    impalaConn.close();
  }

  @Test
  public void sanityCheck() throws VerdictDBDbmsException {
    DbmsConnection dbmsConn = JdbcConnection.create(impalaConn);
    //    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "nation"));
    //    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "region"));
    //    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "part"));
    //    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "supplier"));
    //    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "partsupp"));
    //    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "customer"));
    //    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "orders"));
    //    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "lineitem"));

    //    assertEquals(5, dbmsConn.getColumns(IMPALA_DATABASE, "nation").size());
    //    assertEquals(4, dbmsConn.getColumns(IMPALA_DATABASE, "region").size());
    //    assertEquals(10, dbmsConn.getColumns(IMPALA_DATABASE, "part").size());
    //    assertEquals(8, dbmsConn.getColumns(IMPALA_DATABASE, "supplier").size());
    //    assertEquals(6, dbmsConn.getColumns(IMPALA_DATABASE, "partsupp").size());
    //    assertEquals(9, dbmsConn.getColumns(IMPALA_DATABASE, "customer").size());
    //    assertEquals(10, dbmsConn.getColumns(IMPALA_DATABASE, "orders").size());
    //    assertEquals(17, dbmsConn.getColumns(IMPALA_DATABASE, "lineitem").size());

    DbmsQueryResult rs =
        dbmsConn.execute(String.format("select * from %s.lineitem", IMPALA_DATABASE));
    assertEquals(1000, rs.getRowCount());
  }

  @Test
  public void testScramblingCoordinatorLineitem() throws VerdictDBException {
    testScramblingCoordinator("lineitem");
  }

  @Test
  public void testScramblingCoordinatorOrders() throws VerdictDBException {
    testScramblingCoordinator("orders");
  }

  public void testScramblingCoordinator(String tablename) throws VerdictDBException {
    JdbcConnection conn = JdbcConnection.create(impalaConn);
    //    conn.setOutputDebugMessage(true);

    String scrambleSchema = IMPALA_DATABASE;
    String scratchpadSchema = IMPALA_DATABASE;
    long blockSize = 100;
    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(conn, scrambleSchema, scratchpadSchema, blockSize);

    // perform scrambling
    String originalSchema = IMPALA_DATABASE;
    String originalTable = tablename;
    String scrambledTable = tablename + "_scrambled";
    conn.execute(String.format("drop table if exists %s.%s", IMPALA_DATABASE, scrambledTable));
    scrambler.scramble(originalSchema, originalTable, originalSchema, scrambledTable, "uniform");

    // tests
    List<Pair<String, String>> originalColumns = conn.getColumns(IMPALA_DATABASE, originalTable);
    List<Pair<String, String>> columns = conn.getColumns(IMPALA_DATABASE, scrambledTable);
    for (int i = 0; i < originalColumns.size(); i++) {
      assertEquals(originalColumns.get(i).getLeft(), columns.get(i).getLeft());
      assertEquals(originalColumns.get(i).getRight(), columns.get(i).getRight());
    }
    assertEquals(originalColumns.size() + 2, columns.size());

    List<String> partitions = conn.getPartitionColumns(IMPALA_DATABASE, scrambledTable);
    assertEquals(Arrays.asList("verdictdbblock"), partitions);

    DbmsQueryResult result1 =
        conn.execute(String.format("select count(*) from %s.%s", IMPALA_DATABASE, originalTable));
    DbmsQueryResult result2 =
        conn.execute(String.format("select count(*) from %s.%s", IMPALA_DATABASE, scrambledTable));
    result1.next();
    result2.next();
    assertEquals(result1.getInt(0), result2.getInt(0));

    DbmsQueryResult result =
        conn.execute(
            String.format(
                "select min(verdictdbblock), max(verdictdbblock) from %s.%s",
                IMPALA_DATABASE, scrambledTable));

    //    // DEBUG
    //    DbmsQueryResult getall = conn.execute(
    //        String.format("select verdictdbblock from %s.%s", IMPALA_DATABASE, scrambledTable));
    //    getall.printContent();

    result.next();
    assertEquals(0, result.getInt(0));
    // assertEquals((int) Math.ceil(result2.getInt(0) / (float) blockSize) - 1, result.getInt(1));
  }
}

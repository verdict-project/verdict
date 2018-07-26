package org.verdictdb.coordinator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.SparkConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SparkUniformScramblingCoordinatorTest {
  
  static SparkSession spark;
  
  static final String TEST_SCHEMA = "scrambling_coordinator_test";
  
  static DbmsConnection conn;
  
  @BeforeClass
  public static void setupSpark() throws SQLException, VerdictDBDbmsException {
    String appname = "scramblingCoordinatorTest";
    spark = DatabaseConnectionHelpers.setupSpark(appname, TEST_SCHEMA);
    conn = new SparkConnection(spark);
  }
  
  @AfterClass
  public static void tearDown() {
    spark.sql(String.format("DROP SCHEMA IF EXISTS %s CASCADE", TEST_SCHEMA));
  }

  @Test
  public void sanityCheck() throws VerdictDBDbmsException {
//    System.out.println(conn.getTables(TEST_SCHEMA));
    DbmsQueryResult result = conn.execute(String.format("select * from `%s`.lineitem", TEST_SCHEMA));
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

  public void testScramblingCoordinator(String tablename) throws VerdictDBException {

    String scrambleSchema = TEST_SCHEMA;
    String scratchpadSchema = TEST_SCHEMA;
    long blockSize = 100;
    ScramblingCoordinator scrambler = new ScramblingCoordinator(conn, scrambleSchema, scratchpadSchema, blockSize);

    // perform scrambling
    String originalSchema = TEST_SCHEMA;
    String originalTable = tablename;
    String scrambledTable = tablename + "_scrambled";
    conn.execute(String.format("drop table if exists %s.%s", TEST_SCHEMA, scrambledTable));
    scrambler.scramble(originalSchema, originalTable, originalSchema, scrambledTable, "uniform");

    // tests
    List<Pair<String, String>> originalColumns = conn.getColumns(TEST_SCHEMA, originalTable);
    List<Pair<String, String>> columns = conn.getColumns(TEST_SCHEMA, scrambledTable);
    
    System.out.println(originalColumns);
    System.out.println(columns);
    
    for (int i = 0; i < originalColumns.size(); i++) {
      assertEquals(originalColumns.get(i).getLeft(), columns.get(i).getLeft());
      assertEquals(originalColumns.get(i).getRight(), columns.get(i).getRight());
    }
    assertEquals(originalColumns.size()+2, columns.size());

    List<String> partitions = conn.getPartitionColumns(TEST_SCHEMA, scrambledTable);
    assertEquals(Arrays.asList("verdictdbblock"), partitions);

    DbmsQueryResult result1 = 
        conn.execute(String.format("select count(*) from %s.%s", TEST_SCHEMA, originalTable));
    DbmsQueryResult result2 = 
        conn.execute(String.format("select count(*) from %s.%s", TEST_SCHEMA, scrambledTable));
    result1.next();
    result2.next();
    assertEquals(result1.getInt(0), result2.getInt(0));

    DbmsQueryResult result = 
        conn.execute(
            String.format("select min(verdictdbblock), max(verdictdbblock) from %s.%s", 
                TEST_SCHEMA, scrambledTable));
    result.next();
    assertEquals(0, result.getInt(0));
    assertEquals((int) Math.ceil(result2.getInt(0) / (float) blockSize) - 1, result.getInt(1));
  }

}

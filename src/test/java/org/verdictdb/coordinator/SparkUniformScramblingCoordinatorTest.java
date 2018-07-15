package org.verdictdb.coordinator;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.SparkConnection;
import org.verdictdb.exception.VerdictDBDbmsException;

public class SparkUniformScramblingCoordinatorTest {
  
  static SparkSession spark;
  
  static final String TEST_SCHEMA = "scrambling_coordinator_test";
  
  static SparkConnection conn;
  
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

}

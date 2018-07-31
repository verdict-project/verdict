package org.verdictdb.coordinator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

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

public class PostgreSqlUniformScramblingCoordinatorTest {

  private static Connection postgresConn;

  private static Statement postgresStmt;

  private static final String POSTGRES_HOST;

  private static final String POSTGRES_DATABASE = "test";

  private static final String POSTGRES_SCHEMA = "scrambling_coordinator_test";

  private static final String POSTGRES_USER = "postgres";

  private static final String POSTGRES_PASSWORD = "";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      POSTGRES_HOST = "postgres";
    } else {
      POSTGRES_HOST = "localhost";
    }
  }

  @BeforeClass
  public static void setupPostgresDatabase() throws SQLException, VerdictDBDbmsException, IOException {
    String postgresConnectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    postgresConn =
        DatabaseConnectionHelpers.setupPostgresql(
            postgresConnectionString, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_SCHEMA);
    postgresStmt = postgresConn.createStatement();
    postgresStmt.execute(String.format("create schema if not exists \"%s\"", POSTGRES_SCHEMA));
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    postgresStmt.execute(String.format("drop schema if exists \"%s\" CASCADE", POSTGRES_SCHEMA));
  }

  @Test
  public void sanityCheck() throws VerdictDBDbmsException {
    DbmsConnection conn = JdbcConnection.create(postgresConn);
    DbmsQueryResult result = conn.execute(String.format("select * from \"%s\".lineitem", POSTGRES_SCHEMA));
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
    DbmsConnection conn = JdbcConnection.create(postgresConn);

    String scrambleSchema = POSTGRES_SCHEMA;
    String scratchpadSchema = POSTGRES_SCHEMA;
    long blockSize = 100;
    ScramblingCoordinator scrambler = new ScramblingCoordinator(conn, scrambleSchema, scratchpadSchema, blockSize);

    // perform scrambling
    String originalSchema = POSTGRES_SCHEMA;
    String originalTable = tablename;
    String scrambledTable = tablename + "_scrambled";
    conn.execute(String.format("drop table if exists %s.%s", POSTGRES_SCHEMA, scrambledTable));
    scrambler.scramble(originalSchema, originalTable, originalSchema, scrambledTable, "uniform");

    // tests
    List<Pair<String, String>> originalColumns = conn.getColumns(POSTGRES_SCHEMA, originalTable);
    List<Pair<String, String>> columns = conn.getColumns(POSTGRES_SCHEMA, scrambledTable);
    for (int i = 0; i < originalColumns.size(); i++) {
      assertEquals(originalColumns.get(i).getLeft(), columns.get(i).getLeft());
      assertEquals(originalColumns.get(i).getRight(), columns.get(i).getRight());
    }
    assertEquals(originalColumns.size()+2, columns.size());

    List<String> partitions = conn.getPartitionColumns(POSTGRES_SCHEMA, scrambledTable);
    assertEquals(Arrays.asList("verdictdbblock"), partitions);

    DbmsQueryResult result1 =
        conn.execute(String.format("select count(*) from %s.%s", POSTGRES_SCHEMA, originalTable));
    DbmsQueryResult result2 =
        conn.execute(String.format("select count(*) from %s.%s", POSTGRES_SCHEMA, scrambledTable));
    result1.next();
    result2.next();
    assertEquals(result1.getInt(0), result2.getInt(0));

    DbmsQueryResult result =
        conn.execute(
            String.format("select min(verdictdbblock), max(verdictdbblock) from %s.%s",
                POSTGRES_SCHEMA, scrambledTable));
    result.next();
    assertEquals(0, result.getInt(0));
    assertEquals((int) Math.ceil(result2.getInt(0) / (float) blockSize) - 1, result.getInt(1));
  }


}

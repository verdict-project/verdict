package org.verdictdb.core.scrambling;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PostgresUniformScramblingPlanTest {

  static Connection conn;
  private static Statement stmt;
  private static final String POSTGRES_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      POSTGRES_HOST = "postgres";
    } else {
      POSTGRES_HOST = "localhost";
    }
  }

  private static final String POSTGRES_DATABASE = "test";

  private static final String POSTGRES_USER = "postgres";

  private static final String POSTGRES_PASSWORD = "";

  @BeforeClass
  public static void setupDatabase() throws SQLException {
    String postgresConnectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    conn = DriverManager.getConnection(postgresConnectionString, POSTGRES_USER, POSTGRES_PASSWORD);

    conn.createStatement().execute("create schema if not exists oldschema");
    conn.createStatement().execute("create schema if not exists newschema");
    conn.createStatement().execute("drop table if exists oldschema.oldtable");
    conn.createStatement()
        .execute("create table if not exists oldschema.oldtable (id smallint, title text)");
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    conn.createStatement().execute("drop table if exists oldschema.oldtable");
    conn.createStatement().execute("drop schema if exists oldschema");
    conn.createStatement().execute("drop table if exists newschema.newtable");
    conn.createStatement().execute("drop schema if exists newschema");
  }

  @Test
  public void testUniformScramblingPlanEmptyTable() throws VerdictDBException, SQLException {
    conn.createStatement().execute("delete from oldschema.oldtable");

    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    int blockSize = 3;
    int maxBlockCount = 100;
    ScramblingMethod method = new UniformScramblingMethod(blockSize, maxBlockCount);
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "tiercolumn");
    options.put("blockColumnName", "blockcolumn");

    conn.createStatement()
        .execute(String.format("drop table if exists %s.%s", newSchemaName, newTableName));
    ScramblingPlan plan =
        ScramblingPlan.create(
            newSchemaName, newTableName,
            oldSchemaName, oldTableName,
            method, options);
    //    System.out.println(plan.getReportingNode());

    DbmsConnection dbmsConn = JdbcConnection.create(conn);
    ExecutablePlanRunner.runTillEnd(dbmsConn, plan);
  }

  @Test
  public void testUniformScramblingPlanNonEmptyTable() throws VerdictDBException, SQLException {
    int numRow = 10;
    for (int i = 0; i < numRow; i++) {
      conn.createStatement()
          .execute(
              String.format(
                  "insert into oldschema.oldtable values (%d, '%s')",
                  i, RandomStringUtils.randomAlphanumeric(4)));
    }

    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    int blockSize = 2;
    ScramblingMethod method = new UniformScramblingMethod(blockSize);
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "tiercolumn");
    options.put("blockColumnName", "blockcolumn");
    options.put("blockCount", "3"); // dongyoungy: This is currently not supported.

    conn.createStatement()
        .execute(String.format("drop table if exists %s.%s", newSchemaName, newTableName));
    ScramblingPlan plan =
        ScramblingPlan.create(
            newSchemaName, newTableName,
            oldSchemaName, oldTableName,
            method, options);
    //    System.out.println(plan.getReportingNode());

    DbmsConnection dbmsConn = JdbcConnection.create(conn);
    ExecutablePlanRunner.runTillEnd(dbmsConn, plan);

    int blockCount = method.getBlockCount();

    DbmsQueryResult result =
        dbmsConn.execute(String.format("select * from %s.%s", newSchemaName, newTableName));
    result.printContent();

    int partitionRowTotal = 0;
    for (int i = 0; i < blockCount; ++i) {
      DbmsQueryResult r =
          dbmsConn.execute(
              String.format(
                  "select count(*) from %s.%s" + PostgresqlSyntax.CHILD_PARTITION_TABLE_SUFFIX,
                  newSchemaName,
                  newTableName,
                  i));
      if (r.next()) {
        partitionRowTotal += r.getInt(0);
      }
    }

    assertEquals(numRow, partitionRowTotal);

    conn.createStatement().execute("delete from oldschema.oldtable");
  }
}

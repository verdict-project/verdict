/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.scramblingquerying;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.verdictdb.category.PrestoTests;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Created by Dong Young Yoon on 8/16/18. */
@Category(PrestoTests.class)
public class PrestoUniformScramblingQueryTest {

  private static VerdictOption options = new VerdictOption();

  private static final String VERDICT_META_SCHEMA =
      "verdictdbmetaschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
  private static final String VERDICT_TEMP_SCHEMA =
      "verdictdbtempschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static Connection conn, vc;

  private static final String PRESTO_HOST;

  private static final String PRESTO_CATALOG;

  private static final String PRESTO_USER;

  private static final String PRESTO_PASSWORD;

  static {
    PRESTO_HOST = System.getenv("VERDICTDB_TEST_PRESTO_HOST");
    PRESTO_CATALOG = System.getenv("VERDICTDB_TEST_PRESTO_CATALOG");
    PRESTO_USER = System.getenv("VERDICTDB_TEST_PRESTO_USER");
    PRESTO_PASSWORD = System.getenv("VERDICTDB_TEST_PRESTO_PASSWORD");
  }

  private static final String SCHEMA_NAME =
      "verdictdb_tpch_query_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  public PrestoUniformScramblingQueryTest() {}

  @BeforeClass
  public static void setupDatabases() throws SQLException, VerdictDBDbmsException, IOException {
    options.setVerdictMetaSchemaName(VERDICT_META_SCHEMA);
    options.setVerdictTempSchemaName(VERDICT_TEMP_SCHEMA);
    options.setVerdictConsoleLogLevel("all");
    setupPresto();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    ResultSet rs =
        conn.createStatement().executeQuery(String.format("SHOW TABLES IN %s", SCHEMA_NAME));
    while (rs.next()) {
      conn.createStatement()
          .execute(String.format("DROP TABLE IF EXISTS %s.%s", SCHEMA_NAME, rs.getString(1)));
    }
    rs.close();
    conn.createStatement().execute(String.format("DROP SCHEMA IF EXISTS %s", SCHEMA_NAME));
    conn.createStatement()
        .execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", VERDICT_META_SCHEMA));
    rs =
        conn.createStatement()
            .executeQuery(String.format("SHOW TABLES IN %s", VERDICT_META_SCHEMA));
    while (rs.next()) {
      conn.createStatement()
          .execute(
              String.format("DROP TABLE IF EXISTS %s.%s", VERDICT_META_SCHEMA, rs.getString(1)));
    }
    rs.close();
    conn.createStatement().execute(String.format("DROP SCHEMA IF EXISTS %s", VERDICT_META_SCHEMA));
    conn.createStatement()
        .execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", VERDICT_TEMP_SCHEMA));
    rs =
        conn.createStatement()
            .executeQuery(String.format("SHOW TABLES IN %s", VERDICT_TEMP_SCHEMA));
    while (rs.next()) {
      conn.createStatement()
          .execute(
              String.format("DROP TABLE IF EXISTS %s.%s", VERDICT_TEMP_SCHEMA, rs.getString(1)));
    }
    rs.close();
    conn.createStatement().execute(String.format("DROP SCHEMA IF EXISTS %s", VERDICT_TEMP_SCHEMA));
  }

  private static Connection setupPresto() throws SQLException, VerdictDBDbmsException, IOException {
    String connectionString =
        String.format("jdbc:presto://%s/%s/default", PRESTO_HOST, PRESTO_CATALOG);
    String verdictConnectionString =
        String.format(
            "jdbc:verdict:presto://%s/%s/default;verdictdbtempschema=%s&verdictdbmetaschema=%s",
            PRESTO_HOST, PRESTO_CATALOG, VERDICT_TEMP_SCHEMA, VERDICT_META_SCHEMA);
    conn =
        DatabaseConnectionHelpers.setupPresto(
            connectionString, PRESTO_USER, PRESTO_PASSWORD, SCHEMA_NAME);
    vc = DriverManager.getConnection(verdictConnectionString, PRESTO_USER, PRESTO_PASSWORD);
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.orders_scramble FROM %s.orders", SCHEMA_NAME, SCHEMA_NAME));
    return conn;
  }

  @Test
  public void checkExistingPartitionTest() throws SQLException {
    String sql = String.format("DESCRIBE %s.orders_scramble", SCHEMA_NAME);
    ResultSet rs = conn.createStatement().executeQuery(sql);
    Map<String, String> results = new HashMap<>();
    while (rs.next()) {
      String column = rs.getString(1);
      String extra = rs.getString(3);
      results.put(column, extra);
      if (column.equalsIgnoreCase("o_dummy")) {
        assertTrue(extra.toLowerCase().contains("partition"));
      }
      if (column.equalsIgnoreCase("verdictdbblock")) {
        assertTrue(extra.toLowerCase().contains("partition"));
      }
    }
    assertTrue(results.containsKey("o_dummy"));
    assertTrue(results.containsKey("verdictdbblock"));
    assertTrue(results.get("o_dummy").toLowerCase().contains("partition"));
    assertTrue(results.get("verdictdbblock").toLowerCase().contains("partition"));

    // all verdictdbblock must be 0
    sql = String.format("SELECT verdictdbblock FROM %s.orders_scramble", SCHEMA_NAME);
    rs = conn.createStatement().executeQuery(sql);
    while (rs.next()) {
      int val = rs.getInt(1);
      assertEquals(0, val);
    }
  }

  @Test
  public void insertScrambleTest() throws SQLException {
    int rowBefore = 0, rowAfter = 0;
    ResultSet rs =
        conn.createStatement()
            .executeQuery(String.format("SELECT COUNT(*) FROM %s.orders_scramble", SCHEMA_NAME));
    if (rs.next()) {
      rowBefore = rs.getInt(1);
    }
    vc.createStatement()
        .execute(
            String.format(
                "INSERT SCRAMBLE %s.orders_scramble WHERE o_totalprice < 10000", SCHEMA_NAME));

    rs =
        conn.createStatement()
            .executeQuery(String.format("SELECT COUNT(*) FROM %s.orders_scramble", SCHEMA_NAME));
    if (rs.next()) {
      rowAfter = rs.getInt(1);
    }

    assertTrue(rowAfter > rowBefore);
  }

  @Test
  public void runSimpleAggQueryTest() throws SQLException {
    String sql =
        String.format(
            "SELECT AVG(\"orders\".\"o_totalprice\") as \"avg_price\"\n"
                + "FROM \"%s\".\"orders\" \"orders\"\n",
            SCHEMA_NAME, SCHEMA_NAME);
    ResultSet rs1 = vc.createStatement().executeQuery(sql);
    ResultSet rs2 = conn.createStatement().executeQuery(sql);
    int columnCount = rs2.getMetaData().getColumnCount();
    int columnCount2 = rs1.getMetaData().getColumnCount();
    assertEquals(columnCount, columnCount2);
    while (rs1.next() && rs2.next()) {
      assertEquals(Double.parseDouble(rs1.getString(1)), Double.parseDouble(rs2.getString(1)), 0.1);
      System.out.println(String.format("%s : %s", rs1.getString(1), rs2.getString(1)));
    }
    assertEquals(rs1.next(), rs2.next());
  }

  @Test
  public void runQueryWithHavingTest() throws SQLException {
    String sql =
        String.format(
            "SELECT AVG(\"orders\".\"o_totalprice\") as \"avg_price\"\n"
                + "FROM \"%s\".\"orders\" \"orders\"\n"
                + "GROUP BY o_orderstatus\n"
                + "HAVING avg(\"orders\".\"o_orderkey\") > 10\n"
                + "ORDER BY avg(o_orderkey) + min(o_orderkey) + max(o_orderkey)\n",
            SCHEMA_NAME, SCHEMA_NAME);
    ResultSet rs1 = vc.createStatement().executeQuery(sql);
    ResultSet rs2 = conn.createStatement().executeQuery(sql);
    int columnCount = rs2.getMetaData().getColumnCount();
    int columnCount2 = rs1.getMetaData().getColumnCount();
    assertEquals(columnCount, columnCount2);
    while (rs1.next() && rs2.next()) {
      assertEquals(Double.parseDouble(rs1.getString(1)), Double.parseDouble(rs2.getString(1)), 0.1);
      System.out.println(String.format("%s : %s", rs1.getString(1), rs2.getString(1)));
    }
    assertEquals(rs1.next(), rs2.next());
  }

  @Test
  public void runQueryWithHavingTest2() throws SQLException {
    String sql =
        String.format(
            "SELECT o_orderpriority, COUNT(\"orders\".\"o_orderkey\") as \"cnt\"\n"
                + "FROM \"%s\".\"orders\" \"orders\"\n"
                + "GROUP BY o_orderpriority\n"
                + "HAVING max(\"orders\".\"o_totalprice\") >=\n"
                + "1000\n"
                + "ORDER BY o_orderpriority\n",
            SCHEMA_NAME, SCHEMA_NAME);
    ResultSet rs2 = conn.createStatement().executeQuery(sql);
    ResultSet rs1 = vc.createStatement().executeQuery(sql);
    int columnCount = rs2.getMetaData().getColumnCount();
    int columnCount2 = rs1.getMetaData().getColumnCount();
    assertEquals(columnCount, columnCount2);
    while (rs1.next() && rs2.next()) {
      assertEquals(rs1.getString(1), rs2.getString(1));
      assertEquals(rs1.getInt(2), rs2.getInt(2));
    }
    assertEquals(rs1.next(), rs2.next());
  }

  @Test(expected = SQLException.class)
  public void runPrestoNoCatalogTest() throws SQLException {
    String verdictConnectionString = String.format("jdbc:verdict:presto://%s", PRESTO_HOST);
    Connection tempConn =
        DriverManager.getConnection(verdictConnectionString, PRESTO_USER, PRESTO_PASSWORD);
  }

  @Test(expected = SQLException.class)
  public void runQueryWithHavingSubqueryTest() throws SQLException {
    String sql =
        String.format(
            "SELECT o_orderpriority, COUNT(\"orders\".\"o_orderkey\") as \"cnt\"\n"
                + "FROM \"%s\".\"orders\" \"orders\"\n"
                + "GROUP BY o_orderpriority\n"
                + "HAVING max(\"orders\".\"o_totalprice\") >=\n"
                + "(SELECT avg(\"orders\".\"o_totalprice\") FROM \"%s\".\"orders\" \"orders\")\n"
                + "ORDER BY o_orderpriority\n",
            SCHEMA_NAME, SCHEMA_NAME);
    ResultSet rs2 = conn.createStatement().executeQuery(sql);
    ResultSet rs1 = vc.createStatement().executeQuery(sql);
    int columnCount = rs2.getMetaData().getColumnCount();
    int columnCount2 = rs1.getMetaData().getColumnCount();
    assertEquals(columnCount, columnCount2);
    while (rs1.next() && rs2.next()) {
      assertEquals(rs1.getString(1), rs2.getString(1));
      assertEquals(rs1.getInt(2), rs2.getInt(2));
    }
    assertEquals(rs1.next(), rs2.next());
  }
}

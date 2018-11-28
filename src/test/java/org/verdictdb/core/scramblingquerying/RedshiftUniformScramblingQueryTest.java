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

/** Created by Dong Young Yoon on 8/16/18. */
public class RedshiftUniformScramblingQueryTest {

  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, Connection> vcMap = new HashMap<>();

  private static Map<String, String> schemaMap = new HashMap<>();

  private static VerdictOption options = new VerdictOption();

  private static final String VERDICT_META_SCHEMA =
      "verdictdbmetaschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
  private static final String VERDICT_TEMP_SCHEMA =
      "verdictdbtempschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static Connection conn, vc;

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
  }

  private static final String SCHEMA_NAME =
      "verdictdb_tpch_query_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  public RedshiftUniformScramblingQueryTest() {}

  @BeforeClass
  public static void setupDatabases() throws SQLException, VerdictDBDbmsException, IOException {
    options.setVerdictMetaSchemaName(VERDICT_META_SCHEMA);
    options.setVerdictTempSchemaName(VERDICT_TEMP_SCHEMA);
    options.setVerdictConsoleLogLevel("all");
    setupRedshift();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    conn.createStatement().execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", SCHEMA_NAME));
    conn.createStatement()
        .execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", VERDICT_META_SCHEMA));
    conn.createStatement()
        .execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", VERDICT_TEMP_SCHEMA));
  }

  private static Connection setupRedshift()
      throws SQLException, VerdictDBDbmsException, IOException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    String verdictConnectionString =
        String.format(
            "jdbc:verdict:redshift://%s/%s;verdictdbtempschema=%s;verdictdbmetaschema=%s",
            REDSHIFT_HOST, REDSHIFT_DATABASE, SCHEMA_NAME, SCHEMA_NAME);
    conn =
        DatabaseConnectionHelpers.setupRedshift(
            connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD, SCHEMA_NAME);
    vc = DriverManager.getConnection(verdictConnectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    connMap.put("redshift", conn);
    vcMap.put("redshift", vc);
    schemaMap.put("redshift", "");
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
      assertEquals(rs1.getInt(1), rs2.getInt(1));
      System.out.println(String.format("%d : %d", rs1.getInt(1), rs2.getInt(1)));
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
      assertEquals(rs1.getInt(1), rs2.getInt(1));
      System.out.println(String.format("%d : %d", rs1.getInt(1), rs2.getInt(1)));
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
                + "ORDER BY o_orderpriority + 4\n",
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
  public void runQueryWithHavingSubqueryTest() throws SQLException {
    String sql =
        String.format(
            "SELECT o_orderpriority, COUNT(\"orders\".\"o_orderkey\") as \"cnt\"\n"
                + "FROM \"%s\".\"orders\" \"orders\"\n"
                + "GROUP BY o_orderpriority\n"
                + "HAVING max(\"orders\".\"o_totalprice\") >=\n"
                + "(SELECT avg(\"orders\".\"o_totalprice\") FROM \"%s\".\"orders\" \"orders\")\n"
                + "ORDER BY o_orderpriority + 4\n",
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

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.metastore.ScrambleMetaStore;

/** Created by Dong Young Yoon on 8/16/18. */
public class RedshiftScrambleManipulationTest {

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

  public RedshiftScrambleManipulationTest() {}

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
            "jdbc:verdict:redshift://%s/%s;verdictdbtempschema=%s&verdictdbmetaschema=%s",
            REDSHIFT_HOST, REDSHIFT_DATABASE, VERDICT_TEMP_SCHEMA, VERDICT_META_SCHEMA);
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
    return conn;
  }

  @Test
  public void partialScramblesTest() throws SQLException {
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.customer_scramble FROM %s.customer SIZE 0.5 BLOCKSIZE 100",
                SCHEMA_NAME, SCHEMA_NAME));
    ResultSet rs1 =
        conn.createStatement()
            .executeQuery(String.format("SELECT SUM(c_acctbal) FROM %s.customer", SCHEMA_NAME));

    ResultSet rs2 =
        vc.createStatement()
            .executeQuery(String.format("SELECT SUM(c_acctbal) FROM %s.customer", SCHEMA_NAME));

    if (rs1.next() && rs2.next()) {
      long expected = rs1.getLong(1);
      long actual = rs2.getLong(1);
      System.out.println(expected + " : " + actual);
      assertTrue(expected * 0.25 < actual);
      assertTrue(actual < expected * 2);
    }
  }

  @Test
  public void ShowScramblesTest() throws SQLException {
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.orders_scramble1 FROM %s.orders", SCHEMA_NAME, SCHEMA_NAME));
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.orders_scramble2 FROM %s.orders SIZE 0.1 BLOCKSIZE 50",
                SCHEMA_NAME, SCHEMA_NAME));
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.orders_scramble3 FROM %s.orders", SCHEMA_NAME, SCHEMA_NAME));

    String sql = String.format("SHOW SCRAMBLES");
    ResultSet rs = vc.createStatement().executeQuery(sql);
    while (rs.next()) {
      assertEquals(rs.getString(3), SCHEMA_NAME);
      assertTrue(rs.getString(4).startsWith("orders_scramble"));
    }

    ResultSet rs1 =
        conn.createStatement()
            .executeQuery(String.format("SELECT COUNT(*) FROM %s.orders_scramble1", SCHEMA_NAME));
    ResultSet rs2 =
        conn.createStatement()
            .executeQuery(String.format("SELECT COUNT(*) FROM %s.orders_scramble2", SCHEMA_NAME));

    if (rs1.next() && rs2.next()) {
      System.out.println(rs1.getLong(1) + " : " + rs2.getLong(1));
      assertEquals(rs1.getLong(1), 258);
      assertTrue(rs2.getLong(1) < 100);
    }
  }

  @Test
  public void DropScrambleTest() throws SQLException, InterruptedException {
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.orders_scramble4 FROM %s.orders", SCHEMA_NAME, SCHEMA_NAME));
    String sql =
        String.format("DROP SCRAMBLE %s.orders_scramble4 ON %s.orders", SCHEMA_NAME, SCHEMA_NAME);
    vc.createStatement().execute(sql);

    // check whether the actual scramble table has been removed
    sql =
        String.format(
            "SELECT COUNT(*) as cnt FROM pg_tables WHERE schemaname = '%s' AND tablename = '%s'",
            SCHEMA_NAME, "orders_scramble4");
    ResultSet rs1 = conn.createStatement().executeQuery(sql);
    if (rs1.next()) {
      assertEquals(0, rs1.getInt(1));
    }

    // Check whether the metadata of the dropped scramble table is correctly inserted
    sql = String.format("SHOW SCRAMBLES");
    ResultSet rs = vc.createStatement().executeQuery(sql);
    int rowCount = 0;
    while (rs.next()) {
      assertEquals(SCHEMA_NAME, rs.getString(3));
      String scrambleName = rs.getString(4);
      if (scrambleName.equals("orders_scramble4") && rowCount == 0) {
        assertEquals("DELETED", rs.getString(6));
      }
      ++rowCount;
    }
  }

  @Test
  public void DropAllScramblesTest() throws SQLException, InterruptedException {
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.orders_scramble5 FROM %s.orders", SCHEMA_NAME, SCHEMA_NAME));
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.orders_scramble6 FROM %s.orders", SCHEMA_NAME, SCHEMA_NAME));
    String sql = String.format("DROP ALL SCRAMBLE %s.orders", SCHEMA_NAME);
    vc.createStatement().execute(sql);

    // check whether all scramble tables has been removed
    sql =
        String.format(
            "SELECT COUNT(*) as cnt FROM pg_tables WHERE schemaname = '%s' "
                + "AND tablename LIKE '%s%%'",
            SCHEMA_NAME, "orders_scramble");
    ResultSet rs1 = conn.createStatement().executeQuery(sql);
    if (rs1.next()) {
      assertEquals(0, rs1.getInt(1));
    }

    // Check whether the metadata of the dropped scramble table is correctly inserted
    sql = String.format("SHOW SCRAMBLES");
    ResultSet rs = vc.createStatement().executeQuery(sql);
    Set<String> deleted = new HashSet<>();
    int rowCount = 0;
    while (rs.next()) {
      assertEquals(SCHEMA_NAME, rs.getString(3));
      String scrambleName = rs.getString(4);
      assertTrue(scrambleName.startsWith("orders_scramble"));
      if (!deleted.contains(scrambleName)) {
        assertEquals("DELETED", rs.getString(6));
        deleted.add(scrambleName);
      }
      ++rowCount;
    }
  }

  @Test
  public void DropScrambleAndGetScrambleMetaSetTest()
      throws SQLException, VerdictDBDbmsException, InterruptedException {

    // drop all scrambled tables first.
    String sql = String.format("DROP ALL SCRAMBLE %s.orders", SCHEMA_NAME);
    vc.createStatement().execute(sql);

    // create two scrambles
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.orders_scramble7 FROM %s.orders", SCHEMA_NAME, SCHEMA_NAME));
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.orders_scramble8 FROM %s.orders", SCHEMA_NAME, SCHEMA_NAME));

    sql = String.format("DROP SCRAMBLE %s.orders_scramble7 ON %s.orders", SCHEMA_NAME, SCHEMA_NAME);
    vc.createStatement().execute(sql);

    // Test that ScrambleMetaSet from ScrambleMetaStore only contains orders_scramble8
    ScrambleMetaStore store = new ScrambleMetaStore(JdbcConnection.create(conn), options);
    int metaCount = 0;
    for (ScrambleMeta scrambleMeta : store.retrieve()) {
      assertEquals("orders_scramble8", scrambleMeta.getTableName());
      ++metaCount;
    }
    assertEquals(metaCount, 1);
  }

  @Test
  public void DropScrambleWithoutOriginalTableTest() throws SQLException, InterruptedException {
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.orders_scramble9 FROM %s.orders", SCHEMA_NAME, SCHEMA_NAME));
    String sql = String.format("DROP SCRAMBLE %s.orders_scramble9", SCHEMA_NAME, SCHEMA_NAME);
    vc.createStatement().execute(sql);

    // check whether the actual scramble table has been removed
    sql =
        String.format(
            "SELECT COUNT(*) as cnt FROM pg_tables WHERE schemaname = '%s' AND tablename = '%s'",
            SCHEMA_NAME, "orders_scramble9");
    ResultSet rs1 = conn.createStatement().executeQuery(sql);
    if (rs1.next()) {
      assertEquals(0, rs1.getInt(1));
    }

    // Check whether the metadata of the dropped scramble table is correctly inserted
    sql = String.format("SHOW SCRAMBLES");
    ResultSet rs = vc.createStatement().executeQuery(sql);

    // Check the most up-to-date metadata
    if (rs.next()) {
      assertEquals("N/A", rs.getString(1));
      assertEquals(SCHEMA_NAME, rs.getString(3));
      String scrambleName = rs.getString(4);
      if (scrambleName.equals("orders_scramble9")) {
        assertEquals("DELETED", rs.getString(6));
      }
    }
  }

  @Test
  public void DropScrambleWithoutSpecifyingSchemaTest() throws SQLException, InterruptedException {
    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.orders_scramble10 FROM %s.orders", SCHEMA_NAME, SCHEMA_NAME));
    vc.setCatalog(SCHEMA_NAME);
    Statement statement = vc.createStatement();
    String sql = String.format("DROP SCRAMBLE orders_scramble10", SCHEMA_NAME, SCHEMA_NAME);
    statement.execute(String.format("USE %s", SCHEMA_NAME));
    statement.execute(sql);

    // check whether the actual scramble table has been removed
    sql =
        String.format(
            "SELECT COUNT(*) as cnt FROM pg_tables WHERE schemaname = '%s' AND tablename = '%s'",
            SCHEMA_NAME, "orders_scramble10");
    ResultSet rs1 = conn.createStatement().executeQuery(sql);
    if (rs1.next()) {
      assertEquals(0, rs1.getInt(1));
    }

    // Check whether the metadata of the dropped scramble table is correctly inserted
    sql = String.format("SHOW SCRAMBLES");
    ResultSet rs = vc.createStatement().executeQuery(sql);

    // Check the most up-to-date metadata
    if (rs.next()) {
      assertEquals("N/A", rs.getString(1));
      assertEquals(SCHEMA_NAME, rs.getString(3));
      String scrambleName = rs.getString(4);
      if (scrambleName.equals("orders_scramble10")) {
        assertEquals("DELETED", rs.getString(6));
      }
    }
  }
}

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

package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.verdictdb.category.PrestoTests;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.exception.VerdictDBDbmsException;

/** Created by Dong Young Yoon on 8/16/18. */
@Category(PrestoTests.class)
public class PrestoSpatialQueryTest {

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
      "verdictdb_spatial_query_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String TABLE_NAME = "spatial_test";

  public PrestoSpatialQueryTest() {}

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
            "jdbc:verdict:presto://%s/%s/default;loglevel=debug;verdictdbmetaschema=%s;verdictdbtempschema=%s",
            PRESTO_HOST, PRESTO_CATALOG, SCHEMA_NAME, SCHEMA_NAME, SCHEMA_NAME);
    conn = DriverManager.getConnection(connectionString, PRESTO_USER, PRESTO_PASSWORD);
    vc = DriverManager.getConnection(verdictConnectionString, PRESTO_USER, PRESTO_PASSWORD);
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));

    conn.createStatement().execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", SCHEMA_NAME));

    conn.createStatement()
        .execute(
            String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (x int, y int)", SCHEMA_NAME, TABLE_NAME));

    conn.createStatement()
        .execute(String.format("INSERT INTO %s.%s VALUES (1,1)", SCHEMA_NAME, TABLE_NAME));
    conn.createStatement()
        .execute(String.format("INSERT INTO %s.%s VALUES (2,2)", SCHEMA_NAME, TABLE_NAME));
    conn.createStatement()
        .execute(String.format("INSERT INTO %s.%s VALUES (3,3)", SCHEMA_NAME, TABLE_NAME));

    vc.createStatement()
        .execute(
            String.format(
                "CREATE SCRAMBLE %s.spatial_scramble FROM %s.%s",
                SCHEMA_NAME, SCHEMA_NAME, TABLE_NAME));
    return conn;
  }

  @Test
  public void runSimpleSelectSTPointTest() throws SQLException {
    String sql = String.format("SELECT ST_POINT(1,2)", SCHEMA_NAME, TABLE_NAME);
    ResultSet rs1 = vc.createStatement().executeQuery(sql);
    ResultSet rs2 = conn.createStatement().executeQuery(sql);
    int columnCount = rs2.getMetaData().getColumnCount();
    int columnCount2 = rs1.getMetaData().getColumnCount();
    assertEquals(columnCount, columnCount2);
    while (rs1.next() && rs2.next()) {
      assertEquals(rs1.getObject(1), rs2.getObject(1));
      System.out.println(String.format("%s : %s", rs1.getObject(1), rs2.getObject(1)));
    }
    assertEquals(rs1.next(), rs2.next());
  }

  @Test
  public void runSimpleSelectSTContainsTest() throws SQLException {
    String sql =
        String.format(
            "SELECT * FROM %s.%s WHERE "
                + "ST_CONTAINS(ST_POLYGON('polygon ((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_POINT(x,y)) "
                + "ORDER BY x",
            SCHEMA_NAME, TABLE_NAME);
    ResultSet rs1 = vc.createStatement().executeQuery(sql);
    ResultSet rs2 = conn.createStatement().executeQuery(sql);
    int columnCount = rs2.getMetaData().getColumnCount();
    int columnCount2 = rs1.getMetaData().getColumnCount();
    //    assertEquals(columnCount, columnCount2);
    while (rs1.next() && rs2.next()) {
      System.out.println(String.format("%s : %s", rs1.getObject(1), rs2.getObject(1)));
      assertEquals(rs1.getObject(1), rs2.getObject(1));
    }
    assertEquals(rs1.next(), rs2.next());
  }

  @Test
  public void runSimpleSelectSTIntersectsTest() throws SQLException {
    String sql =
        String.format(
            "SELECT * FROM %s.%s WHERE "
                + "ST_INTERSECTS(ST_POLYGON('polygon ((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_POINT(x,y)) "
                + "ORDER BY x",
            SCHEMA_NAME, TABLE_NAME);
    ResultSet rs1 = vc.createStatement().executeQuery(sql);
    ResultSet rs2 = conn.createStatement().executeQuery(sql);
    int columnCount = rs2.getMetaData().getColumnCount();
    int columnCount2 = rs1.getMetaData().getColumnCount();
    //    assertEquals(columnCount, columnCount2);
    while (rs1.next() && rs2.next()) {
      System.out.println(String.format("%s : %s", rs1.getObject(1), rs2.getObject(1)));
      assertEquals(rs1.getObject(1), rs2.getObject(1));
    }
    assertEquals(rs1.next(), rs2.next());
  }
}

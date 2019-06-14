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

import io.prestosql.jdbc.PrestoArray;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.verdictdb.category.PrestoTests;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Created by Dong Young Yoon on 8/16/18. */
@Category(PrestoTests.class)
public class PrestoFunctionTest {

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
    PRESTO_HOST = "localhost:8080";
    PRESTO_CATALOG = "memory";
    PRESTO_USER = "root";
    PRESTO_PASSWORD = "";
  }

  private static final String SCHEMA_NAME =
      "verdictdb_presto_function_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String TABLE_NAME = "func_test";

  public PrestoFunctionTest() {}

  @BeforeClass
  public static void setupDatabases() throws SQLException, VerdictDBDbmsException, IOException {
    options.setVerdictMetaSchemaName(VERDICT_META_SCHEMA);
    options.setVerdictTempSchemaName(VERDICT_TEMP_SCHEMA);
    options.setVerdictConsoleLogLevel("all");
    setupPresto();
  }

  @AfterClass
  public static void tearDown() throws SQLException {}

  private static Connection setupPresto() throws SQLException, VerdictDBDbmsException, IOException {
    String connectionString =
        String.format("jdbc:presto://%s/%s/default", PRESTO_HOST, PRESTO_CATALOG);
    String verdictConnectionString =
        String.format(
            "jdbc:verdict:presto://%s/memory/default;verdictdbtempschema=%s&verdictdbmetaschema=%s",
            PRESTO_HOST, VERDICT_TEMP_SCHEMA, VERDICT_META_SCHEMA);
    conn = DriverManager.getConnection(connectionString, PRESTO_USER, PRESTO_PASSWORD);
    vc = DriverManager.getConnection(verdictConnectionString, PRESTO_USER, PRESTO_PASSWORD);
    return conn;
  }

  @Test
  public void runRegexLike1Test() throws SQLException {
    String sql;
    ResultSet rs;

    sql =
        String.format("SELECT * from tpch.tiny.customer WHERE regexp_like('1a 2b 14m', '\\\\d+b')");
    rs = vc.createStatement().executeQuery(sql);

    rs.close();
  }

  @Test
  public void runRegexFunctionsTest() throws SQLException {
    String sql;
    ResultSet rs;

    // REGEXP_EXTRACT_ALL (case 1)
    sql = String.format("SELECT regexp_extract_all('1a 2b 14m', '\\d+')");
    rs = vc.createStatement().executeQuery(sql);
    assertTrue(rs.next());
    Object[] arr = (Object[]) ((PrestoArray) rs.getObject(1)).getArray();
    assertEquals(3, arr.length);
    assertEquals("1", arr[0]);
    assertEquals("2", arr[1]);
    assertEquals("14", arr[2]);
    rs.close();

    // REGEXP_EXTRACT_ALL (case 2)
    sql = String.format("SELECT regexp_extract_all('1a 2b 14m', '(\\d+)([a-z]+)', 2)");
    rs = vc.createStatement().executeQuery(sql);
    assertTrue(rs.next());
    arr = (Object[]) ((PrestoArray) rs.getObject(1)).getArray();
    assertEquals(3, arr.length);
    assertEquals("a", arr[0]);
    assertEquals("b", arr[1]);
    assertEquals("m", arr[2]);
    rs.close();

    // REGEXP_EXTRACT (case 1)
    sql = String.format("SELECT regexp_extract('1a 2b 14m', '\\d+')");
    rs = vc.createStatement().executeQuery(sql);
    assertTrue(rs.next());
    assertEquals("1", rs.getObject(1));
    rs.close();

    // REGEXP_EXTRACT (case 2)
    sql = String.format("SELECT regexp_extract('1a 2b 14m', '(\\d+)([a-z]+)', 2)");
    rs = vc.createStatement().executeQuery(sql);
    assertTrue(rs.next());
    assertEquals("a", rs.getString(1));
    rs.close();

    // REGEXP_LIKE
    sql = String.format("SELECT regexp_like('1a 2b 14m', '\\d+b')");
    rs = vc.createStatement().executeQuery(sql);
    assertTrue(rs.next());
    assertTrue(rs.getBoolean(1));
    rs.close();

    // REGEXP_REPLACE (case 1)
    sql = String.format("SELECT regexp_replace('1a 2b 14m', '\\d+[ab] ')");
    rs = vc.createStatement().executeQuery(sql);
    assertTrue(rs.next());
    assertEquals("14m", rs.getString(1));
    rs.close();

    // REGEXP_REPLACE (case 2)
    sql = String.format("SELECT regexp_replace('1a 2b 14m', '(\\d+)([ab]) ', '3c$2 ')");
    rs = vc.createStatement().executeQuery(sql);
    assertTrue(rs.next());
    assertEquals("3ca 3cb 14m", rs.getString(1));
    rs.close();

    // REGEXP_SPLIT
    sql = String.format("SELECT regexp_split('1a 2b 14m', '\\s*[a-z]+\\s*')");
    rs = vc.createStatement().executeQuery(sql);
    assertTrue(rs.next());
    arr = (Object[]) ((PrestoArray) rs.getObject(1)).getArray();
    assertEquals(4, arr.length);
    assertEquals("1", arr[0]);
    assertEquals("2", arr[1]);
    assertEquals("14", arr[2]);
    assertEquals("", arr[3]);
    rs.close();
  }
}

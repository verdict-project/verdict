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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Created by Dong Young Yoon on 7/24/18. */
@RunWith(Parameterized.class)
public class VerdictConnectionTest {

  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, Connection> vcMap = new HashMap<>();

  private static final String MYSQL_HOST;

  private String database;

  private static final String[] targetDatabases = {"mysql", "impala", "redshift", "postgresql"};

  public VerdictConnectionTest(String database) {
    this.database = database;
  }

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_USER = "root";

  private static final String MYSQL_PASSWORD = "";

  private static final String IMPALA_HOST;

  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  private static final String IMPALA_USER = "";

  private static final String IMPALA_PASSWORD = "";

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  private static final String POSTGRES_HOST;

  private static final String POSTGRES_DATABASE = "test";

  private static final String POSTGRES_USER = "postgres";

  private static final String POSTGRES_PASSWORD = "";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      POSTGRES_HOST = "postgres";
    } else {
      POSTGRES_HOST = "localhost";
    }
  }

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
  }

  @Before
  public void setup() throws SQLException {
    switch (database) {
      case "mysql":
        setupMysql();
        break;
      case "postgresql":
        setupPostgresql();
        break;
      case "redshift":
        setupRedshift();
        break;
      case "impala":
        setupImpala();
        break;
      default:
        break;
    }
  }

  @After
  public void tearDown() throws SQLException {
    switch (database) {
      case "mysql":
        tearDownMysql();
        break;
      case "postgresql":
        tearDownPostgresql();
        break;
      case "redshift":
        tearDownRedshift();
        break;
      case "impala":
        tearDownImpala();
        break;
      default:
        break;
    }
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection databases() {
    Collection<Object[]> params = new ArrayList<>();

    for (String database : targetDatabases) {
      params.add(new Object[] {database});
    }
    return params;
  }

  private static void setupMysql() throws SQLException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    String vcMysqlConnectionString =
        String.format("jdbc:verdict:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    Connection conn =
        DriverManager.getConnection(mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    Connection vc =
        DriverManager.getConnection(vcMysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    connMap.put("mysql", conn);
    vcMap.put("mysql", vc);
  }

  private static void tearDownMysql() throws SQLException {
    Connection conn = connMap.get("mysql");
    Connection vc = vcMap.get("mysql");
    if (!conn.isClosed()) conn.close();
    if (!vc.isClosed()) vc.close();
  }

  private static void setupImpala() throws SQLException {
    String connectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    String vcConnectionString = String.format("jdbc:verdict:impala://%s", IMPALA_HOST);
    Connection conn = DriverManager.getConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    Connection vc = DriverManager.getConnection(vcConnectionString, IMPALA_USER, IMPALA_PASSWORD);
    connMap.put("impala", conn);
    vcMap.put("impala", vc);
  }

  private static void tearDownImpala() throws SQLException {
    Connection conn = connMap.get("impala");
    Connection vc = vcMap.get("impala");
    if (!conn.isClosed()) conn.close();
    if (!vc.isClosed()) vc.close();
  }

  private static void setupRedshift() throws SQLException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    String vcConnectionString =
        String.format("jdbc:verdict:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Connection conn =
        DriverManager.getConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    Connection vc =
        DriverManager.getConnection(vcConnectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    connMap.put("redshift", conn);
    vcMap.put("redshift", vc);
  }

  private static void tearDownRedshift() throws SQLException {
    Connection conn = connMap.get("redshift");
    Connection vc = vcMap.get("redshift");
    if (!conn.isClosed()) conn.close();
    if (!vc.isClosed()) vc.close();
  }

  private static void setupPostgresql() throws SQLException {
    String connectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    String vcConnectionString =
        String.format("jdbc:verdict:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn =
        DriverManager.getConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    Connection vc =
        DriverManager.getConnection(vcConnectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    connMap.put("postgresql", conn);
    vcMap.put("postgresql", vc);
  }

  private static void tearDownPostgresql() throws SQLException {
    Connection conn = connMap.get("postgresql");
    Connection vc = vcMap.get("postgresql");
    if (!conn.isClosed()) conn.close();
    if (!vc.isClosed()) vc.close();
  }

  @Test
  public void testIsValidAfterConnect() throws SQLException {
    boolean valid1 = connMap.get(database).isValid(10);
    boolean valid2 = vcMap.get(database).isValid(10);

    assertTrue(valid1);
    assertTrue(valid2);

    boolean closed1 = connMap.get(database).isClosed();
    boolean closed2 = vcMap.get(database).isClosed();

    assertFalse(closed1);
    assertFalse(closed2);

    connMap.get(database).close();
    vcMap.get(database).close();
  }

  @Test
  public void testIsNotValidAfterClose() throws SQLException {
    connMap.get(database).close();
    vcMap.get(database).close();

    boolean valid1 = connMap.get(database).isValid(10);
    boolean valid2 = vcMap.get(database).isValid(10);

    assertFalse(valid1);
    assertFalse(valid2);

    boolean closed1 = connMap.get(database).isClosed();
    boolean closed2 = vcMap.get(database).isClosed();

    assertTrue(closed1);
    assertTrue(closed2);
  }
}

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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Created by Dong Young Yoon on 7/24/18. */
@RunWith(Parameterized.class)
public class VerdictConnectionTest {

  private static final String MYSQL_HOST;

  private String database;

  private Pair<Connection, Connection> connectionPair;

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

  private static final String SCHEMA_NAME = "conn_test_" + RandomStringUtils.randomAlphanumeric(8);

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
    Connection conn = connectionPair.getLeft();
    Connection vc = connectionPair.getRight();
    if (!conn.isClosed()) conn.close();
    if (!vc.isClosed()) vc.close();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection databases() {
    Collection<Object[]> params = new ArrayList<>();

    for (String database : targetDatabases) {
      params.add(new Object[] {database});
    }
    return params;
  }

  private void setupMysql() throws SQLException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    String vcMysqlConnectionString =
        String.format("jdbc:verdict:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    Connection conn =
        DriverManager.getConnection(mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    Connection vc =
        DriverManager.getConnection(vcMysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    connectionPair = ImmutablePair.of(conn, vc);
  }

  private void setupImpala() throws SQLException {
    String connectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    String impalaMetaSchema = "verdictdbmeta_impala";
    String vcConnectionString = 
        String.format("jdbc:verdict:impala://%s;verdictdbmetaschema=%s", 
            IMPALA_HOST, impalaMetaSchema);
    Connection conn = DriverManager.getConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    Connection vc = DriverManager.getConnection(vcConnectionString, IMPALA_USER, IMPALA_PASSWORD);
    connectionPair = ImmutablePair.of(conn, vc);
  }

  private void setupRedshift() throws SQLException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    String vcConnectionString =
        String.format(
            "jdbc:verdict:redshift://%s/%s;verdictdbmetaschema=%s",
            REDSHIFT_HOST, REDSHIFT_DATABASE, SCHEMA_NAME);
    Connection conn =
        DriverManager.getConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    Connection vc =
        DriverManager.getConnection(vcConnectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    connectionPair = ImmutablePair.of(conn, vc);
  }

  private void setupPostgresql() throws SQLException {
    String connectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    String vcConnectionString =
        String.format("jdbc:verdict:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn =
        DriverManager.getConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    Connection vc =
        DriverManager.getConnection(vcConnectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    connectionPair = ImmutablePair.of(conn, vc);
  }

  @Test
  public void testIsValidAfterConnect() throws SQLException {
    boolean valid1 = connectionPair.getLeft().isValid(10);
    boolean valid2 = connectionPair.getRight().isValid(10);

    assertTrue(valid1);
    assertTrue(valid2);

    boolean closed1 = connectionPair.getLeft().isClosed();
    boolean closed2 = connectionPair.getRight().isClosed();

    if (!database.equals("mysql")) {
      connectionPair
          .getLeft()
          .createStatement()
          .execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", SCHEMA_NAME));
    }

    assertFalse(closed1);
    assertFalse(closed2);
  }

  @Test
  public void testIsNotValidAfterClose() throws SQLException {

    if (!database.equals("mysql")) {
      connectionPair
          .getLeft()
          .createStatement()
          .execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", SCHEMA_NAME));
    }

    connectionPair.getLeft().close();
    connectionPair.getRight().close();

    boolean valid1 = connectionPair.getLeft().isValid(10);
    boolean valid2 = connectionPair.getRight().isValid(10);

    assertFalse(valid1);
    assertFalse(valid2);

    boolean closed1 = connectionPair.getLeft().isClosed();
    boolean closed2 = connectionPair.getRight().isClosed();

    assertTrue(closed1);
    assertTrue(closed2);
  }
}

package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.jdbc.PgSQLXML;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.exception.VerdictDBDbmsException;

/** Created by Dong Young Yoon on 7/18/18. */
@RunWith(Parameterized.class)
public class JdbcQueryDataTypeTestForAllDatabases {

  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, VerdictConnection> vcMap = new HashMap<>();

  private static Map<String, String> schemaMap = new HashMap<>();

  private static final String MYSQL_HOST;

  private String database;

  private static final String[] targetDatabases = {"mysql", "impala", "redshift", "postgresql"};

  public JdbcQueryDataTypeTestForAllDatabases(String database) {
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

  private static final String MYSQL_DATABASE =
      "data_type_test" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();

  private static final String MYSQL_USER = "root";

  private static final String MYSQL_PASSWORD = "";

  private static final String IMPALA_HOST;

  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  private static final String IMPALA_DATABASE =
      "data_type_test" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();

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

  private static final String SCHEMA_NAME =
      "data_type_test" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();

  private static final String TABLE_NAME =
      "data_type_test" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();

  @BeforeClass
  public static void setup() throws SQLException, VerdictDBDbmsException {
    setupMysql();
    setupPostgresql();
    setupRedshift();
    setupImpala();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    tearDownMysql();
    tearDownPostgresql();
    tearDownRedshift();
    tearDownImpala();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection databases() {
    Collection<Object[]> params = new ArrayList<>();

    for (String database : targetDatabases) {
      params.add(new Object[] {database});
    }
    return params;
  }

  private static void setupMysql() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    String vcMysqlConnectionString =
        String.format(
            "jdbc:verdict:mysql://%s/%s?autoReconnect=true&useSSL=false",
            MYSQL_HOST, MYSQL_DATABASE);
    Connection conn =
        DatabaseConnectionHelpers.setupMySqlForDataTypeTest(
            mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, TABLE_NAME);
    VerdictConnection vc =
        new VerdictConnection(vcMysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    conn.setCatalog(MYSQL_DATABASE);
    connMap.put("mysql", conn);
    vcMap.put("mysql", vc);
    schemaMap.put("mysql", MYSQL_DATABASE + ".");
  }

  private static void tearDownMysql() throws SQLException {
    Connection conn = connMap.get("mysql");
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", MYSQL_DATABASE));
    conn.close();
  }

  private static void setupImpala() throws SQLException, VerdictDBDbmsException {
    String connectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    Connection conn =
        DatabaseConnectionHelpers.setupImpalaForDataTypeTest(
            connectionString, IMPALA_USER, IMPALA_PASSWORD, IMPALA_DATABASE, TABLE_NAME);
    VerdictConnection vc = new VerdictConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    connMap.put("impala", conn);
    vcMap.put("impala", vc);
    schemaMap.put("impala", IMPALA_DATABASE + ".");
  }

  private static void tearDownImpala() throws SQLException {
    Connection conn = connMap.get("impala");
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", IMPALA_DATABASE));
    conn.close();
  }

  private static void setupRedshift() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Connection conn =
        DatabaseConnectionHelpers.setupRedshiftForDataTypeTest(
            connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD, SCHEMA_NAME, TABLE_NAME);
    VerdictConnection vc =
        new VerdictConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    connMap.put("redshift", conn);
    vcMap.put("redshift", vc);
    schemaMap.put("redshift", "");
  }

  private static void tearDownRedshift() throws SQLException {
    Connection conn = connMap.get("redshift");
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", SCHEMA_NAME));
    conn.close();
  }

  private static void setupPostgresql() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn =
        DatabaseConnectionHelpers.setupPostgresqlForDataTypeTest(
            connectionString, POSTGRES_USER, POSTGRES_PASSWORD, SCHEMA_NAME, TABLE_NAME);
    VerdictConnection vc =
        new VerdictConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    connMap.put("postgresql", conn);
    vcMap.put("postgresql", vc);
    schemaMap.put("postgresql", "");
  }

  private static void tearDownPostgresql() throws SQLException {
    Connection conn = connMap.get("postgresql");
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", SCHEMA_NAME));
    conn.close();
  }

  @Test
  public void testDataType() throws SQLException {
    String sql = "";
    switch (database) {
      case "mysql":
        sql = String.format("SELECT * FROM `%s`.`%s`", MYSQL_DATABASE, TABLE_NAME);
        break;
      case "impala":
        sql =
            String.format(
                "SELECT * FROM `%s`.`%s` ORDER BY bigintCol", IMPALA_DATABASE, TABLE_NAME);
        break;
      case "postgresql":
      case "redshift":
        sql = String.format("SELECT * FROM \"%s\".\"%s\"", SCHEMA_NAME, TABLE_NAME);
        break;
      default:
        fail(String.format("Database '%s' not supported.", database));
    }

    Statement jdbcStmt = connMap.get(database).createStatement();
    Statement vcStmt = vcMap.get(database).createStatement();

    ResultSet jdbcRs = jdbcStmt.executeQuery(sql);
    ResultSet vcRs = vcStmt.executeQuery(sql);

    int columnCount = jdbcRs.getMetaData().getColumnCount();
    while (jdbcRs.next() && vcRs.next()) {
      for (int i = 1; i <= columnCount; ++i) {
        String columnName = jdbcRs.getMetaData().getColumnName(i);
        Object theirs = jdbcRs.getObject(i);
        Object ours = vcRs.getObject(i);
        System.out.println(columnName + " >> " + theirs + " : " + ours);
        if (theirs instanceof byte[]) {
          assertTrue(Arrays.equals((byte[]) theirs, (byte[]) ours));
        } else if (theirs instanceof PgSQLXML) {
          PgSQLXML xml1 = (PgSQLXML) theirs;
          PgSQLXML xml2 = (PgSQLXML) ours;
          assertEquals(xml1.getString(), xml2.getString());
        } else {
          assertEquals(jdbcRs.getObject(i), vcRs.getObject(i));
        }
      }
    }
  }
}

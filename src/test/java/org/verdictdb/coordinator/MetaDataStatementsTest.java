package org.verdictdb.coordinator;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.verdictdb.VerdictContext;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.ImpalaSyntax;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;
import org.verdictdb.sqlsyntax.RedshiftSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class MetaDataStatementsTest {

  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, String> schemaMap = new HashMap<>();

  private static Map<String, SqlSyntax> syntaxMap = new HashMap<>();

  private static final String MYSQL_HOST;

  private static VerdictOption options = new VerdictOption();

  private String database;

  private static final String[] targetDatabases = {"mysql", "impala", "redshift", "postgresql"};

  public MetaDataStatementsTest(String database) {
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
      "data_type_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String MYSQL_USER = "root";

  private static final String MYSQL_PASSWORD = "";

  private static final String IMPALA_HOST;

  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  private static final String IMPALA_DATABASE =
      "data_type_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

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

  private static final String PostgresRedshift_SCHEMA_NAME =
      "data_type_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String TABLE_NAME =
      "data_type_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  @BeforeClass
  public static void setup() throws SQLException, VerdictDBDbmsException, IOException {
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
    Connection conn =
        DriverManager.getConnection(mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);
    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS `%s`", MYSQL_DATABASE));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", MYSQL_DATABASE));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE `%s`.`%s`(intcol int, strcol varchar(5))", MYSQL_DATABASE, TABLE_NAME));
    // conn.setCatalog(MYSQL_DATABASE);
    connMap.put("mysql", conn);
    schemaMap.put("mysql", MYSQL_DATABASE + ".");
    syntaxMap.put("mysql", new MysqlSyntax());
  }

  private static void tearDownMysql() throws SQLException {
    Connection conn = connMap.get("mysql");
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", MYSQL_DATABASE));
    conn.close();
  }

  private static void setupImpala() throws SQLException, VerdictDBDbmsException {
    String connectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    Connection conn = DriverManager.getConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);
    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", IMPALA_DATABASE));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", IMPALA_DATABASE));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE `%s`.`%s`(intcol int, strcol varchar(5))", IMPALA_DATABASE, TABLE_NAME));
    connMap.put("impala", conn);
    schemaMap.put("impala", IMPALA_DATABASE + ".");
    syntaxMap.put("impala", new ImpalaSyntax());
  }

  private static void tearDownImpala() throws SQLException {
    Connection conn = connMap.get("impala");
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", IMPALA_DATABASE));
    conn.close();
  }

  private static void setupRedshift() throws SQLException, VerdictDBDbmsException, IOException {
    String connectionString =
        String.format(
            "jdbc:redshift://%s/%s;verdictdbmetaschema=%s",
            REDSHIFT_HOST, REDSHIFT_DATABASE, PostgresRedshift_SCHEMA_NAME);
    Connection conn =
        DriverManager.getConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    JdbcConnection dbmsConn = new JdbcConnection(conn, new RedshiftSyntax());
    dbmsConn.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", PostgresRedshift_SCHEMA_NAME));
    dbmsConn.execute(
        String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", PostgresRedshift_SCHEMA_NAME));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE \"%s\".\"%s\"(intcol int, strcol varchar(5))",
            PostgresRedshift_SCHEMA_NAME, TABLE_NAME));
    options.setVerdictMetaSchemaName(PostgresRedshift_SCHEMA_NAME);

    connMap.put("redshift", conn);
    schemaMap.put("redshift", "");
    syntaxMap.put("redshift", new RedshiftSyntax());
  }

  private static void tearDownRedshift() throws SQLException {
    Connection conn = connMap.get("redshift");
    Statement stmt = conn.createStatement();
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", PostgresRedshift_SCHEMA_NAME));
    conn.close();
  }

  private static void setupPostgresql() throws SQLException, VerdictDBDbmsException, IOException {
    String connectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn =
        DriverManager.getConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    JdbcConnection dbmsConn = new JdbcConnection(conn, new RedshiftSyntax());

    dbmsConn.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", PostgresRedshift_SCHEMA_NAME));
    dbmsConn.execute(
        String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", PostgresRedshift_SCHEMA_NAME));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE \"%s\".\"%s\"(intcol int, strcol varchar(5))",
            PostgresRedshift_SCHEMA_NAME, TABLE_NAME));
    connMap.put("postgresql", conn);
    schemaMap.put("postgresql", "");
    syntaxMap.put("postgresql", new PostgresqlSyntax());
  }

  private static void tearDownPostgresql() throws SQLException {
    Connection conn = connMap.get("postgresql");
    Statement stmt = conn.createStatement();
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", PostgresRedshift_SCHEMA_NAME));
    conn.close();
  }

  @Test
  public void showSchemaTest() throws SQLException, VerdictDBException {
    String sql = "show schemas";
    switch (database) {
      case "mysql":
        break;
      case "impala":
        break;
      case "postgresql":
        sql = "select schema_name from information_schema.schemata";
        break;
      case "redshift":
        sql = "select schema_name from information_schema.schemata";
        break;
      default:
        fail(String.format("Database '%s' not supported.", database));
    }

    Statement jdbcStmt = connMap.get(database).createStatement();
    ResultSet jdbcRs = jdbcStmt.executeQuery(sql);

    DbmsConnection dbmsconn =
        new CachedDbmsConnection(
            new JdbcConnection(connMap.get(database), syntaxMap.get(database)));

    VerdictOption option = new VerdictOption();
    final String verdictmeta = 
        "verdictmeta" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
    option.setVerdictMetaSchemaName(verdictmeta);
    VerdictContext verdict = new VerdictContext(dbmsconn, option);
    
    ExecutionContext exec = new ExecutionContext(
        dbmsconn, verdict.getMetaStore(), verdict.getContextId(), 0, options);
    VerdictSingleResult result = exec.sql("show schemas");

    Set<String> expected = new HashSet<>();
    Set<String> actual = new HashSet<>();
    while (jdbcRs.next()) {
      result.next();
      expected.add(jdbcRs.getString(1));
      actual.add(result.getString(0));
      //      assertEquals(jdbcRs.getString(1), result.getValue(0));
    }
    assertEquals(expected, actual);
    
    // teardown
    dbmsconn.execute(String.format("drop schema if exists %s", verdictmeta));
  }

  @Test
  public void showTablesTest() throws SQLException, VerdictDBException {
    String vcsql = "";
    String sql = "";
    switch (database) {
      case "mysql":
        sql = "SHOW TABLES IN " + MYSQL_DATABASE;
        vcsql = "SHOW TABLES IN " + MYSQL_DATABASE;
        break;
      case "impala":
        sql = "SHOW TABLES IN " + IMPALA_DATABASE;
        vcsql = "SHOW TABLES IN " + IMPALA_DATABASE;
        break;
      case "postgresql":
        sql = new PostgresqlSyntax().getTableCommand(PostgresRedshift_SCHEMA_NAME);
        vcsql = "SHOW TABLES IN " + PostgresRedshift_SCHEMA_NAME;
        break;
      case "redshift":
        sql = new RedshiftSyntax().getTableCommand(PostgresRedshift_SCHEMA_NAME);
        vcsql = "SHOW TABLES IN " + PostgresRedshift_SCHEMA_NAME;
        break;
      default:
        fail(String.format("Database '%s' not supported.", database));
    }

    Statement jdbcStmt = connMap.get(database).createStatement();
    ResultSet jdbcRs = jdbcStmt.executeQuery(sql);

    DbmsConnection dbmsconn =
        new CachedDbmsConnection(
            new JdbcConnection(connMap.get(database), syntaxMap.get(database)));

    VerdictOption option = new VerdictOption();
    final String verdictmeta = 
        "verdictmeta" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
    option.setVerdictMetaSchemaName(verdictmeta);
    VerdictContext verdict = new VerdictContext(dbmsconn, option);
    
    ExecutionContext exec = new ExecutionContext(
        dbmsconn, verdict.getMetaStore(), verdict.getContextId(), 0, options);
    VerdictSingleResult result = exec.sql(vcsql);

    // test
    Set<String> expected = new HashSet<>();
    Set<String> actual = new HashSet<>();
    while (jdbcRs.next()) {
      result.next();
      expected.add(jdbcRs.getString(1));
      actual.add(result.getString(0));
      //      assertEquals(jdbcRs.getString(1), result.getValue(0));
    }
    assertEquals(expected, actual);
    
    // teardown
    dbmsconn.execute(String.format("drop schema if exists %s", verdictmeta));
  }

  @Test
  public void describeColumnsTest() throws SQLException, VerdictDBException {
    String vcsql = "";
    String sql = "";
    switch (database) {
      case "mysql":
        sql = String.format("DESCRIBE %s.%s", MYSQL_DATABASE, TABLE_NAME);
        vcsql = String.format("DESCRIBE %s.%s", MYSQL_DATABASE, TABLE_NAME);
        break;
      case "impala":
        sql = String.format("DESCRIBE %s.%s", IMPALA_DATABASE, TABLE_NAME);
        vcsql = String.format("DESCRIBE %s.%s", IMPALA_DATABASE, TABLE_NAME);
        break;
      case "postgresql":
        sql = new PostgresqlSyntax().getColumnsCommand(PostgresRedshift_SCHEMA_NAME, TABLE_NAME);
        vcsql = String.format("DESCRIBE %s.%s", PostgresRedshift_SCHEMA_NAME, TABLE_NAME);
        break;
      case "redshift":
        sql = new RedshiftSyntax().getColumnsCommand(PostgresRedshift_SCHEMA_NAME, TABLE_NAME);
        vcsql = String.format("DESCRIBE %s.%s", PostgresRedshift_SCHEMA_NAME, TABLE_NAME);
        break;
      default:
        fail(String.format("Database '%s' not supported.", database));
    }

    Statement jdbcStmt = connMap.get(database).createStatement();
    ResultSet jdbcRs = null;
    for (String s : sql.split(";")) {
      jdbcStmt.execute(s);
    }
    jdbcRs = jdbcStmt.getResultSet();

    DbmsConnection dbmsconn =
        new CachedDbmsConnection(
            new JdbcConnection(connMap.get(database), syntaxMap.get(database)));

    VerdictOption option = new VerdictOption();
    final String verdictmeta = 
        "verdictmeta" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
    option.setVerdictMetaSchemaName(verdictmeta);
    VerdictContext verdict = new VerdictContext(dbmsconn, option);
    
    ExecutionContext exec = new ExecutionContext(
        dbmsconn, verdict.getMetaStore(), verdict.getContextId(), 0, options);
    VerdictSingleResult result = exec.sql(vcsql);
    while (jdbcRs.next()) {
      result.next();
      assertEquals(jdbcRs.getString(1), result.getValue(0));
      if (database.equals("postgresql") && jdbcRs.getString(1).equals("strcol")) {
        assertEquals("character varying(5)", result.getValue(1));
      } else {
        assertEquals(jdbcRs.getString(2), result.getValue(1));
      }
    }
    
    // teardown
    dbmsconn.execute(String.format("drop schema if exists %s", verdictmeta));
  }
}

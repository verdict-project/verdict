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
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.jdbc.PgSQLXML;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.exception.VerdictDBException;

/** Created by Dong Young Yoon on 7/18/18. */
@RunWith(Parameterized.class)
public class JdbcQueryDataTypeForAllDatabasesTest {

  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, VerdictConnection> vcMap = new HashMap<>();

  private static Map<String, String> schemaMap = new HashMap<>();

  private static final String MYSQL_HOST;

  private String database;

  private static final String[] targetDatabases = 
      {"mysql", "impala", "redshift", "postgresql", "presto"};

  public JdbcQueryDataTypeForAllDatabasesTest(String database) {
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

  // impala
  private static final String IMPALA_DATABASE =
      "data_type_test" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();

  private static final String IMPALA_USER = "";

  private static final String IMPALA_PASSWORD = "";

  // redshift
  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  // postgres
  private static final String POSTGRES_HOST;

  private static final String POSTGRES_DATABASE = "test";

  private static final String POSTGRES_USER = "postgres";

  private static final String POSTGRES_PASSWORD = "";
  
  // presto
  private static final String PRESTO_USER;
  
  private static final String PRESTO_HOST;
  
  private static final String PRESTO_CATALOG;
  
  private static final String PRESTO_PASSWORD = "";

  private static final String VERDICT_META_SCHEMA =
      "verdictdbmetaschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String VERDICT_TEMP_SCHEMA =
      "verdictdbtempschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static VerdictOption options = new VerdictOption();

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
  
  static {
    PRESTO_HOST = System.getenv("VERDICTDB_TEST_PRESTO_HOST");
    PRESTO_CATALOG = System.getenv("VERDICTDB_TEST_PRESTO_CATALOG");
    PRESTO_USER = System.getenv("VERDICTDB_TEST_PRESTO_USER");
  }

  private static final String SCHEMA_NAME =
      "data_type_test" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String TABLE_NAME =
      "data_type_test" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  @BeforeClass
  public static void setup() throws SQLException, VerdictDBException {
    options.setVerdictMetaSchemaName(VERDICT_META_SCHEMA);
    options.setVerdictTempSchemaName(VERDICT_TEMP_SCHEMA);
    setupMysql();
    setupPostgresql();
    setupRedshift();
    setupImpala();
    setupPresto();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    tearDownMysql();
    tearDownPostgresql();
    tearDownRedshift();
    tearDownImpala();
    tearDownPresto();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> databases() {
    Collection<Object[]> params = new ArrayList<>();

    for (String database : targetDatabases) {
      params.add(new Object[] {database});
    }
    return params;
  }

  private static void setupMysql() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    String vcMysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    Connection conn =
        DatabaseConnectionHelpers.setupMySqlForDataTypeTest(
            mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, TABLE_NAME);
    VerdictConnection vc =
        new VerdictConnection(vcMysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD, options);
    conn.setCatalog(MYSQL_DATABASE);
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictMetaSchemaName()));
    connMap.put("mysql", conn);
    vcMap.put("mysql", vc);
    schemaMap.put("mysql", MYSQL_DATABASE + ".");
  }

  private static void tearDownMysql() throws SQLException {
    Connection conn = connMap.get("mysql");
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", MYSQL_DATABASE));
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", options.getVerdictTempSchemaName()));
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", options.getVerdictMetaSchemaName()));
    conn.close();
  }

  private static void setupImpala() throws SQLException, VerdictDBException {
    String connectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    Connection conn =
        DatabaseConnectionHelpers.setupImpalaForDataTypeTest(
            connectionString, IMPALA_USER, IMPALA_PASSWORD, IMPALA_DATABASE, TABLE_NAME);
    VerdictConnection vc =
        new VerdictConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD, options);
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictMetaSchemaName()));
    connMap.put("impala", conn);
    vcMap.put("impala", vc);
    schemaMap.put("impala", IMPALA_DATABASE + ".");
  }

  private static void tearDownImpala() throws SQLException {
    Connection conn = connMap.get("impala");
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", IMPALA_DATABASE));
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", options.getVerdictMetaSchemaName()));
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", options.getVerdictTempSchemaName()));
    conn.close();
  }

  private static void setupRedshift() throws SQLException, VerdictDBException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Connection conn =
        DatabaseConnectionHelpers.setupRedshiftForDataTypeTest(
            connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD, SCHEMA_NAME, TABLE_NAME);
    VerdictConnection vc =
        new VerdictConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD, options);
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictMetaSchemaName()));
    connMap.put("redshift", conn);
    vcMap.put("redshift", vc);
    schemaMap.put("redshift", "");
  }

  private static void tearDownRedshift() throws SQLException {
    Connection conn = connMap.get("redshift");
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", SCHEMA_NAME));
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", options.getVerdictMetaSchemaName()));
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", options.getVerdictTempSchemaName()));
    conn.close();
  }

  private static void setupPostgresql() throws SQLException, VerdictDBException {
    String connectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn =
        DatabaseConnectionHelpers.setupPostgresqlForDataTypeTest(
            connectionString, POSTGRES_USER, POSTGRES_PASSWORD, SCHEMA_NAME, TABLE_NAME);
    VerdictConnection vc =
        new VerdictConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD, options);
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictMetaSchemaName()));
    connMap.put("postgresql", conn);
    vcMap.put("postgresql", vc);
    schemaMap.put("postgresql", "");
  }

  private static void tearDownPostgresql() throws SQLException {
    Connection conn = connMap.get("postgresql");
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", SCHEMA_NAME));
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", options.getVerdictMetaSchemaName()));
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", options.getVerdictTempSchemaName()));
    conn.close();
  }
  
  private static void setupPresto() throws SQLException, VerdictDBException {
    String connectionString =
        String.format("jdbc:presto://%s/%s", PRESTO_HOST, PRESTO_CATALOG);
    Connection conn =
        DatabaseConnectionHelpers.setupPrestoForDataTypeTest(
            connectionString, PRESTO_USER, PRESTO_PASSWORD, SCHEMA_NAME, TABLE_NAME);
    VerdictConnection vc =
        new VerdictConnection(connectionString, PRESTO_USER, PRESTO_PASSWORD, options);
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictMetaSchemaName()));
    connMap.put("presto", conn);
    vcMap.put("presto", vc);
    schemaMap.put("presto", "");
  }
  
  private static void tearDownPresto() throws SQLException {
    Connection conn = connMap.get("presto");
    Statement stmt = conn.createStatement();
    
    dropPrestoTablesInSchema(conn, SCHEMA_NAME);
    stmt.execute(String.format("DROP SCHEMA IF EXISTS \"%s\"", SCHEMA_NAME));
    
    dropPrestoTablesInSchema(conn, options.getVerdictMetaSchemaName());
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\"", options.getVerdictMetaSchemaName()));
    
    dropPrestoTablesInSchema(conn, options.getVerdictTempSchemaName());
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\"", options.getVerdictTempSchemaName()));
    conn.close();
  }
  
  private static void dropPrestoTablesInSchema(Connection conn, String schema_name) 
      throws SQLException {
    Statement stmt = conn.createStatement();
    List<String> tables = getPrestoTablesInSchema(conn, schema_name);
    for (String table_name : tables) {
      stmt.execute(String.format("drop table \"%s\".\"%s\"", schema_name, table_name));
    }
    stmt.close();
  }
  
  private static List<String> getPrestoTablesInSchema(Connection conn, String schema_name) 
      throws SQLException {
    List<String> tables = new ArrayList<>();
    Statement stmt = conn.createStatement();
    try {
      ResultSet result = stmt.executeQuery(String.format("show tables in \"%s\"", schema_name));
      while(result.next()) {
        tables.add(result.getString(1));
      }
      result.close();
    } catch (SQLException e) {
      
    } finally {
      stmt.close();
    }
    
    return tables;
  }

  @Test
  public void testDataType() throws SQLException, VerdictDBException {
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
        sql =
            String.format(
                "SELECT * FROM \"%s\".\"%s\" ORDER BY bigintcol", SCHEMA_NAME, TABLE_NAME);
        break;
      case "presto":
        sql = 
            String.format(
                "SELECT * FROM \"%s\".\"%s\" ORDER BY tinyintCol", SCHEMA_NAME, TABLE_NAME);
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
          //          assertEquals(jdbcRs.getObject(i), vcRs.getObject(i));
          //          System.out.println(columnName + " >> " + theirs + " : " + ours);
          assertEquals(theirs, ours);
        }
      }
    }
  }
}

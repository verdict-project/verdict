package org.verdictdb.jdbc41;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Created by Dong Young Yoon on 7/18/18. */
@RunWith(Parameterized.class)
public class JdbcTpchQueryForAllDatabasesTest {

  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, Connection> vcMap = new HashMap<>();

  private static Map<String, String> schemaMap = new HashMap<>();

  private static final String MYSQL_HOST;

  private static final int MYSQL_TPCH_QUERY_COUNT = 21;

  private static final int TPCH_QUERY_COUNT = 22;

  private String database = "";

  private String query;

  // TODO: Add support for all four databases
  //  private static final String[] targetDatabases = {"mysql", "impala", "redshift", "postgresql"};
  private static final String[] targetDatabases = {"mysql", "impala", "redshift"};

  public JdbcTpchQueryForAllDatabasesTest(String database, String query) {
    this.database = database;
    this.query = query;
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
      "tpch_test_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();

  private static final String MYSQL_USER = "root";

  private static final String MYSQL_PASSWORD = "";

  private static final String IMPALA_HOST;

  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  private static final String IMPALA_DATABASE = "tpch_2_parquet";

  private static final String IMPALA_USER = "";

  private static final String IMPALA_PASSWORD = "";

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_SCHEMA = "public";

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  private static final String POSTGRES_HOST;

  private static final String POSTGRES_DATABASE = "test";

  private static final String POSTGRES_USER = "postgres";

  private static final String POSTGRES_PASSWORD = "";

  private static final String POSTGRES_SCHEMA = "";

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

  private static final String TABLE_NAME = "mytable";

  private static VerdictConnection mysqlVc;

  @BeforeClass
  public static void setupDatabases() throws SQLException, VerdictDBDbmsException, IOException {
    setupMysql();
    setupImpala();
    setupRedshift();
    // TODO: Add below databases too
    //    setupPostgresql();
  }

  @AfterClass
  public static void tearDown() {
    // TODO: add clean up code for each database once it becomes possible to specify which
    // TODO: database/schema name to use in this unit test
  }

  @Parameterized.Parameters(name = "{0}_tpch_{1}")
  public static Collection<Object[]> databases() {
    Collection<Object[]> params = new ArrayList<>();

    for (String database : targetDatabases) {
      int queryCount = 0;
      switch (database) {
        case "mysql":
          queryCount = MYSQL_TPCH_QUERY_COUNT;
          break;
        case "impala":
        case "redshift":
          queryCount = TPCH_QUERY_COUNT;
          break;
      }
      for (int query = 1; query <= queryCount; ++query) {
        params.add(new Object[] {database, String.valueOf(query)});
      }
      if (database.equals("redshift")) {
        params.add(new Object[] {database, "e1"});
        params.add(new Object[] {database, "e2"});
        params.add(new Object[] {database, "e3"});
      }

      // Uncomment below lines to test a specific query
      //      params.clear();
      //      params.add(new Object[] {database, "15"});
    }
    return params;
  }

  private static Connection setupMysql() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    String vcMysqlConnectionString =
        String.format(
            "jdbc:verdict:mysql://%s/%s?autoReconnect=true&useSSL=false",
            MYSQL_HOST, MYSQL_DATABASE);
    Connection conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE);
    Connection vc =
        DriverManager.getConnection(vcMysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    //    VerdictConnection vc =
    //        new VerdictConnection(vcMysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    conn.setCatalog(MYSQL_DATABASE);
    connMap.put("mysql", conn);
    vcMap.put("mysql", vc);
    schemaMap.put("mysql", MYSQL_DATABASE + ".");
    return conn;
  }

  private static Connection setupImpala() throws SQLException, VerdictDBDbmsException {
    String connectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    String verdictConnectionString = String.format("jdbc:verdict:impala://%s", IMPALA_HOST);
    Connection conn =
        DatabaseConnectionHelpers.setupImpala(
            connectionString, IMPALA_USER, IMPALA_PASSWORD, IMPALA_DATABASE);
    Connection vc =
        DriverManager.getConnection(verdictConnectionString, IMPALA_USER, IMPALA_PASSWORD);
    //    Connection vc = new VerdictConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    connMap.put("impala", conn);
    vcMap.put("impala", vc);
    schemaMap.put("impala", IMPALA_DATABASE + ".");
    return conn;
  }

  private static Connection setupRedshift()
      throws SQLException, VerdictDBDbmsException, IOException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    String verdictConnectionString =
        String.format("jdbc:verdict:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Connection conn =
        DatabaseConnectionHelpers.setupRedshift(
            connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_SCHEMA);
    Connection vc =
        DriverManager.getConnection(verdictConnectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    connMap.put("redshift", conn);
    vcMap.put("redshift", vc);
    schemaMap.put("redshift", "");
    return conn;
  }

  public static Connection setupPostgresql()
      throws SQLException, VerdictDBDbmsException, IOException {
    String connectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn =
        DatabaseConnectionHelpers.setupPostgresql(
            connectionString, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_SCHEMA);
    VerdictConnection vc =
        new VerdictConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    connMap.put("postgresql", conn);
    vcMap.put("postgresql", vc);
    schemaMap.put("postgresql", "");
    return conn;
  }

  @Test
  public void testTpch() throws IOException, SQLException {
    ClassLoader classLoader = getClass().getClassLoader();
    String filename = "";
    switch (database) {
      case "mysql":
        filename = "companya/mysql_queries/tpchMySQLQuery" + query + ".sql";
        break;
      case "impala":
        filename = "companya/impala_queries/tpchImpalaQuery" + query + ".sql";
        break;
      case "redshift":
        filename = "companya/redshift_queries/" + query + ".sql";
        break;
      default:
        fail(String.format("Database '%s' not supported.", database));
    }
    File queryFile = new File(classLoader.getResource(filename).getFile());
    if (queryFile.exists()) {
      String sql = Files.toString(queryFile, Charsets.UTF_8);
      //      sql = "select * from tpch_2_parquet.lineitem limit 1000";

      Statement jdbcStmt = connMap.get(database).createStatement();
      Statement vcStmt = vcMap.get(database).createStatement();

      ResultSet jdbcRs = jdbcStmt.executeQuery(sql);
      ResultSet vcRs = vcStmt.executeQuery(sql);

      int columnCount = jdbcRs.getMetaData().getColumnCount();
      boolean jdbcNext = jdbcRs.next();
      boolean vcNext = vcRs.next();
      while (jdbcNext && vcNext) {
        assertEquals(jdbcNext, vcNext);
        for (int i = 1; i <= columnCount; ++i) {
          System.out.println(jdbcRs.getObject(i) + " : " + vcRs.getObject(i));
          assertEquals(jdbcRs.getObject(i), vcRs.getObject(i));
        }
        jdbcNext = jdbcRs.next();
        vcNext = vcRs.next();
      }
      assertEquals(jdbcNext, vcNext);
    } else {
      System.out.println(String.format("tpch%d does not exist.", query));
    }
  }
}

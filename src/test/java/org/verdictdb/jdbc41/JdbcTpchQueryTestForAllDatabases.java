package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.exception.VerdictDBDbmsException;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * Created by Dong Young Yoon on 7/18/18.
 */
@RunWith(Parameterized.class)
public class JdbcTpchQueryTestForAllDatabases {

  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, VerdictConnection> vcMap = new HashMap<>();

  private static Map<String, String> schemaMap = new HashMap<>();

  private static final String MYSQL_HOST;

  private static final int TPCH_QUERY_COUNT = 22;

  private String database = "";

  private int query;

  // TODO: Add support for all four databases
//  private static final String[] targetDatabases = {"mysql", "impala", "redshift", "postgresql"};
  private static final String[] targetDatabases = {"mysql"};

  public JdbcTpchQueryTestForAllDatabases(String database, int query) {
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

  private static final String MYSQL_DATABASE = "tpch_flat_orc_2";

  private static final String MYSQL_USER = "root";

  private static final String MYSQL_PASSWORD = "";

  private static final String IMPALA_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      IMPALA_HOST = "impala";
    } else {
      IMPALA_HOST = "localhost";
    }
  }

  private static final String IMPALA_DATABASE = "default";

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
  public static void setupDatabases() throws SQLException, VerdictDBDbmsException {
    setupMysql();
    // TODO: Add below databases too
//    setupImpala();
//    setupRedshift();
//    setupPostgresql();
  }

  @Parameterized.Parameters(name="{0}_tpch_{1}")
  public static Collection<Object[]> databases() {
    Collection<Object[]> params = new ArrayList<>();

    for (String database : targetDatabases) {
      for (int query = 1; query <= TPCH_QUERY_COUNT; ++query) {
        params.add(new Object[]{database, query});
      }
    }
    return params;
  }


  private static Connection setupMysql() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    String vcMysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    Connection conn = DatabaseConnectionHelpers.setupMySql(
        mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE);
    VerdictConnection vc = new VerdictConnection(vcMysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    conn.setCatalog(MYSQL_DATABASE);
    connMap.put("mysql", conn);
    vcMap.put("mysql", vc);
    schemaMap.put("mysql", MYSQL_DATABASE + ".");
    return conn;
  }

  // TODO: add tpch setup step for impala
  private static Connection setupImpala() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:impala://%s/%s", IMPALA_HOST, IMPALA_DATABASE);
    Connection conn = DriverManager.getConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    VerdictConnection vc = new VerdictConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    connMap.put("impala", conn);
    vcMap.put("impala", vc);
    schemaMap.put("impala", IMPALA_DATABASE + ".");
    return conn;
  }

  // TODO: add tpch setup step for redshift
  public static Connection setupRedshift() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Connection conn = DriverManager.getConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    VerdictConnection vc = new VerdictConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    connMap.put("redshift", conn);
    vcMap.put("redshift", vc);
    schemaMap.put("redshift", "");
    return conn;
  }

  public static Connection setupPostgresql() throws SQLException, VerdictDBDbmsException, IOException {
    String connectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn = DatabaseConnectionHelpers.setupPostgresql(
        connectionString, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_SCHEMA);
    VerdictConnection vc = new VerdictConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    connMap.put("postgresql", conn);
    vcMap.put("postgresql", vc);
    schemaMap.put("postgresql", "");
    return conn;
  }

  @Test
  public void testTpch() throws IOException, SQLException {
    ClassLoader classLoader = getClass().getClassLoader();
    String filename = "companya/mysql_queries/tpchMySqlQuery" + query + ".sql";
    File queryFile = new File(classLoader.getResource(filename).getFile());
    if (queryFile.exists()) {
      String sql = Files.toString(queryFile, Charsets.UTF_8);

      Statement jdbcStmt = connMap.get(database).createStatement();
      Statement vcStmt = vcMap.get(database).createStatement();

      ResultSet jdbcRs = jdbcStmt.executeQuery(sql);
      ResultSet vcRs = vcStmt.executeQuery(sql);

      int columnCount = jdbcRs.getMetaData().getColumnCount();
      while (jdbcRs.next() && vcRs.next()) {
        for (int i = 1; i <= columnCount; ++i) {
          System.out.println(jdbcRs.getObject(i) + " : " + vcRs.getObject(i));
          assertEquals(jdbcRs.getObject(i), vcRs.getObject(i));
        }
      }
    } else {
      System.out.println(String.format("tpch%d does not exist.", query));
    }
  }

}

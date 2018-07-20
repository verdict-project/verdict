package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.exception.VerdictDBDbmsException;

/**
 * Created by Dong Young Yoon on 7/18/18.
 */
@RunWith(Parameterized.class)
public class JdbcQueryDataTypeTestForAllDatabases {

  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, VerdictConnection> vcMap = new HashMap<>();

  private static Map<String, String> schemaMap = new HashMap<>();

  private static final String MYSQL_HOST;

  private String database = "";

  // TODO: Add support for all four databases
//  private static final String[] targetDatabases = {"mysql", "impala", "redshift", "postgresql"};
  private static final String[] targetDatabases = {"mysql", "postgres", "redshift"};

  public JdbcQueryDataTypeTestForAllDatabases(String database) {
    this.database = database;
  }


  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
    }
    else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE = "data_type_test";

  private static final String MYSQL_USER = "root";

  private static final String MYSQL_PASSWORD = "";

  private static final String MYSQL_TABLE = "mytable";

  private static final String IMPALA_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      IMPALA_HOST = "impala";
    }
    else {
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
    }
    else {
      POSTGRES_HOST = "localhost";
    }
  }

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
  }

  private static final String SCHEMA_NAME = "data_type_test";

  private static final String TABLE_NAME = "mytable";

  private static VerdictConnection mysqlVc;

  @BeforeClass
  public static void setupDatabases() throws SQLException, VerdictDBDbmsException {
    setupMysql();
    setupPostgresql();
    setupRedshift();
    // TODO: Add below databases too
//    setupImpala();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection databases() {
    Collection<Object[]> params = new ArrayList<>();

    for (String database : targetDatabases) {
      params.add(new Object[]{database});
    }
    return params;
  }


  private static Connection setupMysql() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    String vcMysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    Connection conn = DatabaseConnectionHelpers.setupMySqlForDataTypeTest(
        mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_TABLE);
    VerdictConnection vc = new VerdictConnection(vcMysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    conn.setCatalog(MYSQL_DATABASE);
    connMap.put("mysql", conn);
    vcMap.put("mysql", vc);
    schemaMap.put("mysql", MYSQL_DATABASE + ".");
    return conn;
  }

  // TODO: add query data type test setup step for impala
  private static Connection setupImpala() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:impala://%s:21050/%s", IMPALA_HOST, IMPALA_DATABASE);
    Connection conn = DriverManager.getConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    VerdictConnection vc = new VerdictConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    connMap.put("impala", conn);
    vcMap.put("impala", vc);
    schemaMap.put("impala", IMPALA_DATABASE + ".");
    return conn;
  }

  // TODO: add query data type test setup step for redshift
  public static Connection setupRedshift() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Connection conn = DatabaseConnectionHelpers.setupRedshiftForDataTypeTest(
        connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD, SCHEMA_NAME, TABLE_NAME);
    VerdictConnection vc = new VerdictConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    connMap.put("redshift", conn);
    vcMap.put("redshift", vc);
    schemaMap.put("redshift", "");
    return conn;
  }

  public static Connection setupPostgresql() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn = DatabaseConnectionHelpers.setupPostgresqlForDataTypeTest(
        connectionString, POSTGRES_USER, POSTGRES_PASSWORD, SCHEMA_NAME, TABLE_NAME);
    VerdictConnection vc = new VerdictConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    connMap.put("postgresql", conn);
    vcMap.put("postgresql", vc);
    schemaMap.put("postgresql", "");
    return conn;
  }

  @Test
  public void testDataType() throws IOException, SQLException {
    String sql = "";
    if (database.equals("mysql")) {
      sql = String.format("SELECT * FROM `%s`.`%s`", SCHEMA_NAME, TABLE_NAME);
    } else if (database.equals("postgresql") || database.equals("redshift")) {
      sql = String.format("SELECT * FROM \"%s\".\"%s\"", SCHEMA_NAME, TABLE_NAME);
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
        } else {
          assertEquals(jdbcRs.getObject(i), vcRs.getObject(i));
        }
      }
    }
  }

}

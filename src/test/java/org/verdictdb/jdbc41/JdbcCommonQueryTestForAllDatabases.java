package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.verdictdb.exception.VerdictDBDbmsException;

/**
 * Created by Dong Young Yoon on 7/18/18.
 */
@RunWith(Parameterized.class)
public class JdbcCommonQueryTestForAllDatabases {

  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, VerdictConnection> vcMap = new HashMap<>();

  private static Map<String, String> schemaMap = new HashMap<>();

  private static final String MYSQL_HOST;

  private String database = "";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE = "test";

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

  public JdbcCommonQueryTestForAllDatabases(String database) {
    this.database = database;
  }

  @BeforeClass
  public static void setupDatabases() throws SQLException, VerdictDBDbmsException {
    setupMysql();
    setupImpala();
    setupRedshift();
    setupPostgresql();
  }

  @Parameterized.Parameters(name="{0}")
  public static Collection databases() {
    return Arrays.asList("mysql", "impala", "redshift", "postgresql");
  }

  private static void loadData(Connection conn) throws SQLException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female", 15, 170.2, "USA", "2017-10-01 21:22:23"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female", 17, 156.5, "USA", "2017-10-02 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168.1, "CHN", "2017-10-03 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male", 14, 178.6, "USA", "2017-10-04 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male", 18, 190.7, "CHN", "2017-10-05 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Sam", "male", 18, 190.0, "USA", "2017-10-06 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Alice", "female", 18, 190.21, "CHN", "2017-10-07 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Bob", "male", 18, 190.3, "CHN", "2017-10-08 21:22:23"));

    Statement stmt = conn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS people");
    stmt.execute("CREATE TABLE people(id smallint, name varchar(255), gender varchar(8), age float, height float, nation varchar(8), birth timestamp)");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      String age = row.get(3).toString();
      String height = row.get(4).toString();
      String nation = row.get(5).toString();
      String birth = row.get(6).toString();
      stmt.execute(String.format("INSERT INTO people(id, name, gender, age, height, nation, birth) VALUES(%s, '%s', '%s', %s, %s, '%s', '%s')", id, name, gender, age, height, nation, birth));
    }
  }

  private static void loadDataImpala(Connection conn) throws SQLException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female", 15, 170.2, "USA", "2017-10-01 21:22:23"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female", 17, 156.5, "USA", "2017-10-02 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168.1, "CHN", "2017-10-03 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male", 14, 178.6, "USA", "2017-10-04 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male", 18, 190.7, "CHN", "2017-10-05 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Sam", "male", 18, 190.0, "USA", "2017-10-06 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Alice", "female", 18, 190.21, "CHN", "2017-10-07 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Bob", "male", 18, 190.3, "CHN", "2017-10-08 21:22:23"));

    Statement stmt = conn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS people");
    stmt.execute("CREATE TABLE people(id smallint, name string, gender string, age float, height float, nation string, birth timestamp)");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      String age = row.get(3).toString();
      String height = row.get(4).toString();
      String nation = row.get(5).toString();
      String birth = row.get(6).toString();
      stmt.execute(String.format("INSERT INTO people(id, name, gender, age, height, nation, birth) VALUES(%s, '%s', '%s', %s, %s, '%s', '%s')", id, name, gender, age, height, nation, birth));
    }
  }

  private static Connection setupMysql() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    Connection conn = DriverManager.getConnection(mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    VerdictConnection vc = new VerdictConnection(mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    loadData(conn);
    connMap.put("mysql", conn);
    vcMap.put("mysql", vc);
    schemaMap.put("mysql", MYSQL_DATABASE + ".");
    return conn;
  }

  private static Connection setupImpala() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:impala://%s:21050/%s", IMPALA_HOST, IMPALA_DATABASE);
    Connection conn = DriverManager.getConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    VerdictConnection vc = new VerdictConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    loadDataImpala(conn);
    connMap.put("impala", conn);
    vcMap.put("impala", vc);
    schemaMap.put("impala", IMPALA_DATABASE + ".");
    return conn;
  }

  public static Connection setupRedshift() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Connection conn = DriverManager.getConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    VerdictConnection vc = new VerdictConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    loadData(conn);
    connMap.put("redshift", conn);
    vcMap.put("redshift", vc);
    schemaMap.put("redshift", "");
    return conn;
  }

  public static Connection setupPostgresql() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn = DriverManager.getConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    VerdictConnection vc = new VerdictConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    loadData(conn);
    connMap.put("postgresql", conn);
    vcMap.put("postgresql", vc);
    schemaMap.put("postgresql", "");
    return conn;
  }

  @Test
  public void runArbitraryQueryTest() throws SQLException {

    String sql = "SELECT 1+2";

    Statement jdbcStmt = connMap.get(database).createStatement();
    Statement vcStmt = vcMap.get(database).createStatement();

    ResultSet jdbcRs = jdbcStmt.executeQuery(sql);
    ResultSet vcRs = vcStmt.executeQuery(sql);
    while (jdbcRs.next() && vcRs.next()) {
      assertEquals(jdbcRs.getInt(1), vcRs.getInt(1));
    }

  }

  @Test
  public void runSelectQueryTest() throws SQLException {
    String sql = String.format("SELECT * FROM %s", schemaMap.get(database)) + "people order by birth";
    Statement jdbcStmt = connMap.get(database).createStatement();
    Statement vcStmt = vcMap.get(database).createStatement();

    ResultSet jdbcRs = jdbcStmt.executeQuery(sql);
    ResultSet vcRs = vcStmt.executeQuery(sql);
    assertEquals(jdbcRs.getMetaData().getColumnCount(), vcRs.getMetaData().getColumnCount());
    assertEquals(jdbcRs.getMetaData().getColumnCount(), 7);
    while (jdbcRs.next() && vcRs.next()) {
      assertEquals(jdbcRs.getInt(1), vcRs.getInt(1));
      assertEquals(jdbcRs.getString(2), vcRs.getString(2));
      assertEquals(jdbcRs.getString(3), vcRs.getString(3));
      assertEquals(jdbcRs.getFloat(4), vcRs.getFloat(4), 0.000001);
      assertEquals(jdbcRs.getFloat(5), vcRs.getFloat(5), 0.000001);
      assertEquals(jdbcRs.getString(6), vcRs.getString(6));
      assertEquals(jdbcRs.getTimestamp(7), vcRs.getTimestamp(7));
    }
  }

  @Test
  public void runAggregateQueryTest() throws SQLException {
    String sql = String.format("SELECT COUNT(*) FROM %s", schemaMap.get(database)) + "people";
    Statement jdbcStmt = connMap.get(database).createStatement();
    Statement vcStmt = vcMap.get(database).createStatement();

    ResultSet jdbcRs = jdbcStmt.executeQuery(sql);
    ResultSet vcRs = vcStmt.executeQuery(sql);
    while (jdbcRs.next() && vcRs.next()) {
      assertEquals(jdbcRs.getInt(1), vcRs.getInt(1));
    }
  }
}

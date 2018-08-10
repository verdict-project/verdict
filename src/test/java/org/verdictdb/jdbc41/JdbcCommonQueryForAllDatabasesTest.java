package org.verdictdb.jdbc41;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.sql.*;
import java.util.*;

import static org.junit.Assert.assertEquals;

/** Created by Dong Young Yoon on 7/18/18. */
@RunWith(Parameterized.class)
public class JdbcCommonQueryForAllDatabasesTest {

  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, VerdictConnection> vcMap = new HashMap<>();

  private static Map<String, String> schemaMap = new HashMap<>();

  private static VerdictOption options = new VerdictOption();

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
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
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

  private static final String SCHEMA_NAME =
      "verdict_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static VerdictConnection mysqlVc;

  public JdbcCommonQueryForAllDatabasesTest(String database) {
    this.database = database;
  }

  @BeforeClass
  public static void setupDatabases() throws SQLException, VerdictDBDbmsException {
    setupMysql();
    setupImpala();
    setupRedshift();
    setupPostgresql();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    for (String database : connMap.keySet()) {
      Connection conn = connMap.get(database);
      if (conn != null) {
        if (database.equals("mysql")) {
          conn.createStatement().execute(String.format("DROP SCHEMA IF EXISTS %s", SCHEMA_NAME));
        } else {
          conn.createStatement()
              .execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", SCHEMA_NAME));
        }
        conn.close();
      }
    }
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> databases() {
    return Arrays.asList("mysql", "impala", "redshift", "postgresql");
  }

  private static void loadData(Connection conn) throws SQLException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(
        Arrays.<Object>asList(1, "Anju", "female", 15, 170.2, "USA", "2017-10-01 21:22:23"));
    contents.add(
        Arrays.<Object>asList(2, "Sonia", "female", 17, 156.5, "USA", "2017-10-02 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168.1, "CHN", "2017-10-03 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male", 14, 178.6, "USA", "2017-10-04 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male", 18, 190.7, "CHN", "2017-10-05 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Sam", "male", 18, 190.0, "USA", "2017-10-06 21:22:23"));
    contents.add(
        Arrays.<Object>asList(3, "Alice", "female", 18, 190.21, "CHN", "2017-10-07 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Bob", "male", 18, 190.3, "CHN", "2017-10-08 21:22:23"));

    Statement stmt = conn.createStatement();
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", SCHEMA_NAME));
    stmt.execute(String.format("DROP TABLE IF EXISTS %s.people", SCHEMA_NAME));
    stmt.execute(
        String.format(
            "CREATE TABLE %s.people(id smallint, name varchar(255), gender varchar(8), age float, height float, nation varchar(8), birth timestamp)",
            SCHEMA_NAME));
    stmt.execute(
        String.format(
            "CREATE TABLE %s.drop_table(id smallint, name varchar(255), gender varchar(8), age float, height float, nation varchar(8), birth timestamp)",
            SCHEMA_NAME));
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      String age = row.get(3).toString();
      String height = row.get(4).toString();
      String nation = row.get(5).toString();
      String birth = row.get(6).toString();
      stmt.execute(
          String.format(
              "INSERT INTO %s.people(id, name, gender, age, height, nation, birth) VALUES(%s, '%s', '%s', %s, %s, '%s', '%s')",
              SCHEMA_NAME, id, name, gender, age, height, nation, birth));
    }
  }

  private static void loadDataImpala(Connection conn) throws SQLException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(
        Arrays.<Object>asList(1, "Anju", "female", 15, 170.2, "USA", "2017-10-01 21:22:23"));
    contents.add(
        Arrays.<Object>asList(2, "Sonia", "female", 17, 156.5, "USA", "2017-10-02 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168.1, "CHN", "2017-10-03 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male", 14, 178.6, "USA", "2017-10-04 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male", 18, 190.7, "CHN", "2017-10-05 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Sam", "male", 18, 190.0, "USA", "2017-10-06 21:22:23"));
    contents.add(
        Arrays.<Object>asList(3, "Alice", "female", 18, 190.21, "CHN", "2017-10-07 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Bob", "male", 18, 190.3, "CHN", "2017-10-08 21:22:23"));

    Statement stmt = conn.createStatement();
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", SCHEMA_NAME));
    stmt.execute(String.format("DROP TABLE IF EXISTS %s.people", SCHEMA_NAME));
    stmt.execute(
        String.format(
            "CREATE TABLE %s.people(id smallint, name string, gender string, age float, height float, nation string, birth timestamp)",
            SCHEMA_NAME));
    stmt.execute(
        String.format(
            "CREATE TABLE %s.drop_table(id smallint, name varchar(255), gender varchar(8), age float, height float, nation varchar(8), birth timestamp)",
            SCHEMA_NAME));
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      String age = row.get(3).toString();
      String height = row.get(4).toString();
      String nation = row.get(5).toString();
      String birth = row.get(6).toString();
      stmt.execute(
          String.format(
              "INSERT INTO %s.people(id, name, gender, age, height, nation, birth) VALUES(%s, '%s', '%s', %s, %s, '%s', '%s')",
              SCHEMA_NAME, id, name, gender, age, height, nation, birth));
    }
  }

  private static Connection setupMysql() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    Connection conn =
        DriverManager.getConnection(mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    VerdictConnection vc = new VerdictConnection(mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    loadData(conn);
    connMap.put("mysql", conn);
    vcMap.put("mysql", vc);
    schemaMap.put("mysql", MYSQL_DATABASE + ".");
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    return conn;
  }

  private static Connection setupImpala() throws SQLException, VerdictDBDbmsException {
    String connectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    Connection conn = DriverManager.getConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    VerdictConnection vc = new VerdictConnection(connectionString, IMPALA_USER, IMPALA_PASSWORD);
    loadDataImpala(conn);
    connMap.put("impala", conn);
    vcMap.put("impala", vc);
    schemaMap.put("impala", IMPALA_DATABASE + ".");
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    return conn;
  }

  public static Connection setupRedshift() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Connection conn =
        DriverManager.getConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    VerdictConnection vc =
        new VerdictConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    loadData(conn);
    connMap.put("redshift", conn);
    vcMap.put("redshift", vc);
    schemaMap.put("redshift", "");
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    return conn;
  }

  public static Connection setupPostgresql() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn =
        DriverManager.getConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    VerdictConnection vc =
        new VerdictConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    loadData(conn);
    connMap.put("postgresql", conn);
    vcMap.put("postgresql", vc);
    schemaMap.put("postgresql", "");
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
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
    String sql = String.format("SELECT * FROM %s", SCHEMA_NAME) + ".people order by birth";
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
  public void runSelectQueryWithBypassTest() throws SQLException {
    String sql = String.format("SELECT * FROM %s", SCHEMA_NAME) + ".people order by birth";
    String bypassSql =
        String.format("BYPASS SELECT * FROM %s", SCHEMA_NAME) + ".people order by birth";
    Statement jdbcStmt = connMap.get(database).createStatement();
    Statement vcStmt = vcMap.get(database).createStatement();

    ResultSet jdbcRs = jdbcStmt.executeQuery(sql);
    ResultSet vcRs = vcStmt.executeQuery(bypassSql);
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
  public void runDropQueryWithBypassTest() throws SQLException {
    String bypassSql = String.format("BYPASS DROP TABLE IF EXISTS %s.drop_table", SCHEMA_NAME);
    Statement vcStmt = vcMap.get(database).createStatement();
    vcStmt.execute(bypassSql);
    vcStmt.close();
  }

  @Test
  public void runInsertQueryWithBypassTest() throws SQLException {
    Statement stmt = vcMap.get(database).createStatement();
    int count =
        stmt.executeUpdate(
            String.format(
                "BYPASS INSERT INTO %s.people(id, name, gender, age, height, nation, birth) "
                    + "VALUES(10, 'mike', 'male', 38, 190, 'Russia', '1980-10-10 10:10:10')",
                SCHEMA_NAME));
    if (database.equals("impala")) {
      assertEquals(-1, count); // Impala JDBC always returns -1.
    } else {
      assertEquals(1, count);
    }
  }

  @Test
  public void runAggregateQueryTest() throws SQLException {
    String sql = String.format("SELECT COUNT(*) FROM %s", SCHEMA_NAME) + ".people";
    Statement jdbcStmt = connMap.get(database).createStatement();
    Statement vcStmt = vcMap.get(database).createStatement();

    ResultSet jdbcRs = jdbcStmt.executeQuery(sql);
    ResultSet vcRs = vcStmt.executeQuery(sql);
    while (jdbcRs.next() && vcRs.next()) {
      assertEquals(jdbcRs.getInt(1), vcRs.getInt(1));
    }
  }

  @Test
  public void runSelectTopQueryRedshiftTest() throws SQLException {
    if (database.equals("redshift")) {
      String sql = String.format("SELECT TOP 3 * FROM %s", SCHEMA_NAME) + ".people order by birth";
      Statement jdbcStmt = connMap.get(database).createStatement();
      Statement vcStmt = vcMap.get(database).createStatement();

      ResultSet jdbcRs = jdbcStmt.executeQuery(sql);
      ResultSet vcRs = vcStmt.executeQuery(sql);
      int rowCount = 0;
      while (jdbcRs.next() && vcRs.next()) {
        assertEquals(jdbcRs.getInt(1), vcRs.getInt(1));
        assertEquals(jdbcRs.getString(2), vcRs.getString(2));
        assertEquals(jdbcRs.getString(3), vcRs.getString(3));
        assertEquals(jdbcRs.getFloat(4), vcRs.getFloat(4), 0.000001);
        assertEquals(jdbcRs.getFloat(5), vcRs.getFloat(5), 0.000001);
        assertEquals(jdbcRs.getString(6), vcRs.getString(6));
        assertEquals(jdbcRs.getTimestamp(7), vcRs.getTimestamp(7));
        ++rowCount;
      }
      assertEquals(3, rowCount);
    }
  }
}

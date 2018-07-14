package org.verdictdb.core.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.HiveSyntax;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;

public class JdbcConnectionTest {

  private static Connection h2Conn;

  private static Connection postgresConn;

  private static Statement stmt;

  private ResultSetMetaData jdbcResultSetMetaData1, jdbcResultSetMetaData2;

  private ResultSet rs;

  private static Connection mysqlConn;

  private static Statement mysqlStmt;

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_UESR;

  private static final String MYSQL_PASSWORD = "";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
      MYSQL_UESR = "root";
    } else {
      MYSQL_HOST = "localhost";
      MYSQL_UESR = "root";
    }
  }

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

  @BeforeClass
  public static void setupH2Database() throws SQLException {
    final String DB_CONNECTION = "jdbc:h2:mem:testconn;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    h2Conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    mysqlConn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
  }

  @BeforeClass
  public static void setupPostgresqlDatabase() throws SQLException {
    String postgresConnectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    postgresConn = DriverManager.getConnection(postgresConnectionString, POSTGRES_USER, POSTGRES_PASSWORD);

    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female", 15, 170.2, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female", 17, 156.5, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168.1, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(4, "Joe", "male", 14, 178.6, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(5, "JoJo", "male", 18, 190.7, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(6, "Sam", "male", 18, 190.0, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(7, "Alice", "female", 18, 190.21, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(8, "Bob", "male", 18, 190.3, "CHN", "2017-10-12 21:22:23"));
    stmt = postgresConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS PEOPLE");
    stmt.execute("CREATE TABLE PEOPLE(id smallint, name varchar(255), gender varchar(8), age float, height float, nation varchar(8), birth timestamp)");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      String age = row.get(3).toString();
      String height = row.get(4).toString();
      String nation = row.get(5).toString();
      String birth = row.get(6).toString();
      stmt.execute(String.format("INSERT INTO PEOPLE(id, name, gender, age, height, nation, birth) VALUES(%s, '%s', '%s', %s, %s, '%s', '%s')", id, name, gender, age, height, nation, birth));
    }
  }

  @Test
  public void testJdbcConnection() throws VerdictDBDbmsException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju"));
    contents.add(Arrays.<Object>asList(2, "Sonia"));
    contents.add(Arrays.<Object>asList(3, "Asha"));

    JdbcConnection jdbc = new JdbcConnection(h2Conn, new HiveSyntax());

    jdbc.execute("CREATE TABLE PERSON(id int, name varchar(255))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      jdbc.execute(String.format("INSERT INTO PERSON(id, name) VALUES(%s, '%s')", id, name));
    }
  }

  @Test
  public void testPostgresMetaData() throws VerdictDBDbmsException {
    JdbcConnection jdbc = new JdbcConnection(postgresConn, new PostgresqlSyntax());
    System.out.println(jdbc.getColumns("public", "people"));
  }

  @Test
  public void testMySqlShowSchemas() throws VerdictDBDbmsException {
    JdbcConnection jdbc = new JdbcConnection(mysqlConn, new MysqlSyntax());
    System.out.println(jdbc.getSchemas());
  }

}

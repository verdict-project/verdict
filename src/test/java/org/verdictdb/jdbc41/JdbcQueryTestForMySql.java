package org.verdictdb.jdbc41;

import static org.junit.Assert.*;

import org.junit.*;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by Dong Young Yoon on 7/18/18.
 */
public class JdbcQueryTestForMySql {

  static Connection conn;

  private static Statement stmt;

  private ResultSetMetaData jdbcResultSetMetaData1;

  private ResultSetMetaData jdbcResultSetMetaData2;

  private static final String MYSQL_HOST;

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

  private ResultSet rs;

  private static VerdictConnection vc;

  @BeforeClass
  public static void setupDatabase() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    conn = DriverManager.getConnection(mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);

    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female", 15, 170.2, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female", 17, 156.5, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168.1, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male", 14, 178.6, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male", 18, 190.7, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Sam", "male", 18, 190.0, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Alice", "female", 18, 190.21, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Bob", "male", 18, 190.3, "CHN", "2017-10-12 21:22:23"));
    stmt = conn.createStatement();
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
    vc = new VerdictConnection(mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
  }

  @AfterClass
  public static void cleanUp() throws SQLException {
    vc.close();
  }

  @Test
  public void runArbitraryQueryTest() throws VerdictDBDbmsException, SQLException {
    Statement stmt = vc.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1+2");
    assertTrue(rs.next());
    assertEquals(rs.getInt(1), 3);
  }

  @Test
  public void connectWithPropertiesTest() throws VerdictDBDbmsException, SQLException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    Properties prop = new Properties();
    prop.setProperty("user", MYSQL_USER);
    prop.setProperty("password", MYSQL_PASSWORD);
    VerdictConnection testVc = new VerdictConnection(mysqlConnectionString, prop);
    Statement stmt = testVc.createStatement();
    assertNotNull(stmt);
    testVc.close();
  }
}

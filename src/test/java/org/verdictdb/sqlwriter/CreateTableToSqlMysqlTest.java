package org.verdictdb.sqlwriter;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;

public class CreateTableToSqlMysqlTest {

  private static Connection mysqlConn;

  private static Statement mysqlStmt;

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_UESR;

  private static final String MYSQL_PASSWORD = "";
  
  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
      MYSQL_UESR = "mysql";
    } else {
      MYSQL_HOST = "localhost";
      MYSQL_UESR = "root";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    mysqlConn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);

    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female", 15, 170.2, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female", 17, 156.5, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168.1, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male", 14, 178.6, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male", 18, 190.7, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Sam", "male", 18, 190.0, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Alice", "female", 18, 190.21, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Bob", "male", 18, 190.3, "CHN", "2017-10-12 21:22:23"));
    mysqlStmt = mysqlConn.createStatement();
    mysqlStmt.execute("CREATE SCHEMA IF NOT EXISTS TEST");
    mysqlStmt.execute("USE TEST");
    mysqlStmt.execute("DROP TABLE IF EXISTS PEOPLE");
    mysqlStmt.execute("CREATE TABLE PEOPLE(id smallint, name varchar(255), gender varchar(8), age float, height float, nation varchar(8), birth timestamp)");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      String age = row.get(3).toString();
      String height = row.get(4).toString();
      String nation = row.get(5).toString();
      String birth = row.get(6).toString();
      mysqlStmt.execute(String.format("INSERT INTO PEOPLE(id, name, gender, age, height, nation, birth) VALUES(%s, '%s', '%s', %s, %s, '%s', '%s')", id, name, gender, age, height, nation, birth));
    }
  }

  @Test
  public void createTableSelectAllWithSignlePartitionMySqlTest() throws VerdictDBException {
    BaseTable base = new BaseTable("test", "people", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelectQuery create = new CreateTableAsSelectQuery("test", "newtable", relation);
    create.addPartitionColumn("id");
    String expected = "create table `test`.`newtable` partition by key (`id`) select * from `test`.`people` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);

    DbmsConnection dbmsConn = new JdbcConnection(mysqlConn, new MysqlSyntax());
    dbmsConn.execute("drop table if exists test.newtable");
    dbmsConn.execute(actual);
  }

  @Test
  public void createTableSelectAllWithMultiPartitionMySqlTest() throws VerdictDBException {
    BaseTable base = new BaseTable("test", "people", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelectQuery create = new CreateTableAsSelectQuery("test", "newtable", relation);
    create.addPartitionColumn("id");
    create.addPartitionColumn("age");
    //    String expected = "create table `newschema`.`newtable` partitioned by (`part1`, `part2`) as select * from `myschema`.`mytable` as t";
    String expected = "create table `test`.`newtable` partition by key (`id`, `age`) select * from `test`.`people` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);

    DbmsConnection dbmsConn = new JdbcConnection(mysqlConn, new MysqlSyntax());
    dbmsConn.execute("drop table if exists test.newtable");
    dbmsConn.execute(actual);
  }
}

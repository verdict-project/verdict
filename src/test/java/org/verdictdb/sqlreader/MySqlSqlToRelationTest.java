package org.verdictdb.sqlreader;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.UnnamedColumn;

public class MySqlSqlToRelationTest {

  static Connection conn;

  private static Statement stmt;

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    conn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);

    stmt = conn.createStatement();
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
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute("DROP TABLE IF EXISTS people");
  }

  @Test
  public void testSelectAllBaseTable() {
    String actual = "select * from myschema.mytable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new AsteriskColumn()
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    assertEquals(expected, sel);
  }

  @Test
  public void testQuotedQuery() {
    String actual = "select `t`.* from `myschema`.`mytable` as `t`";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new AsteriskColumn("t")
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    assertEquals(expected, sel);
  }

  @Test
  public void testSubstring() {
    String actual = "select substring('abc', 1, 2) from `myschema`.`mytable` as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new ColumnOp("substring", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'abc'"), ConstantColumn.valueOf(1), ConstantColumn.valueOf(2)))
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    assertEquals(expected, sel);
  }

  @Test
  public void testMod() {
    String actual = "select mod(5, 2) from `myschema`.`mytable` as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery expected = SelectQuery.create(Arrays.<SelectItem>asList(
        new ColumnOp("mod", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf(5), ConstantColumn.valueOf(2)))
    ), Arrays.<AbstractRelation>asList(new BaseTable("myschema", "mytable", "t")));
    AbstractRelation sel = sqlToRelation.toRelation(actual);
    assertEquals(expected, sel);
  }
}

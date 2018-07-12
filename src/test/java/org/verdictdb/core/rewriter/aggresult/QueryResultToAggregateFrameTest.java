package org.verdictdb.core.rewriter.aggresult;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.aggresult.AggregateFrame;
import org.verdictdb.core.aggresult.AggregateGroup;
import org.verdictdb.core.connection.JdbcQueryResult;
import org.verdictdb.exception.VerdictDBValueException;

public class QueryResultToAggregateFrameTest {

  static Connection conn;

  @BeforeClass
  public static void setupH2Database() throws SQLException {
    final String DB_CONNECTION = "jdbc:h2:mem:~/test;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
  }

  @AfterClass
  public static void closeH2Connection() throws SQLException {
    conn.close();
  }

  @Test
  public void testH2Connection() throws SQLException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju"));
    contents.add(Arrays.<Object>asList(2, "Sonia"));
    contents.add(Arrays.<Object>asList(3, "Asha"));

    Statement stmt = conn.createStatement();
    stmt.execute("CREATE TABLE PERSON(id int, name varchar(255))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      stmt.execute(String.format("INSERT INTO PERSON(id, name) VALUES(%s, '%s')", id, name));
    }

    ResultSet rs = stmt.executeQuery("SELECT * FROM PERSON");
    int index = 0;
    while (rs.next()) {
      int id = rs.getInt(1);
      String name = rs.getString(2);
      assertEquals(contents.get(index).get(0), id);
      assertEquals(contents.get(index).get(1), name);
      index += 1;
    }
    assertEquals(3, index);
  }

  @Test
  public void testCountQueryToAggregateFrame() throws SQLException, VerdictDBValueException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male"));
    Statement stmt = conn.createStatement();
    stmt.execute("DROP TABLE PEOPLE IF EXISTS");
    stmt.execute("CREATE TABLE PEOPLE(id int, name varchar(255), gender varchar(8))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      stmt.execute(String.format("INSERT INTO PEOPLE(id, name, gender) VALUES(%s, '%s', '%s')", id, name, gender));
    }
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    assertEquals(2, aggregateFrame.getColumnNames().size());
    List<String> gender = Arrays.asList("GENDER");
    List<Object> female = new ArrayList<>();
    female.add("female");
    AggregateGroup group = new AggregateGroup(gender, female);
    assertEquals(new Long(2),  aggregateFrame.getMeasures(group).getAttributeValueAt(0));
  }

  @Test
  public void testSumQueryToAggregateFrame() throws SQLException, VerdictDBValueException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male"));
    Statement stmt = conn.createStatement();
    stmt.execute("DROP TABLE PEOPLE IF EXISTS");
    stmt.execute("CREATE TABLE PEOPLE(id int, name varchar(255), gender varchar(8))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      stmt.execute(String.format("INSERT INTO PEOPLE(id, name, gender) VALUES(%s, '%s', '%s')", id, name, gender));
    }
    ResultSet rs = stmt.executeQuery("SELECT gender, sum(id) as s FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("S", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    assertEquals(2, aggregateFrame.getColumnNames().size());
    List<String> gender = Arrays.asList("GENDER");
    List<Object> female = new ArrayList<>();
    female.add("female");
    AggregateGroup group = new AggregateGroup(gender, female);
    assertEquals(new Long(3),  aggregateFrame.getMeasures(group).getAttributeValueAt(0));
  }

  @Test
  public void testCountQueryToAggregateFrame2() throws SQLException, VerdictDBValueException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female", 15, 170, "USA"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female", 17, 156, "USA"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168, "CHN"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male", 14, 178, "USA"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male", 18, 190, "CHN"));
    contents.add(Arrays.<Object>asList(3, "Sam", "male", 18, 190, "USA"));
    contents.add(Arrays.<Object>asList(3, "Alice", "female", 18, 190, "CHN"));
    contents.add(Arrays.<Object>asList(3, "Bob", "male", 18, 190, "CHN"));
    Statement stmt = conn.createStatement();
    stmt.execute("DROP TABLE PEOPLE IF EXISTS");
    stmt.execute("CREATE TABLE PEOPLE(id int, name varchar(255), gender varchar(8), age int, height int, nation varchar(8))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      String age = row.get(3).toString();
      String height = row.get(4).toString();
      String nation = row.get(5).toString();
      stmt.execute(String.format("INSERT INTO PEOPLE(id, name, gender, age, height, nation) VALUES(%s, '%s', '%s', %s, %s, '%s')", id, name, gender, age, height, nation));
    }
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    assertEquals(3, aggregateFrame.getColumnNames().size());
    List<String> gender = Arrays.asList("GENDER");
    List<Object> female = new ArrayList<>();
    female.add("female");
    AggregateGroup group = new AggregateGroup(gender, female);
    assertEquals(new Long(3),  aggregateFrame.getMeasures(group).getAttributeValueAt(0));
    assertEquals(new Long(50),  aggregateFrame.getMeasures(group).getAttributeValueAt(1));
  }

  @Test
  public void testCountQueryToAggregateFrame3() throws SQLException, VerdictDBValueException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female", 15, 170, "USA"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female", 17, 156, "USA"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168, "CHN"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male", 14, 178, "USA"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male", 18, 190, "CHN"));
    contents.add(Arrays.<Object>asList(3, "Sam", "male", 18, 190, "USA"));
    contents.add(Arrays.<Object>asList(3, "Alice", "female", 18, 190, "CHN"));
    contents.add(Arrays.<Object>asList(3, "Bob", "male", 18, 190, "CHN"));
    Statement stmt = conn.createStatement();
    stmt.execute("DROP TABLE PEOPLE IF EXISTS");
    stmt.execute("CREATE TABLE PEOPLE(id int, name varchar(255), gender varchar(8), age int, height int, nation varchar(8))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      String age = row.get(3).toString();
      String height = row.get(4).toString();
      String nation = row.get(5).toString();
      stmt.execute(String.format("INSERT INTO PEOPLE(id, name, gender, age, height, nation) VALUES(%s, '%s', '%s', %s, %s, '%s')", id, name, gender, age, height, nation));
    }
    ResultSet rs = stmt.executeQuery("SELECT sum(age) as agesum, gender, count(*) as cnt, nation as nation FROM PEOPLE GROUP BY gender, nation");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("NATION");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    assertEquals(4, aggregateFrame.getColumnNames().size());
    List<String> gender = Arrays.asList("GENDER", "NATION");
    List<Object> female = new ArrayList<>();
    female.add("female");
    female.add("USA");
    AggregateGroup group = new AggregateGroup(gender, female);
    assertEquals(new Long(32),  aggregateFrame.getMeasures(group).getAttributeValueAt(0));
    assertEquals(new Long(2),  aggregateFrame.getMeasures(group).getAttributeValueAt(1));
  }
}

package org.verdictdb;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.h2.jdbc.JdbcArray;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.JdbcQueryResult;
import org.verdictdb.core.aggresult.AggregateFrame;
import org.verdictdb.core.aggresult.AggregateFrameQueryResult;
import org.verdictdb.core.sql.SqlToRelationTest;
import org.verdictdb.exception.ValueException;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JdbcDriverTest {

  static Connection conn;

  private JdbcDriver jdbcDriver;

  private static Statement stmt;

  @BeforeClass
  public static void setupH2Database() throws SQLException {
    final String DB_CONNECTION = "jdbc:h2:mem:testconn;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

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
    stmt.execute("DROP TABLE PEOPLE IF EXISTS");
    stmt.execute("CREATE TABLE PEOPLE(id smallint, name varchar(255), gender varchar(8), age int, height float, nation varchar(8), birth timestamp)");
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
  public void getStringIndexTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);
    List<String> actual = new ArrayList<>();
    List<String> expected = Arrays.asList("female", "male");
    while (jdbcDriver.next()) {
      actual.add(jdbcDriver.getString(0));
    }
    assertEquals(expected, actual);
  }

  @Test
  public void getStringLabelTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);
    List<String> actual = new ArrayList<>();
    List<String> expected = Arrays.asList("female", "male");
    while (jdbcDriver.next()) {
      actual.add(jdbcDriver.getString("GENDER"));
    }
    assertEquals(expected, actual);
  }

  @Test
  public void getIntegerIndexTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      if (jdbcDriver.getString(0).equals("female")) {
        assertEquals(3, jdbcDriver.getInt(1));
      } else assertEquals(5, jdbcDriver.getInt(1));
    }
  }

  @Test
  public void getIntegerLabelTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      if (jdbcDriver.getString(0).equals("female")) {
        assertEquals(3, jdbcDriver.getInt("CNT"));
      } else assertEquals(5, jdbcDriver.getInt("CNT"));
    }
  }

  @Test
  public void getLongIndexTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      if (jdbcDriver.getString(0).equals("female")) {
        assertEquals(3, jdbcDriver.getLong(1));
      } else assertEquals(5, jdbcDriver.getLong(1));
    }
  }

  @Test
  public void getLongLabelTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      if (jdbcDriver.getString(0).equals("female")) {
        assertEquals(3, jdbcDriver.getLong("CNT"));
      } else assertEquals(5, jdbcDriver.getLong("CNT"));
    }
  }

  @Test
  public void getDoubleIndexTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      if (jdbcDriver.getString(0).equals("female")) {
        double expected = (170.2 + 156.5 + 190.21) / 3;
        assertEquals(expected, jdbcDriver.getDouble(2), 0.0001);
      } else if (jdbcDriver.getString(0).equals("male")) {
        double expected = (168.1 + 178.6 + 190.7 + 190.0 + 190.3) / 5;
        assertEquals(expected, jdbcDriver.getDouble(2), 0.0001);
      }
    }
  }

  @Test
  public void getDoubleLabelTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      if (jdbcDriver.getString(0).equals("female")) {
        double expected = (170.2 + 156.5 + 190.21) / 3;
        assertEquals(expected, jdbcDriver.getDouble("A"), 0.0001);
      } else if (jdbcDriver.getString(0).equals("male")) {
        double expected = (168.1 + 178.6 + 190.7 + 190.0 + 190.3) / 5;
        assertEquals(expected, jdbcDriver.getDouble("A"), 0.0001);
      }
    }
  }

  @Test
  public void getFloatIndexTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      if (jdbcDriver.getString(0).equals("female")) {
        double expected = (170.2 + 156.5 + 190.21) / 3;
        assertEquals(expected, jdbcDriver.getFloat(2), 0.0001);
      } else if (jdbcDriver.getString(0).equals("male")) {
        double expected = (168.1 + 178.6 + 190.7 + 190.0 + 190.3) / 5;
        assertEquals(expected, jdbcDriver.getFloat(2), 0.0001);
      }
    }
  }

  @Test
  public void getFloatLabelTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      if (jdbcDriver.getString(0).equals("female")) {
        double expected = (170.2 + 156.5 + 190.21) / 3;
        assertEquals(expected, jdbcDriver.getFloat("A"), 0.0001);
      } else if (jdbcDriver.getString(0).equals("male")) {
        double expected = (168.1 + 178.6 + 190.7 + 190.0 + 190.3) / 5;
        assertEquals(expected, jdbcDriver.getFloat("A"), 0.0001);
      }
    }
  }

  @Test
  public void DoubleToIntegerTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      if (jdbcDriver.getString(0).equals("female")) {
        double expected = (170.2 + 156.5 + 190.21) / 3;
        assertEquals((int)expected, jdbcDriver.getInt("A"));
        assertEquals((long)expected, jdbcDriver.getLong("A"));
        assertEquals((short)expected, jdbcDriver.getShort("A"));
        assertEquals((long)expected, jdbcDriver.getBigDecimal("A").longValue());
      } else if (jdbcDriver.getString(0).equals("male")) {
        double expected = (168.1 + 178.6 + 190.7 + 190.0 + 190.3) / 5;
        assertEquals((int)expected, jdbcDriver.getInt("A"));
        assertEquals((long)expected, jdbcDriver.getLong("A"));
        assertEquals((short)expected, jdbcDriver.getShort("A"));
        assertEquals((long)expected, jdbcDriver.getBigDecimal("A").longValue());
      }
    }
  }

  @Test
  public void IntegerToDoubleTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      if (jdbcDriver.getString(0).equals("female")) {
        int expected = 3;
        assertEquals((float) expected, jdbcDriver.getFloat(1), 0.0001);
        assertEquals((double)expected, jdbcDriver.getBigDecimal(1).doubleValue(), 0.0001);
        assertEquals((double)expected, jdbcDriver.getDouble(1), 0.0001);
      } else if (jdbcDriver.getString(0).equals("male")) {
        int expected = 5;
        assertEquals((float) expected, jdbcDriver.getFloat(1), 0.0001);
        assertEquals((double)expected, jdbcDriver.getBigDecimal(1).doubleValue(), 0.0001);
        assertEquals((double)expected, jdbcDriver.getDouble(1), 0.0001);
      }
    }
  }

  @Test
  public void getTimestampTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT birth, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY birth");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("BIRTH");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    agg.add(new ImmutablePair<>("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      Timestamp timestamp = Timestamp.valueOf("2017-10-12 21:22:23.0");
      assertEquals(timestamp, jdbcDriver.getTimestamp(0));
      assertEquals(timestamp, jdbcDriver.getTimestamp("BIRTH"));
      assertEquals(new Date(timestamp.getTime()), jdbcDriver.getDate(0));
      assertEquals(new Time(timestamp.getTime()), jdbcDriver.getTime(0));
    }
  }

  @Test
  public void getBinaryTest() throws SQLException, ValueException, IOException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(12 as Binary) as bin FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("BIN");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      byte[] bin = jdbcDriver.getBytes(2);
      assertEquals(12, bin[3]);
      InputStream binStream = jdbcDriver.getBinaryStream(2);
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(12, binStream.read());
    }
  }

  @Test
  public void getVarbinaryTest() throws SQLException, ValueException, IOException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(12 as varbinary(max)) as bin FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("BIN");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      byte[] bin = jdbcDriver.getBytes(2);
      assertEquals(12, bin[3]);
      InputStream binStream = jdbcDriver.getBinaryStream(2);
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(12, binStream.read());
    }
  }

  @Test
  public void getLongvarbinaryTest() throws SQLException, ValueException, IOException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(12 as longvarbinary(max)) as bin FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("BIN");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      byte[] bin = jdbcDriver.getBytes(2);
      assertEquals(12, bin[3]);
      InputStream binStream = jdbcDriver.getBinaryStream(2);
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(12, binStream.read());
    }
  }

  @Test
  public void getBitTest() throws SQLException, ValueException, IOException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(0 as bit) as bool FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("BOOL");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      byte bin = jdbcDriver.getByte(2);
      assertEquals(0, bin);
      boolean b = jdbcDriver.getBoolean(2);
      assertEquals(false, b);
      int i = jdbcDriver.getInt(2);
      assertEquals(0, i);
    }
  }

  @Test
  public void getBlobTest() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(0x1234567 as blob) as b FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("B");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      Blob bin = jdbcDriver.getBlob(2);
      byte[] b = bin.getBytes(0,4);
      assertEquals(25, b[0]);
      assertEquals(8, b[1]);
      assertEquals(-121, b[2]);
      assertEquals(67, b[3]);
    }
  }

  @Test
  public void getBoolean() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(0 as boolean) as b FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("B");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      Boolean b = jdbcDriver.getBoolean(2);
      assertEquals(false, b);
      int i = jdbcDriver.getInt(2);
      assertEquals(0, i);
    }
  }

  @Test
  public void getClob() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(0x1234567 as clob) as b FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("B");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      Clob bin = jdbcDriver.getClob(2);
      assertEquals(8, bin.length());
    }
  }

  @Test
  public void getArray() throws SQLException, ValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast((1, 2, 3) as array) as b FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<Pair<String, String>> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("B");
    agg.add(new ImmutablePair<>("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    jdbcDriver = new JdbcDriver(aggregateFrameQueryResult, conn);

    while (jdbcDriver.next()) {
      Array a = jdbcDriver.getArray(2);
      Object[] arr = (Object[])a.getArray();
      assertEquals(3, arr.length);
      assertEquals(1, arr[0]);
      assertEquals(2, arr[1]);
      assertEquals(3, arr[2]);
    }
  }
}

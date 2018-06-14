package org.verdictdb.core;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DataTypeConverter;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.sql.syntax.H2Syntax;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DbmsMetadataCacheTest {

  static Connection conn;

  private DbmsMetadataCache metadataCache = new DbmsMetadataCache(new JdbcConnection(conn, new H2Syntax()));

  private static Statement stmt;

  @BeforeClass
  public static void setupH2Database() throws SQLException {
    final String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
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
  public void getSchemaTest() throws SQLException {
    List<String> schemas = metadataCache.getSchemas();
    assertEquals(2, schemas.size());
    assertEquals(true, schemas.get(0).equals("PUBLIC")||schemas.get(1).equals("PUBLIC"));
    assertEquals(true, schemas.get(0).equals("INFORMATION_SCHEMA")||schemas.get(1).equals("INFORMATION_SCHEMA"));
  }

  @Test
  public void getTableTest() throws SQLException {
    List<String> tables = metadataCache.getTables("PUBLIC");
    assertEquals(1, tables.size());
    assertEquals("PEOPLE", tables.get(0));
  }

  @Test
  public void getColumnsTest() throws SQLException {
    List<Pair<String, Integer>> columns = metadataCache.getColumns("PUBLIC", "PEOPLE");
    assertEquals(7, columns.size());
    assertEquals("ID", columns.get(0).getKey());
    assertEquals(DataTypeConverter.typeInt("smallint"), (int)columns.get(0).getValue());
    assertEquals("NAME", columns.get(1).getKey());
    assertEquals(DataTypeConverter.typeInt("varchar"), (int)columns.get(1).getValue());
    assertEquals("GENDER", columns.get(2).getKey());
    assertEquals(DataTypeConverter.typeInt("varchar"), (int)columns.get(2).getValue());
    assertEquals("AGE", columns.get(3).getKey());
    assertEquals(DataTypeConverter.typeInt("integer"), (int)columns.get(3).getValue());
    assertEquals("HEIGHT", columns.get(4).getKey());
    assertEquals(DataTypeConverter.typeInt("double"), (int)columns.get(4).getValue());
    assertEquals("NATION", columns.get(5).getKey());
    assertEquals(DataTypeConverter.typeInt("varchar"), (int)columns.get(5).getValue());
    assertEquals("BIRTH", columns.get(6).getKey());
    assertEquals(DataTypeConverter.typeInt("timestamp"), (int)columns.get(6).getValue());
  }
}

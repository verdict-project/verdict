package org.verdictdb.core.resultset_converter;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class QueryResultToAggregateFrameTest {
  
  private static final String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
  private static final String DB_USER = "";
  private static final String DB_PASSWORD = "";

  @Test
  public void testH2Connection() throws SQLException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju"));
    contents.add(Arrays.<Object>asList(2, "Sonia"));
    contents.add(Arrays.<Object>asList(3, "Asha"));
    
    Connection conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
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

}

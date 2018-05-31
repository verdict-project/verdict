package org.verdictdb.connection;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;

public class JdbcConnectionTest {

  @Test
  public void testH2Connection() throws SQLException {
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju"));
    contents.add(Arrays.<Object>asList(2, "Sonia"));
    contents.add(Arrays.<Object>asList(3, "Asha"));
    
    final String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    Connection conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
    JdbcConnection jdbc = new JdbcConnection(conn);
    
    jdbc.executeUpdate("CREATE TABLE PERSON(id int, name varchar(255))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      jdbc.executeUpdate(String.format("INSERT INTO PERSON(id, name) VALUES(%s, '%s')", id, name));
    }
    
    DbmsQueryResult rs = jdbc.executeQuery("SELECT * FROM PERSON");
    int index = 0;
    while (rs.next()) {
      int id = (Integer) rs.getValue(0);
      String name = (String) rs.getValue(1);
      assertEquals(contents.get(index).get(0), id);
      assertEquals(contents.get(index).get(1), name);
      index += 1;
    }
    assertEquals(3, index);
    assertEquals(2, rs.getColumnCount());
  }

}

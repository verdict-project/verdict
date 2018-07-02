package org.verdictdb.core.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.HiveSyntax;

public class JdbcQueryResultTest {
  
  static Connection conn;
  
  static JdbcConnection jdbc;
  
  static List<List<Object>> contents;
  
  @BeforeClass
  public static void setupH2Database() throws VerdictDBDbmsException, SQLException {
    final String DB_CONNECTION = "jdbc:h2:mem:testqueryresult;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
    
    contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju"));
    contents.add(Arrays.<Object>asList(2, "Sonia"));
    contents.add(Arrays.<Object>asList(3, "Asha"));
    
    jdbc = new JdbcConnection(conn, new HiveSyntax());
    
    jdbc.execute("CREATE TABLE PERSON(id int, name varchar(255))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      jdbc.execute(String.format("INSERT INTO PERSON(id, name) VALUES(%s, '%s')", id, name));
    }
  }

  @Test
  public void testJdbcQueryResultValues() throws SQLException, VerdictDBDbmsException {
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
  
  @Test
  public void testJdbcQueryColumnName() throws VerdictDBDbmsException {
    DbmsQueryResult rs = jdbc.executeQuery("SELECT name as alias1 FROM PERSON");
    assertTrue("alias1".equalsIgnoreCase(rs.getColumnName(0)));
  }

}

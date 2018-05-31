package org.verdictdb.dbms_interface;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcInterface implements DbmsInterface {
  
  Connection conn;
  
  public JdbcInterface(Connection conn) {
    this.conn = conn;
  }

  @Override
  public DbmsQueryResult executeQuery(String query) {
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      JdbcQueryResult jrs = new JdbcQueryResult(rs);
      return jrs;
      
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public int executeUpdate(String query) {
    try {
      Statement stmt = conn.createStatement();
      int r = stmt.executeUpdate(query);
      return r;
    } catch (SQLException e) {
      e.printStackTrace();
      return 0;
    }
  }

}

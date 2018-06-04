package org.verdictdb.connection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.verdictdb.sql.syntax.SyntaxAbstract;

public class JdbcConnection implements DbmsConnection {
  
  Connection conn;
  
  SyntaxAbstract syntax;
  
  public JdbcConnection(Connection conn, SyntaxAbstract syntax) {
    this.conn = conn;
    this.syntax = syntax;
  }
  
  @Override
  public void close() {
    try {
      this.conn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
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

  @Override
  public SyntaxAbstract getSyntax() {
    return syntax;
  }

}

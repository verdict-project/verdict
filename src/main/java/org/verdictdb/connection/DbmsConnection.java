package org.verdictdb.connection;

import java.sql.Connection;

import org.verdictdb.sql.syntax.SyntaxAbstract;

public interface DbmsConnection {
  
  public DbmsQueryResult executeQuery(String query);
  
  public void close();

  /**
   * 
   * @param sql
   * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for 
   * SQL statements that return nothing
   */
  public int executeUpdate(String query);
  
  public SyntaxAbstract getSyntax();

  public Connection getConnection();

}

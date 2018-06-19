package org.verdictdb.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
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

  public List<String> getSchemas() throws SQLException;

  List<String> getTables(String schema) throws SQLException;

  public List<Pair<String, Integer>> getColumns(String schema, String table) throws SQLException;

  public List<String> getPartitionColumns(String schema, String table) throws SQLException;

}

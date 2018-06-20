package org.verdictdb.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.sql.syntax.SyntaxAbstract;

import com.google.common.base.Optional;

public interface DbmsConnection extends MetaDataProvider {
  
  public DbmsQueryResult executeQuery(String query);
  
  /**
   * 
   * @param sql
   * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for 
   * SQL statements that return nothing
   */
  public int executeUpdate(String query);
  
  public SyntaxAbstract getSyntax();

  public Connection getConnection();
  
  public Optional<String> getDefaultSchema();
  
  public void setDefaultSchema();

  public void close();

}

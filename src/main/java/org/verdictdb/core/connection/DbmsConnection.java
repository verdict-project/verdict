package org.verdictdb.core.connection;

import java.sql.Connection;

import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public interface DbmsConnection extends MetaDataProvider {
  
  public DbmsQueryResult executeQuery(String query) throws VerdictDBDbmsException;
  
  /**
   * 
   * @param sql
   * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for 
   * SQL statements that return nothing
   */
  public int executeUpdate(String query) throws VerdictDBDbmsException;
  
  public SqlSyntax getSyntax();

  public Connection getConnection();

  public void close();

}

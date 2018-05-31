package org.verdictdb.dbms_interface;

public interface DbmsInterface {
  
  public DbmsQueryResult executeQuery(String query);

  /**
   * 
   * @param sql
   * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for 
   * SQL statements that return nothing
   */
  public int executeUpdate(String query);

}

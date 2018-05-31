package org.verdictdb.connection;

public interface DbmsQueryResult {
  
  public int getColumnCount();
  
  /**
   * 
   * @param index zero-based index
   * @return
   */
  public String getColumnName(int index);
  
  /**
   * 
   * @param index zero-based index
   * @return
   */
  public int getColumnType(int index);
  
  /**
   * Forward a cursor to rows by one. Similar to JDBC ResultSet.next().
   * @return True if next row exists.
   */
  public boolean next();
  
  /**
   * 
   * @param index zero-based index
   * @return
   */
  public Object getValue(int index);

}

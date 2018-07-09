package org.verdictdb.core.connection;

import java.sql.Date;
import java.sql.Timestamp;

public interface DbmsQueryResult {

  /**
   *
   * @return Meta Data from ResultSet
   */
  public DbmsQueryResultMetaData getMetaData();

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
   * set the index before the first one; when next() is called, the index will move to the first row.
   */
  public void rewind();
  
  /**
   * Forward a cursor to rows by one. Similar to JDBC ResultSet.next().
   * @return True if next row exists.
   */
  public boolean next();
  
  /**
   * 
   * @param index This is a zero-based index.
   * @return
   */
  public Object getValue(int index);
  
  public String getString(int index);
  
  public int getInt(int index);
  
  public long getLong(int index);
  
  public double getDouble(int index);
  
  public float getFloat(int index);
  
  public Date getDate(int index);
  
  public Timestamp getTimestamp(int index);
  
  public void printContent();

}

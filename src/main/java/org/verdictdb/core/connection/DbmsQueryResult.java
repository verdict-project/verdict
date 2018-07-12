package org.verdictdb.core.connection;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;

public interface DbmsQueryResult extends Serializable {

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
   * Returns the total number of rows.
   * @return
   */
  public long getRowCount();
  
  /**
   * 
   * @param index This is a zero-based index.
   * @return
   */
  public Object getValue(int index);
  
  public String getString(int index);
  
  public String getString(String label);
  
  public int getInt(int index);
  
  public int getInt(String label);
  
  public long getLong(int index);
  
  public long getLong(String label);
  
  public double getDouble(int index);
  
  public double getDouble(String label);
  
  public float getFloat(int index);
  
  public float getFloat(String label);
  
  public Date getDate(int index);
  
  public Date getDate(String label);
  
  public byte getByte(int index);
  
  public byte getByte(String label);
  
  public Timestamp getTimestamp(int index);
  
  public Timestamp getTimestamp(String label);
  
  public void printContent();

}

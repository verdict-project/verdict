package org.verdictdb.core.connection;

import java.sql.Timestamp;

import org.verdictdb.exception.VerdictDBTypeException;

import java.sql.Date;

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
   * @param index zero-based index
   * @return
   */
  public Object getValue(int index);
  
  public String getString(int index);
  
  public int getInt(int index) throws VerdictDBTypeException;
  
  public long getLong(int index) throws VerdictDBTypeException;
  
  public double getDouble(int index) throws VerdictDBTypeException;
  
  public float getFloat(int index) throws VerdictDBTypeException;
  
  public Date getDate(int index) throws VerdictDBTypeException;
  
  public Timestamp getTimestamp(int index) throws VerdictDBTypeException;
  
  public void printContent();

}

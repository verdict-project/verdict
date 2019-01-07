/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb;

import java.sql.SQLException;

import org.verdictdb.commons.AttributeValueRetrievalHelper;
import org.verdictdb.commons.DBTablePrinter;
import org.verdictdb.commons.DataTypeConverter;
import org.verdictdb.commons.VerdictResultPrinter;
import org.verdictdb.connection.DbmsQueryResultMetaData;
import org.verdictdb.jdbc41.VerdictResultSet;

/**
 * Represents the result set returned from VerdictDB to the end user.
 *
 * @author Yongjoo Park
 */
public abstract class VerdictSingleResult extends AttributeValueRetrievalHelper {

  public abstract boolean isEmpty();

  /** @return Meta Data from ResultSet */
  public abstract DbmsQueryResultMetaData getMetaData();

  public abstract int getColumnCount();

  /**
   * @param index zero-based index
   * @return
   */
  public abstract String getColumnName(int index);

  /**
   * @param index zero-based index
   * @return
   */
  public abstract int getColumnType(int index);

  public String getColumnTypeName(int index) {
    int typeInt = getColumnType(index);
    return DataTypeConverter.typeName(typeInt);
  }

  /**
   * @param index zero-based index
   * @return
   * NOTE: This is to be used by pyverdict to get more robust column type names
   */
  public abstract String getColumnTypeNamePy(int index);

  /**
   * set the index before the first one; when next() is called, the index will move to the first
   * row.
   */
  public abstract void rewind();

  /**
   * Forward a cursor to rows by one. Similar to JDBC ResultSet.next().
   *
   * @return True if next row exists.
   */
  public abstract boolean next();

  /**
   * Returns the total number of rows.
   *
   * @return
   */
  public abstract long getRowCount();

  /**
   * @param index This is a zero-based index.
   * @return
   */
  public abstract Object getValue(int index);

  public abstract boolean wasNull() throws SQLException;
  
  public String toCsv() {
    return VerdictResultPrinter.SingleResultToCSV(this);
  }
  
  public void printCsv() {
    System.out.println(toCsv());
  }

  // Print in database form
  public void print() {
    VerdictResultSet vrs = new VerdictResultSet(this);
    DBTablePrinter.printResultSet(vrs);
  }

//  // implemented in AttributeValueRetrievalHelper
//  public abstract String getString(int index);
//
//  public String getString(String label);
//
//  public Boolean getBoolean(int index) throws SQLException;
//
//  public Boolean getBoolean(String label) throws SQLException;
//
//  public int getInt(int index);
//
//  public int getInt(String label);
//
//  public long getLong(int index);
//
//  public long getLong(String label);
//
//  public short getShort(int index);
//
//  public short getShort(String label);
//
//  public double getDouble(int index);
//
//  public double getDouble(String label);
//
//  public float getFloat(int index);
//
//  public float getFloat(String label);
//
//  public Date getDate(int index);
//
//  public Date getDate(String label);
//
//  public byte getByte(int index);
//
//  public byte getByte(String label);
//
//  public Timestamp getTimestamp(int index);
//
//  public Timestamp getTimestamp(String label);
//
//  public BigDecimal getBigDecimal(int index, int scale);
//
//  public BigDecimal getBigDecimal(String label, int scale);
//
//  public BigDecimal getBigDecimal(int index);
//
//  public BigDecimal getBigDecimal(String label);
//
//  public byte[] getBytes(int index);
//
//  public byte[] getBytes(String label);
//
//  public Time getTime(int index);
//
//  public Time getTime(String label);
//
//  public InputStream getAsciiStream(int index);
//
//  public InputStream getAsciiStream(String label);
//
//  public InputStream getUnicodeStream(int index);
//
//  public InputStream getUnicodeStream(String label);
//
//  public InputStream getBinaryStream(int index);
//
//  public InputStream getBinaryStream(String label);
//
//  public Ref getRef(int index);
//
//  public Ref getRef(String label);
//
//  public Blob getBlob(int index) throws SQLException;
//
//  public Blob getBlob(String label) throws SQLException;
//
//  public Clob getClob(int index);
//
//  public Clob getClob(String label);
//
//  public Array getArray(int index);
//
//  public Array getArray(String label);
//
//  public URL getURL(int index);
//
//  public URL getURL(String label);
//
//  public RowId getRowId(int index);
//
//  public RowId getRowId(String label);
//
//  public NClob getNClob(int index);
//
//  public NClob getNClob(String label);
//
//  public SQLXML getSQLXML(int index);
//
//  public SQLXML getSQLXML(String label);
//
}

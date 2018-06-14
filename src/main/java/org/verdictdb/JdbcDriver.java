package org.verdictdb;

import static java.sql.Types.*;
import org.verdictdb.connection.DataTypeConverter;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcQueryResult;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class JdbcDriver implements ResultSet {

  private DbmsQueryResult queryResult;

  private Object lastValue = null;

  private Boolean isBeforefirst = true;

  private Boolean isAfterLast = false;

  private Boolean isFirst = false;

  private int rowCount = 0;

  private HashSet<String> numericType = new HashSet<>(Arrays.asList(
      "bigint", "decimal", "float", "integer", "real", "numeric", "tinyint", "smallint", "long", "double"));

  private HashMap<String, Integer> colNameIdx = new HashMap<>();

  private boolean judgeValidType(String expected, int columnindex){
    String actual = DataTypeConverter.typeName(queryResult.getColumnType(columnindex));
    if (queryResult.getColumnType(columnindex) == DOUBLE) actual = "real";
    if (expected.equals("boolean")) {
      return actual.equals("boolean") || actual.equals("bit");
    }
    else if (expected.equals("byte")) {
      return numericType.contains(actual);
    }
    else if (expected.equals("short")) {
      return numericType.contains(actual);
    }
    else if (expected.equals("int")) {
      return numericType.contains(actual);
    }
    else if (expected.equals("long")) {
      return numericType.contains(actual);
    }
    else if (expected.equals("float")) {
      return numericType.contains(actual);
    }
    else if (expected.equals("double")) {
      return numericType.contains(actual);
    }
    else if (expected.equals("bigdecimal")) {
      return numericType.contains(actual);
    }
    else if (expected.equals("bytes")) {
      return  actual.equals("binary") || actual.equals("varbinary") || actual.equals("longvarbinary")
          || actual.equals("blob");
    }
    else if (expected.equals("date")) {
      return actual.equals("date");
    }
    else if (expected.equals("time")) {
      return actual.equals("time");
    }
    else if (expected.equals("timestamp")) {
      return actual.equals("timestamp");
    }
    else if (expected.equals("asciistream")) {
      return actual.equals("clob");
    }
    else if (expected.equals("binarystream")) {
      return actual.equals("blob");
    }
    else if (expected.equals("blob")) {
      return actual.equals("blob");
    }
    else if (expected.equals("clob")) {
      return actual.equals("clob");
    }
    else if (expected.equals("array")) {
      return actual.equals("array");
    }
    else if (expected.equals("ref")) {
      return actual.equals("ref");
    }
    else if (expected.equals("sqlxml")) {
      return actual.equals("xml");
    }
    else if (expected.equals("rowid")) {
      return actual.equals("rowid");
    }
    else if (expected.equals("nclob")) {
      return actual.equals("nclob");
    }
    else return false;
  }

  public JdbcDriver(DbmsQueryResult queryResult) {
    this.queryResult = queryResult;
    for (int i=0; i<queryResult.getColumnCount(); i++) {
      colNameIdx.put(queryResult.getColumnName(i), i);
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (rowCount==1) isFirst = true;
    else isFirst = false;
    if (isBeforefirst) isBeforefirst = false;
    boolean next = queryResult.next();
    rowCount++;
    if (!next) isAfterLast = true;
    return next;
  }

  @Override
  public void close() throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().close();
    }
  }

  @Override
  public boolean wasNull() throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().wasNull();
    }
    else return lastValue == null;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getString(columnIndex);
    }
    lastValue = String.valueOf(queryResult.getValue(columnIndex));
    return (String)lastValue;
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getBoolean(columnIndex);
    }
    if (judgeValidType("boolean", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (boolean)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getByte(columnIndex);
    }
    if (judgeValidType("byte", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (byte)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getShort(columnIndex);
    }
    if (judgeValidType("short", columnIndex)) {
      lastValue = TypeCasting.toShort(queryResult.getValue(columnIndex));
      return (short)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getInt(columnIndex);
    }
    if (judgeValidType("int", columnIndex)) {
      lastValue = TypeCasting.toInteger(queryResult.getValue(columnIndex));
      return (int)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getLong(columnIndex);
    }
    if (judgeValidType("long", columnIndex)) {
      lastValue = TypeCasting.toLong(queryResult.getValue(columnIndex));
      return (long)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getFloat(columnIndex);
    }
    if (judgeValidType("float", columnIndex)) {
      lastValue = TypeCasting.toFloat(queryResult.getValue(columnIndex));
      return (float)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getDouble(columnIndex);
    }
    if (judgeValidType("double", columnIndex)) {
      lastValue = TypeCasting.toDouble(queryResult.getValue(columnIndex));
      return (double)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getBigDecimal(columnIndex, scale);
    }
    if (judgeValidType("bigdecimal", columnIndex)) {
      lastValue = TypeCasting.toBigDecimal(queryResult.getValue(columnIndex), scale);
      return (BigDecimal) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getBytes(columnIndex);
    }
    if (judgeValidType("bytes", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (byte[])lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getDate(columnIndex);
    }
    if (judgeValidType("date", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (Date) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getTime(columnIndex);
    }
    if (judgeValidType("time", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (Time)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getTimestamp(columnIndex);
    }
    if (judgeValidType("timestamp", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (Timestamp) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getAsciiStream(columnIndex);
    }
    if (judgeValidType("asciistream", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (InputStream) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getUnicodeStream(columnIndex);
    }
    if (judgeValidType("unicodestream", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (InputStream) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getBinaryStream(columnIndex);
    }
    if (judgeValidType("binarystream", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (InputStream) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getString(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getString(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getBoolean(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getBoolean(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getByte(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getByte(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getShort(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getShort(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getInt(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getInt(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getLong(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getLong(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getFloat(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getFloat(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getDouble(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getDouble(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getBigDecimal(columnLabel, scale);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getBigDecimal(colNameIdx.get(columnLabel), scale);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getBytes(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getBytes(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getDate(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getDate(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getTime(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getTime(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getTimestamp(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getTimestamp(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getAsciiStream(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getAsciiStream(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getUnicodeStream(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getUnicodeStream(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getBinaryStream(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getBinaryStream(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getWarnings();
    }
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void clearWarnings() throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().clearWarnings();
    }
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public String getCursorName() throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getCursorName();
    }
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getMetaData();
    }
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return queryResult.getValue(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(columnLabel)) {
      return getObject(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    return colNameIdx.get(columnLabel);
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getCharacterStream(columnIndex);
    }
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getCharacterStream(columnLabel);
    }
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getBigDecimal(columnIndex);
    }
    if (judgeValidType("bigdecimal", columnIndex)) {
      lastValue = TypeCasting.toBigDecimal(queryResult.getValue(columnIndex));
      return (BigDecimal) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getBigDecimal(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getBigDecimal(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().isBeforeFirst();
    }
    return isBeforefirst;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().isAfterLast();
    }
    return isAfterLast;
  }

  @Override
  public boolean isFirst() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().isFirst();
    }
    return isFirst;
  }

  @Override
  public boolean isLast() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().isLast();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void beforeFirst() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
       ((JdbcQueryResult) queryResult).getResultSet().beforeFirst();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void afterLast() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().afterLast();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean first() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().first();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean last() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().last();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getRow() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getRow();
    }
    return rowCount;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().absolute(row);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().relative(rows);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean previous() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().previous();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().setFetchDirection(direction);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getFetchDirection();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().setFetchSize(rows);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getFetchSize() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getFetchSize();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getType() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getType();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getConcurrency() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getConcurrency();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().rowUpdated();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().rowInserted();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().rowDeleted();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateNull(columnIndex);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateBoolean(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateByte(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateShort(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateInt(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateLong(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateFloat(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateDouble(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateBigDecimal(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateString(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateBytes(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateDate(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateTime(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateTimestamp(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateAsciiStream(columnIndex, x, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateBinaryStream(columnIndex, x, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateCharacterStream(columnIndex, x, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateObject(columnIndex, x, scaleOrLength);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateObject(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateNull(columnLabel);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateBoolean(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateByte(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateShort(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateInt(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateLong(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateFloat(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateDouble(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateBigDecimal(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateString(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateBytes(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateDate(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateTime(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateTimestamp(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateAsciiStream(columnLabel, x, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateBinaryStream(columnLabel, x, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateCharacterStream(columnLabel, reader, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateObject(columnLabel, x, scaleOrLength);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateObject(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void insertRow() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().insertRow();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateRow() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().updateRow();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void deleteRow() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().deleteRow();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void refreshRow() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().refreshRow();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().cancelRowUpdates();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().moveToInsertRow();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      ((JdbcQueryResult) queryResult).getResultSet().moveToCurrentRow();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Statement getStatement() throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getStatement();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getObject(columnIndex, map);
    }
    else return getObject(columnIndex);
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getRef(columnIndex);
    }
    if (judgeValidType("ref", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (Ref)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getBlob(columnIndex);
    }
    if (judgeValidType("blob", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (Blob)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getClob(columnIndex);
    }
    if (judgeValidType("clob", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (Clob)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getArray(columnIndex);
    }
    if (judgeValidType("array", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (Array) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getObject(columnLabel, map);
    }
    else return getObject(columnLabel);
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getRef(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getRef(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getBlob(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getBlob(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getClob(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getClob(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getArray(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getArray(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getDate(columnIndex, cal);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getDate(columnLabel, cal);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getTime(columnIndex, cal);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getTime(columnLabel, cal);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getTimestamp(columnIndex, cal);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getTimestamp(columnLabel, cal);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getURL(columnIndex);
    }
    if (judgeValidType("array", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (URL) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getURL(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getURL(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
       ((JdbcQueryResult) queryResult).getResultSet().updateRef(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateRef(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateBlob(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateBlob(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateClob(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateClob(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateArray(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateArray(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getRowId(columnIndex);
    }
    if (judgeValidType("rowid", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (RowId) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getRowId(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getRowId(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateRowId(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateRowId(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getHoldability() throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getHoldability();
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean isClosed() throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().isClosed();
    }
    return false;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
       ((JdbcQueryResult) queryResult).getResultSet().updateNString(columnIndex, nString);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateNString(columnLabel, nString);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateNClob(columnIndex, nClob);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateNClob(columnLabel, nClob);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getNClob(columnIndex);
    }
    if (judgeValidType("nclob", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (NClob) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getNClob(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getNClob(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult) {
      return ((JdbcQueryResult) queryResult).getResultSet().getSQLXML(columnIndex);
    }
    if (judgeValidType("sqlxml", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex);
      return (SQLXML) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getSQLXML(columnLabel);
    }
    if (colNameIdx.containsKey(columnLabel)) {
      return getSQLXML(colNameIdx.get(columnLabel));
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateSQLXML(columnIndex, xmlObject);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateSQLXML(columnLabel, xmlObject);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getNString(columnIndex);
    }
    else return getString(columnIndex);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getNString(columnLabel);
    }
    else return getString(columnLabel);
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getNCharacterStream(columnIndex);
    }
    else return getCharacterStream(columnIndex);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().getNCharacterStream(columnLabel);
    }
    else return getCharacterStream(columnLabel);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateNCharacterStream(columnIndex, x, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateNCharacterStream(columnLabel, reader, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateAsciiStream(columnIndex, x, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateBinaryStream(columnIndex, x, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateCharacterStream(columnIndex, x, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateAsciiStream(columnLabel, x, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateBinaryStream(columnLabel, x, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateCharacterStream(columnLabel, reader, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateBlob(columnIndex, inputStream, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateBlob(columnLabel, inputStream, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateClob(columnIndex, reader, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateClob(columnLabel, reader, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateNClob(columnIndex, reader, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateNClob(columnLabel, reader, length);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateNCharacterStream(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateNCharacterStream(columnLabel, reader);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateAsciiStream(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateBinaryStream(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateCharacterStream(columnIndex, x);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateAsciiStream(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateBinaryStream(columnLabel, x);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateCharacterStream(columnLabel, reader);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateBlob(columnIndex, inputStream);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateBlob(columnLabel, inputStream);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateClob(columnIndex, reader);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateClob(columnLabel, reader);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateNClob(columnIndex, reader);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      ((JdbcQueryResult) queryResult).getResultSet().updateNClob(columnLabel, reader);
    }
    else throw new SQLException("Function is not supported yet.");

  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return (T)getObject(columnIndex);
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return (T)getObject(columnLabel);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().unwrap(iface);
    }
    else throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    if (queryResult instanceof JdbcQueryResult){
      return ((JdbcQueryResult) queryResult).getResultSet().isWrapperFor(iface);
    }
    else throw new SQLException("Function is not supported yet.");
  }
}

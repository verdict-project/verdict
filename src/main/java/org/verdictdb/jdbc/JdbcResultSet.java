package org.verdictdb.jdbc;


import static java.sql.Types.DOUBLE;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.verdictdb.connection.DataTypeConverter;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.exception.UnexpectedTypeException;

public class JdbcResultSet implements ResultSet {

  private DbmsQueryResult queryResult;
  
  private ResultSetMetaData metadata;

  private Object lastValue = null;

  private Boolean isBeforefirst = true;

  private Boolean isAfterLast = false;

  private Boolean isFirst = false;

  private int rowCount = 0;

  private HashSet<String> numericType = new HashSet<>(Arrays.asList(
      "bigint", "decimal", "float", "integer", "real", "numeric", "tinyint", "smallint", "long", "double"));

  private HashMap<String, Integer> colNameIdx = new HashMap<>();

  public JdbcResultSet(DbmsQueryResult queryResult) {
    this.queryResult = queryResult;
    for (int i = 0; i < queryResult.getColumnCount(); i++) {
      colNameIdx.put(queryResult.getColumnName(i), i);
    }
    metadata = new JdbcResultSetMetaData(queryResult);
  }

  private boolean isValidType(String expected, int columnindex){
    String actual = DataTypeConverter.typeName(queryResult.getColumnType(columnindex-1));
    if (queryResult.getColumnType(columnindex-1) == DOUBLE) actual = "real";
    if (expected.equals("boolean")) {
      return numericType.contains(actual) || actual.equals("boolean") || actual.equals("bit");
    }
    else if (expected.equals("byte")) {
      return numericType.contains(actual) || actual.equals("boolean") || actual.equals("bit");
    }
    else if (expected.equals("short")) {
      return numericType.contains(actual) || actual.equals("boolean") || actual.equals("bit");
    }
    else if (expected.equals("int")) {
      return numericType.contains(actual) || actual.equals("boolean") || actual.equals("bit");
    }
    else if (expected.equals("long")) {
      return numericType.contains(actual) || actual.equals("boolean") || actual.equals("bit");
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
      return  actual.equals("timestamp") || actual.equals("date") || actual.equals("time");
    }
    else if (expected.equals("time")) {
      return  actual.equals("timestamp") || actual.equals("date") || actual.equals("time");
    }
    else if (expected.equals("timestamp")) {
      return actual.equals("timestamp") || actual.equals("date") || actual.equals("time");
    }
    else if (expected.equals("asciistream")) {
      return actual.equals("clob");
    }
    else if (expected.equals("binarystream")) {
      return actual.equals("blob") || actual.equals("binary") || actual.equals("varbinary")
          || actual.equals("longvarbinary");
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
  public void close() { }

  @Override
  public boolean wasNull() throws SQLException {
    return lastValue == null;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    lastValue = String.valueOf(queryResult.getValue(columnIndex-1));
    return (String)lastValue;
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    if (isValidType("boolean", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      return (boolean)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    try {
      if (isValidType("byte", columnIndex)) {
        lastValue = TypeCasting.toByte(queryResult.getValue(columnIndex-1));
        return (byte)lastValue;
      }
      else {
        throw new UnexpectedTypeException(queryResult.getValue(columnIndex-1));
      }
    }
    catch (UnexpectedTypeException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    try {
      if (isValidType("short", columnIndex)) {
        lastValue = TypeCasting.toShort(queryResult.getValue(columnIndex-1));
        return (short)lastValue;
      }
      else {
        throw new UnexpectedTypeException(queryResult.getValue(columnIndex-1));
      }
    }
    catch (UnexpectedTypeException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    try {
      if (isValidType("int", columnIndex)) {
        lastValue = TypeCasting.toInteger(queryResult.getValue(columnIndex-1));
        return (int)lastValue;
      }
      else {
        throw new UnexpectedTypeException(queryResult.getValue(columnIndex-1));
      }
    }
    catch (UnexpectedTypeException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    try {
      if (isValidType("long", columnIndex)) {
        lastValue = TypeCasting.toLong(queryResult.getValue(columnIndex-1));
        return (long)lastValue;
      }
      else {
        throw new UnexpectedTypeException(queryResult.getValue(columnIndex-1));
      }
    }
    catch (UnexpectedTypeException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    try {
      if (isValidType("float", columnIndex)) {
        lastValue = TypeCasting.toFloat(queryResult.getValue(columnIndex-1));
        return (float)lastValue;
      }
      else {
        throw new UnexpectedTypeException(queryResult.getValue(columnIndex-1));
      }
    }
    catch (UnexpectedTypeException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    try {
      if (isValidType("double", columnIndex)) {
        lastValue = TypeCasting.toDouble(queryResult.getValue(columnIndex-1));
        return (double)lastValue;
      }
      else {
        throw new UnexpectedTypeException(queryResult.getValue(columnIndex-1));
      }
    }
    catch (UnexpectedTypeException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    try {
      if (isValidType("bigdecimal", columnIndex)) {
        lastValue = TypeCasting.toBigDecimal(queryResult.getValue(columnIndex-1), scale);
        return (BigDecimal) lastValue;
      }
      else {
        throw new UnexpectedTypeException(queryResult.getValue(columnIndex-1));
      }
    }
    catch (UnexpectedTypeException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
   if (isValidType("bytes", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      return (byte[])lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    if (isValidType("date", columnIndex)) {
      if (queryResult.getValue(columnIndex-1) instanceof Date){
        lastValue = queryResult.getValue(columnIndex-1);
      }
      else if (queryResult.getValue(columnIndex-1) instanceof Timestamp) {
        lastValue  = new Date(((Timestamp)(queryResult.getValue(columnIndex-1))).getTime());
      }
      else if (queryResult.getValue(columnIndex-1) instanceof Time) {
        lastValue =  new Date(((Time)(queryResult.getValue(columnIndex-1))).getTime());
      }
      return (Date) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    if (isValidType("time", columnIndex)) {
      if (queryResult.getValue(columnIndex-1) instanceof Date){
        lastValue  = new Time(((Date)(queryResult.getValue(columnIndex-1))).getTime());
      }
      else if (queryResult.getValue(columnIndex-1) instanceof Timestamp) {
        lastValue  = new Time(((Timestamp)(queryResult.getValue(columnIndex-1))).getTime());
      }
      else if (queryResult.getValue(columnIndex-1) instanceof Time) {
        lastValue = queryResult.getValue(columnIndex-1);
      }
      return (Time)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    if (isValidType("timestamp", columnIndex)) {
      if (queryResult.getValue(columnIndex-1) instanceof Date){
        lastValue  = new Timestamp(((Date)(queryResult.getValue(columnIndex-1))).getTime());
      }
      else if (queryResult.getValue(columnIndex-1) instanceof Time) {
        lastValue  = new Timestamp(((Time)(queryResult.getValue(columnIndex-1))).getTime());
      }
      else if (queryResult.getValue(columnIndex-1) instanceof Timestamp) {
        lastValue = queryResult.getValue(columnIndex-1);
      }
      return (Timestamp) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    if (isValidType("asciistream", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      return (InputStream) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    if (isValidType("unicodestream", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      return (InputStream) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    if (isValidType("binarystream", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      if (lastValue instanceof byte[]){
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream((byte[]) lastValue);
        return byteArrayInputStream;
      }
      return (InputStream) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }
  
  String standardizedLabel(String label) {
    return label.toLowerCase();
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getString(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getBoolean(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getByte(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getShort(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
   if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getInt(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getLong(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getFloat(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getDouble(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getBigDecimal(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getBytes(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
   if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getDate(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getTime(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getTimestamp(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getAsciiStream(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getUnicodeStream(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getBinaryStream(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return metadata;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return queryResult.getValue(columnIndex-1);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getObject(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    return colNameIdx.get(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    try {
      if (isValidType("bigdecimal", columnIndex)) {
        lastValue = TypeCasting.toBigDecimal(queryResult.getValue(columnIndex-1));
        return (BigDecimal) lastValue;
      }
      else {
        throw new UnexpectedTypeException(queryResult.getValue(columnIndex-1));
      }
    }
   catch (UnexpectedTypeException e) {
     throw new SQLException(e.getMessage());
   }
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getBigDecimal(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return isBeforefirst;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return isAfterLast;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return isFirst;
  }

  @Override
  public boolean isLast() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void afterLast() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getRow() throws SQLException {
    return rowCount;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean relative(int rows) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean previous() throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getFetchDirection() throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getType() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getConcurrency() throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean rowUpdated() throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Statement getStatement() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return getObject(columnIndex);
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    if (isValidType("ref", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      return (Ref)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    if (isValidType("blob", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      return (Blob)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    if (isValidType("clob", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      return (Clob)lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    if (isValidType("array", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      JdbcArray array = new JdbcArray((Object[]) lastValue);
      return array;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return getObject(standardizedLabel(columnLabel));
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getRef(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getBlob(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getClob(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getArray(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    if (isValidType("url", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      return (URL) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
   if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getURL(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    if (isValidType("rowid", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      return (RowId) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getRowId(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    if (isValidType("nclob", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      return (NClob) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getNClob(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    if (isValidType("sqlxml", columnIndex)) {
      lastValue = queryResult.getValue(columnIndex-1);
      return (SQLXML) lastValue;
    }
    else throw new SQLException("Not supported data type.");
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getSQLXML(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    }
    else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return getString(columnIndex);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return getString(standardizedLabel(columnLabel));
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return getCharacterStream(columnIndex);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return getCharacterStream(standardizedLabel(columnLabel));
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
   throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
   throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
   throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLException("Function is not supported yet.");

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Function is not supported yet.");

  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return (T)getObject(columnIndex);
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return (T)getObject(standardizedLabel(columnLabel));
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException("Function is not supported yet.");
  }
}

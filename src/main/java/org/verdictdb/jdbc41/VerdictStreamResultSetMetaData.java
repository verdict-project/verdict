package org.verdictdb.jdbc41;

import org.verdictdb.VerdictSingleResult;
import org.verdictdb.connection.DataTypeConverter;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static java.sql.Types.INTEGER;

/**
 * Created by: Shucheng Zhong on 9/14/18
 * ResultSetMetaData type for stream select query when VerdictStatement.sql() is called.
 *
 */
public class VerdictStreamResultSetMetaData implements ResultSetMetaData {

  private static final String verdictStreamSequenceColumn = "seq";

  private VerdictSingleResult queryResult;

  List<String> caseSensitiveColumnTypes = Arrays.asList("char", "string", "text");

  List<String> signedColumnTypes = Arrays.asList("double", "int", "real");

  public VerdictStreamResultSetMetaData(VerdictSingleResult queryResult) {
    this.queryResult = queryResult;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("Not supported function.");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException("Not supported function.");
  }

  @Override
  public int getColumnCount() throws SQLException {
    return queryResult.getColumnCount() + 1;
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    if (column==1) {
      return false;
    }
    return queryResult.getMetaData().isAutoIncrement.get(column - 2);
    // throw new SQLException("Not supported function.");
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    if (column==1) {
      return false;
    }
    String typeName = DataTypeConverter.typeName(queryResult.getColumnType(column - 2));
    for (String a : caseSensitiveColumnTypes) {
      if (typeName.contains(a)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    if (column==1) {
      return false;
    }
    return queryResult.getMetaData().isCurrency.get(column - 2);
    // throw new SQLException("Not supported function.");
  }

  @Override
  public int isNullable(int column) throws SQLException {
    if (column==1) {
      return columnNoNulls;
    }
    return queryResult.getMetaData().isNullable.get(column - 2);
    // return java.sql.ResultSetMetaData.columnNullableUnknown;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    if (column==1) {
      return false;
    }
    String typeName = DataTypeConverter.typeName(queryResult.getColumnType(column - 2));
    for (String a : signedColumnTypes) {
      if (typeName.contains((a))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    if (column==1) {
      return verdictStreamSequenceColumn.length();
    }
    return queryResult.getMetaData().columnDisplaySize.get(column - 2);
    // return Math.max(getPrecision(column), queryResult.getColumnName(column-1).length());
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    if (column==1) {
      return verdictStreamSequenceColumn;
    }
    return queryResult.getColumnName(column - 1);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    if (column==1) {
      return verdictStreamSequenceColumn;
    }
    return queryResult.getColumnName(column - 1);
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    throw new SQLException("Not supported function.");
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    if (column==1) {
      return 10;
    }
    return queryResult.getMetaData().precision.get(column - 2);
    /*
    int coltype = queryResult.getColumnType(column-1);
    if (coltype == BIGINT) {
      return 19;
    }
    else if (coltype == INTEGER) {
      return 10;
    }
    else if (coltype == SMALLINT) {
      return 5;
    }
    else if (coltype == TINYINT) {
      return 3;
    }
    else if (coltype == DOUBLE || coltype == NUMERIC) {
      return 64;
    }
    else if (coltype == FLOAT) {
      return 17;
    }
    else if (coltype == DATE) {
      return 10;
    }
    else if (coltype == TIME) {
      return 8;
    }
    else if (coltype == TIMESTAMP) {
      return 26;
    }
    else {
      return 0;
    }
    */
  }

  @Override
  public int getScale(int column) throws SQLException {
    if (column==1) {
      return 0;
    }
    return queryResult.getMetaData().scale.get(column - 2);
    /*
    String typeName = DataTypeConverter.typeName(queryResult.getColumnType(column-1));
    if (typeName.contains("double") || typeName.contains("float")) {
      return 10;
    }
    return 0;
    */
  }

  @Override
  public String getTableName(int column) throws SQLException {
    throw new SQLException("Not supported function.");
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    throw new SQLException("Not supported function.");
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    if (column==1) {
      return INTEGER;
    }
    return queryResult.getColumnType(column - 2);
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    if (column==1) {
      return DataTypeConverter.typeName(INTEGER);
    }
    return DataTypeConverter.typeName(queryResult.getColumnType(column - 2));
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    if (column==1) {
      return "java.lang.Integer";
    }
    return queryResult.getMetaData().columnClassName.get(column - 2);
    // return DataTypeConverter.typeName(queryResult.getColumnType(column-1));
  }

}

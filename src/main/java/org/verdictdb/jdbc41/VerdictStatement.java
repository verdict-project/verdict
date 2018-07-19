package org.verdictdb.jdbc41;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;

import org.verdictdb.VerdictContext;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.coordinator.ExecutionContext;
import org.verdictdb.coordinator.VerdictSingleResult;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;

public class VerdictStatement implements java.sql.Statement {
  
  VerdictConnection jdbcConn;
  
  VerdictContext context;
  
  ExecutionContext executionContext;

  VerdictSingleResult result;

  public VerdictStatement(VerdictConnection jdbcConn, VerdictContext context) {
    this.jdbcConn = jdbcConn;
    this.context = context;
    this.executionContext = context.createNewExecutionContext();
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    try {
      result = executionContext.sql(sql);
      return !result.isEmpty();
    } catch (VerdictDBException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    try {
      result = executionContext.sql(sql);
      return new VerdictResultSet(result);
    } catch (VerdictDBException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    try {
      result = executionContext.sql(sql);
      return (int) result.getRowCount();
    } catch (VerdictDBException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void close() throws SQLException {
    // dongyoungy: is this correct for close() to also call terminate() just like cancel()?
    executionContext.terminate();
  }

  @Override
  public boolean isClosed() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return new VerdictResultSet(result);
  }

  @Override
  public java.sql.Connection getConnection() throws SQLException {
    return jdbcConn;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;   // no limit
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxRows() throws SQLException {
    return 0;   // no limit
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancel() throws SQLException {
    executionContext.terminate();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetType() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

}

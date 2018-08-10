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

package org.verdictdb.jdbc41;

import org.verdictdb.VerdictContext;
import org.verdictdb.connection.*;
import org.verdictdb.coordinator.ExecutionContext;
import org.verdictdb.coordinator.VerdictSingleResult;
import org.verdictdb.coordinator.VerdictSingleResultFromDbmsQueryResult;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;

import java.sql.*;

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

  /**
   * Check whether given sql contains 'bypass' keyword at the beginning
   *
   * @param sql original sql
   * @return without 'bypass' keyword if the original sql begins with it. null otherwise.
   */
  private String checkBypass(String sql) {
    if (sql.trim().toLowerCase().startsWith("bypass")) {
      return sql.trim().substring(6);
    }
    return null;
  }

  private VerdictSingleResult executeAsIs(String sql) throws SQLException, VerdictDBDbmsException {
    DbmsConnection dbmsConn = context.getConnection();
    if (dbmsConn instanceof CachedDbmsConnection) {
      dbmsConn = ((CachedDbmsConnection) dbmsConn).getOriginalConnection();
    }
    if (dbmsConn instanceof ConcurrentJdbcConnection) {
      dbmsConn = ((ConcurrentJdbcConnection) dbmsConn).getNextConnection();
    }
    if (dbmsConn instanceof JdbcConnection) {
      Statement stmt = ((JdbcConnection) dbmsConn).getConnection().createStatement();
      boolean exist = stmt.execute(sql);
      if (exist)
        return new VerdictSingleResultFromDbmsQueryResult(new JdbcQueryResult(stmt.getResultSet()));
      else return null;
    } else if (dbmsConn instanceof SparkConnection) {
      return new VerdictSingleResultFromDbmsQueryResult(dbmsConn.execute(sql));
    } else {
      throw new VerdictDBDbmsException("Unsupported DBMS for BYPASS statement.");
    }
  }

  private int executeUpdateAsIs(String sql) throws SQLException, VerdictDBDbmsException {
    DbmsConnection dbmsConn = context.getConnection();
    if (dbmsConn instanceof CachedDbmsConnection) {
      dbmsConn = ((CachedDbmsConnection) dbmsConn).getOriginalConnection();
    }
    if (dbmsConn instanceof ConcurrentJdbcConnection) {
      dbmsConn = ((ConcurrentJdbcConnection) dbmsConn).getNextConnection();
    }
    if (dbmsConn instanceof JdbcConnection) {
      return ((JdbcConnection) dbmsConn).getConnection().createStatement().executeUpdate(sql);
    } else if (dbmsConn instanceof SparkConnection) {
      return (int) dbmsConn.execute(sql).getRowCount();
    } else {
      throw new VerdictDBDbmsException("Unsupported DBMS for BYPASS statement.");
    }
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    try {
      String bypassSql = checkBypass(sql);
      if (bypassSql != null) {
        result = this.executeAsIs(bypassSql);
      } else {
        result = executionContext.sql(sql);
      }

      if (result == null) {
        return false;
      }
      return !result.isEmpty();
    } catch (VerdictDBException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    try {
      String bypassSql = checkBypass(sql);
      if (bypassSql != null) {
        result = this.executeAsIs(bypassSql);
      } else {
        result = executionContext.sql(sql);
      }
      return new VerdictResultSet(result);
    } catch (VerdictDBException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    try {
      String bypassSql = checkBypass(sql);
      if (bypassSql != null) {
        result = null; // This should be null for update.
        return this.executeUpdateAsIs(bypassSql);
      } else {
        result = executionContext.sql(sql);
      }
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
    return 0; // no limit
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxRows() throws SQLException {
    return 0; // no limit
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
  public void clearWarnings() throws SQLException {}

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

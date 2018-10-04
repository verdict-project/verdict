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
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.ConcurrentJdbcConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class VerdictConnection implements java.sql.Connection {

  VerdictContext vc;

  boolean isOpen = false;

  public VerdictConnection(String url) throws SQLException, VerdictDBException {
    vc = VerdictContext.fromConnectionString(url);
    isOpen = true;
  }

  public VerdictConnection(String url, Properties info) throws SQLException, VerdictDBException {
    vc = VerdictContext.fromConnectionString(url, info);
    isOpen = true;
  }

  public VerdictConnection(String url, String user, String password)
      throws SQLException, VerdictDBException {
    vc = VerdictContext.fromConnectionString(url, user, password);
    isOpen = true;
  }

  public VerdictConnection(String url, String user, String password, VerdictOption options)
      throws SQLException, VerdictDBException {
    vc = VerdictContext.fromConnectionString(url, user, password, options);
    isOpen = true;
  }

  private java.sql.DatabaseMetaData getDatabaseMetaData() throws SQLException {
    DbmsConnection conn = vc.getConnection();
    java.sql.DatabaseMetaData metaData = getDatabaseMetaDataFromConnection(conn);

    if (metaData == null) {
      throw new SQLException("Unexpected underlying connection: " + conn);
    } else {
      return metaData;
    }
  }

  private java.sql.DatabaseMetaData getDatabaseMetaDataFromConnection(DbmsConnection conn) {
    if (conn instanceof CachedDbmsConnection) {
      DbmsConnection originalConn = ((CachedDbmsConnection) conn).getOriginalConnection();
      if (originalConn instanceof ConcurrentJdbcConnection) {
        originalConn = ((ConcurrentJdbcConnection) originalConn).getNextConnection();
      }
      return getDatabaseMetaDataFromConnection(originalConn);
    } else if (conn instanceof JdbcConnection) {
      JdbcConnection jdbcConn = (JdbcConnection) conn;
      try {
        return jdbcConn.getMetadata();
      } catch (VerdictDBDbmsException e) {
        e.printStackTrace();
      }
    } else if (conn instanceof ConcurrentJdbcConnection) {
      JdbcConnection jdbcConn = ((ConcurrentJdbcConnection) conn).getNextConnection();
      try {
        return jdbcConn.getMetadata();
      } catch (VerdictDBDbmsException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  @Override
  public java.sql.Statement createStatement() throws SQLException {
    // we create a copy of VerdictContext so that the underlying statement is not
    // shared.
    return new VerdictStatement(this, vc);
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
    return new VerdictPreparedStatement(new VerdictStatement(this, vc));
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return sql;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public void commit() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void close() throws SQLException {
    vc.close();
    //    JdbcConnection conn = vc.getJdbcConnection();
    //    if (conn != null) conn.getConnection().close();
    isOpen = false;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return !isOpen;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return getDatabaseMetaData();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {}

  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    try {
      vc.getConnection().setDefaultSchema(catalog);
    } catch (VerdictDBDbmsException e) {
      e.printStackTrace();
    }
  }

  @Override
  public String getCatalog() throws SQLException {
    return vc.getConnection().getDefaultSchema();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // TODO
  }

  @Override
  public VerdictStatement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public VerdictPreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public VerdictStatement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public VerdictPreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public VerdictPreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public VerdictPreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public VerdictPreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return !vc.isClosed();
    //    JdbcConnection conn = vc.getJdbcConnection();
    //    return (conn != null) && conn.getConnection().isValid(timeout);
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    throw new SQLClientInfoException();
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw new SQLClientInfoException();
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setSchema(String schema) {
    try {
      vc.getConnection().setDefaultSchema(schema);
    } catch (VerdictDBDbmsException e) {
      e.printStackTrace();
    }
  }

  @Override
  public String getSchema() {
    return vc.getConnection().getDefaultSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    vc.abort();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}

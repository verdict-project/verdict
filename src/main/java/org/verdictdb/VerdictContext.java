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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.ExecutionContext;
import org.verdictdb.coordinator.VerdictResultStream;
import org.verdictdb.coordinator.VerdictSingleResult;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.SqlSyntax;
import org.verdictdb.sqlsyntax.SqlSyntaxList;

public class VerdictContext {

  private DbmsConnection conn;

  private final String contextId;

  private long executionSerialNumber = 0;

  /**
   * Maintains the list of open executions. Each query is processed on a separate execution context.
   */
  private List<ExecutionContext> executionContexts = new LinkedList<>();

  public VerdictContext(DbmsConnection conn) {
    this.conn = new CachedDbmsConnection(conn);
    //    this.metadataProvider = new CachedMetaDataProvider(conn);
    this.contextId = RandomStringUtils.randomAlphanumeric(5);
  }

  public static VerdictContext fromJdbcConnection(Connection jdbcConn)
      throws VerdictDBDbmsException {
    DbmsConnection conn = JdbcConnection.create(jdbcConn);
    return new VerdictContext(conn);
  }

  public static VerdictContext fromConnectionString(String jdbcConnectionString)
      throws SQLException, VerdictDBDbmsException {
    attemptLoadDriverClass(jdbcConnectionString);
    Connection jdbcConn = DriverManager.getConnection(jdbcConnectionString);
    return fromJdbcConnection(jdbcConn);
  }

  public static VerdictContext fromConnectionString(String jdbcConnectionString, Properties info)
      throws SQLException, VerdictDBDbmsException {
    attemptLoadDriverClass(jdbcConnectionString);
    Connection jdbcConn = DriverManager.getConnection(jdbcConnectionString, info);
    return fromJdbcConnection(jdbcConn);
  }

  public static VerdictContext fromConnectionString(
      String jdbcConnectionString, String user, String password)
      throws SQLException, VerdictDBDbmsException {
    attemptLoadDriverClass(jdbcConnectionString);
    Connection jdbcConn = DriverManager.getConnection(jdbcConnectionString, user, password);
    return fromJdbcConnection(jdbcConn);
  }

  private static void attemptLoadDriverClass(String jdbcConnectionString) {
    SqlSyntax syntax = SqlSyntaxList.getSyntaxFromConnectionString(jdbcConnectionString);
    Collection<String> driverClassNames = syntax.getCandidateJDBCDriverClassNames();
    for (String className : driverClassNames) {
      try {
        Class.forName(className);
        //        System.out.println(className + " has been loaded into the classpath.");
      } catch (ClassNotFoundException e) {
      }
    }
  }

  public DbmsConnection getConnection() {
    return conn;
  }

  public JdbcConnection getJdbcConnection() {
    DbmsConnection testConn = conn;
    if (testConn instanceof CachedDbmsConnection) {
      testConn = ((CachedDbmsConnection) conn).getOriginalConnection();
    }
    return (testConn instanceof JdbcConnection) ? (JdbcConnection) testConn : null;
  }

  public DbmsConnection getCopiedConnection() {
    return conn.copy();
  }

  public String getContextId() {
    return contextId;
  }

  public ExecutionContext createNewExecutionContext() {
    long execSerialNumber = getNextExecutionSerialNumber();
    ExecutionContext exec = new ExecutionContext(this, execSerialNumber);
    executionContexts.add(exec);
    return exec;
  }

  private synchronized long getNextExecutionSerialNumber() {
    executionSerialNumber++;
    return executionSerialNumber;
  }

  private void removeExecutionContext(ExecutionContext exec) {
    executionContexts.remove(exec);
  }

  /** terminates all open execution context. */
  public void abort() {
    // TODO Auto-generated method stub

  }

  public void scramble(String originalSchema, String originalTable) {}

  public void scramble(
      String originalSchema, String originalTable, String newSchema, String newTable) {}

  /**
   * Returns a reliable result set as an answer. Right now, simply returns the first batch of
   * Continuous results.
   *
   * <p>Automatically spawns an independent execution context, then runs a query using it.
   *
   * @param query Either a select query or a create-scramble query
   * @return A single query result is returned. If the query is a create-scramble query, the number
   *     of inserted rows are returned.
   * @throws VerdictDBException
   */
  public VerdictSingleResult sql(String query) throws VerdictDBException {
    ExecutionContext exec = createNewExecutionContext();
    VerdictSingleResult result = exec.sql(query);
    removeExecutionContext(exec);
    return result;
  }

  /**
   * @param query Either a select query or a create-scramble query.
   * @return Reader enables progressive query result consumption. If this is a create-scramble
   *     query, the number of inserted rows are returned.
   * @throws VerdictDBException
   */
  public VerdictResultStream streamsql(String query) throws VerdictDBException {
    ExecutionContext exec = createNewExecutionContext();
    VerdictResultStream stream = exec.streamsql(query);
    return stream;
  }
}

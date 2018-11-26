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
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.ConcurrentJdbcConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.SparkConnection;
import org.verdictdb.coordinator.ExecutionContext;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.CreateSchemaQuery;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.metastore.CachedScrambleMetaStore;
import org.verdictdb.metastore.ScrambleMetaStore;
import org.verdictdb.metastore.VerdictMetaStore;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;
import org.verdictdb.sqlsyntax.SqlSyntaxList;

public class VerdictContext {

  private DbmsConnection conn;

  private boolean isClosed = false;

  private VerdictMetaStore metaStore;

  private final String contextId;

  private long executionSerialNumber = 0;

  private VerdictOption options;

  private static final VerdictDBLogger log = VerdictDBLogger.getLogger(VerdictContext.class);

  /**
   * Maintains the list of open executions. Each query is processed on a separate execution context.
   */
  private List<ExecutionContext> executionContexts = new LinkedList<>();

  public VerdictContext(DbmsConnection conn) throws VerdictDBException {
    this.conn = new CachedDbmsConnection(conn);
    this.contextId = RandomStringUtils.randomAlphanumeric(5);
    this.options = new VerdictOption();
    this.metaStore = getCachedMetaStore(conn, options);
    initialize(options);
  }

  public VerdictContext(DbmsConnection conn, VerdictOption options) throws VerdictDBException {
    this.conn = new CachedDbmsConnection(conn);
    this.contextId = RandomStringUtils.randomAlphanumeric(5);
    this.options = options;
    this.metaStore = getCachedMetaStore(conn, options);
    initialize(options);
  }

  private VerdictMetaStore getCachedMetaStore(DbmsConnection conn, VerdictOption option) {
    CachedScrambleMetaStore metaStore =
        new CachedScrambleMetaStore(new ScrambleMetaStore(conn, options));
    metaStore.refreshCache();
    return metaStore;
  }

  /**
   * Creates the schema for temp tables.
   *
   * @throws VerdictDBException
   */
  private void initialize(VerdictOption option) throws VerdictDBException {
    String schema = option.getVerdictTempSchemaName();
    CreateSchemaQuery query = new CreateSchemaQuery(schema);
    query.setIfNotExists(true);
    conn.execute(query);
  }

  /**
   * Initializes VerdictContext from a SparkSession instance.
   *
   * @param spark The actual type must be SparkSession; however, the type must not explicitly appear
   *     in this file. If it does, it causes a ClassNotFound error when VerdictContext is used for
   *     JDBC connection (i.e., when not submitted to any Spark cluster) because SparkSession is
   *     imported as "provided" scope in our maven dependency list.
   * @return
   * @throws VerdictDBException
   */
  public static VerdictContext fromSparkSession(Object spark) throws VerdictDBException {
    DbmsConnection conn = new SparkConnection(spark);
    return new VerdictContext(conn);
  }

  public static VerdictContext fromSparkSession(Object spark, VerdictOption option)
      throws VerdictDBException {
    DbmsConnection conn = new SparkConnection(spark);
    return new VerdictContext(conn, option);
  }

  /**
   * This method does not support concurrent execution of queries; thus, should not be used in
   * production.
   *
   * @param jdbcConn
   * @return
   * @throws VerdictDBException
   */
  public static VerdictContext fromJdbcConnection(Connection jdbcConn) throws VerdictDBException {
    DbmsConnection conn = JdbcConnection.create(jdbcConn);
    return new VerdictContext(conn);
  }

  /**
   * Uses a connection pool.
   *
   * @param jdbcConnectionString
   * @return
   * @throws SQLException
   * @throws VerdictDBException
   */
  public static VerdictContext fromConnectionString(String jdbcConnectionString)
      throws VerdictDBException {
    jdbcConnectionString = removeVerdictKeywordIfExists(jdbcConnectionString);
    if (!attemptLoadDriverClass(jdbcConnectionString)) {
      throw new VerdictDBException(
          String.format(
              "JDBC driver not found for the connection string: %s", jdbcConnectionString));
    }
    VerdictOption options = new VerdictOption();
    options.parseConnectionString(jdbcConnectionString);
    if (SqlSyntaxList.getSyntaxFromConnectionString(jdbcConnectionString) instanceof MysqlSyntax) {
      return new VerdictContext(JdbcConnection.create(jdbcConnectionString), options);
    } else {
      return new VerdictContext(ConcurrentJdbcConnection.create(jdbcConnectionString), options);
    }
  }

  /**
   * Uses a connection pool.
   *
   * @param jdbcConnectionString
   * @param info
   * @return
   * @throws SQLException
   * @throws VerdictDBException
   */
  public static VerdictContext fromConnectionString(String jdbcConnectionString, Properties info)
      throws VerdictDBException {
    jdbcConnectionString = removeVerdictKeywordIfExists(jdbcConnectionString);
    if (!attemptLoadDriverClass(jdbcConnectionString)) {
      throw new VerdictDBException(
          String.format(
              "JDBC driver not found for the connection string: %s", jdbcConnectionString));
    }
    VerdictOption options = new VerdictOption();
    options.parseConnectionString(jdbcConnectionString);
    options.parseProperties(info);
    options.parseConnectionString(jdbcConnectionString);
    if (SqlSyntaxList.getSyntaxFromConnectionString(jdbcConnectionString) instanceof MysqlSyntax) {
      return new VerdictContext(JdbcConnection.create(jdbcConnectionString, info), options);
    } else {
      return new VerdictContext(ConcurrentJdbcConnection.create(jdbcConnectionString, info), options);
    }
    //    Connection jdbcConn = DriverManager.getConnection(jdbcConnectionString, info);
    //    return fromJdbcConnection(jdbcConn);
  }

  /**
   * Uses a connection pool.
   *
   * @param jdbcConnectionString
   * @param user
   * @param password
   * @return
   * @throws SQLException
   * @throws VerdictDBException
   */
  public static VerdictContext fromConnectionString(
      String jdbcConnectionString, String user, String password) throws VerdictDBException {
    jdbcConnectionString = removeVerdictKeywordIfExists(jdbcConnectionString);
    if (!attemptLoadDriverClass(jdbcConnectionString)) {
      throw new VerdictDBException(
          String.format(
              "JDBC driver not found for the connection string: %s", jdbcConnectionString));
    }
    Properties info = new Properties();
    info.setProperty("user", user);
    info.setProperty("password", password);
    VerdictOption options = new VerdictOption();
    options.parseConnectionString(jdbcConnectionString);
    if (SqlSyntaxList.getSyntaxFromConnectionString(jdbcConnectionString) instanceof MysqlSyntax) {
      return new VerdictContext(JdbcConnection.create(jdbcConnectionString, info), options);
    } else {
      return new VerdictContext(ConcurrentJdbcConnection.create(jdbcConnectionString, info), options);
    }
  }

  public static VerdictContext fromConnectionString(
      String jdbcConnectionString, VerdictOption options) throws VerdictDBException {
    jdbcConnectionString = removeVerdictKeywordIfExists(jdbcConnectionString);
    attemptLoadDriverClass(jdbcConnectionString);
    options.parseConnectionString(jdbcConnectionString);
    if (SqlSyntaxList.getSyntaxFromConnectionString(jdbcConnectionString) instanceof MysqlSyntax) {
      return new VerdictContext(JdbcConnection.create(jdbcConnectionString), options);
    } else {
      return new VerdictContext(ConcurrentJdbcConnection.create(jdbcConnectionString), options);
    }
  }
  
  private static String removeVerdictKeywordIfExists(String connectionString) {
    String[] tokens = connectionString.split(":");
    if (tokens[1].equalsIgnoreCase("verdict")) {
      StringBuilder newConnectionString = new StringBuilder();
      for (int i = 0; i < tokens.length; i++) {
        if (i != 1) {
          newConnectionString.append(tokens[i]);
        }
      }
      connectionString = newConnectionString.toString();
    } else {
      // do nothing
    }
    return connectionString;
  }

  public static VerdictContext fromConnectionString(
      String jdbcConnectionString, String user, String password, VerdictOption options)
      throws VerdictDBException {
    if (!attemptLoadDriverClass(jdbcConnectionString)) {
      throw new VerdictDBException(
          String.format(
              "JDBC driver not found for the connection string: %s", jdbcConnectionString));
    }
    Properties info = new Properties();
    info.setProperty("user", user);
    info.setProperty("password", password);
    options.parseConnectionString(jdbcConnectionString);
    if (SqlSyntaxList.getSyntaxFromConnectionString(jdbcConnectionString) instanceof MysqlSyntax) {
      return new VerdictContext(JdbcConnection.create(jdbcConnectionString, info), options);
    } else {
      return new VerdictContext(ConcurrentJdbcConnection.create(jdbcConnectionString, info), options);
    }
  }

  private static boolean attemptLoadDriverClass(String jdbcConnectionString) {
    SqlSyntax syntax = SqlSyntaxList.getSyntaxFromConnectionString(jdbcConnectionString);
    if (syntax == null) return false;
    Collection<String> driverClassNames = syntax.getCandidateJDBCDriverClassNames();
    for (String className : driverClassNames) {
      try {
        Class.forName(className);
        log.debug(className + " has been loaded into the classpath.");
      } catch (ClassNotFoundException e) {
      }
    }
    return true;
  }

  public DbmsConnection getConnection() {
    return conn;
  }

  public void setDefaultSchema(String schema) {
    try {
      conn.setDefaultSchema(schema);
    } catch (VerdictDBDbmsException e) {
      e.printStackTrace();
    }
  }
  
  public void setLoglevel(String level) {
    options.setVerdictConsoleLogLevel(level);
  }

  public void close() {
    this.abort(); // terminates all ExecutionContexts first.
    conn.close();
    isClosed = true;
  }

  public boolean isClosed() {
    return isClosed;
  }

  @Deprecated
  public JdbcConnection getJdbcConnection() {
    DbmsConnection testConn = conn;
    if (testConn instanceof CachedDbmsConnection) {
      testConn = ((CachedDbmsConnection) conn).getOriginalConnection();
    }
    return (testConn instanceof JdbcConnection) ? (JdbcConnection) testConn : null;
  }

  public DbmsConnection getCopiedConnection() {
    try {
      return conn.copy();
    } catch (VerdictDBDbmsException e) {
      e.printStackTrace();
    }
    return null;
  }

  public String getContextId() {
    return contextId;
  }

  public VerdictOption getOptions() {
    return options;
  }

  public ExecutionContext createNewExecutionContext() {
    long execSerialNumber = getNextExecutionSerialNumber();
    ExecutionContext exec = null;
//      exec =
//          new ExecutionContext(conn.copy(), metaStore, contextId, execSerialNumber, options.copy());
      // Yongjoo: testing without copy().
    exec = new ExecutionContext(conn, metaStore, contextId, execSerialNumber, options.copy());
    executionContexts.add(exec);
    return exec;
  }

  private synchronized long getNextExecutionSerialNumber() {
    executionSerialNumber++;
    return executionSerialNumber;
  }

  public ScrambleMetaSet getScrambleMetaSet() {
    return metaStore.retrieve();
  }

  public VerdictMetaStore getMetaStore() {
    return metaStore;
  }

  private void removeExecutionContext(ExecutionContext exec) {
    exec.terminate();
    executionContexts.remove(exec);
  }

  /** terminates all open execution context. */
  public void abort() {
    for (ExecutionContext context : executionContexts) {
      context.terminate();
    }
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
    VerdictSingleResult result = exec.sql(query, false);
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

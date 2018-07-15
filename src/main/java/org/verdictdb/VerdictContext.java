package org.verdictdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.CachedMetaDataProvider;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcDbmsConnection;
import org.verdictdb.connection.MetaDataProvider;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.execution.ExecutionContext;


public class VerdictContext {

  private DbmsConnection conn;

//  private MetaDataProvider metadataProvider;
  
  final private String contextId;
  
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

  static public VerdictContext fromJdbcConnection(Connection jdbcConn) throws VerdictDBDbmsException {
    DbmsConnection conn = JdbcDbmsConnection.create(jdbcConn);
    return new VerdictContext(conn);
  }

  static public VerdictContext fromConnectionString(String jdbcConnectionString) throws SQLException, VerdictDBDbmsException {
    Connection jdbcConn = DriverManager.getConnection(jdbcConnectionString);
    return fromJdbcConnection(jdbcConn);
  }

  static public VerdictContext fromConnectionString(String jdbcConnectionString, Properties info)
      throws SQLException, VerdictDBDbmsException {
    Connection jdbcConn = DriverManager.getConnection(jdbcConnectionString);
    return fromJdbcConnection(jdbcConn);
  }

  public DbmsConnection getConnection() {
    return conn;
  }
  
  public String getContextId() {
    return contextId;
  }

  private ExecutionContext createNewExecutionContext() {
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


  public void abort() {
    // TODO Auto-generated method stub

  }

  public void scramble(String originalSchema, String originalTable) {

  }

  public void scramble(String originalSchema, String originalTable, String newSchema, String newTable) {

  }

  /**
   * Returns a reliable result set as an answer. Right now, simply returns the first batch of
   * Continuous results.
   * 
   * Automatically spawns an independent execution context, then runs a query using it.
   * 
   * @param query Either a select query or a create-scramble query
   * @return A single query result is returned. If the query is a create-scramble query, the number
   * of inserted rows are returned.
   */
  public DbmsQueryResult sql(String query) {
    ExecutionContext exec = createNewExecutionContext();
    DbmsQueryResult result = exec.sql(query);
    removeExecutionContext(exec);
    return result;
  }

  /**
   * 
   * @param query Either a select query or a create-scramble query.
   * @return Reader enables progressive query result consumption. If this is a create-scramble 
   * query, the number of inserted rows are returned.
   */
  public ExecutionResultReader contsql(String query) {
    return null;
  }

}

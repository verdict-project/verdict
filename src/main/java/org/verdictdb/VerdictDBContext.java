package org.verdictdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.verdictdb.core.connection.CachedMetaDataProvider;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.connection.MetaDataProvider;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBDbmsException;


public class VerdictDBContext {

  private DbmsConnection conn;

  private MetaDataProvider metadataProvider;

  /**
   * Maintains the list of open executions. Each query is processed on a separate execution context.
   */
  private List<VerdictDBExecutionContext> executionContexts = new LinkedList<>();

  public VerdictDBContext(DbmsConnection conn) {
    this.conn = conn;
    this.metadataProvider = new CachedMetaDataProvider(conn);
  }

  static public VerdictDBContext fromJdbcConnection(Connection jdbcConn) throws VerdictDBDbmsException {
    DbmsConnection conn = JdbcConnection.create(jdbcConn);
    return new VerdictDBContext(conn);
  }

  static public VerdictDBContext fromConnectionString(String jdbcConnectionString) throws SQLException, VerdictDBDbmsException {
    Connection jdbcConn = DriverManager.getConnection(jdbcConnectionString);
    return fromJdbcConnection(jdbcConn);
  }

  static public VerdictDBContext fromConnectionString(String jdbcConnectionString, Properties info)
      throws SQLException, VerdictDBDbmsException {
    Connection jdbcConn = DriverManager.getConnection(jdbcConnectionString);
    return fromJdbcConnection(jdbcConn);
  }

  public DbmsConnection getConnection() {
    return conn;
  }

  private VerdictDBExecutionContext createNewExecutionContext() {
    VerdictDBExecutionContext exec = new VerdictDBExecutionContext(this);
    executionContexts.add(exec);
    return exec;
  }

  private void removeExecutionContext(VerdictDBExecutionContext exec) {
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
   * @param query Either a select query or a create-scramble query
   * @return A single query result is returned. If the query is a create-scramble query, the number
   * of inserted rows are returned.
   */
  public DbmsQueryResult sql(String query) {
    return null;
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

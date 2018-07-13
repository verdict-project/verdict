package org.verdictdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBDbmsException;


public class VerdictDBContext {
  
  DbmsConnection conn;
  
//  DbmsMetadataCache meta = new DbmsMetadataCache();
  
  public VerdictDBContext(DbmsConnection conn) {
    this.conn = conn;
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
  
  public void scramble(String originalSchema, String originalTable, String newSchema, String newTable) {
    
  }
  
  public ExecutionResultReader sql(String query) {
    
    // parse the query
    SelectQuery selectQuery;
    
    // make plan
    // if the plan does not include any aggregates, it will simply be a parsed structure of the original query.
    QueryExecutionPlan plan;
    
    // convert it to an asynchronous plan
    // if the plan does not include any aggregates, this operation should not alter the original plan.
    QueryExecutionPlan asyncPlan;
    
    // simplify the plan
    QueryExecutionPlan simplifiedAsyncPlan = null;
    
    // execute the plan
    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(conn, simplifiedAsyncPlan);
    
    return reader;
  }

  public void abort() {
    // TODO Auto-generated method stub
    
  }

}

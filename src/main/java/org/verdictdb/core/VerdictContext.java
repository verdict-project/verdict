package org.verdictdb.core;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.result.AsyncHandler;

public class VerdictContext {
  
  DbmsConnection conn;
  
  DbmsMetadataCache meta = new DbmsMetadataCache();
  
  public VerdictContext(DbmsConnection conn) {
    this.conn = conn;
  }
  
  public DbmsQueryResult executeQuery(String query) {
    return null;
  }
  
  public void asyncExecuteQuery(String query, AsyncHandler handler) {
    
  }

}

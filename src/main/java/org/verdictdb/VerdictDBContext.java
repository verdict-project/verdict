package org.verdictdb;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.resulthandler.AsyncHandler;

public class VerdictDBContext {
  
  DbmsConnection conn;
  
//  DbmsMetadataCache meta = new DbmsMetadataCache();
  
  public VerdictDBContext(DbmsConnection conn) {
    this.conn = conn;
  }
  
  public DbmsQueryResult executeQuery(String query) {
    return null;
  }
  
  public void asyncExecuteQuery(String query, AsyncHandler handler) {
    
  }

}

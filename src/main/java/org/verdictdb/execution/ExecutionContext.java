package org.verdictdb.execution;

import org.verdictdb.VerdictContext;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;

public class ExecutionContext {
  
  VerdictContext context;
  
  public ExecutionContext(VerdictContext context) {
    this.context = context;
  }
  
  public DbmsQueryResult sql(String query) {
    // determines the type of the given query and forward it to an appropriate coordinator.
    
    // Case 1: scrambling
    
    // Case 2: select querying
    
    // Case 3: configuration (not provided yet)

    return null;
  }
  
  public ExecutionResultReader streamsql(String query) {
    return null;
  }

  public void terminate() {
    // TODO Auto-generated method stub
    
  }

}

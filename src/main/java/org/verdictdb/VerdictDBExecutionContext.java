package org.verdictdb;

import org.verdictdb.core.resulthandler.ExecutionResultReader;

public class VerdictDBExecutionContext {
  
  VerdictDBContext context;
  
  public VerdictDBExecutionContext(VerdictDBContext context) {
    this.context = context;
  }
  
  public ExecutionResultReader sql(String query) {
    // determines the type of the given query and forward it to an appropriate coordinator.
    
    // Case 1: scrambling
    
    // Case 2: select querying
    
    // Case 3: configuration (not provided yet)

    return null;
  }
  
  public void cancel() {
    
  }

}

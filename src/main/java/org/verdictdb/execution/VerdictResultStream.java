package org.verdictdb.execution;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.resulthandler.ExecutionTokenReader;

public class VerdictResultStream extends ExecutionResultReader {
  
  ExecutionContext execContext;
  
  public VerdictResultStream(ExecutionTokenReader reader, ExecutionContext execContext) {
    super(reader);
    this.execContext = execContext;
  }

  @Override
  public DbmsQueryResult next() {
    DbmsQueryResult result = super.next();
    
    // if there is no more result, we close the linked ExecutionContext
    if (hasNext() == false) {
      execContext.terminate();
    }
    
    return result;
  }
  
  

}

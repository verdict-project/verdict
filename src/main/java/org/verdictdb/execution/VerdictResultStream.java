package org.verdictdb.execution;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.resulthandler.ExecutionTokenReader;

public class VerdictResultStream {
  
  ExecutionResultReader reader;
  
  ExecutionContext execContext;
  
  public VerdictResultStream(ExecutionResultReader reader, ExecutionContext execContext) {
    this.reader = reader;
    this.execContext = execContext;
  }

//  public DbmsQueryResult next() {
//    DbmsQueryResult result = super.next();
//    
//    // if there is no more result, we close the linked ExecutionContext
//    // the underlying table must be alive.
//    if (hasNext() == false) {
//      execContext.terminate();
//    }
//    
//    return result;
//  }

}

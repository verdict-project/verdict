package org.verdictdb;

import org.verdictdb.core.resulthandler.ExecutionResultReader;

public abstract class VerdictDBOperationBase {
  
  public VerdictDBOperationBase() {
    super();
  }
  
  public abstract void scramble(
      String originalSchema, String originalTable, 
      String method, String primaryColumn,
      String newSchema, String newTable);

  public void scramble(String originalSchema, String originalTable, String newSchema, String newTable) {

  }

  

}

package org.verdictdb.core.execution;

import org.verdictdb.core.aggresult.AggregateFrame;

public class ExecutionResult {
  
  // either initialized, intermediateresult, complete, failed
  String status = "initialized";
  
  AggregateFrame af;
  
  public static ExecutionResult completeResult() {
    ExecutionResult r = new ExecutionResult();
    r.setStatus("complete");
    return r;
  }

  public String getStatus() {
    return status;
  }
  
  public boolean isComplete() {
    return status.equals("complete");
  }
  
  public boolean isIntermediateResultAvailable() {
    return status.equals("intermediateresult");
  }

  public AggregateFrame getAggregateFrame() {
    if (!isIntermediateResultAvailable()) {
      return null;
    }
    return af;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public void setAggregateFrame(AggregateFrame af) {
    this.af = af;
  }

}

package org.verdictdb.core.execution;

import org.verdictdb.core.aggresult.AggregateFrame;

public class ExecutionResult {
  
  // either initialized, intermediateresult, finished, failed
  String status = "initialized";
  
  AggregateFrame af;
  
  public static ExecutionResult completeResult() {
    ExecutionResult r = new ExecutionResult();
    r.setStatus("finished");
    return r;
  }

  public String getStatus() {
    return status;
  }
  
  public boolean isFinished() {
    return status.equals("finished");
  }
  
  public boolean isResultAvailable() {
    if (status.equals("intermediateresult") || isFinished()) {
      return true;
    }
    return false;
  }

  public AggregateFrame getAggregateFrame() {
    if (!isResultAvailable()) {
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

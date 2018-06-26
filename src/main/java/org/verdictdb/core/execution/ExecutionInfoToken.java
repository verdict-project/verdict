package org.verdictdb.core.execution;

import java.util.HashMap;
import java.util.Map;

import org.verdictdb.core.aggresult.AggregateFrame;

public class ExecutionInfoToken {
  
  // either initialized, intermediateresult, finished, failed
//  String status = "initialized";
  
  Map<String, Object> data = new HashMap<>();
  
  AggregateFrame af = null;
  
  public static ExecutionInfoToken empty() {
    return new ExecutionInfoToken();
  }
  
  public Object getValue(String key) {
    return data.get(key);
  }
  
  public void setKeyValue(String key, Object value) {
    data.put(key, value);
  }
  
//  public static ExecutionResult completeResult() {
//    ExecutionResult r = new ExecutionResult();
//    r.setStatus("finished");
//    return r;
//  }
//
//  public static ExecutionResult failedResult() {
//    ExecutionResult r = new ExecutionResult();
//    r.setStatus("failed");
//    return r;
//  }

//  public String getStatus() {
//    return status;
//  }
//  
//  public boolean isFinished() {
//    return status.equals("finished");
//  }
  
//  public boolean isResultAvailable() {
//    if (status.equals("intermediateresult") || isFinished()) {
//      return true;
//    }
//    return false;
//  }

  public AggregateFrame getAggregateFrame() {
    return af;
  }

//  public void setStatus(String status) {
//    this.status = status;
//  }

  public void setAggregateFrame(AggregateFrame af) {
    this.af = af;
  }

}

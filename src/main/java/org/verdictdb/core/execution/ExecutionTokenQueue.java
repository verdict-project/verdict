package org.verdictdb.core.execution;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class ExecutionTokenQueue {
  
  BlockingDeque<ExecutionInfoToken> internalQueue = new LinkedBlockingDeque<>();

  public void add(ExecutionInfoToken e) {
    internalQueue.add(e);
  }

  public ExecutionInfoToken poll() {
    return internalQueue.poll();
  }
  

}

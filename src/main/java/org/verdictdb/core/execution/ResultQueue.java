package org.verdictdb.core.execution;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class ResultQueue {
  
  BlockingDeque<ExecutionResult> internalQueue = new LinkedBlockingDeque<>();

  public void add(ExecutionResult e) {
    internalQueue.add(e);
  }

  public ExecutionResult poll() {
    return internalQueue.poll();
  }
  

}

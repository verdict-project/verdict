package org.verdictdb.core.execution;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ExecutionTokenQueue {
  
  BlockingDeque<ExecutionInfoToken> internalQueue = new LinkedBlockingDeque<>();

  public void add(ExecutionInfoToken e) {
    internalQueue.add(e);
  }

  public ExecutionInfoToken poll() {
    return internalQueue.poll();
  }
  
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .toString();
  }

}

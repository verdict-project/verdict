package org.verdictdb.core.execplan;

import java.io.Serializable;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ExecutionTokenQueue implements Serializable {
  
  BlockingDeque<ExecutionInfoToken> internalQueue = new LinkedBlockingDeque<>();

  public void add(ExecutionInfoToken e) {
    internalQueue.add(e);
  }

  public ExecutionInfoToken poll() {
    return internalQueue.poll();
  }
  
  public ExecutionInfoToken take() {
    try {
      return internalQueue.takeFirst();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .toString();
  }

  public ExecutionInfoToken peek() {
    return internalQueue.peekFirst();
  }

}

package org.verdictdb.core.execution;

import java.util.Iterator;

import org.verdictdb.exception.VerdictDBException;

public class ExecutionTokenReader implements Iterable<ExecutionInfoToken>, Iterator<ExecutionInfoToken> {

  ExecutionTokenQueue queue;

  // set to true if the status token has been taken from "queue".
  boolean hasEndOfQueueReached = false;

  ExecutionInfoToken queueBuffer = null;
  
  public ExecutionTokenReader() {}

  public ExecutionTokenReader(ExecutionTokenQueue queue) {
    this.queue = queue;
  }

  @Override
  public Iterator<ExecutionInfoToken> iterator() {
    return this;
  }

  void takeOne() {
    queueBuffer = queue.take();
    
    if (queueBuffer.isFailureToken()) {
      VerdictDBException e = (VerdictDBException) queueBuffer.getValue("errorMessage");
      if (e != null) {
//        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (queue == null) {
      return false;
    }
    
    if (queueBuffer == null) {
      takeOne();
      return hasNext();
    }

    if (queueBuffer.isStatusToken()) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public ExecutionInfoToken next() {
    if (queue == null) {
      return null;
    }
    
    if (queueBuffer == null) {
      takeOne();
      return next();
    }

    if (queueBuffer.isStatusToken()) {
      return null;
    } else {
      ExecutionInfoToken result = queueBuffer;
      queueBuffer = null;
      return result;
    }
  }
  
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
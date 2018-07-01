package org.verdictdb.core.execution;

import java.util.Iterator;

import org.verdictdb.core.connection.DbmsQueryResult;

class ExecutionResultReader implements Iterable<DbmsQueryResult>, Iterator<DbmsQueryResult> {

  ExecutionTokenQueue queue;

  // set to true if the status token has been taken from "queue".
  boolean hasEndOfQueueReached = false;

  ExecutionInfoToken queueBuffer = null;

  public ExecutionResultReader(ExecutionTokenQueue queue) {
    this.queue = queue;
  }

  @Override
  public Iterator<DbmsQueryResult> iterator() {
    return this;
  }

  void takeOne() {
    queueBuffer = queue.take();
  }

  @Override
  public boolean hasNext() {
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
  public DbmsQueryResult next() {
    if (queueBuffer == null) {
      takeOne();
      return next();
    }

    if (queueBuffer.isStatusToken()) {
      return null;
    } else {
      DbmsQueryResult result = (DbmsQueryResult) queueBuffer.getValue("queryResult");
      queueBuffer = null;
      return result;
    }
  }

}
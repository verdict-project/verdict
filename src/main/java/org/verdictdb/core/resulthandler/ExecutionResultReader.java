package org.verdictdb.core.resulthandler;

import java.util.Iterator;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionTokenQueue;

public class ExecutionResultReader implements Iterable<DbmsQueryResult>, Iterator<DbmsQueryResult> {

//  ExecutionTokenQueue queue;
//
//  // set to true if the status token has been taken from "queue".
//  boolean hasEndOfQueueReached = false;
//
//  ExecutionInfoToken queueBuffer = null;
  
  ExecutionTokenReader reader;
  
  public ExecutionResultReader() {}
  
  public ExecutionResultReader(ExecutionTokenReader reader) {
    this.reader = reader;
  }

  public ExecutionResultReader(ExecutionTokenQueue queue) {
    this(new ExecutionTokenReader(queue));
  }

  @Override
  public Iterator<DbmsQueryResult> iterator() {
    return this;
  }

  void takeOne() {
    reader.takeOne();
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public DbmsQueryResult next() {
    return (DbmsQueryResult) reader.next().getValue("queryResult");
  }
  
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
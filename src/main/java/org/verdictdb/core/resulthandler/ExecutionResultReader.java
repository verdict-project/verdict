package org.verdictdb.core.resulthandler;

import java.util.Iterator;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionTokenQueue;

public class ExecutionResultReader implements Iterable<DbmsQueryResult>, Iterator<DbmsQueryResult> {
  
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
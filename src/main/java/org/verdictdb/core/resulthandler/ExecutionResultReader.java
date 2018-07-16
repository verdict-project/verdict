package org.verdictdb.core.resulthandler;

import java.util.Iterator;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.execplan.ExecutionTokenQueue;

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
    ExecutionInfoToken token = reader.next();
    if (token == null) {
      return null;
    }
    return (DbmsQueryResult) token;
  }
  
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
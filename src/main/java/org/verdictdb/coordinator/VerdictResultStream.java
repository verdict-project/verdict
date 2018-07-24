package org.verdictdb.coordinator;

import java.util.Iterator;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;

public class VerdictResultStream implements Iterable<VerdictSingleResult>, Iterator<VerdictSingleResult> {
  
  ExecutionResultReader reader;
  
  ExecutionContext execContext;
  
  public VerdictResultStream(ExecutionResultReader reader, ExecutionContext execContext) {
    this.reader = reader;
    this.execContext = execContext;
  }
  
  // TODO
  public VerdictResultStream create(VerdictSingleResult singleResult) {
    return null;
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public VerdictSingleResult next() {
    DbmsQueryResult internalResult = reader.next();
    VerdictSingleResult result = new VerdictSingleResultFromDbmsQueryResult(internalResult);
    return result;
  }

  @Override
  public Iterator<VerdictSingleResult> iterator() {
    return this;
  }
  
  @Override
  public void remove() {
    
  }

  public void close() {
    
  }
  
}

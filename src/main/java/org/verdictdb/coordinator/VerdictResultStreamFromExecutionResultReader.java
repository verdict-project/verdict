package org.verdictdb.coordinator;

import java.util.Iterator;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;

public class VerdictResultStreamFromExecutionResultReader implements VerdictResultStream {

  ExecutionResultReader reader;

  ExecutionContext execContext;

  public VerdictResultStreamFromExecutionResultReader(
      ExecutionResultReader reader,
      ExecutionContext execContext) {
    super();
    this.reader = reader;
    this.execContext = execContext;
  }

  @Override
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

  @Override
  public void close() {

  }

}

package org.verdictdb.coordinator;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;

import java.util.Iterator;

public class VerdictResultStreamFromSingleResult implements VerdictResultStream {

  VerdictSingleResult result;

  boolean nextHaveCalled = false;

  public VerdictSingleResult getResult() {
    return result;
  }

  public void setResult(VerdictSingleResult result) {
    this.result = result;
  }

  public VerdictResultStreamFromSingleResult(VerdictSingleResult result) {
    super();
    this.result = result;
  }

  @Override
  public VerdictResultStream create(VerdictSingleResult singleResult) {
    return new VerdictResultStreamFromSingleResult(result);
  }

  @Override
  public boolean hasNext() {
    return !nextHaveCalled;
  }

  @Override
  public VerdictSingleResult next() {
    nextHaveCalled = true;
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

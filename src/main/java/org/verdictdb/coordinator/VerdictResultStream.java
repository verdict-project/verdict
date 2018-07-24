package org.verdictdb.coordinator;

import java.util.Iterator;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;

public abstract class VerdictResultStream implements Iterable<VerdictSingleResult>, Iterator<VerdictSingleResult> {
  
  // TODO
  public abstract VerdictResultStream create(VerdictSingleResult singleResult);

  @Override
  public abstract boolean hasNext();

  @Override
  public abstract VerdictSingleResult next();

  @Override
  public abstract Iterator<VerdictSingleResult> iterator();
  
  @Override
  public abstract void remove();

  public abstract void close();
  
}

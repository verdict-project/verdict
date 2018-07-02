package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SqlConvertable;
import org.verdictdb.exception.VerdictDBException;

public interface ExecutableNode {
  
  /**
   * The tokens are retrieved from these queues.
   * @return
   */
  public List<ExecutionTokenQueue> getSourceQueues();
  
  /**
   * The result of createToken() is broadcasted to these queues.
   * @return
   */
  public List<ExecutionTokenQueue> getDestinationQueues();
  
  /**
   * Creates a query that should be run. Its result will be handed to createToken().
   * @param tokens
   * @return
   * @throws VerdictDBException 
   */
  public SqlConvertable createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException;
  
  public ExecutionInfoToken createToken(DbmsQueryResult result);
  
  public int getDependentNodeCount();

}

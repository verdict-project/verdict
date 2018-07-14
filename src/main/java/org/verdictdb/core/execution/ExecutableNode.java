package org.verdictdb.core.execution;

import java.util.List;
import java.util.Map;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

public interface ExecutableNode {

//  // Setup methods
//  public void subscribeTo(ExecutableNode node);
//
//  public void subscribeTo(ExecutableNode node, int channel);

  
  // Execution methods
  /**
   * Creates a query that should be run. Its result will be handed to createToken().
   * @param tokens
   * @return
   * @throws VerdictDBException 
   */
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException;

  public ExecutionInfoToken createToken(DbmsQueryResult result);

  /**
   * The tokens are retrieved from these queues.
   * @return
   */
  public List<ExecutionTokenQueue> getSourceQueues();

  /**
   * The result of createToken() is broadcasted to these queues.
   * @return
   */
  public List<ExecutableNode> getSubscribers();

  public void getNotified(ExecutableNode source, ExecutionInfoToken token);

  public int getDependentNodeCount();

  /**
   * The methods are invoked on DbmsConnection and its results are set to the ExecutionInfoToken.
   * 
   * @return Pairs of (token key, method name)
   */
  public Map<String, MethodInvocationInformation> getMethodsToInvokeOnConnection();

}

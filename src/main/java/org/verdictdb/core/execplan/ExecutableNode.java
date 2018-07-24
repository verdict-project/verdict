/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.execplan;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

import java.util.List;
import java.util.Map;

public interface ExecutableNode {

  //  // Setup methods
  //  public void subscribeTo(ExecutableNode node);
  //
  //  public void subscribeTo(ExecutableNode node, int channel);

  // Execution methods
  /**
   * Creates a query that should be run. Its result will be handed to createToken().
   *
   * @param tokens
   * @return
   * @throws VerdictDBException
   */
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException;

  public ExecutionInfoToken createToken(DbmsQueryResult result);

  /**
   * The tokens are retrieved from these queues.
   *
   * @return
   */
  public List<ExecutionTokenQueue> getSourceQueues();

  /**
   * The result of createToken() is broadcasted to these queues.
   *
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

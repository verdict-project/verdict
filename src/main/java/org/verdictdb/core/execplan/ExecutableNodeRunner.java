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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlwriter.QueryToSql;

public class ExecutableNodeRunner implements Runnable {

  DbmsConnection conn;

  ExecutableNode node;

  int successSourceCount = 0;

  int dependentCount;

  public ExecutableNodeRunner(DbmsConnection conn, ExecutableNode node) {
    this.conn = conn;
    this.node = node;
    this.dependentCount = node.getDependentNodeCount();
  }

  public static ExecutionInfoToken execute(DbmsConnection conn, ExecutableNode node)
      throws VerdictDBException {
    return execute(conn, node, Arrays.<ExecutionInfoToken>asList());
  }

  public static ExecutionInfoToken execute(
      DbmsConnection conn, ExecutableNode node, List<ExecutionInfoToken> tokens)
      throws VerdictDBException {
    return (new ExecutableNodeRunner(conn, node)).execute(tokens);
  }

  @Override
  public void run() {
    // no dependency exists
    if (node.getSourceQueues().size() == 0) {
      //      System.out.println("No loop: " + new ToStringBuilder(node,
      // ToStringStyle.DEFAULT_STYLE));

      try {
        executeAndBroadcast(Arrays.<ExecutionInfoToken>asList());
        broadcast(ExecutionInfoToken.successToken());
        return;
      } catch (Exception e) {
        e.printStackTrace();
        broadcast(ExecutionInfoToken.failureToken(e));
      }
    }

    // dependency exists
    while (true) {
      List<ExecutionInfoToken> tokens = retrieve();
      if (tokens == null) {
        continue;
      }

      ExecutionInfoToken failureToken = getFailureTokenIfExists(tokens);
      if (failureToken != null) {
        broadcast(failureToken);
        break;
      }
      if (areAllSuccess(tokens)) {
        //        System.out.println(new ToStringBuilder(node, ToStringStyle.DEFAULT_STYLE) +
        // "sucess count: " + successSourceCount);
        broadcast(ExecutionInfoToken.successToken());
        break;
      }

      // actual processing
      try {
        executeAndBroadcast(tokens);
      } catch (Exception e) {
        e.printStackTrace();
        broadcast(ExecutionInfoToken.failureToken(e));
        break;
      }
    }
  }

  List<ExecutionInfoToken> retrieve() {
    Map<Integer, ExecutionTokenQueue> sourceChannelAndQueues = node.getSourceQueues();
    
    for (ExecutionTokenQueue queue : sourceChannelAndQueues.values()) {
      ExecutionInfoToken rs = queue.peek();
      if (rs == null) {
        return null;
      }
    }
//    for (int i = 0; i < sourceQueues.size(); i++) {
//      ExecutionInfoToken rs = sourceQueues.get(i).peek();
//      if (rs == null) {
//        return null;
//      }
//    }

    // all results available now
    List<ExecutionInfoToken> results = new ArrayList<>();
    for (Entry<Integer, ExecutionTokenQueue> channelAndQueue : sourceChannelAndQueues.entrySet()) {
      int channel = channelAndQueue.getKey();
      ExecutionInfoToken rs = channelAndQueue.getValue().take();
      rs.setKeyValue("channel", channel);
      results.add(rs);
    }
//    for (int i = 0; i < sourceQueues.size(); i++) {
//      ExecutionInfoToken rs = sourceQueues.get(i).take();
//      results.add(rs);
//    }
    return results;
  }

  void broadcast(ExecutionInfoToken token) {
  
    VerdictDBLogger logger = VerdictDBLogger.getLogger(this.getClass());
    logger.trace(String.format("[%s] Broadcasting:", node.toString()));
    for (ExecutableNode dest : node.getSubscribers()) {
      logger.trace(String.format("  -> %s", dest.toString()));
    }
    logger.trace(token.toString());
    
    for (ExecutableNode dest : node.getSubscribers()) {
      ExecutionInfoToken copiedToken = token.deepcopy();
      dest.getNotified(node, copiedToken);
//      logger.trace(String.format("[-> %s]", dest.toString()));
//      logger.trace(copiedToken.toString());
    }
  }

  void executeAndBroadcast(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    ExecutionInfoToken resultToken = execute(tokens);
    if (resultToken != null) {
      broadcast(resultToken);
    }
  }

  public ExecutionInfoToken execute(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    if (tokens.size() > 0 && tokens.get(0).isStatusToken()) {
      return null;
    }

    // basic operations: execute a query and creates a token based on that result.
    SqlConvertible sqlObj = node.createQuery(tokens);
    DbmsQueryResult intermediate = null;
    if (sqlObj != null) {
      String sql = QueryToSql.convert(conn.getSyntax(), sqlObj);
      intermediate = conn.execute(sql);
    }
    ExecutionInfoToken token = node.createToken(intermediate);

    // extended operations: if the node has additional method invocation list, we perform the method
    // calls
    // on DbmsConnection and sets its results in the token.
    if (token == null) {
      token = new ExecutionInfoToken();
    }
    Map<String, MethodInvocationInformation> tokenKeysAndmethodsToInvoke =
        node.getMethodsToInvokeOnConnection();
    for (Entry<String, MethodInvocationInformation> keyAndMethod :
        tokenKeysAndmethodsToInvoke.entrySet()) {
      String tokenKey = keyAndMethod.getKey();
      MethodInvocationInformation methodInfo = keyAndMethod.getValue();
      String methodName = methodInfo.getMethodName();
      Class<?>[] methodParameters = methodInfo.getMethodParameters();
      Object[] methodArguments = methodInfo.getArguments();

      try {
        Method method = conn.getClass().getMethod(methodName, methodParameters);
        Object ret = method.invoke(conn, methodArguments);
        token.setKeyValue(tokenKey, ret);

      } catch (NoSuchMethodException
          | IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException e) {
        e.printStackTrace();
        throw new VerdictDBValueException(e);
      }
    }

    return token;
  }

  ExecutionInfoToken getFailureTokenIfExists(List<ExecutionInfoToken> tokens) {
    for (ExecutionInfoToken t : tokens) {
      //      System.out.println(t);
      if (t.isFailureToken()) {
        //        System.out.println("yes");
        return t;
      }
    }
    return null;
  }

  boolean areAllSuccess(List<ExecutionInfoToken> latestResults) {
    for (ExecutionInfoToken t : latestResults) {
      if (t.isSuccessToken()) {
        successSourceCount++;
      } else {
        return false;
      }
    }

    if (successSourceCount == dependentCount) {
      return true;
    } else {
      return false;
    }
  }
}

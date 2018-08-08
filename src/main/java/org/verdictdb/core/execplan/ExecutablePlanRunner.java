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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.resulthandler.ExecutionTokenReader;
import org.verdictdb.exception.VerdictDBException;

public class ExecutablePlanRunner {

  private DbmsConnection conn;

  private ExecutablePlan plan;

  private int nThreads = 1;

  public ExecutablePlanRunner(DbmsConnection conn, ExecutablePlan plan) {
    this.conn = conn;
    this.plan = plan;
  }

  public static ExecutionTokenReader getTokenReader(DbmsConnection conn, ExecutablePlan plan) {
    return (new ExecutablePlanRunner(conn, plan)).getTokenReader();
  }

  public static ExecutionResultReader getResultReader(DbmsConnection conn, ExecutablePlan plan) {
    return (new ExecutablePlanRunner(conn, plan)).getResultReader();
  }

  public static void runTillEnd(DbmsConnection conn, ExecutablePlan plan)
      throws VerdictDBException {
    ExecutionTokenReader reader = (new ExecutablePlanRunner(conn, plan)).getTokenReader();
    while (true) {
      ExecutionInfoToken token = reader.next();
      //      System.out.println("runTillEnd: " + token);
      if (token == null) {
        break;
      }
    }
  }

  public ExecutionTokenReader getTokenReader() {
    // set up to get the results
    ExecutionTokenReader reader;
    if (plan.getReportingNode() != null) {
      ExecutableNodeBase node = new ExecutableNodeBase(-1);
      //      ExecutionTokenQueue outputQueue = new ExecutionTokenQueue();
      node.subscribeTo((ExecutableNodeBase) plan.getReportingNode());
      //      plan.getReportingNode().getDestinationQueues().add(outputQueue);
      reader = new ExecutionTokenReader(node.getSourceQueues().get(0));
    } else {
      reader = new ExecutionTokenReader();
    }

    // executes the nodes in a round-robin manner
    //    ExecutorService executor = Executors.newCachedThreadPool();

    Map<Integer, ExecutorService> executorPool = new HashMap<>();

    Set<Integer> groupIds = plan.getNodeGroupIDs();
    for (int gid : groupIds) {
      List<ExecutableNode> nodes = plan.getNodesInGroup(gid);
      ExecutorService executor = Executors.newFixedThreadPool(nThreads);
      for (ExecutableNode n : nodes) {
        executor.submit(new ExecutableNodeRunner(conn, n));
      }
      executorPool.put(gid, executor);
    }

    // accepts no more jobs
    for (ExecutorService service : executorPool.values()) {
      service.shutdown();
    }

    return reader;
  }

  public ExecutionResultReader getResultReader() {
    ExecutionTokenReader reader = getTokenReader();
    return new ExecutionResultReader(reader);
  }

  //  public ExecutionResultReader run() {
  //    // set up to get the results
  //    ExecutionResultReader reader;
  //    if (plan.getReportingNode() != null) {
  //      ExecutionTokenQueue outputQueue = new ExecutionTokenQueue();
  //      plan.getReportingNode().getDestinationQueues().add(outputQueue);
  //      reader = new ExecutionResultReader(outputQueue);
  //    } else {
  //      reader = new ExecutionResultReader();
  //    }
  //
  //    // executes the nodes in a round-robin manner
  //    ExecutorService executor = Executors.newCachedThreadPool();
  //
  //    List<Integer> groupIds = plan.getNodeGroupIDs();
  //    List<List<ExecutableNode>> nodeGroups = new ArrayList<>();
  //    for (int gid : groupIds) {
  //      List<ExecutableNode> nodes = plan.getNodesInGroup(gid);
  //      nodeGroups.add(nodes);
  //    }
  //
  //    while (true) {
  //      boolean submittedAtLeastOne = false;
  //      for (int i = 0; i < nodeGroups.size(); i++) {
  //        List<ExecutableNode> nodes = nodeGroups.get(i);
  //        if (!nodes.isEmpty()) {
  //          ExecutableNode node = nodes.remove(0);
  //          System.out.println("Submitting: " + node);
  //          executor.submit(new ExecutableNodeRunner(conn, node));
  //          submittedAtLeastOne = true;
  //        }
  //      }
  //      if (submittedAtLeastOne) {
  //        continue;
  //      } else {
  //        break;
  //      }
  //    }
  //
  //    return reader;
  //  }

}

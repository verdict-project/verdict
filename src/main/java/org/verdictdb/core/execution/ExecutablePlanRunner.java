package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.verdictdb.core.connection.DbmsConnection;

public class ExecutablePlanRunner {

  private DbmsConnection conn;

  private ExecutablePlan plan;

  public ExecutablePlanRunner(DbmsConnection conn, ExecutablePlan plan) {
    this.conn = conn;
    this.plan = plan;
  }

  public ExecutionResultReader run() {
    // set up to get the results
    ExecutionTokenQueue outputQueue = new ExecutionTokenQueue();
    plan.getReportingNode().getDestinationQueues().add(outputQueue);
    ExecutionResultReader reader = new ExecutionResultReader(outputQueue);

    // executes the nodes in a round-robin manner
    ExecutorService executor = Executors.newCachedThreadPool();

    List<Integer> groupIds = plan.getNodeGroupIDs();
    List<List<ExecutableNode>> nodeGroups = new ArrayList<>();
    for (int gid : groupIds) {
      nodeGroups.add(plan.getNodesInGroup(gid));
    }

    while (true) {
      boolean submittedAtLeastOne = false;
      for (int i = 0; i < nodeGroups.size(); i++) {
        List<ExecutableNode> nodes = nodeGroups.get(i);
        if (nodes.size() > 0) {
          ExecutableNode node = nodes.get(0);
          executor.submit(new ExecutableNodeRunner(conn, node));
          submittedAtLeastOne = true;
        }
      }
      if (submittedAtLeastOne) {
        continue;
      } else {
        break;
      }
    }

    return reader;
  }

}

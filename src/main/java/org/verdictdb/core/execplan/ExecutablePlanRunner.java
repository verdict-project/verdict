package org.verdictdb.core.execplan;

import java.util.ArrayList;
import java.util.List;
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

  public static void runTillEnd(DbmsConnection conn, ExecutablePlan plan) throws VerdictDBException {
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
      ExecutableNodeBase node = ExecutableNodeBase.create();
//      ExecutionTokenQueue outputQueue = new ExecutionTokenQueue();
      node.subscribeTo((ExecutableNodeBase) plan.getReportingNode());
//      plan.getReportingNode().getDestinationQueues().add(outputQueue);
      reader = new ExecutionTokenReader(node.getSourceQueues().get(0));
    } else {
      reader = new ExecutionTokenReader();
    }

    // executes the nodes in a round-robin manner
    ExecutorService executor = Executors.newCachedThreadPool();

    List<Integer> groupIds = plan.getNodeGroupIDs();
    List<List<ExecutableNode>> nodeGroups = new ArrayList<>();
    for (int gid : groupIds) {
      List<ExecutableNode> nodes = plan.getNodesInGroup(gid);
      nodeGroups.add(nodes);
    }

    while (true) {
      boolean submittedAtLeastOne = false;
      for (int i = 0; i < nodeGroups.size(); i++) {
        List<ExecutableNode> nodes = nodeGroups.get(i);
        if (!nodes.isEmpty()) {
          ExecutableNode node = nodes.remove(0);
//          System.out.println("Submitting: " + node);
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

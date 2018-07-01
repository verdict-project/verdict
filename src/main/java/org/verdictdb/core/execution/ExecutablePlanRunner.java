package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SqlConvertable;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.QueryToSql;

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


class ExecutableNodeRunner implements Runnable {

  DbmsConnection conn;

  ExecutableNode node;

  public ExecutableNodeRunner(DbmsConnection conn, ExecutableNode node) {
    this.conn = conn;
    this.node = node;
  }

  List<ExecutionInfoToken> retrieve() {
    List<ExecutionTokenQueue> sourceQueues = node.getSourceQueues();

    for (int i = 0; i < sourceQueues.size(); i++) {
      ExecutionInfoToken rs = sourceQueues.get(i).peek();
      if (rs == null) {
        return null;
      }
    }

    // all results available now
    List<ExecutionInfoToken> results = new ArrayList<>();
    for (int i = 0; i < sourceQueues.size(); i++) {
      ExecutionInfoToken rs = sourceQueues.get(i).take();
      results.add(rs);
    }
    return results;
  }

  void broadcast(ExecutionInfoToken token) {
    for (ExecutionTokenQueue dest : node.getDestinationQueues()) {
      dest.add(token);
    }
  }

  void execute(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    SqlConvertable sqlObj = node.createQuery(tokens);
    String sql = QueryToSql.convert(conn.getSyntax(), sqlObj);
    DbmsQueryResult intermediate = conn.executeQuery(sql);
    ExecutionInfoToken resultToken = node.createToken(intermediate);
    broadcast(resultToken);
  }

  boolean doesIncludeFailure(List<ExecutionInfoToken> tokens) {
    for (ExecutionInfoToken t : tokens) {
      if (t.isFailureToken()) {
        return true;
      }
    }
    return false;
  }

  boolean areAllSuccess(List<ExecutionInfoToken> latestResults) {
    for (ExecutionInfoToken t : latestResults) {
      if (t.isStatusToken()) {
        // do nothing
      } else {
        return false;
      }
    }
    return true;
  }

  @Override
  public void run() {
    // no dependency exists
    if (node.getSourceQueues().size() == 0) {
      try {
        execute(Arrays.<ExecutionInfoToken>asList());
        broadcast(ExecutionInfoToken.successToken());
        return;
      } catch (VerdictDBException e) {
        broadcast(ExecutionInfoToken.failureToken());
      }
    }

    // dependency exists
    while (true) {
      List<ExecutionInfoToken> tokens = retrieve();
      if (tokens == null) {
        continue;
      }
      if (doesIncludeFailure(tokens)) {
        broadcast(ExecutionInfoToken.failureToken());
        break;
      }
      if (areAllSuccess(tokens)) {
        broadcast(ExecutionInfoToken.successToken());
        break;
      }
      
      // actual processing
      try {
        execute(tokens);
      } catch (VerdictDBException e) {
        broadcast(ExecutionInfoToken.failureToken());
        break;
      }
    }
  }

}

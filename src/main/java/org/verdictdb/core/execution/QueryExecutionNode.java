package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.rewriter.ScrambleMeta;

import com.google.common.base.Optional;

public abstract class QueryExecutionNode {

  DbmsConnection conn;
  
  SelectQuery query;

  //  QueryExecutionPlan plan;

  // running or complete
  String status = "running";
  
  // these are assumed to be not order-sensitive
  List<QueryExecutionNode> parents = new ArrayList<>();

  // these are assumed to be not order-sensitive
  List<QueryExecutionNode> dependents = new ArrayList<>();

  // these are the queues to which this node will broadcast its results (to upstream nodes).
  List<BlockingDeque<ExecutionResult>> broadcastQueues = new ArrayList<>();

  // these are the results coming from the producers (downstream operations).
  // multiple producers may share a single result queue.
  // these queues are assumed to be order-sensitive
  List<BlockingDeque<ExecutionResult>> listeningQueues = new ArrayList<>();

  // latest results from listening queues
  List<Optional<ExecutionResult>> latestResults = new ArrayList<>();

  public QueryExecutionNode(DbmsConnection conn, SelectQuery query) {
    this.conn = conn;
    this.query = query;
    //    this.plan = plan;
  }
  
  public SelectQuery getQuery() {
    return query;
  }
  
  public List<QueryExecutionNode> getParents() {
    return parents;
  }

  public List<QueryExecutionNode> getDependents() {
    return dependents;
  }

  /**
   * For multi-threading, the parent of this node is responsible for running this method as a separate thread.
   * @param resultQueue
   */
  public void execute() {
    // Start the execution of all children
    for (QueryExecutionNode child : dependents) {
      child.execute();
    }

    // Execute this node if there are some results available
    ExecutorService executor = Executors.newSingleThreadExecutor();
    while (true) {
      readLatestResultsFromDependents();

      final List<ExecutionResult> latestResults = getLatestResultsIfAvailable();

      // Only when all results are available, the internal operations of this node are performed.
      if (latestResults != null || areDependentsAllComplete()) {
        // run this on a separate thread
        executor.submit(new Runnable() {
          @Override
          public void run() {
            ExecutionResult rs = executeNode(latestResults);
            broadcast(rs);
            //            resultQueue.add(rs);
          }
        });
      }

      if (areDependentsAllComplete()) {
        break;
      }
    }

    // finishes only when no threads are running for this node.
    try {
      executor.shutdown();
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);  // convention for waiting forever
    } catch (InterruptedException e) {
      executor.shutdownNow();
    }
    setComplete();
  }

  public abstract ExecutionResult executeNode(List<ExecutionResult> downstreamResults);
  
  void addParent(QueryExecutionNode parent) {
    parents.add(parent);
  }

  // setup method
  public void addDependency(QueryExecutionNode dep) {
    dependents.add(dep);
    dep.addParent(this);
  }

  // setup method
  public BlockingDeque<ExecutionResult> generateListeningQueue() {
    BlockingDeque<ExecutionResult> queue = new LinkedBlockingDeque<>();
    listeningQueues.add(queue);
    latestResults.add(Optional.<ExecutionResult>absent());
    return queue;
  }

  // setup method
  public void addBroadcastingQueue(BlockingDeque<ExecutionResult> queue) {
    broadcastQueues.add(queue);
  }

  public boolean isComplete() {
    return status.equals("complete");
  }

  void setComplete() {
    status = "complete";
  }

  void broadcast(ExecutionResult result) {
    for (BlockingDeque<ExecutionResult> listener : broadcastQueues) {
      listener.add(result);
    }
  }

  void readLatestResultsFromDependents() {
    for (int i = 0; i < listeningQueues.size(); i++) {
      ExecutionResult rs = listeningQueues.get(i).poll();
      if (rs == null) {
        // do nothing
      } else {
        latestResults.set(i, Optional.of(rs));
      }
    }
  }

  List<ExecutionResult> getLatestResultsIfAvailable() {
    boolean allResultsAvailable = true;
    List<ExecutionResult> results = new ArrayList<>();
    for (Optional<ExecutionResult> r : latestResults) {
      if (!r.isPresent()) {
        allResultsAvailable = false;
        break;
      }
      results.add(r.get());
    }
    if (allResultsAvailable) {
      return results;
    } else {
      return null;
    }
  }

  boolean areDependentsAllComplete() {
    for (QueryExecutionNode node : dependents) {
      if (node.isComplete()) {
        // do nothing
      } else {
        return false;
      }
    }
    return true;
  }
  

  // identify nodes that are (1) aggregates and (2) are not descendants of any other aggregates.
  void identifyTopAggNodes(List<QueryExecutionNode> topAggNodes) {
    if (this instanceof AggExecutionNode) {
      topAggNodes.add(this);
      return;
    }
    for (QueryExecutionNode dep : getDependents()) {
      dep.identifyTopAggNodes(topAggNodes);
    }
  }
  

  /**
   * 
   * @param scrambleMeta
   * @return True if there exists scrambledTable in the from list or in the non-aggregate subqueries.
   */
  boolean doesContainScrambledTablesInDescendants(ScrambleMeta scrambleMeta) {
    if (!(this instanceof AggExecutionNode) && !(this instanceof ProjectionExecutionNode)) {
      return false;
    }
    
    SelectQuery query = getQuery();
    if (query == null) {
      return false;
    }
    List<AbstractRelation> sources = query.getFromList();
    for (AbstractRelation s : sources) {
      if (s instanceof BaseTable) {
        String schemaName = ((BaseTable) s).getSchemaName();
        String tableName = ((BaseTable) s).getTableName();
        if (scrambleMeta.isScrambled(schemaName, tableName)) {
          return true;
        }
      }
      // TODO: should handle joined tables as well.
    }
    
    for (QueryExecutionNode dep : getDependents()) {
      if (dep instanceof AggExecutionNode) {
        // ignore agg node since it will be a blocking operation.
      } else {
        if (dep.doesContainScrambledTablesInDescendants(scrambleMeta)) {
          return true;
        }
      }
    }
    return false;
  }
  
  List<QueryExecutionNode> getLeafNodes() {
    List<QueryExecutionNode> leaves = new ArrayList<>();
    if (getDependents().size() == 0) {
      leaves.add(this);
      return leaves;
    }
    
    for (QueryExecutionNode dep : getDependents()) {
      leaves.addAll(dep.getLeafNodes());
    }
    return leaves;
  }

}

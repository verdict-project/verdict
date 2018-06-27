package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.execution.ola.AggExecutionNodeBlock;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.GroupingAttribute;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.query.SqlConvertable;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.exception.VerdictDBException;

import com.google.common.base.Optional;

public abstract class QueryExecutionNode {

//  DbmsConnection conn;
  
  SelectQuery selectQuery;

  QueryExecutionPlan plan;

  // initialized, running, or complete
  String status = "initialized";
  
  // these are assumed to be not order-sensitive
  List<QueryExecutionNode> parents = new ArrayList<>();

  // these are assumed to be not order-sensitive
  List<QueryExecutionNode> dependents = new ArrayList<>();

  // these are the queues to which this node will broadcast its results (to upstream nodes).
  List<ExecutionTokenQueue> broadcastingQueues = new ArrayList<>();

  // these are the results coming from the producers (downstream operations).
  // multiple producers may share a single result queue.
  // these queues are assumed to be order-sensitive
  List<ExecutionTokenQueue> listeningQueues = new ArrayList<>();

  // latest results from listening queues
  List<Optional<ExecutionInfoToken>> latestResults = new ArrayList<>();
  
  public QueryExecutionNode(QueryExecutionPlan plan) {
    this.plan = plan;
  }

  public QueryExecutionNode(QueryExecutionPlan plan, SelectQuery query) {
    this(plan);
    this.selectQuery = query;
  }
  
  public SelectQuery getSelectQuery() {
    return selectQuery;
  }
  
  public void setSelectQuery(SelectQuery query) {
    this.selectQuery = query;
  }
  
  public List<QueryExecutionNode> getParents() {
    return parents;
  }

  public List<QueryExecutionNode> getDependents() {
    return dependents;
  }
  
  public QueryExecutionNode getDependent(int index) {
    return dependents.get(index);
  }
  
  public String getStatus() {
    return status;
  }
  
  public void setStatus(String status) {
    this.status = status;
  }
  
  public QueryExecutionPlan getPlan() {
    return plan;
  }

  /**
   * For multi-threading, the parent of this node is responsible for running this method as a separate thread.
   * @param resultQueue
   */
  public void execute(final DbmsConnection conn) {
    // Start the execution of all children
    for (QueryExecutionNode child : dependents) {
      if (!child.getStatus().equals("initialized")) {
        continue;
      }
      child.setStatus("running"); 
      child.execute(conn);
    }

    // Execute this node if there are some results available
    ExecutorService executor = Executors.newSingleThreadExecutor();
    while (true) {
      readLatestResultsFromDependents();

      final List<ExecutionInfoToken> latestResults = getLatestResultsIfAvailable();

      // Only when all results are available, the internal operations of this node are performed.
      if (latestResults != null || areDependentsAllComplete()) {
        // run this on a separate thread
        executor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              ExecutionInfoToken rs = executeNode(conn, latestResults);
              broadcast(rs);
            } catch (VerdictDBException e) {
              e.printStackTrace();
              setStatus("failed");
            }
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
    if (!isFailed()) {
      setSuccess();
    }
  }

  /**
   * This function must not make a call to the conn field.
   * @param downstreamResults
   * @return
   * @throws VerdictDBException 
   */
  public abstract ExecutionInfoToken executeNode(
      DbmsConnection conn, 
      List<ExecutionInfoToken> downstreamResults)
      throws VerdictDBException;
  
  void addParent(QueryExecutionNode parent) {
    parents.add(parent);
  }

  // setup method
  public void addDependency(QueryExecutionNode dep) {
    dependents.add(dep);
    dep.addParent(this);
  }

  // setup method
  public ExecutionTokenQueue generateListeningQueue() {
    ExecutionTokenQueue queue = new ExecutionTokenQueue();
    listeningQueues.add(queue);
    latestResults.add(Optional.<ExecutionInfoToken>absent());
    return queue;
  }

  // setup method
  public void addBroadcastingQueue(ExecutionTokenQueue queue) {
    broadcastingQueues.add(queue);
  }
  
  public void clearBroadcastingQueues() {
    broadcastingQueues.clear();
  }
  
  public List<ExecutionTokenQueue> getBroadcastingQueues() {
    return broadcastingQueues;
  }
  
  public ExecutionTokenQueue getBroadcastingQueue(int index) {
    return broadcastingQueues.get(index);
  }
  
  public List<ExecutionTokenQueue> getListeningQueues() {
    return listeningQueues;
  }
  
  public ExecutionTokenQueue getListeningQueue(int index) {
    return listeningQueues.get(index);
  }

  public boolean isSuccess() {
    return status.equals("success");
  }
  
  public boolean isFailed() {
    return status.equals("failed");
  }

  void setSuccess() {
    status = "success";
  }

  void broadcast(ExecutionInfoToken result) {
    for (ExecutionTokenQueue listener : broadcastingQueues) {
      listener.add(result);
    }
  }

  void readLatestResultsFromDependents() {
    for (int i = 0; i < listeningQueues.size(); i++) {
      ExecutionInfoToken rs = listeningQueues.get(i).poll();
      if (rs == null) {
        // do nothing
      } else {
        latestResults.set(i, Optional.of(rs));
        System.out.println("Received: " + rs.toString());
      }
    }
  }

  List<ExecutionInfoToken> getLatestResultsIfAvailable() {
    boolean allResultsAvailable = true;
    List<ExecutionInfoToken> results = new ArrayList<>();
    for (Optional<ExecutionInfoToken> r : latestResults) {
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
      if (node.isSuccess() || node.isFailed()) {
        // do nothing
      } else {
        return false;
      }
    }
    return true;
  }

  // identify nodes that are (1) aggregates and (2) are not descendants of any other aggregates.
  List<AggExecutionNodeBlock> identifyTopAggBlocks() {
    List<AggExecutionNodeBlock> aggblocks = new ArrayList<>();
    
    if (this instanceof AggExecutionNode) {
      AggExecutionNodeBlock block = new AggExecutionNodeBlock(plan, this);
      aggblocks.add(block);
      return aggblocks;
    }
    for (QueryExecutionNode dep : getDependents()) {
      List<AggExecutionNodeBlock> depAggBlocks = dep.identifyTopAggBlocks();
      aggblocks.addAll(depAggBlocks);
    }
    
    return aggblocks;
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
    
    SelectQuery query = (SelectQuery) getSelectQuery();
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

  public abstract QueryExecutionNode deepcopy();
  
  void copyFields(QueryExecutionNode from, QueryExecutionNode to) {
    to.selectQuery = from.selectQuery.deepcopy();
    to.status = from.status;
    to.parents.addAll(from.parents);
    to.dependents.addAll(from.dependents);
    to.broadcastingQueues.addAll(from.broadcastingQueues);
    to.listeningQueues.addAll(from.listeningQueues);
    to.latestResults.addAll(from.latestResults);
  }
  
  public void print() {
    print(0);
  }
  
  void print(int indentSpace) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < indentSpace; i++) {
      builder.append(" ");
    }
    builder.append(this.toString());
    System.out.println(builder.toString());
    
    for (QueryExecutionNode dep : dependents) {
      dep.print(indentSpace + 2);
    }
  }
  
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .append("status", status)
        .append("listeningQueues", listeningQueues)
        .append("broadcastingQueues", broadcastingQueues)
        .append("selectQuery", selectQuery)
        .toString();
  }

}

package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

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
  
  int successDependentCount = 0;
  
  int failedDependentCount = 0;

  // these are the queues to which this node will broadcast its results (to upstream nodes).
  List<ExecutionTokenQueue> broadcastingQueues = new ArrayList<>();

  // these are the results coming from the producers (downstream operations).
  // multiple producers may share a single result queue.
  // these queues are assumed to be order-sensitive
  private List<ExecutionTokenQueue> listeningQueues = new ArrayList<>();

  // latest results from listening queues
  private List<Optional<ExecutionInfoToken>> latestResults = new ArrayList<>();
  
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
  
  public void executeAndWaitForTermination(DbmsConnection conn) throws VerdictDBValueException {
    try {
      ExecutorService executor = Executors.newFixedThreadPool(plan.getMaxNumberOfThreads());
      execute(conn, executor);
      executor.shutdown();
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);  // convention for waiting forever
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * For multi-threading, run executeNode() on a separate thread.
   * 
   * @param resultQueue
   * @throws VerdictDBValueException 
   */
  public void execute(final DbmsConnection conn, ExecutorService executor) throws VerdictDBValueException {
    if (listeningQueues.size() != latestResults.size()) {
      throw new VerdictDBValueException("Field constraint mismatch.");
    }
    
    // The fact that it is not in "initialized" means this node already have been into "running" status before.
    // Also, the children of this node have already been called execute() method.
    if (!getStatus().equals("initialized")) {
      return;
    }
    
    // Start the execution of all children
    // Some of those children may have already started by its another parent; then, calling execute()
    // will have no effect.
    for (QueryExecutionNode child : dependents) { 
      child.execute(conn, executor);
    }
    
    // Now we start the execution of this current node.
    // Set the status of this node
    setStatus("running");
//    System.out.println("Starts the exec of " + this);
    
    executor.submit(new Runnable() {
      int process(DbmsConnection conn, List<ExecutionInfoToken> tokens) {
        try {
          ExecutionInfoToken rs = executeNode(conn, tokens);
          broadcast(rs);
          return 0;
        } catch (VerdictDBException e) {
          e.printStackTrace();
        }
        return -1;
      }
      
      @Override
      public void run() {
        while (true) {
          // no dependency
          if (listeningQueues.size() == 0) {
            int ret = process(conn, Arrays.<ExecutionInfoToken>asList());
            if (ret == 0) {
              broadcast(ExecutionInfoToken.successToken());
              setSuccess();   // only for printing purpose
            } else {
              broadcast(ExecutionInfoToken.failureToken());
              setFailure();   // only for printing purpose
            }
            break;
          }
          
          // dependency exists
          readLatestResultsFromDependents();    // update both (1) status and (2) latestQueue
          
//          try {
//            TimeUnit.SECONDS.sleep(1);
//            System.out.println(QueryExecutionNode.this);
//            System.out.println(successDependentCount);
//            System.out.println(failedDependentCount);
////            System.out.println();
//          } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//          }
          
          // base conditions
          if (doesFailedDependentExist()) {
            broadcast(ExecutionInfoToken.failureToken());
            setFailure();    // only for printing purpose
            break;
          }
          if (areDependentsAllSuccess()) {
            broadcast(ExecutionInfoToken.successToken());
            setSuccess();    // only for printing purpose
            break;
          }
          
          // see if a complete set of results are available
          List<ExecutionInfoToken> latestResults = getLatestResultsIfAvailable();
//          System.out.println(QueryExecutionNode.this);
//          System.out.println(QueryExecutionNode.this.latestResults);
//          System.out.println(latestResults);
          if (latestResults == null) {
            continue;
          }
          
          int ret = process(conn, latestResults);
          if (ret != 0) {
            broadcast(ExecutionInfoToken.failureToken());
            setFailure();   // only for printing purpose
            break;
          }
          
        } // end of while loop
        
      }
    });
    
   
    
    // should immediately return without waiting for the termination of jobs
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
  public ExecutionTokenQueue generateListeningQueue() throws VerdictDBValueException {
    ExecutionTokenQueue queue = new ExecutionTokenQueue();
    listeningQueues.add(queue);
    latestResults.add(Optional.<ExecutionInfoToken>absent());
    if (listeningQueues.size() != latestResults.size()) {
      throw new VerdictDBValueException("Invalid field constraint.");
    }
    return queue;
  }
  
  public ExecutionTokenQueue generateReplacementListeningQueue(int index) throws VerdictDBValueException {
    ExecutionTokenQueue queue = new ExecutionTokenQueue();
    listeningQueues.set(index, queue);
    if (listeningQueues.size() != latestResults.size()) {
      throw new VerdictDBValueException("Invalid field constraint.");
    }
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
    return getStatus().equals("success");
  }
  
  public boolean isFailed() {
    return getStatus().equals("failed");
  }

  void setSuccess() {
    setStatus("success");
  }
  
  void setFailure() {
    setStatus("failure");
  }

  void broadcast(ExecutionInfoToken result) {
    for (ExecutionTokenQueue listener : broadcastingQueues) {
      listener.add(result);
//      System.out.println(new ToStringBuilder(this) + " sent: " + result);
    }
  }

  void readLatestResultsFromDependents() {
    for (int i = 0; i < listeningQueues.size(); i++) {
      if (latestResults.get(i).isPresent()) {
        continue;
      }
      
      ExecutionInfoToken rs = listeningQueues.get(i).poll();
      if (rs == null) {
        // do nothing
      } else if (rs.isStatusToken()) {
        if (rs.isSuccessToken()) {
          successDependentCount++;
        } else if (rs.isFailureToken()) {
          failedDependentCount++;
        }
      } else {
        latestResults.set(i, Optional.of(rs));
        System.out.println(new ToStringBuilder(this) + " Received: " + rs.toString());
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
      clearCachedLatestResults();
      return results;
    } else {
      return null;
    }
  }
  
  void clearCachedLatestResults() {
    int size = latestResults.size();
    for (int i = 0; i < size; i++) {
      latestResults.set(i, Optional.<ExecutionInfoToken>absent());
    }
  }

  boolean areDependentsAllComplete() {
    if (successDependentCount + failedDependentCount >= dependents.size()) {
      return true;
    } else {
      return false;
    }
    
//    boolean allComplete = true;
//    for (ExecutionInfoToken t : tokens) {
//      if (t.isSuccessToken() || t.isFailureToken()) {
//        // do nothing
//      } else {
//        allComplete = false;
//        break;
//      }
//    }
//    return allComplete;
    
//    for (QueryExecutionNode node : dependents) {
//      System.out.println(node.getStatus());
//      if (node.isSuccess() || node.isFailed()) {
//        // do nothing
//      } else {
//        return false;
//      }
//    }
//    return true;
  }
  
  boolean areDependentsAllSuccess() {
    if (successDependentCount >= dependents.size()) {
      return true;
    } else {
      return false;
    }
//    boolean allSuccess = true;
//    for (ExecutionInfoToken t : tokens) {
//      if (t.isSuccessToken()) {
//        // do nothing
//      } else {
//        allSuccess = false;
//        break;
//      }
//    }
//    return allSuccess;
  }
  
  boolean doesFailedDependentExist() {
    return failedDependentCount > 0;
  }

//  // identify nodes that are (1) aggregates and (2) are not descendants of any other aggregates.
//  List<AggExecutionNodeBlock> identifyTopAggBlocks() {
//    List<AggExecutionNodeBlock> aggblocks = new ArrayList<>();
//    
//    if (this instanceof AggExecutionNode) {
//      AggExecutionNodeBlock block = new AggExecutionNodeBlock(plan, this);
//      aggblocks.add(block);
//      return aggblocks;
//    }
//    for (QueryExecutionNode dep : getDependents()) {
//      List<AggExecutionNodeBlock> depAggBlocks = dep.identifyTopAggBlocks();
//      aggblocks.addAll(depAggBlocks);
//    }
//    
//    return aggblocks;
//  }
  
  

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
        .append("latestResults", latestResults)
        .append("selectQuery", selectQuery)
        .toString();
  }

}

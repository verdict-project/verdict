package org.verdictdb.core.querying;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.execution.ExecutableNode;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public abstract class BaseQueryNode implements ExecutableNode {

  SelectQuery selectQuery;
  
  // these are assumed to be not order-sensitive
  List<BaseQueryNode> parents = new ArrayList<>();

  // these are assumed to be not order-sensitive
  List<BaseQueryNode> dependents = new ArrayList<>();
  
  // these are the queues to which this node will broadcast its results (to upstream nodes).
  List<ExecutionTokenQueue> broadcastingQueues = new ArrayList<>();

  // these are the results coming from the producers (downstream operations).
  // multiple producers may share a single result queue.
  // these queues are assumed to be order-sensitive
  private List<ExecutionTokenQueue> listeningQueues = new ArrayList<>();

  // latest results from listening queues
//  private List<Optional<ExecutionInfoToken>> latestResults = new ArrayList<>();
  
  public BaseQueryNode() {}

  public BaseQueryNode(SelectQuery query) {
//    this(plan);
    this.selectQuery = query;
  }
  
  public SelectQuery getSelectQuery() {
    return selectQuery;
  }
  
  public void setSelectQuery(SelectQuery query) {
    this.selectQuery = query;
  }
  
  public List<BaseQueryNode> getParents() {
    return parents;
  }

  public List<BaseQueryNode> getDependents() {
    return dependents;
  }
  
  public BaseQueryNode getDependent(int index) {
    return dependents.get(index);
  }
  
//  public void executeAndWaitForTermination(DbmsConnection conn) throws VerdictDBValueException {
//    try {
//      ExecutorService executor = Executors.newFixedThreadPool(plan.getMaxNumberOfThreads());
//      execute(conn, executor);
//      executor.shutdown();
//      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);  // convention for waiting forever
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//  }

  
  void addParent(BaseQueryNode parent) {
    parents.add(parent);
  }

  // setup method
  public void addDependency(BaseQueryNode dep) {
    dependents.add(dep);
    dep.addParent(this);
  }

  // setup method
  public ExecutionTokenQueue generateListeningQueue() throws VerdictDBValueException {
    ExecutionTokenQueue queue = new ExecutionTokenQueue();
    listeningQueues.add(queue);
    return queue;
  }
  
  public ExecutionTokenQueue generateReplacementListeningQueue(int index) throws VerdictDBValueException {
    ExecutionTokenQueue queue = new ExecutionTokenQueue();
    listeningQueues.set(index, queue);
    return queue;
  }
  
  public void removeListeningQueue(int index) {
    listeningQueues.remove(index);
  }

  // setup method
  public void addBroadcastingQueue(ExecutionTokenQueue queue) {
    broadcastingQueues.add(queue);
  }
  
  public void clearBroadcastingQueues() {
    broadcastingQueues.clear();
  }
  
  @Override
  public List<ExecutionTokenQueue> getDestinationQueues() {
    return getBroadcastingQueues();
  }
  
  public List<ExecutionTokenQueue> getBroadcastingQueues() {
    return broadcastingQueues;
  }
  
  public ExecutionTokenQueue getBroadcastingQueue(int index) {
    return broadcastingQueues.get(index);
  }
  
  @Override
  public List<ExecutionTokenQueue> getSourceQueues() {
    return getListeningQueues();
  }
  
  public List<ExecutionTokenQueue> getListeningQueues() {
    return listeningQueues;
  }
  
  public ExecutionTokenQueue getListeningQueue(int index) {
    return listeningQueues.get(index);
  }

  /**
   * 
   * @param scrambleMeta
   * @return True if there exists scrambledTable in the from list or in the non-aggregate subqueries.
   */
  boolean doesContainScrambledTablesInDescendants(ScrambleMeta scrambleMeta) {
    if (!(this instanceof AggExecutionNode) && !(this instanceof ProjectionNode)) {
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
    
    for (BaseQueryNode dep : getDependents()) {
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
  
  public abstract BaseQueryNode deepcopy();
  
  void copyFields(BaseQueryNode from, BaseQueryNode to) {
    to.selectQuery = from.selectQuery.deepcopy();
    to.parents.addAll(from.parents);
    to.dependents.addAll(from.dependents);
    to.broadcastingQueues.addAll(from.broadcastingQueues);
    to.listeningQueues.addAll(from.listeningQueues);
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
    
    for (BaseQueryNode dep : dependents) {
      dep.print(indentSpace + 2);
    }
  }
  
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .append("listeningQueues", listeningQueues)
        .append("broadcastingQueues", broadcastingQueues)
        .append("selectQuery", selectQuery)
        .toString();
  }

}

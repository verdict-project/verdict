package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.verdictdb.connection.DbmsConnection;

import com.google.common.base.Optional;

public abstract class QueryExecutionNode {
  
  DbmsConnection conn;
  
//  QueryExecutionPlan plan;
  
  // running or complete
  String status = "running";
  
  List<QueryExecutionNode> dependents = new ArrayList<>();
  
  // these are the queues to which this node will broadcast its results (to upstream nodes).
  List<BlockingDeque<ExecutionResult>> broadcastQueues = new ArrayList<>();
  
  // these are the results coming from the producers (downstream operations).
  // multiple producers may share a single result queue.
  List<BlockingDeque<ExecutionResult>> listeningQueues = new ArrayList<>();
  
  // latest results from listening queues
  List<Optional<ExecutionResult>> latestResults = new ArrayList<>();
  
  public QueryExecutionNode(DbmsConnection conn) {
    this.conn = conn;
//    this.plan = plan;
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

  // setup method
  public void addDependent(QueryExecutionNode dep) {
    dependents.add(dep);
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

}

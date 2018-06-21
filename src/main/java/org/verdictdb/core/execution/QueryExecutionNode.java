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
  
  List<QueryExecutionNode> children = new ArrayList<>();
  
  List<Optional<ExecutionResult>> resultsFromChildren = new ArrayList<>();
  
  public QueryExecutionNode(DbmsConnection conn) {
    this.conn = conn;
  }

  public List<QueryExecutionNode> getChildren() {
    return children;
  }

  public void addChild(QueryExecutionNode child) {
    children.add(child);
    resultsFromChildren.add(Optional.<ExecutionResult>absent());
  }
  
  /**
   * For multi-threading, the parent of this node is responsible for running this method as a separate thread.
   * @param resultQueue
   */
  public void execute(final BlockingDeque<ExecutionResult> resultQueue) {
    // Start the execution of all children
    List<LinkedBlockingDeque<ExecutionResult>> queueForChildren = new ArrayList<>();
    for (QueryExecutionNode child : children) {
      LinkedBlockingDeque<ExecutionResult> queueForChild = new LinkedBlockingDeque<>();
      child.execute(queueForChild);
      queueForChildren.add(queueForChild);
    }
    
    // Execute this node if there are some results available
    ExecutorService executor = Executors.newSingleThreadExecutor();
    while (true) {
      for (int i = 0; i < queueForChildren.size(); i++) {
        ExecutionResult rs = queueForChildren.get(i).poll();
        if (rs == null) {
          // do nothing
        } else {
          resultsFromChildren.set(i, Optional.of(rs));
        }
      }
      
      // this part could be a time consuming part
      boolean allResultsAvailable = true;
      final List<ExecutionResult> results = new ArrayList<>();
      for (Optional<ExecutionResult> r : resultsFromChildren) {
        if (!r.isPresent()) {
          allResultsAvailable = false;
          break;
        }
        results.add(r.get());
      }
      
      if (allResultsAvailable) {
        // run this on a separate thread
        executor.submit(new Runnable() {
          @Override
          public void run() {
            ExecutionResult thisResult = executeInternally(results);
            resultQueue.add(thisResult);
          }
        });
        
        // don't wait for the results from children anymore if they all completed.
        boolean atLeastOneIncomplete = false;
        for (Optional<ExecutionResult> r : resultsFromChildren) {
          if (!r.isPresent() || !r.get().isFinished()) {
            atLeastOneIncomplete = true;
          }
        }
        if (atLeastOneIncomplete == false) {
          break;
        }
      }
    }
    
    // finishes only when no threads are running for this node.
    try {
      executor.shutdown();
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);  // convention for waiting forever
    } catch (InterruptedException e) {
      executor.shutdownNow();
    }
    resultQueue.add(ExecutionResult.completeResult());
  }
  
  public abstract ExecutionResult executeInternally(List<ExecutionResult> resultFromChildren);

}

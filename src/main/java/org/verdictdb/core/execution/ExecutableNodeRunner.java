package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SqlConvertable;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.QueryToSql;

public class ExecutableNodeRunner implements Runnable {

  DbmsConnection conn;

  ExecutableNode node;

  public ExecutableNodeRunner(DbmsConnection conn, ExecutableNode node) {
    this.conn = conn;
    this.node = node;
  }
  
  public static ExecutionInfoToken execute(DbmsConnection conn, ExecutableNode node) 
      throws VerdictDBException {
    return execute(conn, node, Arrays.<ExecutionInfoToken>asList());
  }
  
  public static ExecutionInfoToken execute(
      DbmsConnection conn, 
      ExecutableNode node, 
      List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return (new ExecutableNodeRunner(conn, node)).execute(tokens);
  }

  @Override
  public void run() {
    // no dependency exists
    if (node.getSourceQueues().size() == 0) {
      try {
        executeAndBroadcast(Arrays.<ExecutionInfoToken>asList());
        broadcast(ExecutionInfoToken.successToken());
        return;
      } catch (VerdictDBException e) {
        e.printStackTrace();
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
        executeAndBroadcast(tokens);
      } catch (VerdictDBException e) {
        e.printStackTrace();
        broadcast(ExecutionInfoToken.failureToken());
        break;
      }
    }
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
//    System.out.println("broadcasting: " + token);
    for (ExecutionTokenQueue dest : node.getDestinationQueues()) {
      dest.add(token);
    }
  }
  
  void executeAndBroadcast(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    ExecutionInfoToken resultToken = execute(tokens);
    broadcast(resultToken);
  }

  public ExecutionInfoToken execute(List<ExecutionInfoToken> tokens) throws VerdictDBException {
//    System.out.println("execute: " + node);
    SqlConvertable sqlObj = node.createQuery(tokens);
    boolean doesResultExist = false;
    if (sqlObj != null) {
      String sql = QueryToSql.convert(conn.getSyntax(), sqlObj);
      doesResultExist = conn.execute(sql);
    }
    
    DbmsQueryResult intermediate = null;
    if (doesResultExist) {
      intermediate = conn.getResult();
    }
    return node.createToken(intermediate);
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

}
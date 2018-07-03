/*
 * Copyright 2018 University of Michigan
 * 
 * You must contact Barzan Mozafari (mozafari@umich.edu) or Yongjoo Park (pyongjoo@umich.edu) to discuss
 * how you could use, modify, or distribute this code. By default, this code is not open-sourced and we do
 * not license this code.
 */

package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.querying.TempIdCreator;
import org.verdictdb.core.rewriter.aggresult.AggNameAndType;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * Represents an execution of a single aggregate query (without nested components).
 * 
 * Steps:
 * 1. identify agg and nonagg columns of a given select agg query.
 * 2. convert the query into multiple block-agg queries.
 * 3. issue those block-agg queries one by one.
 * 4. combine the results of those block-agg queries as the answers to those queries arrive.
 * 5. depending on the interface, call an appropriate result handler.
 * 
 * @author Yongjoo Park
 *
 */
public class AsyncAggExecutionNode extends ExecutableNodeBase {

  ScrambleMeta scrambleMeta;

  // group-by columns
  List<String> nonaggColumns;
  //  
  // agg columns. pairs of their column names and their types (i.e., sum, avg, count)
  List<AggNameAndType> aggColumns;

  SelectQuery originalQuery;

//  List<AsyncAggExecutionNode> children = new ArrayList<>();

  int tableNum = 1;
  
  ExecutionInfoToken savedToken = null;

//  String getNextTempTableName(String tableNamePrefix) {
//    return tableNamePrefix + tableNum++;
//  }

  private AsyncAggExecutionNode() {
    super();
  }
  
//  public static AsyncAggExecutionNode castAndCreate(TempIdCreator idCreator,
//      List<QueryNodeBase> individualAggs,
//      List<QueryNodeBase> combiners) throws VerdictDBValueException {
//    
//    List<ExecutableNodeBase> ind = new ArrayList<>();
//    List<ExecutableNodeBase> com = new ArrayList<>();
//    for (QueryNodeBase n : individualAggs) {
//      ind.add(n);
//    }
//    for (QueryNodeBase c : combiners) {
//      com.add(c);
//    }
//    return create(idCreator, ind, com);
//  }
  
  public static AsyncAggExecutionNode create(
      TempIdCreator idCreator,
      List<ExecutableNodeBase> individualAggs,
      List<ExecutableNodeBase> combiners) throws VerdictDBValueException {

    AsyncAggExecutionNode node = new AsyncAggExecutionNode();
//    ExecutionTokenQueue rootQueue = node.generateListeningQueue();
    
    // first agg -> root
    node.subscribeTo(individualAggs.get(0), 0);
//    individualAggs.get(0).addBroadcastingQueue(rootQueue);
//    node.addDependency(individualAggs.get(0));

    // combiners -> root
    for (ExecutableNodeBase c : combiners) {
      node.subscribeTo(c, 0);
//      c.addBroadcastingQueue(rootQueue);
//      node.addDependency(c);
    }

    return node;
  }
  
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    savedToken = tokens.get(0);
    return null;
  }
  
  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return savedToken;
  }

//  @Override
//  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) 
//      throws VerdictDBException {
//    ExecutionInfoToken token = super.executeNode(conn, downstreamResults);
//    System.out.println("AsyncNode execution " + getSelectQuery());
//    try {
//      TimeUnit.SECONDS.sleep(1);
//      this.print();
////      for (QueryExecutionNode n : getDependents()) {
////        System.out.println(n + " " + n.getStatus());
////      }
//    } catch (InterruptedException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
//    return downstreamResults.get(0);
//  }

  @Override
  public ExecutableNodeBase deepcopy() {
    AsyncAggExecutionNode copy = new AsyncAggExecutionNode();
    copyFields(this, copy);
    return copy;
  }

  void copyFields(AsyncAggExecutionNode from, AsyncAggExecutionNode to) {
    to.scrambleMeta = from.scrambleMeta;
    to.nonaggColumns = from.nonaggColumns;
    to.aggColumns = from.aggColumns;
  }

  public ScrambleMeta getScrambleMeta() {
    return scrambleMeta;
  }

  public void setScrambleMeta(ScrambleMeta meta) {
    this.scrambleMeta = meta;
  }
}

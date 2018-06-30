/*
 * Copyright 2018 University of Michigan
 * 
 * You must contact Barzan Mozafari (mozafari@umich.edu) or Yongjoo Park (pyongjoo@umich.edu) to discuss
 * how you could use, modify, or distribute this code. By default, this code is not open-sourced and we do
 * not license this code.
 */

package org.verdictdb.core.execution.ola;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.ScrambleMeta;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.execution.QueryExecutionNode;
import org.verdictdb.core.execution.QueryExecutionPlan;
import org.verdictdb.core.query.*;
import org.verdictdb.core.rewriter.aggresult.AggNameAndType;
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
public class AsyncAggExecutionNode extends QueryExecutionNode {

  ScrambleMeta scrambleMeta;

  // group-by columns
  List<String> nonaggColumns;
  //  
  // agg columns. pairs of their column names and their types (i.e., sum, avg, count)
  List<AggNameAndType> aggColumns;

  SelectQuery originalQuery;

//  List<AsyncAggExecutionNode> children = new ArrayList<>();

//  int tableNum = 1;
  List<BaseTable> scrambleTables = new ArrayList<>();

  int tableNum = 1;

//  String getNextTempTableName(String tableNamePrefix) {
//    return tableNamePrefix + tableNum++;
//  }

  private AsyncAggExecutionNode(QueryExecutionPlan plan) {
    super(plan);
  }
  
  public static AsyncAggExecutionNode create(
      QueryExecutionPlan plan,
      List<QueryExecutionNode> individualAggs,
      List<QueryExecutionNode> combiners) throws VerdictDBValueException {

    AsyncAggExecutionNode node = new AsyncAggExecutionNode(plan);
    ExecutionTokenQueue rootQueue = node.generateListeningQueue();

    // set new broadcasting nodes

    // first agg -> root
    individualAggs.get(0).addBroadcastingQueue(rootQueue);
    node.addDependency(individualAggs.get(0));

//    // first agg -> first combiner
//    ExecutionTokenQueue firstCombinerQueue = combiners.get(0).generateListeningQueue();
//    individualAggs.get(0).addBroadcastingQueue(firstCombinerQueue);
//    combiners.get(0).addDependency(individualAggs.get(0));
//
//    // combiners -> next combiners
//    for (int i = 1; i < combiners.size(); i++) {
//      ExecutionTokenQueue combinerQueue = combiners.get(i).generateListeningQueue();
//      combiners.get(i-1).addBroadcastingQueue(combinerQueue);
//      combiners.get(i).addDependency(combiners.get(i-1));
//    }

//    // individual aggs (except for the first one) -> combiners
//    for (int i = 0; i < combiners.size(); i++) {
//      ExecutionTokenQueue combinerQueue = combiners.get(i).generateListeningQueue();
//      individualAggs.get(i+1).addBroadcastingQueue(combinerQueue);
//      combiners.get(i).addDependency(individualAggs.get(i+1));
//    }

    // combiners -> root
    for (QueryExecutionNode c : combiners) {
      c.addBroadcastingQueue(rootQueue);
      node.addDependency(c);
    }

//    for (int i = 1; i < individualAggs.size(); i++) {
//      ExecutionTokenQueue q = combiners.get(i-1).generateListeningQueue();
//      individualAggs.get(i).addBroadcastingQueue(q);
//    }
//    for (QueryExecutionNode c : combiners) {
//      c.addBroadcastingQueue(queue);
//      node.addDependency(c);
//    }
    return node;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) 
      throws VerdictDBException {
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


    return downstreamResults.get(0);
  }

  @Override
  public QueryExecutionNode deepcopy() {
    AsyncAggExecutionNode copy = new AsyncAggExecutionNode(getPlan());
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

  public List<BaseTable> getScrambleTables() {
    return scrambleTables;
  }

  // Find out scramble tables in from list.
  public void setScrambleTables()  {
    List<AbstractRelation> fromlist = originalQuery.getFromList();
    for (AbstractRelation table:fromlist) {
      if (table instanceof BaseTable) {
        if (scrambleMeta.isScrambled(((BaseTable) table).getSchemaName(), ((BaseTable) table).getTableName())) {
          scrambleTables.add((BaseTable) table);
        }
      }
      else if (table instanceof JoinTable) {
        for (AbstractRelation joinTable:((JoinTable) table).getJoinList()) {
          if (joinTable instanceof BaseTable) {
            if (scrambleMeta.isScrambled(((BaseTable) joinTable).getSchemaName(), ((BaseTable) joinTable).getTableName())) {
              scrambleTables.add((BaseTable) table);
            }
          }
        }
      }
    }
  }

  public void addScrambleTable(BaseTable t) {
    scrambleTables.add(t);
  }

  public void setScrambleMeta(ScrambleMeta meta) {
    this.scrambleMeta = meta;
  }
}

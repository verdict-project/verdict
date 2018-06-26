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
import java.util.concurrent.BlockingDeque;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.execution.CreateTableAsSelectExecutionNode;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.execution.QueryExecutionNode;
import org.verdictdb.core.execution.QueryExecutionPlan;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.query.UnnamedColumn;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.rewriter.aggresult.AggNameAndType;
import org.verdictdb.core.rewriter.query.AggQueryRewriter;
import org.verdictdb.core.rewriter.query.AggblockMeta;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.exception.VerdictDBException;

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
public class AsyncAggExecutionNode extends CreateTableAsSelectExecutionNode {

  ScrambleMeta scrambleMeta;

  // group-by columns
  List<String> nonaggColumns;
  //  
  // agg columns. pairs of their column names and their types (i.e., sum, avg, count)
  List<AggNameAndType> aggColumns;

  SelectQuery originalQuery;

  List<AsyncAggExecutionNode> children = new ArrayList<>();

  int tableNum = 1;

  String getNextTempTableName(String tableNamePrefix) {
    return tableNamePrefix + tableNum++;
  }

  private AsyncAggExecutionNode(String scratchpadSchema) {
    super(scratchpadSchema);
  }
  
  public static AsyncAggExecutionNode create(
      String scratchpadScheman,
      List<QueryExecutionNode> individualAggs,
      List<QueryExecutionNode> combiners) {
    AsyncAggExecutionNode node = new AsyncAggExecutionNode(scratchpadScheman);
    ExecutionTokenQueue queue = node.generateListeningQueue();
    individualAggs.get(0).addBroadcastingQueue(queue);
    node.addDependency(individualAggs.get(0));
    for (QueryExecutionNode c : combiners) {
      c.addBroadcastingQueue(queue);
      node.addDependency(c);
    }
    return node;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) {
    ExecutionInfoToken token = super.executeNode(conn, downstreamResults);
    return token;
  }

}

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
import org.verdictdb.core.execution.ExecutionResult;
import org.verdictdb.core.execution.QueryExecutionNode;
import org.verdictdb.core.execution.SingleAggExecutionNode;
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
import org.verdictdb.exception.UnexpectedTypeException;
import org.verdictdb.exception.ValueException;
import org.verdictdb.exception.VerdictDbException;

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

  List<AsyncAggExecutionNode> children = new ArrayList<>();

  int tableNum = 1;

  String getNextTempTableName(String tableNamePrefix) {
    return tableNamePrefix + tableNum++;
  }

  private AsyncAggExecutionNode(DbmsConnection conn) {
    super(conn, null);
    // TODO Auto-generated constructor stub
  }
  
  public static AsyncAggExecutionNode create(
      DbmsConnection conn,
      List<QueryExecutionNode> individualAggs,
      List<QueryExecutionNode> combiners) {
    return null;
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    return null;
  }

  //  /**
  //   * Progressively aggregates the query, and feed the available results to the upstream.
  //   * 
  //   * @param conn
  //   * @param scrambleMeta
  //   * @param query
  //   * @throws UnexpectedTypeException
  //   * @throws ValueException
  //   */
  //  public AsyncAggExecutionNode(
  //      DbmsConnection conn, 
  //      ScrambleMeta scrambleMeta, 
  //      String resultSchemaName, 
  //      String resultTableName,
  //      SelectQuery query) 
  //      throws VerdictDbException {
  //    super(conn, query);
  //    this.scrambleMeta = scrambleMeta;
  //    this.originalQuery = query;
  ////    Pair<List<String>, List<AggNameAndType>> cols = identifyAggColumns(originalQuery.getSelectList());
  ////    nonaggColumns = cols.getLeft();
  ////    aggColumns = cols.getRight();
  //    
  //    // query rewriting into block-aggregate queries.
  //    // TODO
  //    AggQueryRewriter aggQueryRewriter = new AggQueryRewriter(scrambleMeta);
  //    List<Pair<AbstractRelation, AggblockMeta>> aggblockQueriesWithMeta = aggQueryRewriter.rewrite(originalQuery);
  //    int stepCount = aggblockQueriesWithMeta.size();
  //    
  //    // Generate agg nodes and combiner nodes
  //    // Only a single listener is generated and shared for them. But, combiners will have their own listeners.
  //    BlockingDeque<ExecutionResult> aggResultsQueue = generateListeningQueue();
  //    
  //    // generate agg nodes
  //    List<SingleAggExecutionNode> aggNodes = new ArrayList<>();
  //    for (int i = 0; i < stepCount; i++) {
  //      SelectQuery aggquery = (SelectQuery) aggblockQueriesWithMeta.get(i).getLeft();
  //      AggblockMeta aggmeta = aggblockQueriesWithMeta.get(i).getRight();
  //      aggNodes.add(new SingleAggExecutionNode(conn, aggmeta, resultSchemaName, getNextTempTableName(resultTableName), aggquery));
  //    }
  //    aggNodes.get(0).addBroadcastingQueue(aggResultsQueue);
  //    addDependency(aggNodes.get(0));
  //    
  //    // generate combiner nodes
  //    List<AggCombinerExecutionNode> combinerNodes = new ArrayList<>();
  //    for (int i = 0; i < stepCount-1; i++) {
  //      AggCombinerExecutionNode combiner = new AggCombinerExecutionNode(conn);
  //      BlockingDeque<ExecutionResult> queueToCombiner1= combiner.generateListeningQueue();
  //      BlockingDeque<ExecutionResult> queueToCombiner2= combiner.generateListeningQueue();
  //      if (i == 0) {
  //        aggNodes.get(i).addBroadcastingQueue(queueToCombiner1);
  //        aggNodes.get(i+1).addBroadcastingQueue(queueToCombiner2);
  //      }
  //      else {
  //        combinerNodes.get(i-1).addBroadcastingQueue(queueToCombiner1);
  //        aggNodes.get(i+1).addBroadcastingQueue(queueToCombiner2);
  //      }
  //      combiner.addBroadcastingQueue(aggResultsQueue);
  //      addDependency(combiner);
  //      combinerNodes.add(combiner);
  //    }
  //  }

  //  Pair<List<String>, List<AggNameAndType>> identifyAggColumns(List<SelectItem> items) 
  //      throws UnexpectedTypeException, ValueException {
  //    List<String> nonagg = new ArrayList<>();
  //    List<AggNameAndType> aggcols = new ArrayList<>();
  //
  //    for (SelectItem item : items) {
  //      if (item.isAggregateColumn()) {
  //        aggcols.add(new AggNameAndType(getAliasName(item), inferAggType(item)));
  //      }
  //      else {
  //        nonagg.add(getAliasName(item));
  //      }
  //    }
  //
  //    return Pair.of(nonagg, aggcols);
  //  }

  //  String getAliasName(SelectItem item) throws UnexpectedTypeException {
  //    if (item instanceof AliasedColumn) {
  //      return ((AliasedColumn) item).getAliasName();
  //    } else {
  //      throw new UnexpectedTypeException("select items must have been aliased.");
  //    }
  //  }

  //  String inferAggType(SelectItem item) throws ValueException {
  //    if (item instanceof AliasedColumn) {
  //      return inferAggType(((AliasedColumn) item).getColumn());
  //    }
  //
  //    if (item instanceof UnnamedColumn) {
  //      if (item instanceof ColumnOp) {
  //        String opType = ((ColumnOp) item).getOpType();
  //        if (opType.equals("sum")) {
  //          return "sum";
  //        } else if (opType.equals("avg")) {
  //          return "avg";
  //        } else if (opType.equals("count")) {
  //          return "count";
  //        }
  //
  //        String foundType = "none";
  //        List<UnnamedColumn> cols = ((ColumnOp) item).getOperands();
  //        for (UnnamedColumn col : cols) {
  //          String type = inferAggType(col);
  //          if (foundType.equals("none")) {
  //            foundType = type;
  //          } else {
  //            throw new ValueException("more than one aggregate function found in a single select item.");
  //          }
  //        }
  //        return foundType;
  //      }
  //      else {
  //        return "none";
  //      }
  //    }
  //    else {
  //      return "none";
  //    }
  //  }


}



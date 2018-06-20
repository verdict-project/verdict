/*
 * Copyright 2018 University of Michigan
 * 
 * You must contact Barzan Mozafari (mozafari@umich.edu) or Yongjoo Park (pyongjoo@umich.edu) to discuss
 * how you could use, modify, or distribute this code. By default, this code is not open-sourced and we do
 * not license this code.
 */

package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.aggresult.AggregateFrame;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.core.query.UnnamedColumn;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.rewriter.aggresult.AggNameAndType;
import org.verdictdb.core.rewriter.aggresult.AggResultCombiner;
import org.verdictdb.core.rewriter.aggresult.SingleAggResultRewriter;
import org.verdictdb.core.rewriter.query.AggQueryRewriter;
import org.verdictdb.core.rewriter.query.AliasRenamingRules;
import org.verdictdb.core.sql.SelectQueryToSql;
import org.verdictdb.exception.UnexpectedTypeException;
import org.verdictdb.exception.ValueException;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.resulthandler.AsyncHandler;
import org.verdictdb.sql.syntax.HiveSyntax;

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
public class AggExecutionNode {
  
  DbmsConnection conn;
  
  ScrambleMeta meta;
  
  // group-by columns
  List<String> nonaggColumns;
//  
  // agg columns. pairs of their column names and their types (i.e., sum, avg, count)
  List<AggNameAndType> aggColumns;
  
  SelectQueryOp originalQuery;
  
  List<AggExecutionNode> dependencies = new ArrayList<>();
  
  public AggExecutionNode(DbmsConnection conn, ScrambleMeta meta, SelectQueryOp query) 
      throws UnexpectedTypeException, ValueException {
    this.conn = conn;
    this.meta = meta;
    this.originalQuery = query;
    Pair<List<String>, List<AggNameAndType>> cols = identifyAggColumns(originalQuery.getSelectList());
    nonaggColumns = cols.getLeft();
    aggColumns = cols.getRight();
  }
  
  Pair<List<String>, List<AggNameAndType>> identifyAggColumns(List<SelectItem> items) 
      throws UnexpectedTypeException, ValueException {
    List<String> nonagg = new ArrayList<>();
    List<AggNameAndType> aggcols = new ArrayList<>();
    
    for (SelectItem item : items) {
      if (item.isAggregateColumn()) {
        aggcols.add(new AggNameAndType(getAliasName(item), inferAggType(item)));
      }
      else {
        nonagg.add(getAliasName(item));
      }
    }
    
    return Pair.of(nonagg, aggcols);
  }
  
  String getAliasName(SelectItem item) throws UnexpectedTypeException {
    if (item instanceof AliasedColumn) {
      return ((AliasedColumn) item).getAliasName();
    } else {
      throw new UnexpectedTypeException("select items must have been aliased.");
    }
  }
  
  String inferAggType(SelectItem item) throws ValueException {
    if (item instanceof AliasedColumn) {
      return inferAggType(((AliasedColumn) item).getColumn());
    }
    
    if (item instanceof UnnamedColumn) {
      if (item instanceof ColumnOp) {
        String opType = ((ColumnOp) item).getOpType();
        if (opType.equals("sum")) {
          return "sum";
        } else if (opType.equals("avg")) {
          return "avg";
        } else if (opType.equals("count")) {
          return "count";
        }
        
        String foundType = "none";
        List<UnnamedColumn> cols = ((ColumnOp) item).getOperands();
        for (UnnamedColumn col : cols) {
          String type = inferAggType(col);
          if (foundType.equals("none")) {
            foundType = type;
          } else {
            throw new ValueException("more than one aggregate function found in a single select item.");
          }
        }
        return foundType;
      }
      else {
        return "none";
      }
    }
    else {
      return "none";
    }
  }
  
  /**
   * Rewrites a query (into multiple block-agg queries), then simply runs the first of those rewritten
   * block-agg queries.
   * 
   * @return
   * @throws VerdictDbException
   */
  public DbmsQueryResult singleExecute() throws VerdictDbException {
    AggQueryRewriter aggQueryRewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> aggWithErrorQueries = aggQueryRewriter.rewrite(originalQuery);
    
    // rewrite the query
    AbstractRelation q = aggWithErrorQueries.get(0);
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String query_string = relToSql.toSql(q);
    
    // extract column types from the rewritten
    Pair<List<String>, List<AggNameAndType>> rewrittenNonaggAndAgg =
        identifyAggColumns(((SelectQueryOp) q).getSelectList());
    List<String> rewrittenNonaggColumns = rewrittenNonaggAndAgg.getLeft();
    List<AggNameAndType> rewrittenAggColumns = rewrittenNonaggAndAgg.getRight();
    
    DbmsQueryResult rawResult = conn.executeQuery(query_string);
    AggregateFrame newAggResult = 
        AggregateFrame.fromDmbsQueryResult(rawResult, rewrittenNonaggColumns, rewrittenAggColumns);
    
    // changes the intermediate aggregates to the final aggregates
    SingleAggResultRewriter aggResultRewriter = new SingleAggResultRewriter(newAggResult);
    AggregateFrame rewritten = aggResultRewriter.rewrite(nonaggColumns, aggColumns);
    DbmsQueryResult resultToUser = rewritten.toDbmsQueryResult();
    return resultToUser;
  }
  
  public void asyncExecute(AsyncHandler handler) throws VerdictDbException {
    AggQueryRewriter aggQueryRewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> aggWithErrorQueries = aggQueryRewriter.rewrite(originalQuery);
    AggregateFrame combinedAggResult = null;
    
    // execute the rewritten queries one by one
    for (int i = 0; i < aggWithErrorQueries.size(); i++) {
      AbstractRelation q = aggWithErrorQueries.get(i);
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String query_string = relToSql.toSql(q);
      
      Pair<List<String>, List<AggNameAndType>> rewrittenNonaggAndAgg =
          identifyAggColumns(((SelectQueryOp) q).getSelectList());
      List<String> rewrittenNonaggColumns = rewrittenNonaggAndAgg.getLeft();
      List<AggNameAndType> rewrittenAggColumns = rewrittenNonaggAndAgg.getRight();
      
      DbmsQueryResult rawResult = conn.executeQuery(query_string);
      AggregateFrame newAggResult = AggregateFrame.fromDmbsQueryResult(rawResult, rewrittenNonaggColumns, rewrittenAggColumns);
      
      // combine with previous answers
      if (i == 0) {
        combinedAggResult = newAggResult;
      }
      else {
        combinedAggResult = AggResultCombiner.combine(combinedAggResult, newAggResult);
      }
      
      // convert to a user-friendly answer
      SingleAggResultRewriter aggResultRewriter = new SingleAggResultRewriter(combinedAggResult);
      AggregateFrame rewritten = aggResultRewriter.rewrite(rewrittenNonaggColumns, rewrittenAggColumns);
      DbmsQueryResult resultToUser = rewritten.toDbmsQueryResult();
      handler.handle(resultToUser);
    }
  }
  
  public DbmsQueryResult execute() {
    return null;
  }
  
}


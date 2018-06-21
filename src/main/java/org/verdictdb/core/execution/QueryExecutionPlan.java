/*
 * Copyright 2018 University of Michigan
 * 
 * You must contact Barzan Mozafari (mozafari@umich.edu) or Yongjoo Park (pyongjoo@umich.edu) to discuss
 * how you could use, modify, or distribute this code. By default, this code is not open-sourced and we do
 * not license this code.
 */

package org.verdictdb.core.execution;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.DbmsMetaDataCache;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.sql.NonValidatingSQLParser;
import org.verdictdb.exception.UnexpectedTypeException;
import org.verdictdb.exception.ValueException;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.SyntaxAbstract;

public class QueryExecutionPlan {
  
  SelectQueryOp query;
  
  ScrambleMeta scrambleMeta;
  
  List<QueryExecutionNode> roots;
  
  PostProcessor postProcessor;
  
//  /**
//   * 
//   * @param queryString A select query
//   * @throws UnexpectedTypeException 
//   */
//  public AggQueryExecutionPlan(DbmsConnection conn, SyntaxAbstract syntax, String queryString) throws VerdictDbException {
//    this(conn, syntax, (SelectQueryOp) new NonValidatingSQLParser().toRelation(queryString));
//  }
  
  /**
   * 
   * @param query  A well-formed select query object
   * @throws ValueException 
   * @throws VerdictDbException 
   */
  public QueryExecutionPlan(
      DbmsConnection conn, 
      SyntaxAbstract syntax, 
      ScrambleMeta scrambleMeta, 
      SelectQueryOp query) throws VerdictDbException {
    this.scrambleMeta = scrambleMeta;
    if (!query.isAggregateQuery()) {
      throw new UnexpectedTypeException(query);
    }
    this.query = query;
    Pair<List<QueryExecutionNode>, PostProcessor> plan = makePlan(conn, syntax, query);
    this.roots = plan.getLeft();
    this.postProcessor = plan.getRight();
  }
  
  /** 
   * Creates a tree in which each node is AggQueryExecutionNode. Each AggQueryExecutionNode corresponds to
   * an aggregate query, whether it is the main query or a subquery.
   * 
   * 1. Restrict the aggregate subqueries to appear only in the where clause.
   * 2. If an aggregate subquery appears in the where clause, the subquery itself should be a single
   *    AggQueryExecutionNode even if it contains another aggregate subqueries within it.
   * 3. Except for the root nodes, all other nodes are not approximated.
   * 4. AggQueryExecutionNode must not include any correlated predicates.
   * 5. The results of intermediate AggQueryExecutionNode should be stored as a materialized view.
   * 
   * @param conn
   * @param query
   * @return Pair of roots of the tree and post-processing interface.
   * @throws ValueException 
   * @throws UnexpectedTypeException 
   */
  // TODO
  Pair<List<QueryExecutionNode>, PostProcessor> makePlan(DbmsConnection conn, SyntaxAbstract syntax, SelectQueryOp query) 
      throws VerdictDbException {
    
    // identify aggregate subqueries and create seprate nodes for them
    
    // generate temp table names for those aggregate subqueries and use them in their ancestors.
    
    return Pair.of(Arrays.asList(new QueryExecutionNode(conn, scrambleMeta, query)), null);
  }
  
  public void execute(DbmsConnection conn) {
    // execute roots
    
    // after executions are all finished.
    cleanUp();
  }
  
  // clean up any intermediate materialized tables
  void cleanUp() {
    
  }

}

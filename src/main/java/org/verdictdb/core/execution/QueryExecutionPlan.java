/*
 * Copyright 2018 University of Michigan
 *
 * You must contact Barzan Mozafari (mozafari@umich.edu) or Yongjoo Park (pyongjoo@umich.edu) to discuss
 * how you could use, modify, or distribute this code. By default, this code is not open-sourced and we do
 * not license this code.
 */

package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingDeque;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.DbmsMetaDataCache;
import org.verdictdb.core.query.*;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.sql.NonValidatingSQLParser;
import org.verdictdb.core.sql.SelectQueryToSql;
import org.verdictdb.exception.UnexpectedTypeException;
import org.verdictdb.exception.ValueException;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.SyntaxAbstract;

public class QueryExecutionPlan {

  SelectQueryOp query;

  ScrambleMeta scrambleMeta;

  QueryExecutionNode root;

  static int tempTableIndex = 1;

//  PostProcessor postProcessor;

//  /**
//   * 
//   * @param queryString A select query
//   * @throws UnexpectedTypeException 
//   */
//  public AggQueryExecutionPlan(DbmsConnection conn, SyntaxAbstract syntax, String queryString) throws VerdictDbException {
//    this(conn, syntax, (SelectQueryOp) new NonValidatingSQLParser().toRelation(queryString));
//  }

  /**
   * @param query A well-formed select query object
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
    this.root = makePlan(conn, syntax, query);
//    this.postProcessor = plan.getRight();
  }

  // check whether query contains scramble table in its from list
  private Boolean checkScrambleTable(List<AbstractRelation> fromlist) {
    for (AbstractRelation table : fromlist) {
      if (table instanceof BaseTable) {
        if (scrambleMeta.isScrambled(((BaseTable) table).getSchemaName(), ((BaseTable) table).getTableName())) {
          return true;
        }
      } else if (table instanceof JoinTable) {
        if (checkScrambleTable(((JoinTable) table).getJoinList())) {
          return true;
        }
      }
    }
    return false;
  }

  private SelectQueryOp replaceAggregateSubquery(DbmsConnection conn, SyntaxAbstract syntax, SelectQueryOp subquery) throws VerdictDbException {
    conn.executeUpdate("create schema if not exists " + syntax.getQuoteString() + "verdictdb_temp" +
        syntax.getQuoteString() + ";");
    // create tempory table for aggregate subquery
    SelectQueryToSql relToSql = new SelectQueryToSql(syntax);
    List<SelectItem> newSelectList = new ArrayList<>();
    for (SelectItem selectItem : subquery.getSelectList()) {
      if (selectItem instanceof AsteriskColumn)
        newSelectList.add(new AsteriskColumn());
      else
        newSelectList.add(new AliasedColumn(new BaseColumn("verdictdb_temp", "tmptable" + tempTableIndex,
            ((AliasedColumn) selectItem).getAliasName()), ((AliasedColumn) selectItem).getAliasName()));
    }
    String createTable = "create table " + syntax.getQuoteString() + "verdictdb_temp" +
    syntax.getQuoteString() + "."  + syntax.getQuoteString() + "tmptable" + tempTableIndex + syntax.getQuoteString()
        + " as " + relToSql.toSql(subquery);
    conn.executeUpdate(createTable);
    String aliasName = null;
    if (subquery.getAliasName().isPresent()) aliasName = subquery.getAliasName().get();
    subquery = SelectQueryOp.getSelectQueryOp(newSelectList, new BaseTable("verdictdb_temp", "tmptable" + tempTableIndex++));
    if (aliasName!=null) subquery.setAliasName(aliasName);
    return subquery;
  }

  private Pair<List<QueryExecutionNode>, List<SelectQueryOp>> recursiveReplaceSubquery(
      DbmsConnection conn, SyntaxAbstract syntax, SelectQueryOp query) throws VerdictDbException{
    // check whether outer query has scramble table stored in scrambleMeta
    boolean scrambleTableinOuterQuery = checkScrambleTable(query.getFromList());

    // identify aggregate subqueries and create separate nodes for them
    List<QueryExecutionNode> children = new ArrayList<>();
    List<SelectQueryOp> rootToReplace = new ArrayList<>();
    Pair<List<QueryExecutionNode>, List<SelectQueryOp>> result;

    // From List
    for (AbstractRelation table : query.getFromList()) {
      int index = query.getFromList().indexOf(table);
      if (table instanceof SelectQueryOp) {
        if (table.isAggregateQuery()) {
          // use inner query as root
          if (!scrambleTableinOuterQuery && checkScrambleTable(((SelectQueryOp) table).getFromList())) {
            rootToReplace.add((SelectQueryOp) table);
            table = replaceAggregateSubquery(conn, syntax, (SelectQueryOp) table);
          } else {
            // If aggregate, first recursive build the tree of the node, then replace the query in its original plan
            QueryExecutionNode child = makePlan(conn, syntax, (SelectQueryOp) table);
            children.add(child);
            // rewrite the inner aggregate query
            table = replaceAggregateSubquery(conn, syntax, (SelectQueryOp) table);
          }
        }
        else { // If non-aggregate, recursive use replaceAggregateSubquery find all subquries
          result = recursiveReplaceSubquery(conn, syntax, (SelectQueryOp) table);
          rootToReplace.addAll(result.getRight());
          children.addAll(result.getLeft());
        }
      } else if (table instanceof JoinTable) {
        for (AbstractRelation jointTable : ((JoinTable) table).getJoinList()) {
          int joinindex = ((JoinTable) table).getJoinList().indexOf(jointTable);
          if (jointTable instanceof SelectQueryOp) {
            if (jointTable.isAggregateQuery()) {
              // use inner query as root
              if (!scrambleTableinOuterQuery && checkScrambleTable(((SelectQueryOp) jointTable).getFromList())) {
                rootToReplace.add((SelectQueryOp) jointTable);
                jointTable = replaceAggregateSubquery(conn, syntax, (SelectQueryOp) jointTable);
              } else {
                // If aggregate, first recursive build the tree of the node, then replace the query in its original plan
                QueryExecutionNode child = makePlan(conn, syntax, (SelectQueryOp) jointTable);
                children.add(child);
                // rewrite the inner aggregate query
                jointTable = replaceAggregateSubquery(conn, syntax, (SelectQueryOp) jointTable);
              }
            }
            else { // If non-aggregate, recursive use replaceAggregateSubquery find all subquries
              result = recursiveReplaceSubquery(conn, syntax, (SelectQueryOp) jointTable);
              rootToReplace.addAll(result.getRight());
              children.addAll(result.getLeft());
            }
          }
          ((JoinTable) table).getJoinList().set(joinindex, jointTable);
        }
      }
      query.getFromList().set(index, table);
    }

    // Filter List
    UnnamedColumn where = null;
    if (query.getFilter().isPresent()) {
      where = query.getFilter().get();
    }
    if (where != null) {
      List<UnnamedColumn> filters = new ArrayList<>();
      filters.add(where);
      while (!filters.isEmpty()) {
        UnnamedColumn filter = filters.get(0);
        filters.remove(0);
        if (filter instanceof ColumnOp) {
          filters.addAll(((ColumnOp) filter).getOperands());
        } else if (filter instanceof SubqueryColumn) {
          SelectQueryOp subquery = ((SubqueryColumn) filter).getSubquery();
          if (subquery.isAggregateQuery()) {
            // use inner query as root
            if (!scrambleTableinOuterQuery && checkScrambleTable(subquery.getFromList())) {
              rootToReplace.add(subquery);
              ((SubqueryColumn) filter).setSubquery(replaceAggregateSubquery(conn, syntax, subquery));
            } else {
              // If aggregate, first recursive build the tree of the node, then replace the query in its original plan
              QueryExecutionNode child = makePlan(conn, syntax, subquery);
              children.add(child);
              // rewrite the inner aggregate query
              ((SubqueryColumn) filter).setSubquery(replaceAggregateSubquery(conn, syntax, subquery));
            }
          }
          else { // If non-aggregate, recursive use replaceAggregateSubquery find all subquries
            result = recursiveReplaceSubquery(conn, syntax, subquery);
            rootToReplace.addAll(result.getRight());
            children.addAll(result.getLeft());
          }
        }
      }
    }

    return new ImmutablePair<>(children, rootToReplace);
  }

  /**
   * Creates a tree in which each node is AggQueryExecutionNode. Each AggQueryExecutionNode corresponds to
   * an aggregate query, whether it is the main query or a subquery.
   * <p>
   * 1. Restrict the aggregate subqueries to appear only in the where clause.
   * 2. If an aggregate subquery appears in the where clause, the subquery itself should be a single
   * AggQueryExecutionNode even if it contains another aggregate subqueries within it.
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
  QueryExecutionNode makePlan(DbmsConnection conn, SyntaxAbstract syntax, SelectQueryOp query)
      throws VerdictDbException {

    // identify aggregate subqueries and create separate nodes for them
    Pair<List<QueryExecutionNode>, List<SelectQueryOp>> result = recursiveReplaceSubquery(conn, syntax, query);
    List<QueryExecutionNode> subqueryToReplace = result.getLeft();
    List<SelectQueryOp> rootToReplace = result.getRight();

    QueryExecutionNode rootNode;
    if (rootToReplace.isEmpty()) {
      rootNode = new AsyncAggExecutionNode(conn, scrambleMeta, query);
    } else rootNode = new PostProcessor(conn, rootToReplace);
    rootNode.children.addAll(subqueryToReplace);
    return rootNode;
  }

  public void execute(DbmsConnection conn) {
    // execute roots

    // after executions are all finished.
    cleanUp(conn, null);
  }

  // clean up any intermediate materialized tables
  void cleanUp(DbmsConnection conn, SyntaxAbstract syntax) {
    while (tempTableIndex>1) {
      tempTableIndex--;
      conn.executeUpdate(String.format("drop table if exists %sverdictdb_temp%s.%stmptable%s%s", syntax.getQuoteString()
      , syntax.getQuoteString(), syntax.getQuoteString(), tempTableIndex, syntax.getQuoteString()));
    }
  }

}

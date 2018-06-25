package org.verdictdb.core.execution.ola;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.execution.ExecutionResult;
import org.verdictdb.core.execution.QueryExecutionNode;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.ConstantColumn;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.exception.ValueException;

/**
 * Contains the references to the ExecutionNodes that contain scrambled tables. This does not include
 * the scrambled tables in sub aggregate queries.
 * 
 * @author Yongjoo Park
 *
 */
public class AggExecutionNodeBlock {
  
  DbmsConnection conn;
  
  QueryExecutionNode root;

  public AggExecutionNodeBlock(DbmsConnection conn, QueryExecutionNode root) {
    this.conn = conn;
    this.root = root;
  }
  
  public QueryExecutionNode getRoot() {
    return root;
  }

  /**
   * 
   * @param nodeBlock
   * @return Returns the root of the multiple aggregation nodes (each of which involves different combinations
   * of partitions)
   * @throws ValueException 
   */
  public QueryExecutionNode constructProgressiveAggNodes(ScrambleMeta scrambleMeta) throws ValueException {
    List<QueryExecutionNode> individualAggNodes = new ArrayList<>();
    List<QueryExecutionNode> combiners = new ArrayList<>();
    
    // first, plan how to perform block aggregation
    List<Pair<QueryExecutionNode, Pair<String, String>>> scrambledNodes = identifyNodesWithScrambles(root);
    List<Pair<String, String>> scrambles = new ArrayList<>();
    for (Pair<QueryExecutionNode, Pair<String, String>> a : scrambledNodes) {
      scrambles.add(a.getRight());
    }
    AggBlockMeta aggMeta = new AggBlockMeta(scrambleMeta, scrambles);
    
    // second, according to the plan, create individual nodes that perform aggregations.
    for (int i = 0; i < aggMeta.totalBlockAggCount(); i++) {
      QueryExecutionNode copy = deepcopyExcludingDependentAggregates(root);
      List<Pair<QueryExecutionNode, Pair<String, String>>> scrambledNodeCopies = identifyNodesWithScrambles(copy);
      
      // add extra predicates to restrain each aggregation to particular parts of base tables.
      for (Pair<QueryExecutionNode, Pair<String, String>> a : scrambledNodeCopies) {
        QueryExecutionNode scrambledNode = a.getLeft();
        String schemaName = a.getRight().getLeft();
        String tableName = a.getRight().getRight();
        Pair<Integer, Integer> span = aggMeta.getAggBlockSpanForTable(schemaName, tableName, i);
        String aggblockColumn = scrambleMeta.getAggregationBlockColumn(schemaName, tableName);
        SelectQuery q = scrambledNode.getQuery();
        String aliasName = findAliasFor(schemaName, tableName, q.getFromList());
        
        int left = span.getLeft();
        int right = span.getRight();
        if (left == right) {
          q.addFilterByAnd(ColumnOp.equal(new BaseColumn(aliasName, aggblockColumn), ConstantColumn.valueOf(left)));
        } else {
          q.addFilterByAnd(ColumnOp.greaterequal(
              new BaseColumn(aliasName, aggblockColumn),
              ConstantColumn.valueOf(left)));
          q.addFilterByAnd(ColumnOp.lessequal(
              new BaseColumn(aliasName, aggblockColumn),
              ConstantColumn.valueOf(right)));
        }
      }
      
      individualAggNodes.add(copy);
    }
    
    // third, stack combiners
    for (int i = 0; i < aggMeta.totalBlockAggCount()-1; i++) {
      AggCombinerExecutionNode combiner = AggCombinerExecutionNode.create(
          conn,
          individualAggNodes.get(0), 
          individualAggNodes.get(1));
      combiners.add(combiner);
    }
    
    // fourth, re-link the listening queue for the new AsyncAggNode
    QueryExecutionNode newRoot = AsyncAggExecutionNode.create(conn, individualAggNodes, combiners);
    List<BlockingDeque<ExecutionResult>> broadcastingQueue = root.getBroadcastQueues();
    for (BlockingDeque<ExecutionResult> queue : broadcastingQueue) {
      newRoot.addBroadcastingQueue(queue);
    }
    
    return newRoot;
  }
  
  String findAliasFor(String schemaName, String tableName, List<AbstractRelation> fromList) {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * 
   * @param root2
   * @return Pair of node and (schemaName, tableName) of the scrambled table. If there are multiple scrambled
   * tables in a single query, return the same node with different schemaName.tableName.
   */
  List<Pair<QueryExecutionNode, Pair<String, String>>> identifyNodesWithScrambles(QueryExecutionNode root2) {
    // TODO Auto-generated method stub
    return null;
  }

  public QueryExecutionNode deepcopyExcludingDependentAggregates(QueryExecutionNode root) {
    return null;
  }

}

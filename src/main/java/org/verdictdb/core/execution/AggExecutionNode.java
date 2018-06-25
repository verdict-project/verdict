package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.*;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.exception.VerdictDbException;

public class AggExecutionNode extends QueryExecutionNodeWithDependencies {
  
  String resultSchemaName;
  
  String resultTableName;

  protected AggExecutionNode(String scratchpadSchemaName) {
    super(scratchpadSchemaName);
  }
  
  public static AggExecutionNode create(SelectQuery query, String scratchpadSchemaName) {
    AggExecutionNode node = new AggExecutionNode(scratchpadSchemaName);
    convertSubqueriesIntoDependentNodes(query, node);
    return node;
  }
  
  public SelectQuery getQuery() {
    return (SelectQuery) query;
  }

//  /**
//   * Make this agg execution node perform progressive aggregation.
//   * 
//   * @param scrambleMeta
//   * @throws VerdictDbException 
//   */
//  public QueryExecutionNode toAsyncAgg(ScrambleMeta scrambleMeta) throws VerdictDbException {
//    QueryExecutionNode newNode = new AsyncAggExecutionNode(conn, scrambleMeta, resultSchemaName, resultTableName, query);
//    
//    // make that newNode runs only after the dependencies of this current node complete.
//    List<QueryExecutionNode> leaves = newNode.getLeafNodes();
//    for (QueryExecutionNode leaf : leaves) {
//      for (QueryExecutionNode dep : getDependents()) {
//        leaf.addDependency(dep);
//      }
//    }
//    return newNode;
//  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    // TODO Auto-generated method stub
    return super.executeNode(downstreamResults);
  }

//  void generateDependency() throws VerdictDbException {
//    
//  }

}

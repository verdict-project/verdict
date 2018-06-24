package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.exception.VerdictDbException;

public class AggExecutionNode extends CreateTableAsSelectExecutionNode {
  
  String resultSchemaName;
  
  String resultTableName;

  public AggExecutionNode(
      DbmsConnection conn,
      String resultSchemaName, 
      String resultTableName,
      SelectQuery query) {
    super(conn, resultSchemaName, resultTableName, query);
    try {
      generateDependency();
    } catch (VerdictDbException e){
      e.printStackTrace();
    }
  }

  /**
   * Make this agg execution node perform progressive aggregation.
   * 
   * @param scrambleMeta
   * @throws VerdictDbException 
   */
  public QueryExecutionNode toAsyncAgg(ScrambleMeta scrambleMeta) throws VerdictDbException {
    QueryExecutionNode newNode = new AsyncAggExecutionNode(conn, scrambleMeta, resultSchemaName, resultTableName, query);
    
    // make that newNode runs only after the dependencies of this current node complete.
    List<QueryExecutionNode> leaves = newNode.getLeafNodes();
    for (QueryExecutionNode leaf : leaves) {
      for (QueryExecutionNode dep : getDependents()) {
        leaf.addDependency(dep);
      }
    }
    return newNode;
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    // TODO Auto-generated method stub
    return super.executeNode(downstreamResults);
  }

}

package org.verdictdb.core.execution;

import java.util.List;
import java.util.concurrent.BlockingDeque;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.rewriter.query.AggblockMeta;

/**
 * Computes an aggregate and saves the results as a table
 * @author Yongjoo Park
 *
 */
public class SingleAggExecutionNode extends QueryExecutionNode {
  
  SelectQuery query;

  public SingleAggExecutionNode(
      DbmsConnection conn, 
      SelectQuery query, 
      AggblockMeta aggmeta, 
      String resultSchemaName,
      String resultTableName) {
    super(conn);
    this.query = query;
    
    // generate a subnode
    CreateTableAsSelectExecutionNode node = 
        new CreateTableAsSelectExecutionNode(conn, resultSchemaName, resultTableName, query);
    BlockingDeque<ExecutionResult> listeningQueue = generateListeningQueue();
    node.addBroadcastingQueue(listeningQueue);
    addDependent(node);
  }

  /**
   * @return Contains "schemaName" and "tableName" of the created table.
   */
  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    ExecutionResult result = downstreamResults.get(0);
    return result;
  }

}

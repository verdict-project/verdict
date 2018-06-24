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
  
  AggblockMeta aggmeta;

  public SingleAggExecutionNode(
      DbmsConnection conn, 
      AggblockMeta aggmeta,
      String resultSchemaName,
      String resultTableName,
      SelectQuery query) {
    super(conn, query);
    this.query = query;
    this.aggmeta = aggmeta;
    
    // generate a subnode
    CreateTableAsSelectExecutionNode node = 
        new CreateTableAsSelectExecutionNode(conn, resultSchemaName, resultTableName, query);
    BlockingDeque<ExecutionResult> listeningQueue = generateListeningQueue();
    node.addBroadcastingQueue(listeningQueue);
    addDependency(node);
  }

  /**
   * @return Contains "schemaName" and "tableName" of the created table.
   */
  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    ExecutionResult downstream = downstreamResults.get(0);
    ExecutionResult result = new ExecutionResult();
    result.setKeyValue("schemaName", downstream.getValue("schemaName"));
    result.setKeyValue("tableName", downstream.getValue("tableName"));
    result.setKeyValue("aggmeta", aggmeta);
    return result;
  }

}

package org.verdictdb.core.execution;

import java.util.Arrays;
import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.execution.ola.AggCombinerExecutionNode;
import org.verdictdb.core.execution.ola.AsyncAggExecutionNode;
import org.verdictdb.core.execution.ola.Dimension;
import org.verdictdb.core.execution.ola.HyperTableCube;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public class AggExecutionNode extends CreateTableAsSelectExecutionNode {

  protected AggExecutionNode(QueryExecutionPlan plan) {
    super(plan);
  }
  
  public static AggExecutionNode create(QueryExecutionPlan plan, SelectQuery query) throws VerdictDBValueException {
    AggExecutionNode node = new AggExecutionNode(plan);
    SubqueriesToDependentNodes.convertSubqueriesToDependentNodes(query, node);
    node.setSelectQuery(query);
    
    return node;
  }
  
  public SelectQuery getSelectQuery() {
    return selectQuery;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) 
      throws VerdictDBException {
    ExecutionInfoToken result = super.executeNode(conn, downstreamResults);

    // This node is one of the individual aggregate nodes inside an AsyncAggExecutionNode
    if (parents.get(0) instanceof AsyncAggScaleExecutionNode) {
      QueryExecutionNode asyncNode = parents.get(0);
      int index = -1;
      if (asyncNode.getParents().size()==2) {
        index = 0;
        asyncNode = asyncNode.getParents().get(1);
      }
      else {
        AsyncAggExecutionNode asyncRoot = asyncNode.getParents().get(0).getParents().size()==2?
            (AsyncAggExecutionNode) asyncNode.getParents().get(0).getParents().get(1):(AsyncAggExecutionNode) asyncNode.getParents().get(0).getParents().get(0);
        index = asyncRoot.getDependents().indexOf(asyncNode.getParents().get(0));
        asyncNode = asyncRoot;
      }
      // Assume only one scramble table in the query
      BaseTable scrambleTable = ((AsyncAggExecutionNode)asyncNode).getScrambleTables().get(0);
      Dimension dimension = new Dimension(scrambleTable.getSchemaName(), scrambleTable.getTableName(), index, index);
      result.setKeyValue("hyperTableCube", Arrays.asList(new HyperTableCube(Arrays.asList(dimension))));
    }
    return result;
  }

  @Override
  public QueryExecutionNode deepcopy() {
    AggExecutionNode node = new AggExecutionNode(plan);
    copyFields(this, node);
    return node;
  }
  
}

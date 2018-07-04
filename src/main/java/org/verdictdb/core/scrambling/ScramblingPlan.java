/*
 * Copyright 2018 University of Michigan
 *
 * You must contact Barzan Mozafari (mozafari@umich.edu) or Yongjoo Park (pyongjoo@umich.edu) to discuss
 * how you could use, modify, or distribute this code. By default, this code is not open-sourced and we do
 * not license this code.
 */

package org.verdictdb.core.scrambling;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutableNode;
import org.verdictdb.core.execution.ExecutablePlan;
import org.verdictdb.core.querying.ColumnMetadataRetrievalNode;
import org.verdictdb.core.querying.CreateTableAsSelectNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.IdCreator;
import org.verdictdb.core.querying.PartitionMetadataRetrievalNode;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;

/**
 * Execution plan for scrambling.
 * 
 * This plan consists of three nodes:
 * <ol>
 * <li>metadata retrieval node</li>
 * <li>statistics computation node</li>
 * <li>actual scramble table creation node</li>
 * </ol>
 * 
 * Those nodes should be provided by the ScramblingMethod instance
 * 
 * @author Yongjoo Park
 *
 */
public class ScramblingPlan extends SimpleTreePlan {
  
//  ScramblingMethod method;
  
  DbmsQueryResult statistics;
  
  private ScramblingPlan(ExecutableNodeBase root) {
    super(root);
  }
  
  /**
   * 
   * @param newSchemaName
   * @param newTableName
   * @param oldSchemaName
   * @param oldTableName
   * @param method
   * @param options Key-value map. It must contain the following keys:
   *                "blockColumnName", "tierColumnName", "blockCount" (optional)
   * @return
   */
  public static ScramblingPlan create(
      String newSchemaName, String newTableName,
      String oldSchemaName, String oldTableName,
      ScramblingMethod method,
      Map<String, String> options) {
    
    // create a node for step 1 - column meta data retrieval
    ExecutableNodeBase columnMetaDataNode = new ColumnMetadataRetrievalNode(oldSchemaName, oldTableName);
    ExecutableNodeBase partitionMetaDataNode = new PartitionMetadataRetrievalNode(oldSchemaName, oldTableName);
    
    // create a node for step 2 - statistics retrieval
    ExecutableNodeBase statRetreival = 
        StatisticsRetrievalNode.create(method.getStatisticsQueryGenerator(), oldSchemaName, oldTableName);
    statRetreival.subscribeTo(columnMetaDataNode, 0);
    statRetreival.subscribeTo(partitionMetaDataNode, 1);
    
    // create a node for step 3 - scrambling
    ExecutableNodeBase scramblingNode = 
        ScramblingNode.create(newSchemaName, newTableName, oldSchemaName, oldTableName, method, options);
    scramblingNode.subscribeTo(statRetreival, 0);
    
    ScramblingPlan scramblingPlan = new ScramblingPlan(scramblingNode);
    return scramblingPlan;
  }

}

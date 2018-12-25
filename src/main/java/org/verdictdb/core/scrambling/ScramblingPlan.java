/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.scrambling;

import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.UnnamedColumn;

import java.util.List;
import java.util.Map;

/**
 * Execution plan for scrambling.
 *
 * <p>This plan consists of three nodes:
 *
 * <ol>
 *   <li>metadata retrieval node
 *   <li>statistics computation node
 *   <li>actual scramble table creation node
 * </ol>
 *
 * Those nodes should be provided by the ScramblingMethod instance
 *
 * @author Yongjoo Park
 */
public class ScramblingPlan extends SimpleTreePlan {

  //  ScramblingMethod method;

  //  DbmsQueryResult statistics;

  private ScramblingPlan(ExecutableNodeBase root) {
    super(root);
  }

  public static final String COLUMN_METADATA_KEY = "scramblingPlan:columnMetaData";

  static final String PARTITION_METADATA_KEY = "scramblingPlan:partitionMetaData";

  static final String PRIMARYKEY_METADATA_KEY = "scramblingPlan:primarykeyMetaData";

  public static ScramblingPlan create(
      String newSchemaName,
      String newTableName,
      String oldSchemaName,
      String oldTableName,
      ScramblingMethod method,
      Map<String, String> options) {
    return create(newSchemaName, newTableName, oldSchemaName, oldTableName, method, null, options);
  }

  /**
   * Limitations: <br>
   * Currently, this class only works for the databases that support "CREATE TABLE ... PARTITION BY
   * () SELECT". Also, this class does not inherit all properties of the original tables.
   *
   * @param newSchemaName
   * @param newTableName
   * @param oldSchemaName
   * @param oldTableName
   * @param method
   * @param options Key-value map. It must contain the following keys: "blockColumnName",
   *     "tierColumnName", "blockCount" (optional)
   * @return
   */
  public static ScramblingPlan create(
      String newSchemaName,
      String newTableName,
      String oldSchemaName,
      String oldTableName,
      ScramblingMethod method,
      UnnamedColumn predicate,
      Map<String, String> options) {

    // create a node for step 1 - column meta data retrieval
    // these nodes will set "scramblingPlan:columnMetaData" and "scramblingPlan:partitionMetaData"
    // keys.
    ExecutableNodeBase columnMetaDataNode =
        ColumnMetadataRetrievalNode.create(oldSchemaName, oldTableName, COLUMN_METADATA_KEY);
    ExecutableNodeBase partitionMetaDataNode =
        PartitionMetadataRetrievalNode.create(oldSchemaName, oldTableName, PARTITION_METADATA_KEY);
    ExecutableNodeBase primaryKeyMetaDataNode =
        PrimaryKeyMetaDataRetrievalNode.create(
            oldSchemaName, oldTableName, PRIMARYKEY_METADATA_KEY);

    // create a node for step 2 - statistics retrieval
    // since uniform scrambling does not return any nodes, the step 3 will be run immediately.
    List<ExecutableNodeBase> statsNodes =
        method.getStatisticsNode(
            oldSchemaName,
            oldTableName,
            COLUMN_METADATA_KEY,
            PARTITION_METADATA_KEY,
            PRIMARYKEY_METADATA_KEY);
    for (ExecutableNodeBase n : statsNodes) {
      n.subscribeTo(columnMetaDataNode, 100);
      n.subscribeTo(partitionMetaDataNode, 101);
      n.subscribeTo(primaryKeyMetaDataNode, 102);
    }

    // create a node for step 3 - scrambling
    ExecutableNodeBase scramblingNode =
        ScramblingNode.create(
            newSchemaName, newTableName, oldSchemaName, oldTableName, method, predicate, options);

    scramblingNode.subscribeTo(columnMetaDataNode, 100); // for total table size
    scramblingNode.subscribeTo(primaryKeyMetaDataNode, 101);
    for (int i = 0; i < statsNodes.size(); i++) {
      scramblingNode.subscribeTo(statsNodes.get(i), i);
    }

    ScramblingPlan scramblingPlan = new ScramblingPlan(scramblingNode);
    return scramblingPlan;
  }
}

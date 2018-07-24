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

package org.verdictdb.core.querying;

import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;

public class QueryExecutionPlanFactory {

  /**
   * Creates a node tree and return it as an instance of QueryExecutionPlan.
   *
   * @param query
   * @return
   */
  public static QueryExecutionPlan create(
      String scratchpadSchemaName) {
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan();
    queryExecutionPlan.idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(
      String scratchpadSchemaName,
      ScrambleMetaSet scrambleMeta) {
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan();
    queryExecutionPlan.idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    queryExecutionPlan.scrambleMeta = scrambleMeta;
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(
      String scratchpadSchemaName,
      ScrambleMetaSet scrambleMeta,
      SelectQuery query) throws VerdictDBException {
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan();
    queryExecutionPlan.idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    queryExecutionPlan.scrambleMeta = scrambleMeta;
    queryExecutionPlan.root = SelectAllExecutionNode.create(queryExecutionPlan.idCreator, query);
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(
      String scratchpadSchemaName,
      ExecutableNodeBase root) {
    QueryExecutionPlan queryExecutionPlan = create(scratchpadSchemaName);
    queryExecutionPlan.root = root;
    return queryExecutionPlan;
  }

  static ExecutableNodeBase createRootAndItsDependents(SelectQuery query) {

    // identify the query type and calls an appropriate function defined below.

    return null;
  }

  static SelectAllExecutionNode createSelectAllExecutionNodeAndItsDependents(SelectQuery query) {
    // move an existing static create() factory method here.
    return null;
  }

  // create more functions like this.

}

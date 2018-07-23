package org.verdictdb.core.querying;

import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;

public class QueryExecutionPlanFactory {
  
  /**
   * Creates a node tree and return it as an instance of QueryExecutionPlan.
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

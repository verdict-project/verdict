package org.verdictdb.core.querying;

import java.util.HashMap;
import java.util.List;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

public class ProjectionNode extends CreateTableAsSelectNode {

  // Whether the node has gone through the check of multi tier rewrite
  Boolean AsyncPlanMultiTierRewriteCheck = false;

  HashMap<ScrambleMeta, List<String>> scrambleTableTierColumnAlias = new HashMap<>();
  
  public ProjectionNode(IdCreator namer, SelectQuery query) {
    super(namer, query);
  }

  public static ProjectionNode create(IdCreator namer, SelectQuery query) {
    ProjectionNode node = new ProjectionNode(namer, null);
    SubqueriesToDependentNodes.convertSubqueriesToDependentNodes(query, node);
    node.setSelectQuery(query);
    return node;
  }

  public HashMap<ScrambleMeta, List<String>> getScrambleTableTierColumnAlias() {
    return scrambleTableTierColumnAlias;
  }

  public Boolean getAsyncPlanMultiTierRewriteCheck() {
    return AsyncPlanMultiTierRewriteCheck;
  }

  public void haveAsyncPlanMultiTierRewriteCheck() {
    AsyncPlanMultiTierRewriteCheck = true;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return super.createQuery(tokens);
  }
  
  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return super.createToken(result);
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    ProjectionNode node = new ProjectionNode(namer, selectQuery);
    copyFields(this, node);
    return node;
  }
}

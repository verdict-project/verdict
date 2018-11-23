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

import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;

/**
 * The aliases for aggregate columns are identified and set in 
 * the AsyncQueryExecutionPlan.createUnfoldSelectlistWithBasicAgg().
 *
 */
public class AggExecutionNode extends CreateTableAsSelectNode {

  private static final long serialVersionUID = 6222493718874657695L;

  // List<HyperTableCube> cubes = new ArrayList<>();

  protected AggExecutionNode(IdCreator namer, SelectQuery query) {
    super(namer, query);
//    // stores the alias names for aggregate columns
//    List<SelectItem> selectItems = query.getSelectList();
//    for (SelectItem item : selectItems) {
//      if (item instanceof AliasedColumn) {
//        String alias = ((AliasedColumn) item).getAliasName();
//        UnnamedColumn col = ((AliasedColumn) item).getColumn(); 
//        if (col instanceof ColumnOp) {
//          if (((ColumnOp) col).isMinAggregate()) {
//            aggMeta.addMinAlias(alias);
//          } else if (((ColumnOp) col).isMaxAggregate()) {
//            aggMeta.addMaxAlias(alias);
//          } else if (col.isAggregateColumn()) {
//            aggMeta.addAggAlias(alias);
//          }
//        }
//      } else {
//        // this is not expected to happen.
//      }
//    }
  }

  public static AggExecutionNode create(IdCreator namer, SelectQuery query) {
    AggExecutionNode node = new AggExecutionNode(namer, null);
    SubqueriesToDependentNodes.convertSubqueriesToDependentNodes(query, node);
    node.setSelectQuery(query);

    return node;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return super.createQuery(tokens);
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = super.createToken(result);
    token.setKeyValue("aggMeta", aggMeta);
    token.setKeyValue("dependentQuery", this.selectQuery);
    return token;
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    AggExecutionNode node = new AggExecutionNode(namer, selectQuery);
    copyFields(this, node);
    selectQuery = selectQuery.selectListDeepCopy();
    return node;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .appendSuper(super.toString())
        .append("aggmeta", aggMeta)
        .build();
  }
}

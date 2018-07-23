/*
 *    Copyright 2017 University of Michigan
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

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

import java.util.List;

public class StatisticsRetrievalNode extends QueryNodeBase {

  private static final long serialVersionUID = -2135920046094297794L;

  String schemaName;

  String tableName;

  StatiticsQueryGenerator queryGenerator;

  public StatisticsRetrievalNode(String schemaName, String tableName) {
    super(null);
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  public static StatisticsRetrievalNode create(
      StatiticsQueryGenerator queryGenerator, String schemaName, String tableName) {
    StatisticsRetrievalNode node = new StatisticsRetrievalNode(schemaName, tableName);
    node.queryGenerator = queryGenerator;
    return node;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    @SuppressWarnings("unchecked")
    List<Pair<String, String>> columnNamesAndTypes =
        (List<Pair<String, String>>) tokens.get(0).getValue("columnMeta");
    selectQuery = queryGenerator.create(schemaName, tableName, columnNamesAndTypes, null);
    return selectQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("queryResult", result);
    return token;
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    QueryNodeBase node = new StatisticsRetrievalNode(schemaName, tableName);
    copyFields(this, node);
    return node;
  }

  protected void copyFields(StatisticsRetrievalNode from, StatisticsRetrievalNode to) {
    super.copyFields(from, to);
    to.queryGenerator = from.queryGenerator;
  }
}

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

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class UniformScramblingMethod extends ScramblingMethodBase {

  private final String MAIN_TABLE_SOURCE_ALIAS = "t";

  private int totalNumberOfblocks = -1;

  public UniformScramblingMethod(long blockSize) {
    super(blockSize);
  }

  @Override
  public List<ExecutableNodeBase> getStatisticsNode(
      String oldSchemaName,
      String oldTableName,
      String columnMetaTokenKey,
      String partitionMetaTokenKey) {
    TableSizeCountNode countNode = new TableSizeCountNode(oldSchemaName, oldTableName);
    return Arrays.<ExecutableNodeBase>asList(countNode);
  }

  @Override
  public List<UnnamedColumn> getTierExpressions(Map<String, Object> metaData) {
    return Arrays.asList();
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTier(
      Map<String, Object> metaData, int tier) {

    DbmsQueryResult tableSizeResult =
        (DbmsQueryResult) metaData.get(TableSizeCountNode.class.getSimpleName());
    tableSizeResult.next();
    long tableSize = tableSizeResult.getLong(TableSizeCountNode.TOTAL_COUNT_ALIAS_NAME);
    totalNumberOfblocks = (int) Math.ceil(tableSize / (float) blockSize);

    List<Double> prob = new ArrayList<>();
    for (int i = 0; i < totalNumberOfblocks; i++) {
      prob.add((i + 1) / (double) totalNumberOfblocks);
    }

    storeCumulativeProbabilityDistribution(tier, prob);

    return prob;
  }

  @Override
  public AbstractRelation getScramblingSource(
      String originalSchema, String originalTable, Map<String, Object> metaData) {
    String tableSourceAlias = MAIN_TABLE_SOURCE_ALIAS;
    return new BaseTable(originalSchema, originalTable, tableSourceAlias);
  }

  @Override
  public String getMainTableAlias() {
    return MAIN_TABLE_SOURCE_ALIAS;
  }

  @Override
  public int getBlockCount() {
    return totalNumberOfblocks;
  }

  @Override
  public int getTierCount() {
    return 1;
  }
}

class TableSizeCountNode extends QueryNodeBase {

  private static final long serialVersionUID = 4363953197389542868L;

  private String schemaName;

  private String tableName;

  public static final String TOTAL_COUNT_ALIAS_NAME = "verdictdbtotalcount";

  public TableSizeCountNode(String schemaName, String tableName) {
    super(null);
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    if (tokens.size() == 0) {
      // no token information passed
      throw new VerdictDBValueException("No token is passed.");
    }

    String tableSourceAlias = "t";

    // compose a select list
    List<SelectItem> selectList = new ArrayList<>();
    selectList.add(new AliasedColumn(ColumnOp.count(), TOTAL_COUNT_ALIAS_NAME));

    selectQuery =
        SelectQuery.create(selectList, new BaseTable(schemaName, tableName, tableSourceAlias));
    return selectQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue(this.getClass().getSimpleName(), result);
    return token;
  }
}

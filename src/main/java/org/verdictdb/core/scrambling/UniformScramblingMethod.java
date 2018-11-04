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

import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class UniformScramblingMethod extends ScramblingMethodBase {

  private final String MAIN_TABLE_SOURCE_ALIAS = "t";

  private int totalNumberOfblocks = -1;

  private int actualNumberOfBlocks = -1;

  private static final long EFFECTIVE_TABLE_SIZE_THRESHOLD = 100000;

  public UniformScramblingMethod(long blockSize, int maxBlockCount, double relativeSize) {
    super(blockSize, maxBlockCount, relativeSize);
  }

  public UniformScramblingMethod(long blockSize, int maxBlockCount) {
    super(blockSize, maxBlockCount, 1.0);
  }

  public UniformScramblingMethod(long blockSize) {
    super(blockSize, 100, 1.0);
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

  private List<Double> calculateBlockCountsAndCumulativeProbabilityDistForTier(
      Map<String, Object> metaData, int tier) {

    DbmsQueryResult tableSizeResult =
        (DbmsQueryResult) metaData.get(TableSizeCountNode.class.getSimpleName());
    tableSizeResult.next();
    long tableSize = tableSizeResult.getLong(TableSizeCountNode.TOTAL_COUNT_ALIAS_NAME);
    long effectiveRowCount =
        (relativeSize < 1) ? (long) Math.ceil(tableSize * relativeSize) : tableSize;
    if (relativeSize < 1 && effectiveRowCount < EFFECTIVE_TABLE_SIZE_THRESHOLD) {
      VerdictDBLogger.getLogger(UniformScramblingMethod.class)
          .warn(
              String.format(
                  "The reduced scramble table will have %d rows, "
                      + "which may be too small for accurate approximation",
                  effectiveRowCount));
    }
    actualNumberOfBlocks =
        (int) Math.min(maxBlockCount, Math.ceil(effectiveRowCount / (double) blockSize));

    // This guards the case when table is empty.
    if (actualNumberOfBlocks == 0) actualNumberOfBlocks = 1;

    blockSize = (long) Math.ceil(effectiveRowCount / (double) actualNumberOfBlocks);

    if (blockSize == 0) blockSize = 1; // just a sanity check
    totalNumberOfblocks = (int) Math.ceil(tableSize / (double) blockSize);

    List<Double> prob = new ArrayList<>();
    for (int i = 0; i < actualNumberOfBlocks; i++) {
      prob.add((i + 1) / (double) totalNumberOfblocks);
    }

    storeCumulativeProbabilityDistribution(tier, prob);

    return prob;
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTier(
      Map<String, Object> metaData, int tier) {

    return calculateBlockCountsAndCumulativeProbabilityDistForTier(metaData, tier);
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
  public UnnamedColumn getBlockExprForTier(int tier, Map<String, Object> metaData) {

    calculateBlockCountsAndCumulativeProbabilityDistForTier(metaData, tier);

    UnnamedColumn blockForTierExpr =
        ColumnOp.cast(
            ColumnOp.floor(
                ColumnOp.multiply(ColumnOp.rand(), ConstantColumn.valueOf(totalNumberOfblocks))),
            ConstantColumn.valueOf("int"));

    return blockForTierExpr;
  }

  @Override
  public int getBlockCount() {
    return totalNumberOfblocks;
  }

  @Override
  public int getActualBlockCount() {
    return actualNumberOfBlocks;
  }

  @Override
  public int getTierCount() {
    return 1;
  }

  @Override
  public double getRelativeSize() {
    return relativeSize;
  }
}

class TableSizeCountNode extends QueryNodeBase {

  private static final long serialVersionUID = 4363953197389542868L;

  private String schemaName;

  private String tableName;

  public static final String TOTAL_COUNT_ALIAS_NAME = "verdictdbtotalcount";

  public TableSizeCountNode(String schemaName, String tableName) {
    super(-1, null);
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

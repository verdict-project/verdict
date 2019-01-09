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
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class UniformScramblingMethod extends ScramblingMethodBase {

  protected String type = "uniform";

  private static final long serialVersionUID = 6191110854831260985L;

  private final String MAIN_TABLE_SOURCE_ALIAS = "t";

  private int totalNumberOfblocks = -1;

  private int actualNumberOfBlocks = -1;

  private static final long EFFECTIVE_TABLE_SIZE_THRESHOLD = 100000;

  public UniformScramblingMethod() {
    super(0, 0, 0);
  }

  public UniformScramblingMethod(long blockSize, int maxBlockCount, double relativeSize) {
    super(blockSize, maxBlockCount, relativeSize);
  }

  public UniformScramblingMethod(long blockSize, int maxBlockCount) {
    super(blockSize, maxBlockCount, 1.0);
  }

  public UniformScramblingMethod(long blockSize) {
    super(blockSize, 100, 1.0);
  }

  public UniformScramblingMethod(Map<Integer, List<Double>> probDist) {
    super(probDist);
  }

  @Override
  public List<ExecutableNodeBase> getStatisticsNode(
      String oldSchemaName,
      String oldTableName,
      String columnMetaTokenKey,
      String partitionMetaTokenKey,
      String primarykeyMetaTokenKey) {
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
    // note that this tableSize does not account for filter condition (WHERE clause)
    long tableSize = tableSizeResult.getLong(TableSizeCountNode.TOTAL_COUNT_ALIAS_NAME);
    long effectiveRowCount =
        (relativeSize < 1) ? (long) Math.ceil(tableSize * relativeSize) : tableSize;
    if (relativeSize < 1 && effectiveRowCount < EFFECTIVE_TABLE_SIZE_THRESHOLD) {
      VerdictDBLogger.getLogger(UniformScramblingMethod.class)
          .warn(
              String.format(
                  "The reduced scramble table will have at most %d rows, "
                      + "which may be too small for accurate approximation",
                  effectiveRowCount));
    }

    List<Double> prob = new ArrayList<>();

    // if actualNumberOfBlocks and totalNumberOfBlocks have already been calculated
    // (i.e., scramble already exists and we are appending),
    // then we only use those existing blocks without creating new ones.
    if (actualNumberOfBlocks < 1 && totalNumberOfblocks < 1) {

      if (!storedProbDist.containsKey(tier)) {
        actualNumberOfBlocks =
            (int) Math.min(maxBlockCount, Math.ceil(effectiveRowCount / (double) blockSize));

        // This guards the case when table is empty.
        if (actualNumberOfBlocks == 0) actualNumberOfBlocks = 1;

        long blockSizeToUse = (long) Math.ceil(effectiveRowCount / (double) actualNumberOfBlocks);

        if (blockSizeToUse == 0) blockSizeToUse = 1; // just a sanity check

        // including the ones that will be thrown away due to relative size < 1.0
        totalNumberOfblocks = (int) Math.ceil(tableSize / (double) blockSizeToUse);

        for (int i = 0; i < actualNumberOfBlocks; i++) {
          prob.add((i + 1) / (double) totalNumberOfblocks);
        }

        storeCumulativeProbabilityDistribution(tier, prob);
      } else {
        // if stored prob. dist. exists, we calculate actualNumberOfBlocks and totalNumberOfBlocks
        // based on prob. dist.
        prob = storedProbDist.get(tier);
        actualNumberOfBlocks = prob.size();
        if (prob.get(actualNumberOfBlocks - 1) == 1.0) {
          totalNumberOfblocks = actualNumberOfBlocks;
        } else {
          double increment = prob.get(0);
          totalNumberOfblocks = (int) Math.floor(1.0 / increment);
        }
      }
    } else {
      prob = storedProbDist.get(tier);
    }

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

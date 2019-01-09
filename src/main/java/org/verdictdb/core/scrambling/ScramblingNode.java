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

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.IdCreator;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * The last stage of scramling process: creates a new table based on some statistics.
 *
 * @author Yongjoo Park
 */
public class ScramblingNode extends CreateScrambledTableNode {

  private static final long serialVersionUID = 3921018031181756963L;

  //  Map<String, String> options;

  public ScramblingNode(
      IdCreator namer,
      String originalSchemaName,
      String originalTableName,
      ScramblingMethod method,
      String tierColumnName,
      String blockColumnName,
      UnnamedColumn predicate,
      List<String> existingPartitionColumns,
      boolean createIfNotExists) {

    super(
        namer,
        null,
        originalSchemaName,
        originalTableName,
        method,
        tierColumnName,
        blockColumnName,
        predicate,
        existingPartitionColumns,
        createIfNotExists);
  }

  /**
   * @param newSchemaName
   * @param newTableName
   * @param oldSchemaName
   * @param oldTableName
   * @param method
   * @param options Key-value map. It must contain the following keys: "blockColumnName",
   *     "tierColumnName"
   * @return
   */
  public static ScramblingNode create(
      final String newSchemaName,
      final String newTableName,
      String oldSchemaName,
      String oldTableName,
      ScramblingMethod method,
      Map<String, String> options) {
    return create(newSchemaName, newTableName, oldSchemaName, oldTableName, method, null, options);
  }

  public static ScramblingNode create(
      final String newSchemaName,
      final String newTableName,
      String oldSchemaName,
      String oldTableName,
      ScramblingMethod method,
      UnnamedColumn predicate,
      Map<String, String> options) {

    IdCreator idCreator =
        new IdCreator() {
          @Override
          public String generateAliasName() {
            return null; // we don't need this method
          }

          @Override
          public String generateAliasName(String keyword) {
            return null; // we don't need this method
          }

          @Override
          public int generateSerialNumber() {
            return 0;
          }

          @Override
          public Pair<String, String> generateTempTableName() {
            return Pair.of(newSchemaName, newTableName);
          }
        };

    String tierColumnName = options.get("tierColumnName");
    String blockColumnName = options.get("blockColumnName");
    String createIfNotExistsStr = options.get("createIfNotExists");
    String existingPartitionColumnStr = options.get("existingPartitionColumns");
    List<String> existingPartitionColumns =
        (existingPartitionColumnStr == null || existingPartitionColumnStr.isEmpty())
            ? new ArrayList<String>()
            : Arrays.asList(existingPartitionColumnStr.split(","));
    boolean createIfNotExists = false;
    if (createIfNotExistsStr != null && createIfNotExistsStr.equals("true")) {
      createIfNotExists = true;
    }
    return new ScramblingNode(
        idCreator,
        oldSchemaName,
        oldTableName,
        method,
        tierColumnName,
        blockColumnName,
        predicate,
        existingPartitionColumns,
        createIfNotExists);
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    Map<String, Object> metaData = new HashMap<>();
    for (ExecutionInfoToken token : tokens) {
      for (Entry<String, Object> keyValue : token.entrySet()) {
        String key = keyValue.getKey();
        Object value = keyValue.getValue();
        metaData.put(key, value);
      }
    }
    selectQuery = composeQuery(metaData);

    // add partitioning for block agg column
    addPartitionColumn(blockColumnName);

    return super.createQuery(tokens);
  }

  SelectQuery composeQuery(Map<String, Object> metaData) {
    // read option values
    List<UnnamedColumn> tierPredicates = method.getTierExpressions(metaData);
    int tierCount = tierPredicates.size() + 1;

    // partition columns should be added just before block column
    List<BaseColumn> partitionColumnsToAdd = new ArrayList<>();

    // composed select item expressions will be added to this list
    List<SelectItem> selectItems = new ArrayList<>();
    @SuppressWarnings("unchecked")
    List<Pair<String, String>> columnNamesAndTypes =
        (List<Pair<String, String>>) metaData.get(ScramblingPlan.COLUMN_METADATA_KEY);
    final String mainTableAlias = method.getMainTableAlias();
    for (Pair<String, String> nameAndType : columnNamesAndTypes) {
      String name = nameAndType.getLeft();
      if (partitionColumns.contains(name)) {
        partitionColumnsToAdd.add(new BaseColumn(mainTableAlias, name));
      } else {
        selectItems.add(new BaseColumn(mainTableAlias, name));
      }
    }

    // compose tier expression
    List<UnnamedColumn> tierOperands = new ArrayList<>();
    UnnamedColumn tierExpr = null;
    if (tierPredicates.size() == 0) {
      tierExpr = ConstantColumn.valueOf(0);
    } else if (tierPredicates.size() > 0) {
      for (int i = 0; i < tierPredicates.size(); i++) {
        UnnamedColumn pred = tierPredicates.get(i);
        tierOperands.add(pred);
        tierOperands.add(ConstantColumn.valueOf(i));
      }
      tierOperands.add(ConstantColumn.valueOf(tierPredicates.size()));
      tierExpr = ColumnOp.casewhen(tierOperands);
    }
    selectItems.add(new AliasedColumn(tierExpr, tierColumnName));

    // add existing partition columns at the end
    selectItems.addAll(partitionColumnsToAdd);

    // compose block expression
    UnnamedColumn blockExpr = null;
    List<UnnamedColumn> blockOperands = new ArrayList<>();

    for (int i = 0; i < tierCount; i++) {
      UnnamedColumn blockForTierExpr = method.getBlockExprForTier(i, metaData);
      /*
      List<Double> cumulProb = method.getCumulativeProbabilityDistributionForTier(metaData, i);
      List<Double> condProb = computeConditionalProbabilityDistribution(cumulProb);
      int blockCount = cumulProb.size();

      List<UnnamedColumn> blockForTierOperands = new ArrayList<>();
      for (int j = 0; j < blockCount; j++) {
        blockForTierOperands.add(
            ColumnOp.lessequal(ColumnOp.rand(), ConstantColumn.valueOf(condProb.get(j))));
        blockForTierOperands.add(ConstantColumn.valueOf(j));
      }
      UnnamedColumn blockForTierExpr;

      if (blockForTierOperands.size() <= 1) {
        blockForTierExpr = ConstantColumn.valueOf(0);
      } else {
        blockForTierExpr = ColumnOp.casewhen(blockForTierOperands);
      }
      */
      if (i < tierCount - 1) {
        // "when" part in the case-when-else expression
        // for the last tier, we don't need this "when" part
        blockOperands.add(ColumnOp.equal(tierExpr, ConstantColumn.valueOf(i)));
      }
      blockOperands.add(blockForTierExpr);
    }

    // use a simple (non-nested) case expression when there is only a single tier
    if (tierCount == 1) {
      blockExpr = blockOperands.get(0);
    } else {
      blockExpr = ColumnOp.casewhen(blockOperands);
    }

    selectItems.add(new AliasedColumn(blockExpr, blockColumnName));

    // compose the final query
    AbstractRelation tableSource =
        method.getScramblingSource(originalSchemaName, originalTableName, metaData);
    //    SelectQuery scramblingQuery =
    //        SelectQuery.create(
    //            selectItems,
    //            new BaseTable(oldSchemaName, oldTableName, MAIN_TABLE_SOURCE_ALIAS_NAME));
    SelectQuery scramblingQuery = SelectQuery.create(selectItems, tableSource, predicate);

    return scramblingQuery;
  }

  /**
   * To use a series of rand() in a case clause, we instead need this conditional probability.
   *
   * @param cumulativeProbabilityDistribution
   * @return
   */
  static List<Double> computeConditionalProbabilityDistribution(
      List<Double> cumulativeProbabilityDistribution) {
    List<Double> cond = new ArrayList<>();
    int length = cumulativeProbabilityDistribution.size();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        cond.add(cumulativeProbabilityDistribution.get(i));
      } else {
        double numerator =
            cumulativeProbabilityDistribution.get(i) - cumulativeProbabilityDistribution.get(i - 1);
        double denominator = 1.0 - cumulativeProbabilityDistribution.get(i - 1);
        double condProb = 0;
        if (denominator != 0) {
          condProb = numerator / denominator;
        }
        cond.add(condProb);
      }
    }
    return cond;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return super.createToken(result);
  }
}

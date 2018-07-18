package org.verdictdb.core.scrambling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

/**
 * The last stage of scramling process: creates a new table based on some statistics.
 * 
 * @author Yongjoo Park
 *
 */
public class ScramblingNode extends CreateScrambledTableNode {

  private static final long serialVersionUID = 3921018031181756963L;

  //  Map<String, String> options;

  public ScramblingNode(
      IdCreator namer, 
      String originalSchemaName, String originalTableName,
      ScramblingMethod method, String tierColumnName, String blockColumnName) {

    super(namer, null, originalSchemaName, originalTableName, method, tierColumnName, blockColumnName);
  }

  /**
   * 
   * @param newSchemaName
   * @param newTableName
   * @param oldSchemaName
   * @param oldTableName
   * @param method
   * @param options Key-value map. It must contain the following keys:
   *                "blockColumnName", "tierColumnName"
   * @return
   */
  public static ScramblingNode create(
      final String newSchemaName, final String newTableName,
      String oldSchemaName, String oldTableName,
      ScramblingMethod method, Map<String, String> options) {

    IdCreator idCreator = new IdCreator() {
      @Override
      public String generateAliasName() {
        return null;    // we don't need this method
      }
      @Override
      public Pair<String, String> generateTempTableName() {
        return Pair.of(newSchemaName, newTableName);
      }
    };
    
    String tierColumnName = options.get("tierColumnName");
    String blockColumnName = options.get("blockColumnName");
    return new ScramblingNode(idCreator, oldSchemaName, oldTableName, method, tierColumnName, blockColumnName);
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
    
    // composed select item expressions will be added to this list
    List<SelectItem> selectItems = new ArrayList<>();
    @SuppressWarnings("unchecked")
    List<Pair<String, String>> columnNamesAndTypes = 
    (List<Pair<String, String>>) metaData.get(ScramblingPlan.COLUMN_METADATA_KEY);
    final String mainTableAlias = method.getMainTableAlias();
    for (Pair<String, String> nameAndType : columnNamesAndTypes) {
      String name = nameAndType.getLeft();
      selectItems.add(new BaseColumn(mainTableAlias, name));
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

    // compose block expression
    UnnamedColumn blockExpr = null;
    List<UnnamedColumn> blockOperands = new ArrayList<>();
    for (int i = 0; i < tierCount; i++) {
      List<Double> cumulProb = method.getCumulativeProbabilityDistributionForTier(metaData, i);
      List<Double> condProb = computeConditionalProbabilityDistribution(cumulProb);
      int blockCount = cumulProb.size();

      List<UnnamedColumn> blockForTierOperands = new ArrayList<>();
      for (int j = 0; j < blockCount; j++) {
        blockForTierOperands.add(ColumnOp.lessequal(ColumnOp.rand(), ConstantColumn.valueOf(condProb.get(j))));
        blockForTierOperands.add(ConstantColumn.valueOf(j));
      }
      UnnamedColumn blockForTierExpr;;
      if (blockForTierOperands.size() <= 1) {
        blockForTierExpr = ConstantColumn.valueOf(0);
      } else {
        blockForTierExpr = ColumnOp.casewhen(blockForTierOperands);
      }

      if (i < tierCount-1) {
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
    AbstractRelation tableSource = method.getScramblingSource(originalSchemaName, originalTableName, metaData);
    //    SelectQuery scramblingQuery = 
    //        SelectQuery.create(
    //            selectItems, 
    //            new BaseTable(oldSchemaName, oldTableName, MAIN_TABLE_SOURCE_ALIAS_NAME));
    SelectQuery scramblingQuery = 
        SelectQuery.create(
            selectItems, 
            tableSource);

    return scramblingQuery;
  }

  /**
   * To use a series of rand() in a case clause, we instead need this conditional probability.
   * 
   * @param cumulativeProbabilityDistribution
   * @return
   */
  List<Double> computeConditionalProbabilityDistribution(List<Double> cumulativeProbabilityDistribution) {
    List<Double> cond = new ArrayList<>();
    int length = cumulativeProbabilityDistribution.size();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        cond.add(cumulativeProbabilityDistribution.get(i));
      } else {
        double numerator = cumulativeProbabilityDistribution.get(i) - cumulativeProbabilityDistribution.get(i-1);
        double denominator = 1.0 - cumulativeProbabilityDistribution.get(i-1);
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

package org.verdictdb.core.scrambling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.querying.CreateTableAsSelectNode;
import org.verdictdb.core.querying.IdCreator;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
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
public class ScramblingNode extends CreateTableAsSelectNode {

  String oldSchemaName;

  String oldTableName;

  ScramblingMethod method;

  Map<String, String> options;

  public ScramblingNode(
      IdCreator namer, 
      String oldSchemaName,
      String oldTableName,
      ScramblingMethod method, 
      Map<String, String> options) {
    super(namer, null);
    this.oldSchemaName = oldSchemaName;
    this.oldTableName = oldTableName;
    this.method = method;
    this.options = options;
  }

  /**
   * 
   * @param newSchemaName
   * @param newTableName
   * @param oldSchemaName
   * @param oldTableName
   * @param method
   * @param options Key-value map. It must contain the following keys:
   *                "blockColumnName", "tierColumnName", "blockCount" (optional)
   * @return
   */
  public static ScramblingNode create(
      final String newSchemaName, 
      final String newTableName,
      String oldSchemaName,
      String oldTableName,
      ScramblingMethod method,
      Map<String, String> options) {

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

    return new ScramblingNode(idCreator, oldSchemaName, oldTableName, method, options);
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    DbmsQueryResult statistics = null;
    if (tokens.size() > 0) {
      statistics = (DbmsQueryResult) tokens.get(0).getValue("queryResult");
    }
    selectQuery = composeQuery(statistics);
    return super.createQuery(tokens);
  }

  SelectQuery composeQuery(DbmsQueryResult statistics) {
    // read option values
    List<UnnamedColumn> tierPredicates = method.getTierExpressions(statistics);
    int tierCount = tierPredicates.size() + 1;
    int blockCount = 0;
    if (options.containsKey("blockCount")) {
      blockCount = Integer.valueOf(options.get("blockCount"));
    } else {
      method.getBlockCount(statistics);
    }
    String tierColumnName = options.get("tierColumnName");
    String blockColumnName = options.get("blockColumnName");

    // composed select item expressions will be added to this list
    List<SelectItem> selectItems = new ArrayList<>();
    selectItems.add(new AsteriskColumn());

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
      tierExpr = ColumnOp.whenthenelse(tierOperands);
    }
    selectItems.add(new AliasedColumn(tierExpr, tierColumnName));

    // compose block expression
    UnnamedColumn blockExpr = null;
    List<UnnamedColumn> blockOperands = new ArrayList<>();
    for (int i = 0; i < tierCount; i++) {
      List<Double> cumulProb = method.getCumulativeProbabilityDistributionForTier(statistics, i, blockCount);
      List<Double> condProb = computeConditionalProbabilityDistribution(cumulProb);

      List<UnnamedColumn> blockForTierOperands = new ArrayList<>();
      for (int j = 0; j < blockCount; j++) {
        blockForTierOperands.add(ColumnOp.lessequal(ColumnOp.rand(), ConstantColumn.valueOf(condProb.get(j))));
        blockForTierOperands.add(ConstantColumn.valueOf(j));
      }
      UnnamedColumn blockForTierExpr = ColumnOp.whenthenelse(blockForTierOperands);

      if (i < tierCount-1) {
        // "when" part in the case-when-else expression
        // for the last tier, we don't need this "when" part
        blockOperands.add(ColumnOp.equal(tierExpr, ConstantColumn.valueOf(i)));
      }
      blockOperands.add(blockForTierExpr);
    }
    
    
   

    blockExpr = ColumnOp.whenthenelse(blockOperands);
    selectItems.add(new AliasedColumn(blockExpr, blockColumnName));

    // compose the final query
    SelectQuery scramblingQuery = 
        SelectQuery.create(selectItems, new BaseTable(oldSchemaName, oldTableName));

    return scramblingQuery;
  }

  List<Double> computeConditionalProbabilityDistribution(List<Double> cumulativeProbabilityDistribution) {
    List<Double> cond = new ArrayList<>();
    int length = cumulativeProbabilityDistribution.size();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        cond.add(cumulativeProbabilityDistribution.get(i));
      } else {
        double numerator = cumulativeProbabilityDistribution.get(i) - cumulativeProbabilityDistribution.get(i-1);
        double denominator = 1.0 - cumulativeProbabilityDistribution.get(i-1);
        cond.add(numerator / denominator);
      }
    }
    return cond;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return super.createToken(result);
  }

}

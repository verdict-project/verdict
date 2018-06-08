package org.verdictdb.core.rewriter.aggresult;

import static org.verdictdb.core.rewriter.query.AliasRenamingRules.countEstimateAliasName;
import static org.verdictdb.core.rewriter.query.AliasRenamingRules.countSubsampleAliasName;
import static org.verdictdb.core.rewriter.query.AliasRenamingRules.expectedErrorAliasName;
import static org.verdictdb.core.rewriter.query.AliasRenamingRules.expectedValueAliasName;
import static org.verdictdb.core.rewriter.query.AliasRenamingRules.sumEstimateAliasName;
import static org.verdictdb.core.rewriter.query.AliasRenamingRules.sumScaledCountAliasName;
import static org.verdictdb.core.rewriter.query.AliasRenamingRules.sumScaledSumAliasName;
import static org.verdictdb.core.rewriter.query.AliasRenamingRules.sumSquaredScaledCountAliasName;
import static org.verdictdb.core.rewriter.query.AliasRenamingRules.sumSquaredScaledSumAliasName;
import static org.verdictdb.core.rewriter.query.AliasRenamingRules.sumSubsampleSizeAliasName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.aggresult.AggregateFrame;
import org.verdictdb.core.aggresult.AggregateGroup;
import org.verdictdb.core.aggresult.AggregateMeasures;
import org.verdictdb.exception.ValueException;
import org.verdictdb.exception.VerdictDbException;


public class SingleAggResultRewriter {

  AggregateFrame rawResultSet;

  List<String> columnNames;
  
  Map<String, Integer> indexCache = new HashMap<>();

  public SingleAggResultRewriter() {}

  public SingleAggResultRewriter(AggregateFrame rawResultSet) {
    this.rawResultSet = rawResultSet;
    this.columnNames = rawResultSet.getColumnNames();
  }

  /**
   * Rewrites the raw result set into something meaningful.
   *
   * Rewritten (or converted) result set should have the following columns:
   * 1. groups (that appear in the group by clause)
   * 2. aggregate columns
   * 3.
   *
   * @param rawResultSet
   * @param aggColumns Each pair is (original agg alias, type of agg)
   * @return
   * @throws VerdictDbException
   */
  public AggregateFrame rewrite(List<String> nonaggColumns, List<Pair<String, String>> aggColumns) 
      throws VerdictDbException {
    validityCheck(nonaggColumns, aggColumns);
    AggregateFrame converted = new AggregateFrame(getNewColumnNames(nonaggColumns, aggColumns));
    for (Entry<AggregateGroup, AggregateMeasures> groupAndMeasures : rawResultSet.groupAndMeasuresSet()) {
      AggregateGroup singleGroup = groupAndMeasures.getKey();
      AggregateMeasures singleMeasures = groupAndMeasures.getValue();
      AggregateMeasures convertedMeasures = rewriteMeasures(singleMeasures, aggColumns);
      converted.addRow(singleGroup, convertedMeasures);
    }
    return converted;
  }
  
  AggregateMeasures rewriteMeasures(AggregateMeasures originalMeasures, List<Pair<String, String>> aggColumns)
      throws ValueException {
    AggregateMeasures rewrittenMeasures = new AggregateMeasures();
    for (Pair<String, String> agg : aggColumns) {
      String aggname = agg.getLeft();
      String aggtype = agg.getRight();
      
      if (aggtype.equals("sum")) {
        Object sumEstimate = getMeasureValue(originalMeasures, sumEstimateAliasName(aggname));
        Object sumScaledSum = getMeasureValue(originalMeasures, sumScaledSumAliasName(aggname));
        Object sumSquaredScaledSum = getMeasureValue(originalMeasures, sumSquaredScaledSumAliasName(aggname));
        Object countSubsamples = getMeasureValue(originalMeasures, countSubsampleAliasName());
        Object sumSubsampleSizes = getMeasureValue(originalMeasures, sumSubsampleSizeAliasName());
        List<Pair<String, Object>> rewrittenSingle =
            rewriteSumMeasure(aggname, sumEstimate, sumScaledSum, sumSquaredScaledSum,
                countSubsamples, sumSubsampleSizes);
        for (Pair<String, Object> nameAndValue : rewrittenSingle) {
          rewrittenMeasures.addMeasure(nameAndValue.getLeft(), nameAndValue.getRight());
        }
      }
      else if (aggtype.equals("count")) {
        Object countEstimate = getMeasureValue(originalMeasures, countEstimateAliasName(aggname));
        Object sumScaledCount = getMeasureValue(originalMeasures, sumScaledCountAliasName(aggname));
        Object sumSquaredScaledCount = getMeasureValue(originalMeasures, sumSquaredScaledCountAliasName(aggname));
        Object countSubsamples = getMeasureValue(originalMeasures, countSubsampleAliasName());
        Object sumSubsampleSizes = getMeasureValue(originalMeasures, sumSubsampleSizeAliasName());
        List<Pair<String, Object>> rewrittenSingle =
            rewriteCountMeasure(aggname, countEstimate, sumScaledCount, sumSquaredScaledCount,
                countSubsamples, sumSubsampleSizes);
        for (Pair<String, Object> nameAndValue : rewrittenSingle) {
          rewrittenMeasures.addMeasure(nameAndValue.getLeft(), nameAndValue.getRight());
        }
      }
      else if (aggtype.equals("avg")) {
        Object sumEstimate = getMeasureValue(originalMeasures, sumEstimateAliasName(aggname));
        Object sumScaledSum = getMeasureValue(originalMeasures, sumScaledSumAliasName(aggname));
        Object sumSquaredScaledSum = getMeasureValue(originalMeasures, sumSquaredScaledSumAliasName(aggname));
        Object countEstimate = getMeasureValue(originalMeasures, countEstimateAliasName(aggname));
        Object sumScaledCount = getMeasureValue(originalMeasures, sumScaledCountAliasName(aggname));
        Object sumSquaredScaledCount = getMeasureValue(originalMeasures, sumSquaredScaledCountAliasName(aggname));
        Object countSubsamples = getMeasureValue(originalMeasures, countSubsampleAliasName());
        Object sumSubsampleSizes = getMeasureValue(originalMeasures, sumSubsampleSizeAliasName());
        List<Pair<String, Object>> rewrittenSingle =
            rewriteAvgMeasure(aggname,
                sumEstimate, sumScaledSum, sumSquaredScaledSum,
                countEstimate, sumScaledCount, sumSquaredScaledCount,
                countSubsamples, sumSubsampleSizes);
        for (Pair<String, Object> nameAndValue : rewrittenSingle) {
          rewrittenMeasures.addMeasure(nameAndValue.getLeft(), nameAndValue.getRight());
        }
      }
    }
    return rewrittenMeasures;
  }

  List<Pair<String, Object>> rewriteSumMeasure(String aggname, Object sumEstimate, Object sumScaledSum,
      Object sumSquaredScaledSum, Object countSubsamples, Object sumSubsampleSizes) {
    List<Pair<String, Object>> rewrittenMeasures = new ArrayList<>();
    
    double mu = (Double) sumEstimate;
    double mu_nsi = (Double) sumScaledSum;
    double musquared_nsi = (Double) sumSquaredScaledSum;
    double b = ((Integer) countSubsamples).doubleValue();
    double n = ((Integer) sumSubsampleSizes).doubleValue();
    double mu_err = (musquared_nsi - 2*mu_nsi + mu*mu*n) / (b*n);
    
    rewrittenMeasures.add(Pair.of(expectedValueAliasName(aggname), (Object) mu));
    rewrittenMeasures.add(Pair.of(expectedErrorAliasName(aggname), (Object) Math.sqrt(mu_err)));
    return rewrittenMeasures;
  }

  List<Pair<String, Object>> rewriteCountMeasure(String aggname, Object countEstimate, Object sumScaledCount,
      Object sumSquaredScaledCount, Object countSubsamples, Object sumSubsampleSizes) {
    List<Pair<String, Object>> rewrittenMeasures = new ArrayList<>();
    
    double mu = (Double) countEstimate;
    double mu_nsi = (Double) sumScaledCount;
    double musquared_nsi = (Double) sumSquaredScaledCount;
    double b = ((Integer) countSubsamples).doubleValue();
    double n = ((Integer) sumSubsampleSizes).doubleValue();
    double mu_err = (musquared_nsi - 2*mu_nsi + mu*mu*n) / (b*n);
    
    rewrittenMeasures.add(Pair.of(expectedValueAliasName(aggname), (Object) mu));
    rewrittenMeasures.add(Pair.of(expectedErrorAliasName(aggname), (Object) Math.sqrt(mu_err)));
    return rewrittenMeasures;
  }

  List<Pair<String, Object>> rewriteAvgMeasure(String aggname,
      Object sumEstimate, Object sumScaledSum, Object sumSquaredScaledSum,
      Object countEstimate, Object sumScaledCount, Object sumSquaredScaledCount,
      Object countSubsamples, Object sumSubsampleSizes) {
    List<Pair<String, Object>> rewrittenMeasures = new ArrayList<>();
    
    double mu_s = (Double) sumEstimate;
    double mu_nsi_s = (Double) sumScaledSum;
    double musquared_nsi_s = (Double) sumSquaredScaledSum;
    
    double mu_c = (Double) countEstimate;
    double mu_nsi_c = (Double) sumScaledCount;
    double musquared_nsi_c = (Double) sumSquaredScaledCount;
    
    double b = ((Integer) countSubsamples).doubleValue();
    double n = ((Integer) sumSubsampleSizes).doubleValue();
    
    double mu_err_s = (musquared_nsi_s - 2*mu_nsi_s + mu_s*mu_s*n) / (b*n);
    double mu_err_c = (musquared_nsi_c - 2*mu_nsi_c + mu_c*mu_c*n) / (b*n);
    
    double mu = mu_s / mu_c;
    double mu_err = mu_err_s / (mu_c*mu_c) + (mu_s * mu_s * mu_err_c) / Math.pow(mu_c,4);
    
    rewrittenMeasures.add(Pair.of(expectedValueAliasName(aggname), (Object) mu));
    rewrittenMeasures.add(Pair.of(expectedErrorAliasName(aggname), (Object) Math.sqrt(mu_err)));
    
    return rewrittenMeasures;
  }

  void validityCheck(List<String> nonaggColumns, List<Pair<String, String>> aggColumns) throws ValueException {
    for (String nonagg: nonaggColumns) {
      mustContain(nonagg);
    }

    for (Pair<String, String> agg : aggColumns) {
      String aggAlias = agg.getLeft();
      String aggType = agg.getRight();

      if (aggType.equals("sum")) {
        mustContain(sumEstimateAliasName(aggAlias));
        mustContain(sumScaledSumAliasName(aggAlias));
        mustContain(sumSquaredScaledSumAliasName(aggAlias));
        mustContain(countSubsampleAliasName());
        mustContain(sumSubsampleSizeAliasName());
      }
      else if (aggType.equals("count")) {
        mustContain(countEstimateAliasName(aggAlias));
        mustContain(sumScaledCountAliasName(aggAlias));
        mustContain(sumSquaredScaledCountAliasName(aggAlias));
        mustContain(countSubsampleAliasName());
        mustContain(sumSubsampleSizeAliasName());
      }
      else if (aggType.equals("avg")) {
        mustContain(sumEstimateAliasName(aggAlias));
        mustContain(sumScaledSumAliasName(aggAlias));
        mustContain(sumSquaredScaledSumAliasName(aggAlias));
        mustContain(countEstimateAliasName(aggAlias));
        mustContain(sumScaledCountAliasName(aggAlias));
        mustContain(sumSquaredScaledCountAliasName(aggAlias));
        mustContain(countSubsampleAliasName());
        mustContain(sumSubsampleSizeAliasName());
      }
    }
  }

  List<String> getNewColumnNames(List<String> nonaggColumns, List<Pair<String, String>> aggColumns) {
    List<String> newColumnNames = new ArrayList<>();
    for (String name : nonaggColumns) {
      newColumnNames.add(name);
    }
    for (Pair<String, String> agg : aggColumns) {
      String aggname = agg.getLeft();
      newColumnNames.add(expectedValueAliasName(aggname));
      newColumnNames.add(expectedErrorAliasName(aggname));
    }
    return newColumnNames;
  }

  Object getMeasureValue(AggregateMeasures measures, String aliasName) throws ValueException {
    if (indexCache.containsKey(aliasName)) {
      return measures.getAttributeValueAt(indexCache.get(aliasName));
    }
    else {
      int index = measures.getIndexOfAttributeName(aliasName);
      indexCache.put(aliasName, index);
      return measures.getAttributeValueAt(index);
    }
  }

  void mustContain(String colName) throws ValueException {
    if (!columnNames.contains(colName)) {
      throw new ValueException("The expected column name does not exist: " + colName);
    }
  }

}

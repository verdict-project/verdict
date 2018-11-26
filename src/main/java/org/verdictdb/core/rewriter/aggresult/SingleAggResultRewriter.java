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

package org.verdictdb.core.rewriter.aggresult;

import static org.verdictdb.core.rewriter.AliasRenamingRules.countEstimateAliasName;
import static org.verdictdb.core.rewriter.AliasRenamingRules.countSubsampleAliasName;
import static org.verdictdb.core.rewriter.AliasRenamingRules.expectedErrorAliasName;
import static org.verdictdb.core.rewriter.AliasRenamingRules.expectedValueAliasName;
import static org.verdictdb.core.rewriter.AliasRenamingRules.sumEstimateAliasName;
import static org.verdictdb.core.rewriter.AliasRenamingRules.sumScaledCountAliasName;
import static org.verdictdb.core.rewriter.AliasRenamingRules.sumScaledSumAliasName;
import static org.verdictdb.core.rewriter.AliasRenamingRules.sumSquaredScaledCountAliasName;
import static org.verdictdb.core.rewriter.AliasRenamingRules.sumSquaredScaledSumAliasName;
import static org.verdictdb.core.rewriter.AliasRenamingRules.sumSubsampleSizeAliasName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.TypeCasting;
import org.verdictdb.core.aggresult.AggregateFrame;
import org.verdictdb.core.aggresult.AggregateGroup;
import org.verdictdb.core.aggresult.AggregateMeasures;
import org.verdictdb.core.rewriter.AliasRenamingRules;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * Given intermediate aggregate form (either the result of a single query execution or the result of
 * the aggregates of multiple query results), we convert it to a user-friendly form.
 *
 * @author Yongjoo Park
 */
public class SingleAggResultRewriter {

  AggregateFrame rawResultSet;

  List<String> columnNames;

  Map<String, Integer> indexCache = new HashMap<>();

  public SingleAggResultRewriter(AggregateFrame rawResultSet) {
    this.rawResultSet = rawResultSet;
    this.columnNames = rawResultSet.getColumnNames();
  }

  /**
   * Rewrites the raw result set into the result set with (intuitive) error bounds.
   *
   * <p>Rewritten (or converted) result set should have the following columns: 1. groups (that
   * appear in the group by clause) 2. aggregate columns
   *
   * @param rawResultSet
   * @param aggColumns Each pair is (original agg alias, type of agg)
   * @return
   * @throws VerdictDBException
   */
  public AggregateFrame rewrite(List<String> nonaggColumns, List<AggNameAndType> aggColumns)
      throws VerdictDBException {
    ensureColumnNamesValidity(nonaggColumns, aggColumns);
    AggregateFrame converted = new AggregateFrame(getNewColumnNames(nonaggColumns, aggColumns));

    // regroup based on the groups other than the tier column
    Map<AggregateGroup, List<Pair<Integer, AggregateMeasures>>> regroupedData = new HashMap<>();
    for (Entry<AggregateGroup, AggregateMeasures> groupAndMeasures :
        rawResultSet.groupAndMeasuresSet()) {
      List<String> newGroupNames = new ArrayList<>();
      List<Object> newGroupValues = new ArrayList<>();
      int tierNumber = -1;

      AggregateGroup previousGroup = groupAndMeasures.getKey();
      List<String> groupNames = previousGroup.getAttributeNames();
      List<Object> groupValues = previousGroup.getAttributeValues();
      for (int i = 0; i < groupNames.size(); i++) {
        if (groupNames.get(i).equals(AliasRenamingRules.tierAliasName())) {
          tierNumber = Integer.valueOf(groupValues.get(i).toString());
        } else {
          newGroupNames.add(groupNames.get(i));
          newGroupValues.add(groupValues.get(i));
        }
      }

      AggregateGroup newGroup = new AggregateGroup(newGroupNames, newGroupValues);
      if (!regroupedData.containsKey(newGroup)) {
        regroupedData.put(newGroup, new ArrayList<Pair<Integer, AggregateMeasures>>());
      }
      regroupedData.get(newGroup).add(Pair.of(tierNumber, groupAndMeasures.getValue()));
    }

    // rewrite measure values
    for (Entry<AggregateGroup, List<Pair<Integer, AggregateMeasures>>>
        groupAndConsolidatedMeasures : regroupedData.entrySet()) {
      AggregateGroup singleGroup = groupAndConsolidatedMeasures.getKey();
      List<Pair<Integer, AggregateMeasures>> consolidatedMeasures =
          groupAndConsolidatedMeasures.getValue();
      AggregateMeasures convertedMeasures = rewriteMeasures(consolidatedMeasures, aggColumns);
      converted.addRow(singleGroup, convertedMeasures);
    }
    //    for (Entry<AggregateGroup, AggregateMeasures> groupAndMeasures :
    // rawResultSet.groupAndMeasuresSet()) {
    //      AggregateGroup singleGroup = groupAndMeasures.getKey();
    //      AggregateMeasures singleMeasures = groupAndMeasures.getValue();
    //      AggregateMeasures convertedMeasures = rewriteMeasures(singleMeasures, aggColumns);
    //      converted.addRow(singleGroup, convertedMeasures);
    //    }
    return converted;
  }

  AggregateMeasures rewriteMeasures(
      List<Pair<Integer, AggregateMeasures>> tierAndMeasures, List<AggNameAndType> aggColumns)
      throws VerdictDBValueException, VerdictDBTypeException {
    AggregateMeasures rewrittenMeasures = new AggregateMeasures();
    for (AggNameAndType agg : aggColumns) {
      String aggname = agg.getName();
      String aggtype = agg.getAggType();
      AggregateMeasures originalMeasures =
          tierAndMeasures.get(0).getRight(); // only for uniform sampling

      if (aggtype.equals("sum")) {
        Object sumEstimate = getMeasureValue(originalMeasures, sumEstimateAliasName(aggname));
        Object sumScaledSum = getMeasureValue(originalMeasures, sumScaledSumAliasName(aggname));
        Object sumSquaredScaledSum =
            getMeasureValue(originalMeasures, sumSquaredScaledSumAliasName(aggname));
        Object countSubsamples = getMeasureValue(originalMeasures, countSubsampleAliasName());
        Object sumSubsampleSizes = getMeasureValue(originalMeasures, sumSubsampleSizeAliasName());
        List<Pair<String, Object>> rewrittenSingle =
            rewriteSumMeasure(
                aggname,
                sumEstimate,
                sumScaledSum,
                sumSquaredScaledSum,
                countSubsamples,
                sumSubsampleSizes);
        for (Pair<String, Object> nameAndValue : rewrittenSingle) {
          rewrittenMeasures.addMeasure(nameAndValue.getLeft(), nameAndValue.getRight());
        }
      } else if (aggtype.equals("count")) {
        Object countEstimate = getMeasureValue(originalMeasures, countEstimateAliasName(aggname));
        Object sumScaledCount = getMeasureValue(originalMeasures, sumScaledCountAliasName(aggname));
        Object sumSquaredScaledCount =
            getMeasureValue(originalMeasures, sumSquaredScaledCountAliasName(aggname));
        Object countSubsamples = getMeasureValue(originalMeasures, countSubsampleAliasName());
        Object sumSubsampleSizes = getMeasureValue(originalMeasures, sumSubsampleSizeAliasName());
        List<Pair<String, Object>> rewrittenSingle =
            rewriteCountMeasure(
                aggname,
                countEstimate,
                sumScaledCount,
                sumSquaredScaledCount,
                countSubsamples,
                sumSubsampleSizes);
        for (Pair<String, Object> nameAndValue : rewrittenSingle) {
          rewrittenMeasures.addMeasure(nameAndValue.getLeft(), nameAndValue.getRight());
        }
      } else if (aggtype.equals("avg")) {
        Object sumEstimate = getMeasureValue(originalMeasures, sumEstimateAliasName(aggname));
        Object sumScaledSum = getMeasureValue(originalMeasures, sumScaledSumAliasName(aggname));
        Object sumSquaredScaledSum =
            getMeasureValue(originalMeasures, sumSquaredScaledSumAliasName(aggname));
        Object countEstimate = getMeasureValue(originalMeasures, countEstimateAliasName(aggname));
        Object sumScaledCount = getMeasureValue(originalMeasures, sumScaledCountAliasName(aggname));
        Object sumSquaredScaledCount =
            getMeasureValue(originalMeasures, sumSquaredScaledCountAliasName(aggname));
        Object countSubsamples = getMeasureValue(originalMeasures, countSubsampleAliasName());
        Object sumSubsampleSizes = getMeasureValue(originalMeasures, sumSubsampleSizeAliasName());
        List<Pair<String, Object>> rewrittenSingle =
            rewriteAvgMeasure(
                aggname,
                sumEstimate,
                sumScaledSum,
                sumSquaredScaledSum,
                countEstimate,
                sumScaledCount,
                sumSquaredScaledCount,
                countSubsamples,
                sumSubsampleSizes);
        for (Pair<String, Object> nameAndValue : rewrittenSingle) {
          rewrittenMeasures.addMeasure(nameAndValue.getLeft(), nameAndValue.getRight());
        }
      }
    }
    return rewrittenMeasures;
  }

  List<Pair<String, Object>> rewriteSumMeasure(
      String aggname,
      Object sumEstimate,
      Object sumScaledSum,
      Object sumSquaredScaledSum,
      Object countSubsamples,
      Object sumSubsampleSizes)
      throws VerdictDBTypeException {
    List<Pair<String, Object>> rewrittenMeasures = new ArrayList<>();

    double mu = TypeCasting.toDouble(sumEstimate);
    double mu_nsi = TypeCasting.toDouble(sumScaledSum);
    double musquared_nsi = TypeCasting.toDouble(sumSquaredScaledSum);
    double b = TypeCasting.toDouble(countSubsamples);
    double n = TypeCasting.toDouble(sumSubsampleSizes);
    double mu_err = (musquared_nsi - 2 * mu_nsi + mu * mu * n) / (b * n);

    rewrittenMeasures.add(Pair.of(expectedValueAliasName(aggname), (Object) mu));
    rewrittenMeasures.add(Pair.of(expectedErrorAliasName(aggname), (Object) Math.sqrt(mu_err)));
    return rewrittenMeasures;
  }

  List<Pair<String, Object>> rewriteCountMeasure(
      String aggname,
      Object countEstimate,
      Object sumScaledCount,
      Object sumSquaredScaledCount,
      Object countSubsamples,
      Object sumSubsampleSizes)
      throws VerdictDBTypeException {
    List<Pair<String, Object>> rewrittenMeasures = new ArrayList<>();

    double mu = TypeCasting.toDouble(countEstimate);
    double mu_nsi = TypeCasting.toDouble(sumScaledCount);
    double musquared_nsi = (Double) sumSquaredScaledCount;
    double b = TypeCasting.toDouble(countSubsamples);
    double n = TypeCasting.toDouble(sumSubsampleSizes);
    double mu_err = (musquared_nsi - 2 * mu_nsi + mu * mu * n) / (b * n);

    rewrittenMeasures.add(Pair.of(expectedValueAliasName(aggname), (Object) mu));
    rewrittenMeasures.add(Pair.of(expectedErrorAliasName(aggname), (Object) Math.sqrt(mu_err)));
    return rewrittenMeasures;
  }

  List<Pair<String, Object>> rewriteAvgMeasure(
      String aggname,
      Object sumEstimate,
      Object sumScaledSum,
      Object sumSquaredScaledSum,
      Object countEstimate,
      Object sumScaledCount,
      Object sumSquaredScaledCount,
      Object countSubsamples,
      Object sumSubsampleSizes)
      throws VerdictDBTypeException {
    List<Pair<String, Object>> rewrittenMeasures = new ArrayList<>();

    double mu_s = TypeCasting.toDouble(sumEstimate);
    double mu_nsi_s = TypeCasting.toDouble(sumScaledSum);
    double musquared_nsi_s = TypeCasting.toDouble(sumSquaredScaledSum);

    double mu_c = TypeCasting.toDouble(countEstimate);
    double mu_nsi_c = TypeCasting.toDouble(sumScaledCount);
    double musquared_nsi_c = TypeCasting.toDouble(sumSquaredScaledCount);

    double b = TypeCasting.toDouble(countSubsamples);
    double n = TypeCasting.toDouble(sumSubsampleSizes);

    double mu_err_s = (musquared_nsi_s - 2 * mu_nsi_s + mu_s * mu_s * n) / (b * n);
    double mu_err_c = (musquared_nsi_c - 2 * mu_nsi_c + mu_c * mu_c * n) / (b * n);

    double mu = mu_s / mu_c;
    double mu_err = mu_err_s / (mu_c * mu_c) + (mu_s * mu_s * mu_err_c) / Math.pow(mu_c, 4);

    rewrittenMeasures.add(Pair.of(expectedValueAliasName(aggname), (Object) mu));
    rewrittenMeasures.add(Pair.of(expectedErrorAliasName(aggname), (Object) Math.sqrt(mu_err)));

    return rewrittenMeasures;
  }

  void ensureColumnNamesValidity(List<String> nonaggColumns, List<AggNameAndType> aggColumns)
      throws VerdictDBValueException {
    for (String nonagg : nonaggColumns) {
      mustContain(nonagg);
    }

    for (AggNameAndType agg : aggColumns) {
      String aggAlias = agg.getName();
      String aggType = agg.getAggType();

      if (aggType.equals("sum")) {
        mustContain(sumEstimateAliasName(aggAlias));
        mustContain(sumScaledSumAliasName(aggAlias));
        mustContain(sumSquaredScaledSumAliasName(aggAlias));
        mustContain(countSubsampleAliasName());
        mustContain(sumSubsampleSizeAliasName());
      } else if (aggType.equals("count")) {
        mustContain(countEstimateAliasName(aggAlias));
        mustContain(sumScaledCountAliasName(aggAlias));
        mustContain(sumSquaredScaledCountAliasName(aggAlias));
        mustContain(countSubsampleAliasName());
        mustContain(sumSubsampleSizeAliasName());
      } else if (aggType.equals("avg")) {
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

  List<String> getNewColumnNames(List<String> nonaggColumns, List<AggNameAndType> aggColumns) {
    List<String> newColumnNames = new ArrayList<>();
    for (String name : nonaggColumns) {
      newColumnNames.add(name);
    }
    for (AggNameAndType agg : aggColumns) {
      String aggname = agg.getName();
      newColumnNames.add(expectedValueAliasName(aggname));
      newColumnNames.add(expectedErrorAliasName(aggname));
    }
    return newColumnNames;
  }

  Object getMeasureValue(AggregateMeasures measures, String aliasName)
      throws VerdictDBValueException {
    if (indexCache.containsKey(aliasName)) {
      return measures.getAttributeValueAt(indexCache.get(aliasName));
    } else {
      int index = measures.getIndexOfAttributeName(aliasName);
      indexCache.put(aliasName, index);
      return measures.getAttributeValueAt(index);
    }
  }

  void mustContain(String colName) throws VerdictDBValueException {
    if (!columnNames.contains(colName)) {
      throw new VerdictDBValueException("The expected column name does not exist: " + colName);
    }
  }
}

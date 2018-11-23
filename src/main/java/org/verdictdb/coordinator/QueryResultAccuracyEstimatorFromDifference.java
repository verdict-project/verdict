package org.verdictdb.coordinator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.verdictdb.VerdictSingleResult;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;

/**
 * Estimates the difference based on the difference between two consequent result sets.
 * 
 * Limitation: Currently, this only works properly when the most outer query includes aggregate
 *             columns.
 * 
 * For comparison, the grouping attributes (i.e., non-aggregate attributes) are used as a key,
 * and the non-grouping attributes (i.e., aggregate attributes) are used as values.
 * 
 * If the changes in the values are smaller than a predefined threshold (e.g., 5%) for every key,
 * the answer is considered to be accurate.
 *
 */
public class QueryResultAccuracyEstimatorFromDifference extends QueryResultAccuracyEstimator {

//  Coordinator runningCoordinator;

  // the values of the result should be within [(1-valueError)*prevValue, (1+valueError)*prevValue]
  // of the previous result.
  // Otherwise, it will fetch next result.
  private Double valueError = 0.05;

  // the #row of the result should be within  [(1-groupCountError)*prev#row,
  // (1+groupCountError)*prev#row] of the previous result.
  // Otherwise, it will fetch next result.
  private Double groupCountError = 0.05;

  // Stores the latest result in an alternative form.
  // The key is the values of grouping columns; the value is the values of non-grouping columns
  // The grouping columns are analyzed using the original query.
  private HashMap<List<Object>, List<Object>> groupToNonGroupMap;
  
  // Used for inferring grouping and aggregate columns.
  private SelectQuery originalQuery;
  
  // this field is set when the first result set is added (using the add() method).
//  private Set<Integer> groupingColumnIndexes = new HashSet<>();
  private Set<Integer> nongroupingColumnIndxes = new HashSet<>();

  QueryResultAccuracyEstimatorFromDifference(SelectQuery originalQuery) {
    this.originalQuery = originalQuery;
//    this.runningCoordinator = runningCoordinator;
  }
  
  @Override
  public void add(VerdictSingleResult rs) {
    super.add(rs);
    
    // Using the first answer, we estimate grouping and aggregate columns.
    if (getAnswerCount() != 1) {
      return;
    }
    
    List<SelectItem> selectItems = originalQuery.getSelectList();
    VerdictSingleResult singleAnswer = answers.get(0);
    
    // estimate the number of columns that would be projected by '*'.
    int numColExceptforAsterisk = 0;
    int numAsterisk = 0;
    for (SelectItem item : selectItems) {
      if (item instanceof AsteriskColumn) {
        numAsterisk++;
      } else {
        numColExceptforAsterisk++;
      }
    }
    
    int numColForAsterisk = (numAsterisk == 0)? 0:
      (singleAnswer.getColumnCount() - numColExceptforAsterisk) / numAsterisk;
    
    // obtain the index of grouping and non-grouping attribute indexes.
    int i = 0;
    for (SelectItem item : selectItems) {
      if (item instanceof AsteriskColumn) {
        for (int j = 0; j < numColForAsterisk; j++) {
          nongroupingColumnIndxes.add(i);
          i++;
        }
      } else if (item.isAggregateColumn()) {
        nongroupingColumnIndxes.add(i);
        i++;
      } else {
        i++;
      }
    }
  }

  public void setValueError(Double valueError) {
    this.valueError = valueError;
  }

  public void setGroupCountError(Double groupCountError) {
    this.groupCountError = groupCountError;
  }

  /**
   * fetch the answer from stream until converge
   *
   * @return the accurate answer
   */
  @Override
  public boolean isLastResultAccurate() {
    if (!checkConverge()) {
      return false;
    } else {
      log.debug("The approximate answers differed less than thresholds, "
          + "and thus, considered to be Accurate.");
      return true;
    }
  }

  private boolean checkConverge() {
    // base condition check
    if (nongroupingColumnIndxes.size() == 0) {
      log.debug("No aggregate columns exist. The result is assumed to be exact.");
      return true;
    }

//  // query result without asyncAggregate
//  if (currentAnswer.getMetaData() == null || currentAnswer.getMetaData().isAggregate.isEmpty()) {
//    return true;
//  }
    
    // some variables we will use in this function
    HashMap<List<Object>, List<Object>> newAggregatedMap = new HashMap<>();
    VerdictSingleResult currentAnswer = answers.get(answers.size() - 1);
    
    while (currentAnswer.next()) {
      List<Object> aggregateValues = new ArrayList<>();
      List<Object> groupValues = new ArrayList<>();
      // dyoon: this does not seem to handle "SELECT *" correctly resulting in
      // IndexOutOfBoundsException
      // please take a look and apply a proper fix later.
      for (int i = 0; i < currentAnswer.getColumnCount(); i++) {
        if (nongroupingColumnIndxes.contains(i)) {
//        if (currentAnswer.getMetaData().isAggregate.get(i)) {
          aggregateValues.add(currentAnswer.getValue(i));
        } else {
          groupValues.add(currentAnswer.getValue(i));
        }
      }
      newAggregatedMap.put(groupValues, aggregateValues);
    }
    currentAnswer.rewind();
    
    
    if (answers.size() <= 1) {
      groupToNonGroupMap = newAggregatedMap;
      return false;
    }
    
    // Now actual check starts.
    VerdictSingleResult previousAnswer = answers.get(answers.size() - 2);

    // Check 1: check if #groupCountError is converged
    if (currentAnswer.getRowCount() < previousAnswer.getRowCount() * (1 - groupCountError)
        || currentAnswer.getRowCount() > previousAnswer.getRowCount() * (1 + groupCountError)) {
      return false;
    }

    // Check 2: if aggregate values have converged.
    Boolean isValueConverged = true;
    for (List<Object> groupingValues : newAggregatedMap.keySet()) {
      if (isValueConverged && groupToNonGroupMap.containsKey(groupingValues)) {
        List<Object> prevAggregatedValues = groupToNonGroupMap.get(groupingValues);
        List<Object> aggregatedValues = newAggregatedMap.get(groupingValues);
        
        for (int idx = 0; idx < aggregatedValues.size(); idx++) {
          Object prevObj = prevAggregatedValues.get(idx);
          Object newObj = aggregatedValues.get(idx);
          
          double newValue, prevValue;
          // if v is Integer type or Double type, it is safe to case to double
          // Otherwise, if v is BigDecimal type, it needs to be convert to double
          if (newObj instanceof BigDecimal) {
            newValue = ((BigDecimal) newObj).doubleValue();
            prevValue = ((BigDecimal) prevObj).doubleValue();
          } else {
            newValue = (double) newObj;
            prevValue = (double) prevAggregatedValues.get(idx);
          }
          if (prevValue < newValue * (1 - valueError) 
              || prevValue > newValue * (1 + valueError)) {
            log.debug(
                String.format("Not accurate enough. Prev: %f, New: %f", prevValue, newValue));
            isValueConverged = false;
            break;
          }
        }
      }
      
      if (!isValueConverged) {
        break;
      }
    }
    
    // replaces the old values with the new values
    groupToNonGroupMap = newAggregatedMap;

    return isValueConverged;
  }
}

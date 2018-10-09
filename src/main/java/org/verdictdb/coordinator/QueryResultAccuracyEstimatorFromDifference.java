package org.verdictdb.coordinator;

import org.verdictdb.VerdictResultStream;
import org.verdictdb.VerdictSingleResult;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class QueryResultAccuracyEstimatorFromDifference extends QueryResultAccuracyEstimator {

  VerdictResultStream resultStream;

  Coordinator runningCoordinator;

  // the values of the result should be within [(1-valueError)*prevValue, (1+valueError)*prevValue] of the previous result.
  // Otherwise, it will fetch next result.
  Double valueError = 0.05;

  // the #column of the result should be within  [(1-groupCountError)*prev#column, (1+groupCountError)*prev#column] of the previous result.
  // Otherwise, it will fetch next result.
  Double groupCountError = 0.05;

  // key is the values of non-aggregated column, value is the values of aggregated column
  HashMap<List<Object>, List<Object>> aggregatedMap = new HashMap<>();

  QueryResultAccuracyEstimatorFromDifference(VerdictResultStream resultStream, Coordinator runningCoordinator) {
    this.resultStream = resultStream;
    this.runningCoordinator = runningCoordinator;
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
      log.debug("Break condition has reached.");
      resultStream.close();
      log.debug("Aborts an ExecutionContext: " + this);
      if (runningCoordinator != null) {
        Coordinator c = runningCoordinator;
        runningCoordinator = null;
        c.abort();
      }
      return true;
    }
  }

  private boolean checkConverge() {
    HashMap<List<Object>, List<Object>> newAggregatedMap = new HashMap<>();
    VerdictSingleResult currentAnswer = answers.get(answers.size() - 1);
    while (currentAnswer.next()) {
      List<Object> aggregatedValues = new ArrayList<>();
      List<Object> nonAggregatedValues = new ArrayList<>();
      for (int i = 0; i < currentAnswer.getColumnCount(); i++) {
        if (currentAnswer.getMetaData().isAggregate.get(i)) {
          aggregatedValues.add(currentAnswer.getValue(i));
        } else {
          nonAggregatedValues.add(currentAnswer.getValue(i));
        }
      }
      newAggregatedMap.put(nonAggregatedValues, aggregatedValues);
    }
    aggregatedMap = newAggregatedMap;
    currentAnswer.rewind();

    if (answers.size() == 1) {
      return false;
    }
    VerdictSingleResult previousAnswer = answers.get(answers.size() - 2);


    // check if #groupCountError is converged
    if (currentAnswer.getRowCount() < previousAnswer.getRowCount() * (1 - groupCountError)
        || currentAnswer.getRowCount() > previousAnswer.getRowCount() * (1 + groupCountError)) {
      return false;
    }

    Boolean isValueConverged = true;
    for (List<Object> nonAggregatedValues : newAggregatedMap.keySet()) {
      if (isValueConverged && aggregatedMap.containsKey(nonAggregatedValues)) {
        List<Object> prevAggregatedValues = aggregatedMap.get(nonAggregatedValues);
        List<Object> aggregatedValues = newAggregatedMap.get(nonAggregatedValues);
        for (Object v : aggregatedValues) {
          int idx = aggregatedValues.indexOf(v);
          double newValue, oldValue;
          if (v instanceof BigDecimal) {
            newValue = ((BigDecimal) v).doubleValue();
            oldValue = ((BigDecimal) prevAggregatedValues.get(idx)).doubleValue();
          } else {
            newValue = (double) v;
            oldValue = (double) prevAggregatedValues.get(idx);
          }
          if (newValue < oldValue * (1 - valueError)
              || newValue > oldValue * (1 + valueError)) {
            isValueConverged = false;
            break;
          }
        }
      }
      if (!isValueConverged) {
        break;
      }
    }

    return isValueConverged;
  }
}

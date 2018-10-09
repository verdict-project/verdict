package org.verdictdb.coordinator;

import org.verdictdb.VerdictResultStream;
import org.verdictdb.VerdictSingleResult;

public class QueryResultAccuracyEstimatorWithGroupSize extends QueryResultAccuracyEstimator {

  private int resultNumToBreak = 2;


  Coordinator runningCoordinator;

  QueryResultAccuracyEstimatorWithGroupSize(int resultNumToBreak, Coordinator runningCoordinator) {
    this.resultNumToBreak = resultNumToBreak;
    this.runningCoordinator = runningCoordinator;
  }


  /**
   * fetch the answer from stream until fetching $resultNumToBreak answers
   * @return the accurate answer
   */
  @Override
  public boolean isLastResultAccurate() {
    if (answers.size()<resultNumToBreak) {
      return false;
    } else {
      log.debug("Break condition has reached.");
      log.debug("Aborts an ExecutionContext: " + this);
      if (runningCoordinator != null) {
        Coordinator c = runningCoordinator;
        runningCoordinator = null;
        c.abort();
      }
      return true;
    }
  }

}

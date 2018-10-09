package org.verdictdb.coordinator;

import org.verdictdb.VerdictResultStream;
import org.verdictdb.VerdictSingleResult;

public class QueryResultAccuracyEstimatorWithGroupSize extends QueryResultAccuracyEstimator {

  private int resultNumToBreak = 2;

  VerdictResultStream resultStream;

  Coordinator runningCoordinator;

  QueryResultAccuracyEstimatorWithGroupSize(int resultNumToBreak, VerdictResultStream resultStream, Coordinator runningCoordinator) {
    this.resultNumToBreak = resultNumToBreak;
    this.resultStream = resultStream;
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

}

package org.verdictdb.coordinator;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.verdictdb.VerdictResultStream;
import org.verdictdb.VerdictSingleResult;
import org.apache.commons.collections.IteratorUtils;

import java.util.Iterator;
import java.util.List;

public class AsyncAggExecutionRulerByCount extends AsyncAggExecutionRuler {

  private int resultNumToBreak = 2;

  VerdictResultStream resultStream;

  Coordinator runningCoordinator;

  AsyncAggExecutionRulerByCount(int resultNumToBreak, VerdictResultStream resultStream, Coordinator runningCoordinator) {
    this.resultNumToBreak = resultNumToBreak;
    this.resultStream = resultStream;
    this.runningCoordinator = runningCoordinator;
  }


  /**
   * fetch the answer from stream until fetching $resultNumToBreak answers
   * @return the accurate answer
   */
  @Override
  public VerdictSingleResult fetchAnswerUntilAccurate() {
    try {
      while (resultStream.hasNext()) {
        answers.add(resultStream.next());
        if (answers.size() >= resultNumToBreak) {
          log.debug("Break condition has reached.");
          break;
        }
      }
    } catch (RuntimeException e) {
      throw e;
    }
    resultStream.close();
    log.debug("Aborts an ExecutionContext: " + this);
    if (runningCoordinator != null) {
      Coordinator c = runningCoordinator;
      runningCoordinator = null;
      c.abort();
    }
    return answers.get(answers.size()-1);
  }

}

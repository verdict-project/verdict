package org.verdictdb.coordinator;

import com.google.common.collect.Lists;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.VerdictDBLogger;

import java.util.ArrayList;
import java.util.List;

public abstract class AsyncAggExecutionRuler {

  VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

  List<VerdictSingleResult> answers = new ArrayList<>();

  List<VerdictSingleResult> getAnswers() {
    return answers;
  }

  /**
   * fetch the answer from stream until the criterion of accuracy has been reached
   * @return the accurate answer
   */
  public VerdictSingleResult fetchAnswerUntilAccurate() {
    return null;
  }
}

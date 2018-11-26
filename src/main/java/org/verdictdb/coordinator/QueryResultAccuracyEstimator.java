package org.verdictdb.coordinator;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.VerdictDBLogger;

public abstract class QueryResultAccuracyEstimator {

  protected VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

  protected List<VerdictSingleResult> answers = new ArrayList<>();

  public List<VerdictSingleResult> getAnswers() {
    return answers;
  }

  public int getAnswerCount() { 
    return answers.size(); 
  }

  public void add(VerdictSingleResult rs) {
    answers.add(rs);
  }

  /**
   * fetch the answer from stream until the criterion of accuracy has been reached
   * @return the accurate answer
   */
  public boolean isLastResultAccurate() {
    return false;
  }
}

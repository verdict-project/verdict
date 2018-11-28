package org.verdictdb.connection;

public interface QueryProgressStatus {
  
  /**
   * @return  A fraction between 0 and 1.
   */
  public double getProgressRatio();

}

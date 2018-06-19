package org.verdictdb.core.query;

public interface SelectItem {
  
  /**
   * @return True if a column is a non-subquery column and includes aggergate functions.
   */
  public boolean isAggregateColumn();
  
}

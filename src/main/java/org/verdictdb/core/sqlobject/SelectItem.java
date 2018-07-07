package org.verdictdb.core.sqlobject;

public interface SelectItem {
  
  /**
   * @return True if a column is a non-subquery column and includes aggergate functions.
   */
  public boolean isAggregateColumn();


  public SelectItem deepcopy();
}

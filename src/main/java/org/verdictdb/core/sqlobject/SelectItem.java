package org.verdictdb.core.sqlobject;

import java.io.Serializable;

public interface SelectItem extends Serializable {
  
  /**
   * @return True if a column is a non-subquery column and includes aggergate functions.
   */
  public boolean isAggregateColumn();


  public SelectItem deepcopy();
}

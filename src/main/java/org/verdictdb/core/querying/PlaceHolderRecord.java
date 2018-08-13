package org.verdictdb.core.querying;

import java.io.Serializable;

import org.verdictdb.core.sqlobject.BaseTable;

public class PlaceHolderRecord implements Serializable {

  private static final long serialVersionUID = -6893613573198545114L;

  private BaseTable placeholderTable;
  
  private int subscriptionChannel;
  
  public PlaceHolderRecord(BaseTable placeholderTable, int subscriptionChannel) {
    this.placeholderTable = placeholderTable;
    this.subscriptionChannel = subscriptionChannel;
  }
  
  public BaseTable getPlaceholderTable() {
    return placeholderTable;
  }
  
  public int getSubscriptionChannel() {
    return subscriptionChannel;
  }
}
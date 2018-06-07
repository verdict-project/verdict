package org.verdictdb.result;

import org.verdictdb.connection.DbmsQueryResult;

public abstract class AsyncHandler implements Runnable {

  DbmsQueryResult result;
  
  boolean askedToStop;
  
  public void setResult(DbmsQueryResult result) {
    this.result = result;
    this.askedToStop = false;
  }
  
  public abstract boolean handle(DbmsQueryResult result);

  @Override
  public void run() {
    askedToStop = handle(result);
  }
  

}

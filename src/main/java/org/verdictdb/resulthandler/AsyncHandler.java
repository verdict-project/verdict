package org.verdictdb.resulthandler;

import org.verdictdb.DbmsQueryResult;

public abstract class AsyncHandler implements Runnable {

  DbmsQueryResult result;
  
//  boolean askedToStop;
  
  public void setResult(DbmsQueryResult result) {
    this.result = result;
//    this.askedToStop = false;
  }
  
  public abstract void handle(DbmsQueryResult result);

  @Override
  public void run() {
    handle(result);
  }
  

}

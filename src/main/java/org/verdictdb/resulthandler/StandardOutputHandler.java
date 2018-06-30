package org.verdictdb.resulthandler;

import org.verdictdb.DbmsQueryResult;

public class StandardOutputHandler extends AsyncHandler {

  @Override
  public void handle(DbmsQueryResult result) {
    result.printContent();
  }

}

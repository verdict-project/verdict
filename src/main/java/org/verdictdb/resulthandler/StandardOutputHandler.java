package org.verdictdb.resulthandler;

import org.verdictdb.connection.DbmsQueryResult;

public class StandardOutputHandler extends AsyncHandler {

  @Override
  public boolean handle(DbmsQueryResult result) {
    result.printContent();
    return true;
  }

}

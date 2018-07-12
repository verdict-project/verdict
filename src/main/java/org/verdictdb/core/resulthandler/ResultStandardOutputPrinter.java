package org.verdictdb.core.resulthandler;

import org.verdictdb.core.connection.DbmsQueryResult;

public class ResultStandardOutputPrinter {
  
  ExecutionResultReader reader;
  
  public ResultStandardOutputPrinter(ExecutionResultReader reader) {
    this.reader = reader;
  }
  
  public static void run(ExecutionResultReader reader) {
    (new ResultStandardOutputPrinter(reader)).run();
  }
  
  public void run() {
    for (DbmsQueryResult result : reader) {
      handle(result);
    }
  }
  
  public void handle(DbmsQueryResult result) {
    result.printContent();
  }

}

/*
 *    Copyright 2017 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.resulthandler;

import org.verdictdb.connection.DbmsQueryResult;

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

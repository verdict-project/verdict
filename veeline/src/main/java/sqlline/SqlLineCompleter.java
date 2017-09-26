/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Modified BSD License
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at:
//
// http://opensource.org/licenses/BSD-3-Clause
*/
package sqlline;

import java.util.List;

import jline.console.completer.Completer;

/**
 * Completer for SQLline. It dispatches to sub-completers based on the
 * current arguments.
 */
class SqlLineCompleter
    implements Completer {
  private SqlLine sqlLine;

  public SqlLineCompleter(SqlLine sqlLine) {
    this.sqlLine = sqlLine;
  }

  public int complete(String buf, int pos, List<CharSequence> candidates) {
    if (buf != null
        && buf.startsWith(SqlLine.COMMAND_PREFIX)
        && !buf.startsWith(SqlLine.COMMAND_PREFIX + "all")
        && !buf.startsWith(SqlLine.COMMAND_PREFIX + "sql")) {
      return sqlLine.getCommandCompleter().complete(buf, pos, candidates);
    } else {
      if (sqlLine.getDatabaseConnection() != null
          && (sqlLine.getDatabaseConnection().getSqlCompleter() != null)) {
        return sqlLine.getDatabaseConnection().getSqlCompleter()
            .complete(buf, pos, candidates);
      } else {
        return -1;
      }
    }
  }
}

// End SqlLineCompleter.java

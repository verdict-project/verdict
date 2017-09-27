/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * OutputFormat for values separated by a delimiter.
 *
 * <p><strong>TODO</strong>:
 * Handle character escaping
 */
class SeparatedValuesOutputFormat implements OutputFormat {
  private final SqlLine sqlLine;
  private char separator;

  public SeparatedValuesOutputFormat(SqlLine sqlLine, char separator) {
    this.sqlLine = sqlLine;
    setSeparator(separator);
  }

  public int print(Rows rows) {
    int count = 0;
    while (rows.hasNext()) {
      printRow(rows, rows.next());
      count++;
    }

    return count - 1; // sans header row
  }

  public void printRow(Rows rows, Rows.Row row) {
    String[] vals = row.values;
    StringBuilder buf = new StringBuilder();
    for (String val : vals) {
      buf.append(buf.length() == 0 ? "" : "" + getSeparator())
          .append('\'')
          .append(val == null ? "" : val)
          .append('\'');
    }
    sqlLine.output(buf.toString());
  }

  public void setSeparator(char separator) {
    this.separator = separator;
  }

  public char getSeparator() {
    return this.separator;
  }
}

// End SeparatedValuesOutputFormat.java

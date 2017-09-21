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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Rows implementation which buffers all rows in a linked list.
 */
class BufferedRows extends Rows {
  private final List<Row> list;

  private final Iterator<Row> iterator;

  BufferedRows(SqlLine sqlLine, ResultSet rs) throws SQLException {
    super(sqlLine, rs);

    list = new LinkedList<Row>();

    int count = rsMeta.getColumnCount();

    list.add(new Row(count));

    while (rs.next()) {
      list.add(new Row(count, rs));
    }

    iterator = list.iterator();
  }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public Row next() {
    return iterator.next();
  }

  void normalizeWidths() {
    int[] max = null;
    for (Row row : list) {
      if (max == null) {
        max = new int[row.values.length];
      }

      for (int j = 0; j < max.length; j++) {
        max[j] = Math.max(max[j], row.sizes[j] + 1);
      }
    }

    for (Row row : list) {
      row.sizes = max;
    }
  }
}

// End BufferedRows.java

/*
 *    Copyright 2018 University of Michigan
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

package org.verdictdb.sqlreader;

import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.metastore.ScrambleMetaStore;

import java.util.Iterator;
import java.util.List;

/** Created by Dong Young Yoon on 7/31/18. */
public class ScrambleTableReplacer {
  private ScrambleMetaStore store;

  public ScrambleTableReplacer(ScrambleMetaStore store) {
    this.store = store;
  }

  public void replace(SelectQuery query) {
    List<AbstractRelation> fromList = query.getFromList();
    for (int i = 0; i < fromList.size(); i++) {
      fromList.set(i, replaceTable(fromList.get(i)));
    }
  }

  private AbstractRelation replaceTable(AbstractRelation table) {
    if (table instanceof BaseTable) {
      BaseTable bt = (BaseTable) table;
      // replace original table with its scrambled table if exists.
      if (store != null) {
        ScrambleMetaSet metaSet = store.retrieve();
        Iterator<ScrambleMeta> iterator = metaSet.iterator();
        while (iterator.hasNext()) {
          ScrambleMeta meta = iterator.next();
          // substitute names with those of the first scrambled table found.
          if (meta.getOriginalSchemaName().equals(bt.getSchemaName())
              && meta.getOriginalTableName().equals(bt.getTableName())) {
            bt.setSchemaName(meta.getSchemaName());
            bt.setTableName(meta.getTableName());
            break;
          }
        }
      }
    } else if (table instanceof JoinTable) {
      JoinTable jt = (JoinTable) table;
      for (AbstractRelation relation : jt.getJoinList()) {
        this.replaceTable(relation);
      }
    } else if (table instanceof SelectQuery) {
      SelectQuery subquery = (SelectQuery) table;
      this.replace(subquery);
    }

    return table;
  }
}

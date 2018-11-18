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

import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;

import java.util.Iterator;
import java.util.List;

/** Created by Dong Young Yoon on 7/31/18. */
public class ScrambleTableReplacer {

  //  private ScrambleMetaStore store;

  private ScrambleMetaSet metaSet;

  private int replaceCount = 0;

  private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

  public ScrambleTableReplacer(ScrambleMetaSet metaSet) {
    this.metaSet = metaSet;
  }

  public int replace(SelectQuery query) {
    return replace(query, true);
  }

  public int replace(SelectQuery query, boolean doReset) {
    if (doReset) { 
      replaceCount = 0;
    }
    
    // check select list
    BaseColumn countDistinctColumn = null;
    boolean containAggregatedItem = false;
    boolean containCountDistinctItem = false;
    for (SelectItem selectItem : query.getSelectList()) {
      if (selectItem instanceof AliasedColumn
          && ((AliasedColumn) selectItem).getColumn() instanceof ColumnOp) {
        ColumnOp opcolumn = (ColumnOp) ((AliasedColumn) selectItem).getColumn();
        if (opcolumn.doesColumnOpContainOpType("countdistinct")) {
          opcolumn.replaceAllColumnOpOpType("countdistinct", "approx_countdistinct");
          containCountDistinctItem = true;
          
          if (opcolumn.getOperand() instanceof BaseColumn) {
            countDistinctColumn = (BaseColumn) opcolumn.getOperand();
          }
        }
        if ((((AliasedColumn) selectItem).getColumn()).isAggregateColumn()) {
          containAggregatedItem = true;
        }
      }
    }
    
    // if both count-distinct and other aggregates appear
    if (containAggregatedItem && containCountDistinctItem) {
      throw new RuntimeException("This line is not supposed to be reached.");
    }
    // if no count-distinct appears and other aggregates appear
    else if (containAggregatedItem) {
      List<AbstractRelation> fromList = query.getFromList();
      for (int i = 0; i < fromList.size(); i++) {
        fromList.set(i, replaceTableForSimpleAggregates(fromList.get(i)));
      }
    }
    // if only count-distinct appears
    else if (containCountDistinctItem) {
      List<AbstractRelation> fromList = query.getFromList();
      for (int i = 0; i < fromList.size(); i++) {
        fromList.set(i, replaceTableForCountDistinct(fromList.get(i), countDistinctColumn));
      }
    }
    
    return replaceCount;
  }

  /**
   * Replaces an original table if there exists a corresponding scramble.
   * Use a hash scramble.
   * 
   * @param table
   * @return
   */
  private AbstractRelation replaceTableForCountDistinct(
      AbstractRelation table, 
      BaseColumn countDistinctColumn) {
    
    if (table instanceof BaseTable) {
      BaseTable bt = (BaseTable) table;
      
      for (ScrambleMeta meta : metaSet) {
        // substitute names with those of the first scrambled table found.
        if (meta.getOriginalSchemaName().equals(bt.getSchemaName())
            && meta.getOriginalTableName().equals(bt.getTableName())
            && meta.getMethod().equalsIgnoreCase("hash")
            && countDistinctColumn.getColumnName().equals(meta.getHashColumn())) {
          ++replaceCount;
          bt.setSchemaName(meta.getSchemaName());
          bt.setTableName(meta.getTableName());

          log.info(
              String.format(
                  "Automatic table replacement: %s.%s -> %s.%s",
                  meta.getOriginalSchemaName(),
                  meta.getOriginalTableName(),
                  meta.getSchemaName(),
                  meta.getTableName()));

          break;
        }
      }
    } else if (table instanceof JoinTable) {
      JoinTable jt = (JoinTable) table;
      for (AbstractRelation relation : jt.getJoinList()) {
        this.replaceTableForCountDistinct(relation, countDistinctColumn);
      }
    } else if (table instanceof SelectQuery) {
      SelectQuery subquery = (SelectQuery) table;
      this.replace(subquery, false);
    }
    
    return table;
  }
  
  private AbstractRelation replaceTableForSimpleAggregates(AbstractRelation table) {
    if (table instanceof BaseTable) {
      BaseTable bt = (BaseTable) table;
      
      for (ScrambleMeta meta : metaSet) {
        // substitute names with those of the first scrambled table found.
        if (meta.getOriginalSchemaName().equals(bt.getSchemaName())
            && meta.getOriginalTableName().equals(bt.getTableName())) {
          ++replaceCount;
          bt.setSchemaName(meta.getSchemaName());
          bt.setTableName(meta.getTableName());

          log.info(
              String.format(
                  "Automatic table replacement: %s.%s -> %s.%s",
                  meta.getOriginalSchemaName(),
                  meta.getOriginalTableName(),
                  meta.getSchemaName(),
                  meta.getTableName()));

          break;
        }
      }
    } else if (table instanceof JoinTable) {
      JoinTable jt = (JoinTable) table;
      for (AbstractRelation relation : jt.getJoinList()) {
        this.replaceTableForSimpleAggregates(relation);
      }
    } else if (table instanceof SelectQuery) {
      SelectQuery subquery = (SelectQuery) table;
      this.replace(subquery, false);
    }

    return table;
  }
}

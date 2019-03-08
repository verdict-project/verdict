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

import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.coordinator.SelectQueryCoordinator;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBValueException;

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

  /**
   * Replaces the tables with their corresponding scrambles if possible.
   *
   * @param query
   * @return The number of scrambles present in the query. Note that this number not only includes
   *     the scrambles added by replacements but also the scrambles that are directly specified by
   *     users.
   * @throws VerdictDBValueException
   */
  public int replaceQuery(SelectQuery query) throws VerdictDBValueException {
    return replaceQuery(query, true, null);
  }

  private int replaceQuery(
      SelectQuery query, boolean doReset, Triple<Boolean, Boolean, BaseColumn> outerInspectionInfo)
      throws VerdictDBValueException {
    if (doReset) {
      replaceCount = 0;
    }

    // check select list
    Triple<Boolean, Boolean, BaseColumn> inspectionInfo =
        SelectQueryCoordinator.inspectAggregatesInSelectList(query);
    boolean containAggregatedItem = inspectionInfo.getLeft();
    boolean containCountDistinctItem = inspectionInfo.getMiddle();
    BaseColumn countDistinctColumn = inspectionInfo.getRight();

    if (containCountDistinctItem && countDistinctColumn == null) {
      throw new VerdictDBValueException(
          "A base column is not found " + "inside the count-distinct function.");
    }

    // this is to handle the case that an outer query includes aggregate functions,
    // the current query is simply a projection.
    if (outerInspectionInfo != null && !containAggregatedItem && !containCountDistinctItem) {
      containAggregatedItem = outerInspectionInfo.getLeft();
      containCountDistinctItem = outerInspectionInfo.getMiddle();
      inspectionInfo = outerInspectionInfo;
    }

    // if both count-distinct and other aggregates appear
    if (containAggregatedItem && containCountDistinctItem) {
      throw new RuntimeException("This line is not supposed to be reached.");
    }
    // if no count-distinct appears and other aggregates appear
    else if (containAggregatedItem) {
      List<AbstractRelation> fromList = query.getFromList();
      for (int i = 0; i < fromList.size(); i++) {
        fromList.set(i, replaceTableForSimpleAggregates(fromList.get(i), inspectionInfo));
      }
    }
    // if only count-distinct appears
    else if (containCountDistinctItem) {
      List<AbstractRelation> fromList = query.getFromList();
      for (int i = 0; i < fromList.size(); i++) {
        fromList.set(i, replaceTableForCountDistinct(fromList.get(i), inspectionInfo));
      }
    }
    // no aggregate appears; check any subqueries
    else {
      List<AbstractRelation> fromList = query.getFromList();
      for (int i = 0; i < fromList.size(); i++) {
        AbstractRelation rel = fromList.get(i);
        if (rel instanceof JoinTable) {
          for (AbstractRelation joined : ((JoinTable) rel).getJoinList()) {
            if (joined instanceof SelectQuery) {
              replaceQuery((SelectQuery) joined, false, null);
            }
          }
        } else if (rel instanceof SelectQuery) {
          replaceQuery((SelectQuery) rel, false, null);
        }
      }
    }

    return replaceCount;
  }

  /**
   * Replaces an original table if there exists a corresponding scramble. Use a hash scramble.
   *
   * @param table
   * @return
   * @throws VerdictDBValueException
   */
  private AbstractRelation replaceTableForCountDistinct(
      AbstractRelation table, Triple<Boolean, Boolean, BaseColumn> inspectionInfo)
      throws VerdictDBValueException {

    BaseColumn countDistinctColumn = inspectionInfo.getRight();

    if (table instanceof BaseTable) {
      BaseTable bt = (BaseTable) table;

      for (ScrambleMeta meta : metaSet) {
        // Detects scrambled tables.
        // The scramble meta are expected to be retrieved from the recently created ones.
        if (meta.getSchemaName().equals(bt.getSchemaName())
            && meta.getTableName().equals(bt.getTableName())
            && meta.getMethodWithDefault("uniform").equalsIgnoreCase("hash")
            && countDistinctColumn.getColumnName().equals(meta.getHashColumn())) {
          ++replaceCount;

          log.info(
              String.format("Scramble detected: %s.%s", meta.getSchemaName(), meta.getTableName()));

          //          bt.setSchemaName(meta.getSchemaName());
          //          bt.setTableName(meta.getTableName());

          //          log.info(
          //              String.format(
          //                  "Automatic table replacement: %s.%s -> %s.%s",
          //                  meta.getOriginalSchemaName(),
          //                  meta.getOriginalTableName(),
          //                  meta.getSchemaName(),
          //                  meta.getTableName()));

          break;
        } else if (meta.getSchemaName().equals(bt.getSchemaName())
            && meta.getTableName().equals(bt.getTableName())) {
          // IF the scramble is directly specified.
          ++replaceCount;
        }
      }
    } else if (table instanceof JoinTable) {
      JoinTable jt = (JoinTable) table;
      for (AbstractRelation relation : jt.getJoinList()) {
        this.replaceTableForCountDistinct(relation, inspectionInfo);
      }
    } else if (table instanceof SelectQuery) {
      SelectQuery subquery = (SelectQuery) table;
      this.replaceQuery(subquery, false, inspectionInfo);
    }

    return table;
  }

  private AbstractRelation replaceTableForSimpleAggregates(
      AbstractRelation table, Triple<Boolean, Boolean, BaseColumn> inspectionInfo)
      throws VerdictDBValueException {
    if (table instanceof BaseTable) {
      BaseTable bt = (BaseTable) table;

      for (ScrambleMeta meta : metaSet) {
        // Detects scrambled tables.
        if (meta.getSchemaName().equals(bt.getSchemaName())
            && meta.getTableName().equals(bt.getTableName())
            && meta.isMethodCompatibleWithSimpleAggregates()) {
          ++replaceCount;
          log.info(
              String.format("Scramble detected: %s.%s", meta.getSchemaName(), meta.getTableName()));

          //          bt.setSchemaName(meta.getSchemaName());
          //          bt.setTableName(meta.getTableName());
          //
          //          log.info(
          //              String.format(
          //                  "Automatic table replacement: %s.%s -> %s.%s",
          //                  meta.getOriginalSchemaName(),
          //                  meta.getOriginalTableName(),
          //                  meta.getSchemaName(),
          //                  meta.getTableName()));
          break;
        } else if (meta.getSchemaName().equals(bt.getSchemaName())
            && meta.getTableName().equals(bt.getTableName())) {
          // IF the scramble is directly specified.
          ++replaceCount;
        }
      }
    } else if (table instanceof JoinTable) {
      JoinTable jt = (JoinTable) table;
      for (AbstractRelation relation : jt.getJoinList()) {
        this.replaceTableForSimpleAggregates(relation, inspectionInfo);
      }
    } else if (table instanceof SelectQuery) {
      SelectQuery subquery = (SelectQuery) table;
      this.replaceQuery(subquery, false, inspectionInfo);
    }

    return table;
  }
}

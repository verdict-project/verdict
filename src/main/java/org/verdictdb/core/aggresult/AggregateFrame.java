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

package org.verdictdb.core.aggresult;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.DbmsQueryResultMetaData;
import org.verdictdb.core.rewriter.aggresult.AggNameAndType;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * Represents a data frame (after aggregation).
 *
 * @author Yongjoo Park
 */
public class AggregateFrame implements Serializable {

  String aggFrameType = "raw";

  List<String> orderedColumnNames;

  List<Integer> columnTypes = new ArrayList<>();

  List<String> columnTypeNames = new ArrayList<>();

  DbmsQueryResultMetaData dbmsQueryResultMetaData = new DbmsQueryResultMetaData();

  Map<AggregateGroup, AggregateMeasures> data = new HashMap<>();

  public AggregateFrame(List<String> orderedColumnNames) throws VerdictDBValueException {
    this.orderedColumnNames = orderedColumnNames;
    Set<String> colNames = new HashSet<>(orderedColumnNames);
    if (colNames.size() != orderedColumnNames.size()) {
      throw new VerdictDBValueException("The column names seem to include duplicates.");
    }
  }

  public static AggregateFrame empty() {
    try {
      return new AggregateFrame(new ArrayList<String>());
    } catch (VerdictDBValueException e) {
      return null;
    }
  }

  public static AggregateFrame fromDmbsQueryResult(
      DbmsQueryResult result, List<String> nonaggColumns, List<AggNameAndType> aggColumns)
      throws VerdictDBValueException {

    List<String> colName = new ArrayList<>();
    List<String> nonaggColumnsName = new ArrayList<>();
    List<String> aggColumnsName = new ArrayList<>();

    for (String col : nonaggColumns) {
      nonaggColumnsName.add(col.toLowerCase());
    }
    for (AggNameAndType pair : aggColumns) {
      aggColumnsName.add(pair.getName().toLowerCase());
    }
    HashSet<String> aggColumnsSet = new HashSet<>(aggColumnsName);
    HashSet<String> nonaggColumnsSet = new HashSet<>(nonaggColumnsName);
    List<Integer> aggColumnIndex = new ArrayList<>();
    List<Integer> nonaggColumnIndex = new ArrayList<>();
    List<String> orderedAggColumnName = new ArrayList<>();
    List<String> orderedNonaggColumnName = new ArrayList<>();
    List<Integer> columnTypes = new ArrayList<>();
    List<String> columnTypeNames = new ArrayList<>();

    // Get Ordered column name for nonAgg and Agg
    for (int i = 0; i < result.getColumnCount(); i++) {
      String col = result.getColumnName(i).toLowerCase();
      colName.add(col);
      columnTypes.add(result.getColumnType(i));
      columnTypeNames.add(result.getColumnTypeName(i));
      if (aggColumnsSet.contains(col)) {
        orderedAggColumnName.add(col);
        aggColumnIndex.add(i);
      } else if (nonaggColumnsSet.contains(col)) {
        orderedNonaggColumnName.add(col);
        nonaggColumnIndex.add(i);
      } else {
        throw new VerdictDBValueException(
            String.format(
                "An existing column (%s) does not belong to any of specified columns.", col));
      }
    }

    AggregateFrame aggregateFrame = new AggregateFrame(colName);
    aggregateFrame.setColumnTypes(columnTypes);
    aggregateFrame.setColumnTypeNames(columnTypeNames);
    aggregateFrame.dbmsQueryResultMetaData = result.getMetaData();

    while (result.next()) {
      List<Object> aggValue = new ArrayList<>();
      List<Object> nonaggValue = new ArrayList<>();
      for (int i : aggColumnIndex) {
        aggValue.add(result.getValue(i));
      }
      for (int i : nonaggColumnIndex) {
        nonaggValue.add(result.getValue(i));
      }
      aggregateFrame.addRow(
          new AggregateGroup(orderedNonaggColumnName, nonaggValue),
          new AggregateMeasures(orderedAggColumnName, aggValue));
    }
    return aggregateFrame;
  }

  public DbmsQueryResult toDbmsQueryResult() {
    return new AggregateFrameQueryResult(this);
  }

  public void setColumnTypes(List<Integer> columnTypes) {
    this.columnTypes = columnTypes;
  }

  public void setColumnTypeNames(List<String> columnTypeNames) {
    this.columnTypeNames = columnTypeNames;
  }

  public List<Integer> getColumnTypes() {
    return columnTypes;
  }

  public List<String> getColumnTypeNames() {
    return columnTypeNames;
  }

  public List<String> getColumnNames() {
    return orderedColumnNames;
  }

  public void addRow(AggregateGroup group, AggregateMeasures measures) {
    data.put(group, measures);
  }

  public void addRow(AggregateMeasures measures) {
    data.put(AggregateGroup.empty(), measures);
  }

  public AggregateMeasures getMeasures(AggregateGroup group) {
    return data.get(group);
  }

  public AggregateMeasures getMeasures() throws VerdictDBValueException {
    if (data.size() > 1) {
      throw new VerdictDBValueException(
          "The number of rows is larger than 1. A group must be specified.");
    }
    return data.get(AggregateGroup.empty());
  }

  public Set<Entry<AggregateGroup, AggregateMeasures>> groupAndMeasuresSet() {
    return data.entrySet();
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}

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

package org.verdictdb.core.scrambling;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ScrambleMetaSet implements Serializable, Iterable<ScrambleMeta> {

  private static final long serialVersionUID = 5106301901144427405L;

  Map<Pair<String, String>, ScrambleMeta> meta = new HashMap<>();

  public ScrambleMetaSet() {}

  /**
   * Returns the column name used for indicating aggregation block. Typically, the underlying
   * database is physically partitioned on this column to speed up accessing particular blocks.
   *
   * @param schemaName
   * @param tableName
   * @return
   */
  public String getAggregationBlockColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getAggregationBlockColumn();
  }

  public ScrambleMeta getSingleMeta(String schemaName, String tableName) {
    return meta.get(new ImmutablePair<String, String>(schemaName, tableName));
  }

  /**
   * Returns the number of the blocks for a specified scrambled table.
   *
   * @param schemaName
   * @param tableName
   * @return
   */
  public int getAggregationBlockCount(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getAggregationBlockCount();
  }

  @Deprecated
  public String getSubsampleColumn(String aliasName) {
    return meta.get(metaKey(aliasName)).getSubsampleColumn();
  }

  public String getSubsampleColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getSubsampleColumn();
  }

  @Deprecated
  public String getTierColumn(String aliasName) {
    return meta.get(metaKey(aliasName)).getTierColumn();
  }

  public String getTierColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getTierColumn();
  }

  /**
   * Add a new scramble meta entry. If there exists an existing entry for the same scrambled table,
   * the value is overwritten.
   *
   * @param tablemeta
   */
  public void addScrambleMeta(ScrambleMeta tablemeta) {
    String schema = tablemeta.getSchemaName();
    String table = tablemeta.getTableName();
    meta.put(metaKey(schema, table), tablemeta);
  }

  @Deprecated
  public void insertScrambleMetaEntry(
      String aliasName,
      //      String inclusionProbabilityColumn,
      //      String inclusionProbBlockDiffColumn,
      String subsampleColumn,
      String tierColumn) {
    ScrambleMeta tableMeta = new ScrambleMeta();
    //    tableMeta.setAliasName(aliasName);
    //    tableMeta.setAggregationBlockColumn(aggregationBlockColumn);
    tableMeta.setSubsampleColumn(subsampleColumn);
    tableMeta.setTierColumn(tierColumn);
    //    tableMeta.setInclusionProbabilityColumn(inclusionProbabilityColumn);
    //    tableMeta.setInclusionProbabilityBlockDifferenceColumn(inclusionProbBlockDiffColumn);
    //    tableMeta.setAggregationBlockCount(aggregationBlockCount);
    meta.put(metaKey(aliasName), tableMeta);
  }

  public void insertScrambleMetaEntry(
      String schemaName,
      String tableName,
      String aggregationBlockColumn,
      String subsampleColumn,
      String tierColumn,
      int aggregationBlockCount) {
    ScrambleMeta tableMeta = new ScrambleMeta();
    tableMeta.setSchemaName(schemaName);
    tableMeta.setTableName(tableName);
    tableMeta.setAggregationBlockColumn(aggregationBlockColumn);
    tableMeta.setSubsampleColumn(subsampleColumn);
    tableMeta.setTierColumn(tierColumn);
    tableMeta.setAggregationBlockCount(aggregationBlockCount);
    meta.put(metaKey(schemaName, tableName), tableMeta);
  }

  @Deprecated
  public boolean isScrambled(String aliasName) {
    return meta.containsKey(metaKey(aliasName));
  }

  public boolean isScrambled(String schemaName, String tableName) {
    return meta.containsKey(metaKey(schemaName, tableName));
  }

  @Deprecated
  private Pair<String, String> metaKey(String aliasName) {
    return Pair.of("aliasName", aliasName);
  }

  private Pair<String, String> metaKey(String schemaName, String tableName) {
    return Pair.of(schemaName, tableName);
  }

  @Override
  public Iterator<ScrambleMeta> iterator() {
    return meta.values().iterator();
  }
}

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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

/**
 * A set of scrambleMeta items.
 * 
 * The internal order of ScrambleMeta items is equal to the insertion order.
 * @author pyongjoo
 *
 */
public class ScrambleMetaSet implements Serializable, Iterable<ScrambleMeta> {

  private static final long serialVersionUID = 5106301901144427405L;

  // key: (schema, table) of a scramble
  // value: meta information
  List<Pair<Pair<String, String>, ScrambleMeta>> metaSet = new ArrayList<>();

  public ScrambleMetaSet() {}

  public static ScrambleMetaSet createFromCollection(Collection<ScrambleMeta> metaList) {
    ScrambleMetaSet metaset = new ScrambleMetaSet();
    for (ScrambleMeta meta : metaList) {
      metaset.addScrambleMeta(meta);
    }
    return metaset;
  }
  
  private ScrambleMeta getMetaFor(Pair<String, String> metakey) {
    for (Pair<Pair<String, String>, ScrambleMeta> item : metaSet) {
      Pair<String, String> key = item.getKey();
      ScrambleMeta m = item.getValue();
      if (key.equals(metakey)) {
        return m;
      }
    }
    return null;
  }
  
  private boolean doesContain(Pair<String, String> metakey) {
    for (Pair<Pair<String, String>, ScrambleMeta> item : metaSet) {
      Pair<String, String> key = item.getKey();
      if (key.equals(metakey)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the column name used for indicating aggregation block. Typically, the underlying
   * database is physically partitioned on this column to speed up accessing particular blocks.
   *
   * @param schemaName
   * @param tableName
   * @return
   */
  public String getAggregationBlockColumn(String schemaName, String tableName) {
    return getSingleMeta(schemaName, tableName).getAggregationBlockColumn();
  }

  public ScrambleMeta getSingleMeta(String schemaName, String tableName) {
    return getMetaFor(getMetaKey(schemaName, tableName));
  }

  /**
   * Returns the number of the blocks for a specified scrambled table.
   *
   * @param schemaName
   * @param tableName
   * @return
   */
  public int getAggregationBlockCount(String schemaName, String tableName) {
    return getSingleMeta(schemaName, tableName).getAggregationBlockCount();
  }

  @Deprecated
  public String getSubsampleColumn(String aliasName) {
    return getMetaFor(metaKey(aliasName)).getSubsampleColumn();
  }

  public String getSubsampleColumn(String schemaName, String tableName) {
    return getMetaFor(getMetaKey(schemaName, tableName)).getSubsampleColumn();
  }

  @Deprecated
  public String getTierColumn(String aliasName) {
    return getMetaFor(metaKey(aliasName)).getTierColumn();
  }

  public String getTierColumn(String schemaName, String tableName) {
    return getSingleMeta(schemaName, tableName).getTierColumn();
  }
  
  public String getScramblingMethod(String schemaName, String tableName) {
    return getSingleMeta(schemaName, tableName).getMethodWithDefault("uniform");
  }
  
  public String getHashColumn(String schemaName, String tableName) {
    return getSingleMeta(schemaName, tableName).getHashColumn();
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
    metaSet.add(Pair.of(getMetaKey(schema, table), tablemeta));
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
    metaSet.add(Pair.of(metaKey(aliasName), tableMeta));
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
    metaSet.add(Pair.of(getMetaKey(schemaName, tableName), tableMeta));
  }

  @Deprecated
  public boolean isScrambled(String aliasName) {
    return doesContain(metaKey(aliasName));
  }

  public boolean isScrambled(String schemaName, String tableName) {
    return doesContain(getMetaKey(schemaName, tableName));
  }

  @Deprecated
  private Pair<String, String> metaKey(String aliasName) {
    return Pair.of("aliasName", aliasName);
  }

  private Pair<String, String> getMetaKey(String schemaName, String tableName) {
    return Pair.of(schemaName, tableName);
  }

  @Override
  public Iterator<ScrambleMeta> iterator() {
    List<ScrambleMeta> metas = new ArrayList<>();
    for (Pair<Pair<String, String>, ScrambleMeta> item : metaSet) {
      metas.add(item.getValue());
    }
    return metas.iterator();
  }

  public ScrambleMeta getMetaForTable(String schema, String table) {
    return getSingleMeta(schema, table);
  }
}

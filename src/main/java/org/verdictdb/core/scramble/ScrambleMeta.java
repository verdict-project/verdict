package org.verdictdb.core.scramble;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class ScrambleMeta {

  Map<Pair<String, String>, ScrambleMetaForTable> meta = new HashMap<>();

  public ScrambleMeta() {}

  /**
   * Returns the column name used for indicating aggregation block. Typically, the underlying database
   * is physically partitioned on this column to speed up accessing particular blocks.
   * 
   * @param schemaName
   * @param tableName
   * @return
   */
  public String getAggregationBlockColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getAggregationBlockColumn();
  }

  public ScrambleMetaForTable getMetaForTable(String schemaName, String tableName) {
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
  
  public void insertScrambleMetaEntry(ScrambleMetaForTable tablemeta) {
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
    ScrambleMetaForTable tableMeta = new ScrambleMetaForTable();
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
    ScrambleMetaForTable tableMeta = new ScrambleMetaForTable();
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
}



package org.verdictdb.core.rewriter;

import java.util.HashMap;
import java.util.Map;

public class ScrambleMeta {

  Map<String, ScrambleMetaForTable> meta = new HashMap<>();

  public ScrambleMeta() {}

  public String getAggregationBlockColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getAggregationBlockColumn();
  }

  public int getAggregationBlockCount(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getAggregationBlockCount();
  }

  public String getSubsampleColumn(String aliasName) {
    return meta.get(metaKey(aliasName)).getSubsampleColumn();
  }

  public String getSubsampleColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getSubsampleColumn();
  }

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

  public void insertScrambleMetaEntry(
      String aliasName,
//      String inclusionProbabilityColumn,
//      String inclusionProbBlockDiffColumn,
      String subsampleColumn,
      String tierColumn) {
    ScrambleMetaForTable tableMeta = new ScrambleMetaForTable();
    tableMeta.setAliasName(aliasName);
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
//      String inclusionProbabilityColumn,
//      String inclusionProbBlockDiffColumn,
      String subsampleColumn,
      String tierColumn,
      int aggregationBlockCount) {
    ScrambleMetaForTable tableMeta = new ScrambleMetaForTable();
    tableMeta.setSchemaName(schemaName);
    tableMeta.setTableName(tableName);
    tableMeta.setAggregationBlockColumn(aggregationBlockColumn);
    tableMeta.setSubsampleColumn(subsampleColumn);
    tableMeta.setTierColumn(tierColumn);
    //    tableMeta.setInclusionProbabilityColumn(inclusionProbabilityColumn);
    //    tableMeta.setInclusionProbabilityBlockDifferenceColumn(inclusionProbBlockDiffColumn);
    tableMeta.setAggregationBlockCount(aggregationBlockCount);
    meta.put(metaKey(schemaName, tableName), tableMeta);
  }

  public boolean isScrambled(String aliasName) {
    return meta.containsKey(metaKey(aliasName));
  }

  public boolean isScrambled(String schemaName, String tableName) {
    return meta.containsKey(metaKey(schemaName, tableName));
  }

  private String metaKey(String aliasName) {
    return aliasName;
  }

  private String metaKey(String schemaName, String tableName) {
    return schemaName + "#" + tableName;
  }
}



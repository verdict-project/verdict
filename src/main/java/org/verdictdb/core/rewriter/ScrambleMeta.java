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

  //  public String getInclusionProbabilityBlockDifferenceColumn(String aliasName) {
  //    return meta.get(metaKey(aliasName)).getInclusionProbabilityBlockDifferenceColumn();
  //  }
  //
  //  public String getInclusionProbabilityBlockDifferenceColumn(String schemaName, String tableName) {
  //    return meta.get(metaKey(schemaName, tableName)).getInclusionProbabilityBlockDifferenceColumn();
  //  }
  //
  //  public String getInclusionProbabilityColumn(String aliasName) {
  //    return meta.get(metaKey(aliasName)).getInclusionProbabilityColumn();
  //  }
  //
  //  public String getInclusionProbabilityColumn(String schemaName, String tableName) {
  //    return meta.get(metaKey(schemaName, tableName)).getInclusionProbabilityColumn();
  //  }

  //  public List<String> getPartitionAttributes(String schemaName, String tableName) {
  //    return meta.get(metaKey(schemaName, tableName)).getPartitionAttributes();
  //  }

  //  public String getPartitionColumn(String schemaName, String tableName) {
  //    return meta.get(metaKey(schemaName, tableName)).getPartitionColumn();
  //  }

  //  public int getPartitionCount(String schemaName, String tableName) {
  //    return meta.get(metaKey(schemaName, tableName)).getPartitionCount();
  //  }

  public String getSubsampleColumn(String aliasName) {
    return meta.get(metaKey(aliasName)).getSubsampleColumn();
  }

  public String getSubsampleColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getSubsampleColumn();
  }

  //  public String getInclusionProbabilityBlockDifferenceColumn(String aliasName) {
  //    return meta.get(metaKey(aliasName)).getInclusionProbabilityBlockDifferenceColumn();
  //  }
  //
  //  public String getInclusionProbabilityBlockDifferenceColumn(String schemaName, String tableName) {
  //    return meta.get(metaKey(schemaName, tableName)).getInclusionProbabilityBlockDifferenceColumn();
  //  }
  //
  //  public String getInclusionProbabilityColumn(String aliasName) {
  //    return meta.get(metaKey(aliasName)).getInclusionProbabilityColumn();
  //  }
  //
  //  public String getInclusionProbabilityColumn(String schemaName, String tableName) {
  //    return meta.get(metaKey(schemaName, tableName)).getInclusionProbabilityColumn();
  //  }

  public String getTierColumn(String aliasName) {
    return meta.get(metaKey(aliasName)).getTierColumn();
  }

  public String getTierColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getTierColumn();
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

  //  public void insertScrumbleMetaEntry(
  //      String schemaName,
  //      String tableName,
  //      String partitionColumn,
  //      String inclusionProbabilityColumn,
  //      String subsampleColumn,
  //      List<String> partitionAttributeValues) {
  //    ScrambleMetaForTable tableMeta = new ScrambleMetaForTable();
  //    tableMeta.setSchemaName(schemaName);
  //    tableMeta.setTableName(tableName);
  //    tableMeta.setPartitionColumn(partitionColumn);
  //    tableMeta.setSubsampleColumn(subsampleColumn);
  //    tableMeta.setInclusionProbabilityColumn(inclusionProbabilityColumn);
  //
  //    for (String v : partitionAttributeValues) {
  //      tableMeta.addPartitionAttributeValue(v);
  //    }
  //
  //    meta.put(metaKey(schemaName, tableName), tableMeta);
  //  }

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



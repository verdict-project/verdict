package org.verdictdb.core.rewriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScrambleMeta {

  Map<String, ScrambleMetaForTable> meta = new HashMap<>();

  public ScrambleMeta() {}

  public void insertScrambleMetaEntry(
      String schemaName,
      String tableName,
      String aggregationBlockColumn,
      String inclusionProbabilityColumn,
      String inclusionProbBlockDiffColumn,
      String subsampleColumn,
      int aggregationBlockCount) {
    ScrambleMetaForTable tableMeta = new ScrambleMetaForTable();
    tableMeta.setSchemaName(schemaName);
    tableMeta.setTableName(tableName);
    tableMeta.setAggregationBlockColumn(aggregationBlockColumn);
    tableMeta.setSubsampleColumn(subsampleColumn);
    tableMeta.setInclusionProbabilityColumn(inclusionProbabilityColumn);
    tableMeta.setInclusionProbabilityBlockDifferenceColumn(inclusionProbBlockDiffColumn);
    tableMeta.setAggregationBlockCount(aggregationBlockCount);
    meta.put(metaKey(schemaName, tableName), tableMeta);
  }

  public void insertScrambleMetaEntry(
      String aliasName,
      String inclusionProbabilityColumn,
      String inclusionProbBlockDiffColumn,
      String subsampleColumn) {
    ScrambleMetaForTable tableMeta = new ScrambleMetaForTable();
    tableMeta.setAliasName(aliasName);
    //    tableMeta.setAggregationBlockColumn(aggregationBlockColumn);
    tableMeta.setSubsampleColumn(subsampleColumn);
    tableMeta.setInclusionProbabilityColumn(inclusionProbabilityColumn);
    tableMeta.setInclusionProbabilityBlockDifferenceColumn(inclusionProbBlockDiffColumn);
    //    tableMeta.setAggregationBlockCount(aggregationBlockCount);
    meta.put(metaKey(aliasName), tableMeta);
  }

  private String metaKey(String schemaName, String tableName) {
    return schemaName + "#" + tableName;
  }

  private String metaKey(String aliasName) {
    return aliasName;
  }

  public String getAggregationBlockColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getAggregationBlockColumn();
  }

  public int getAggregationBlockCount(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getAggregationBlockCount();
  }

  public String getInclusionProbabilityColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getInclusionProbabilityColumn();
  }
  
  public String getInclusionProbabilityColumn(String aliasName) {
    return meta.get(metaKey(aliasName)).getInclusionProbabilityColumn();
  }

  public String getSubsampleColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getSubsampleColumn();
  }

  public String getSubsampleColumn(String aliasName) {
    return meta.get(metaKey(aliasName)).getSubsampleColumn();
  }

  public String getInclusionProbabilityBlockDifferenceColumn(String schemaName, String tableName) {
    return meta.get(metaKey(schemaName, tableName)).getInclusionProbabilityBlockDifferenceColumn();
  }
  
  public String getInclusionProbabilityBlockDifferenceColumn(String aliasName) {
    return meta.get(metaKey(aliasName)).getInclusionProbabilityBlockDifferenceColumn();
  }

  public boolean isScrambled(String schemaName, String tableName) {
    return meta.containsKey(metaKey(schemaName, tableName));
  }

  public boolean isScrambled(String aliasName) {
    return meta.containsKey(metaKey(aliasName));
  }

}


/**
 * Table-specific information
 * @author Yongjoo Park
 *
 */
class ScrambleMetaForTable {

  String schemaName;

  String tableName;

  String aliasName;

  String aggregationBlockColumn;

  String inclusionProbabilityColumn;

  String inclusionProbabilityBlockDifferenceColumn;

  String subsampleColumn;

  int aggregationBlockCount;


  public ScrambleMetaForTable() {}

  public String getInclusionProbabilityBlockDifferenceColumn() {
    return inclusionProbabilityBlockDifferenceColumn;
  }

  public void setAggregationBlockCount(int aggregationBlockCount) {
    this.aggregationBlockCount = aggregationBlockCount;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getAliasName() {
    return aliasName;
  }

  public void setAliasName(String aliasName) {
    this.aliasName = aliasName;
  }

  public int getAggregationBlockCount() {
    return aggregationBlockCount;
  }

  public String getAggregationBlockColumn() {
    return aggregationBlockColumn;
  }

  public void setAggregationBlockColumn(String aggregationBlockColumn) {
    this.aggregationBlockColumn = aggregationBlockColumn;
  }

  public String getInclusionProbabilityColumn() {
    return inclusionProbabilityColumn;
  }

  public void setInclusionProbabilityColumn(String inclusionProbabilityColumn) {
    this.inclusionProbabilityColumn = inclusionProbabilityColumn;
  }

  public void setInclusionProbabilityBlockDifferenceColumn(String inclusionProbabilityBlockDifferenceColumn) {
    this.inclusionProbabilityBlockDifferenceColumn = inclusionProbabilityBlockDifferenceColumn;
  }

  public void setSubsampleColumn(String subsampleColumn) {
    this.subsampleColumn = subsampleColumn;
  }

  public String getSubsampleColumn() {
    return subsampleColumn;
  }
}

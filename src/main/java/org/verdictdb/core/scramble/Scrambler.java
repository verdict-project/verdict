package org.verdictdb.core.scramble;

import org.verdictdb.core.rewriter.ScrambleMetaForTable;

public class Scrambler {
  
  String originalSchemaName;
  
  String originalTableName;
  
  String scrambledSchemaName;
  
  String scrambledTableName;
  
  int aggregationBlockCount;
  
  // Configuration parameters
  String aggregationBlockColumn = "verdictAggBlock";
  
  String inclusionProbabilityColumn = "verdictIncProb";
  
  String inclusionProbabilityBlockDifferenceColumn = "verdictIncProbBlockDiff";
  
  String subsampleColumn = "verdictSubsampleId";
  
  public Scrambler(
      String originalSchemaName, String originalTableName,
      String scrambledSchemaName, String scrambledTableName,
      int aggregationBlockCount) {
    this.originalSchemaName = originalSchemaName;
    this.originalTableName = originalTableName;
    this.scrambledSchemaName = scrambledSchemaName;
    this.scrambledTableName = scrambledTableName;
    this.aggregationBlockCount = aggregationBlockCount;
  }
  
  ScrambleMetaForTable generateMeta() {
    ScrambleMetaForTable meta = new ScrambleMetaForTable();
    meta.setSchemaName(scrambledSchemaName);
    meta.setTableName(scrambledTableName);
    meta.setAggregationBlockCount(aggregationBlockCount);
    meta.setAggregationBlockColumn(aggregationBlockColumn);
    meta.setInclusionProbabilityColumn(inclusionProbabilityColumn);
    meta.setInclusionProbabilityBlockDifferenceColumn(inclusionProbabilityBlockDifferenceColumn);
    meta.setSubsampleColumn(subsampleColumn);
    return meta;
  }

}

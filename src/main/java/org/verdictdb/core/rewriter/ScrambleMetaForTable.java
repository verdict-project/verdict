package org.verdictdb.core.rewriter;

/**
 * Table-specific information
 * @author Yongjoo Park
 *
 */
public class ScrambleMetaForTable {
  
  // Key is either (schemaName, tableName) or aliasName

  String schemaName;

  String tableName;
  
  String aliasName;

//  String partitionColumn;
//  List<String> partitionAttributeValues = new ArrayList<>();

  String inclusionProbabilityColumn;

  String subsampleColumn;

  String aggregationBlockColumn;

  String inclusionProbabilityBlockDifferenceColumn;

  int aggregationBlockCount;

  public ScrambleMetaForTable() {}

//  public void addPartitionAttributeValue(String partitionAttributeValue) {
//    partitionAttributeValues.add(partitionAttributeValue);
//  }

  //  public void setPartitionColumn(String partitionColumn) {
  //    this.partitionColumn = partitionColumn;
  //  }
  
    public void setSchemaName(String schemaName) {
      this.schemaName = schemaName;
    }

  public void setTableName(String tableName) {
      this.tableName = tableName;
    }

  //  public void setPartitionColumn(String partitionColumn) {
  //    this.partitionColumn = partitionColumn;
  //  }
  
    public void setAliasName(String aliasName) {
    this.aliasName = aliasName;
  }

  public void setAggregationBlockColumn(String aggregationBlockColumn) {
    this.aggregationBlockColumn = aggregationBlockColumn;
  }

  //  public List<String> getPartitionAttributes() {
  //    return partitionAttributeValues;
  //  }
  //
  //  public String getPartitionAttributeValue(int i) {
  //    return partitionAttributeValues.get(i);
  //  }
  
  //  public String getPartitionColumn() {
  //    return partitionColumn;
  //  }
  
  //  public int getPartitionCount() {
  //    return partitionAttributeValues.size();
  //  }
  
    public void setAggregationBlockCount(int aggregationBlockCount) {
    this.aggregationBlockCount = aggregationBlockCount;
  }

  public void setInclusionProbabilityColumn(String inclusionProbabilityColumn) {
    this.inclusionProbabilityColumn = inclusionProbabilityColumn;
  }

  public void setInclusionProbabilityBlockDifferenceColumn(String inclusionProbabilityBlockDifferenceColumn) {
    this.inclusionProbabilityBlockDifferenceColumn = inclusionProbabilityBlockDifferenceColumn;
  }

  //  public void setPartitionColumn(String partitionColumn) {
  //    this.partitionColumn = partitionColumn;
  //  }
  
    public void setSubsampleColumn(String subsampleColumn) {
      this.subsampleColumn = subsampleColumn;
    }

  //  public List<String> getPartitionAttributes() {
    //    return partitionAttributeValues;
    //  }
    //
    //  public String getPartitionAttributeValue(int i) {
    //    return partitionAttributeValues.get(i);
    //  }
    
    //  public String getPartitionColumn() {
    //    return partitionColumn;
    //  }
    
    //  public int getPartitionCount() {
    //    return partitionAttributeValues.size();
    //  }
    
      public String getSchemaName() {
        return schemaName;
      }

    public String getTableName() {
      return tableName;
    }

    public String getAliasName() {
      return aliasName;
    }

  public String getAggregationBlockColumn() {
    return aggregationBlockColumn;
  }

  public int getAggregationBlockCount() {
    return aggregationBlockCount;
  }

  public String getInclusionProbabilityColumn() {
    return inclusionProbabilityColumn;
  }

  public String getInclusionProbabilityBlockDifferenceColumn() {
    return inclusionProbabilityBlockDifferenceColumn;
  }

  

//  public List<String> getPartitionAttributes() {
//    return partitionAttributeValues;
//  }
//
//  public String getPartitionAttributeValue(int i) {
//    return partitionAttributeValues.get(i);
//  }

//  public String getPartitionColumn() {
//    return partitionColumn;
//  }

//  public int getPartitionCount() {
//    return partitionAttributeValues.size();
//  }

  public String getSubsampleColumn() {
    return subsampleColumn;
  }

  

  

//  public void setPartitionColumn(String partitionColumn) {
//    this.partitionColumn = partitionColumn;
//  }

}


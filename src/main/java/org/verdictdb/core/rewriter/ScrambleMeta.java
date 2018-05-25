package org.verdictdb.core.rewriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScrambleMeta {
    
    Map<String, ScrambleMetaForTable> meta = new HashMap<>();
    
    public ScrambleMeta() {}
    
    public void insertScrumbleMetaEntry(
            String schemaName,
            String tableName,
            String partitionColumn,
            String inclusionProbabilityColumn,
            String subsampleColumn,
            List<String> partitionAttributeValues) {
        ScrambleMetaForTable tableMeta = new ScrambleMetaForTable();
        tableMeta.setSchemaName(schemaName);
        tableMeta.setTableName(tableName);
        tableMeta.setPartitionColumn(partitionColumn);
        tableMeta.setSubsampleColumn(subsampleColumn);
        tableMeta.setInclusionProbabilityColumn(inclusionProbabilityColumn);
        
        for (String v : partitionAttributeValues) {
            tableMeta.addPartitionAttributeValue(v);
        }
        
        meta.put(metaKey(schemaName, tableName), tableMeta);
    }
    
    private String metaKey(String schemaName, String tableName) {
        return schemaName + "#" + tableName;
    }
    
    public String getPartitionColumn(String schemaName, String tableName) {
        return meta.get(metaKey(schemaName, tableName)).getPartitionColumn();
    }
    
    public int getPartitionCount(String schemaName, String tableName) {
        return meta.get(metaKey(schemaName, tableName)).getPartitionCount();
    }
    
    public String getInclusionProbabilityColumn(String schemaName, String tableName) {
        return meta.get(metaKey(schemaName, tableName)).getInclusionProbabilityColumn();
    }
    
    public List<String> getPartitionAttributes(String schemaName, String tableName) {
        return meta.get(metaKey(schemaName, tableName)).getPartitionAttributes();
    }

    public String getSubsampleColumn(String schemaName, String tableName) {
        return meta.get(metaKey(schemaName, tableName)).getSubsampleColumn();
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
    
    String partitionColumn;
    
    List<String> partitionAttributeValues = new ArrayList<>();
    
    String inclusionProbabilityColumn;
    
    String subsampleColumn;
    
    
    public ScrambleMetaForTable() {}
    
    public void addPartitionAttributeValue(String partitionAttributeValue) {
        partitionAttributeValues.add(partitionAttributeValue);
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

    public int getPartitionCount() {
        return partitionAttributeValues.size();
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }
    
    public String getPartitionAttributeValue(int i) {
        return partitionAttributeValues.get(i);
    }

    public String getInclusionProbabilityColumn() {
        return inclusionProbabilityColumn;
    }

    public void setInclusionProbabilityColumn(String inclusionProbabilityColumn) {
        this.inclusionProbabilityColumn = inclusionProbabilityColumn;
    }
    
    public List<String> getPartitionAttributes() {
        return partitionAttributeValues;
    }
    
    public void setSubsampleColumn(String subsampleColumn) {
        this.subsampleColumn = subsampleColumn;
    }

    public String getSubsampleColumn() {
        return subsampleColumn;
    }
}

package org.verdictdb.core.rewriter;

import java.util.HashMap;
import java.util.Map;

public class ScrambleMeta {
    
    Map<String, ScrambleMetaForTable> meta = new HashMap<>();
    
    public ScrambleMeta() {}
    
    public void insertScrumbleMetaEntry(
            String schemaName,
            String tableName,
            String partitionColumn,
            String inclusionProbabilityColumn,
            int partitionCount) {
        ScrambleMetaForTable tableMeta = new ScrambleMetaForTable();
        tableMeta.setSchemaName(schemaName);
        tableMeta.setTableName(tableName);
        tableMeta.setPartitionColumn(partitionColumn);
        tableMeta.setPartitionCount(partitionCount);
        tableMeta.setInclusionProbabilityColumn(inclusionProbabilityColumn);
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
    
    int partitionCount;
    
    String inclusionProbabilityColumn;
    
    
    public ScrambleMetaForTable() {}
    
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
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }
    
    public String getPartitionAttributeValue(int i) {
        return String.valueOf(i);
    }

    public String getInclusionProbabilityColumn() {
        return inclusionProbabilityColumn;
    }

    public void setInclusionProbabilityColumn(String inclusionProbabilityColumn) {
        this.inclusionProbabilityColumn = inclusionProbabilityColumn;
    }
}

package org.verdictdb.core.rewriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.lang3.tuple.Pair;

public class ScrambleMeta {
    
    Map<String, ScrambleMetaForTable> meta = new HashMap<>();
    
    public ScrambleMeta() {}
    
    public void insertScrumbleMetaEntry(
            String schemaName,
            String tableName,
            String partitionColumn,
            String inclusionProbabilityColumn,
            SortedMap<String, Long> partitionInfo) {
        ScrambleMetaForTable tableMeta = new ScrambleMetaForTable();
        tableMeta.setSchemaName(schemaName);
        tableMeta.setTableName(tableName);
        tableMeta.setPartitionColumn(partitionColumn);
        tableMeta.setInclusionProbabilityColumn(inclusionProbabilityColumn);
        
        for (Map.Entry<String, Long> e : partitionInfo.entrySet()) {
            tableMeta.addPartitionInfo(e.getKey(), e.getValue());
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
    
    public Pair<String, Long> getPartitionAttributeAndSize(String schemaName, String tableName, int partitionNumber) {
        return meta.get(metaKey(schemaName, tableName)).getPartitionAttributeAndSize(partitionNumber);
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
    
    List<Long> partitionSizes = new ArrayList<>();
    
    List<String> partitionAttributeValues = new ArrayList<>();
    
    String inclusionProbabilityColumn;
    
    
    public ScrambleMetaForTable() {}
    
    public void addPartitionInfo(String partitionAttributeValue, long partitionSize) {
        partitionAttributeValues.add(partitionAttributeValue);
        partitionSizes.add(partitionSize);
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
        return partitionSizes.size();
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
    
    public Pair<String, Long> getPartitionAttributeAndSize(int partitionNumber) {
        String attr = partitionAttributeValues.get(partitionNumber);
        Long size = partitionSizes.get(partitionNumber);
        return Pair.of(attr, size);
    }
}

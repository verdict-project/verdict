package org.verdictdb.core.rewriter;

public class SampleMeta {
    
    int partitionCount;
    
    String partitionColumn;
    
    public SampleMeta() {}
    
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

}

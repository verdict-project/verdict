package org.verdictdb.core.sqlobject;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Example: CRAETE TABLE test1 PARTITION OF test FOR VALUES IN (1);
 *
 * @author Yongjoo Park, Dong Young Yoon
 */
public class CreateScrambledTableQuery extends CreateTableQuery {

  private static final long serialVersionUID = 1L;

  protected String originalSchemaName;

  protected String originalTableName;

  protected String tierColumnName;

  protected String blockColumnName;

  protected SelectQuery select;

  protected List<Pair<String, String>> columnMeta;

  protected List<String> partitionColumns = new ArrayList<>();

  protected boolean overwrite = false;

  protected int blockCount = 1;

  public CreateScrambledTableQuery(String originalSchemaName, String originalTableName, String schemaName,
                                   String tableName, String tierColumnName, String blockColumnName, SelectQuery select, int blockCount,
                                   List<Pair<String, String>> columnMeta) {
    this.originalSchemaName = originalSchemaName;
    this.originalTableName = originalTableName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.tierColumnName = tierColumnName;
    this.blockColumnName = blockColumnName;
    this.select = select;
    this.blockCount = blockCount;
    this.columnMeta = columnMeta;
  }

  public void addPartitionColumn(String column) {
    partitionColumns.add(column);
  }

  public boolean getOverwrite() {
    return overwrite;
  }

  public String getOriginalSchemaName() {
    return originalSchemaName;
  }

  public String getOriginalTableName() {
    return originalTableName;
  }

  public String getTierColumnName() {
    return tierColumnName;
  }

  public String getBlockColumnName() {
    return blockColumnName;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public SelectQuery getSelect() {
    return select;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public int getBlockCount() {
    return blockCount;
  }

  public List<Pair<String, String>> getColumnMeta() {
    return columnMeta;
  }

  @Override
  public boolean equals(Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

}

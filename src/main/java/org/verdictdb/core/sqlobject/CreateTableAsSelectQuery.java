package org.verdictdb.core.sqlobject;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Represents a create table query.
 * 
 * https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala_create_table.html
 * 
 * @author Yongjoo Park
 *
 */
public class CreateTableAsSelectQuery extends CreateTableQuery {
  
  private static final long serialVersionUID = -4077488589201481833L;
  
  protected SelectQuery select;
  
  protected List<String> partitionColumns = new ArrayList<>();
  
  protected boolean overwrite = false;
  
  public CreateTableAsSelectQuery(String schemaName, String tableName, SelectQuery select) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.select = select;
  }

  public void addPartitionColumn(String column) {
    partitionColumns.add(column);
  }

  public boolean getOverwrite() {
    return overwrite;
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

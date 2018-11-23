/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
 * <p>https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala_create_table.html
 *
 * @author Yongjoo Park
 */
public class CreateTableAsSelectQuery extends CreateTableQuery {

  private static final long serialVersionUID = -4077488589201481833L;

  protected CreateTableQuery originalQuery;

  protected SelectQuery select;

  protected List<String> partitionColumns = new ArrayList<>();

  // the number of blocks for each partitioning column
  protected List<Integer> partitionCounts = new ArrayList<>();

  protected List<String> primaryColumns = new ArrayList<>();

  protected boolean overwrite = false;

  public CreateTableAsSelectQuery(String schemaName, String tableName, SelectQuery select) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.select = select;
  }

  public CreateTableAsSelectQuery(CreateScrambledTableQuery query) {
    this.originalQuery = query;
    this.schemaName = query.schemaName;
    this.tableName = query.tableName;
    this.partitionColumns.addAll(query.partitionColumns);
    this.partitionCounts.add(query.getBlockCount());
    this.select = query.select;
    this.overwrite = query.overwrite;
    this.ifNotExists = query.ifNotExists;
    this.primaryColumns = query.primaryKeyColumnName;
    // A PRIMARY KEY must include all columns in the table's partitioning function
    if (!this.primaryColumns.isEmpty()) {
      this.primaryColumns.addAll(this.partitionColumns);
    }
  }

  public void addPartitionColumn(String column) {
    partitionColumns.add(column);
  }

  public void addPartitionCount(int count) {
    partitionCounts.add(count);
  }

  public boolean getOverwrite() {
    return overwrite;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public List<Integer> getPartitionCounts() {
    return partitionCounts;
  }

  public List<String> getPrimaryColumns() { return primaryColumns; }

  public SelectQuery getSelect() {
    return select;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public CreateTableQuery getOriginalQuery() {
    return originalQuery;
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

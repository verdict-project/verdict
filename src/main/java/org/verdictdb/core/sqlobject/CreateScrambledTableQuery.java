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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Example: CRAETE TABLE test1 PARTITION OF test FOR VALUES IN (1);
 *
 * @author Yongjoo Park, Dong Young Yoon
 */
public class CreateScrambledTableQuery extends CreateTableQuery {

  private static final long serialVersionUID = -5937857840290786646L;

  protected String originalSchemaName;

  protected String originalTableName;

  protected String tierColumnName;

  protected String blockColumnName;

  protected SelectQuery select;

  protected List<Pair<String, String>> columnMeta;

  protected List<String> partitionColumns = new ArrayList<>();

  protected boolean overwrite = false;

  protected int blockCount = 1;

  protected int actualBlockCount = 1;

  public CreateScrambledTableQuery(
      String originalSchemaName,
      String originalTableName,
      String schemaName,
      String tableName,
      String tierColumnName,
      String blockColumnName,
      SelectQuery select,
      int blockCount,
      int actualBlockCount,
      List<Pair<String, String>> columnMeta,
      boolean createIfNotExists) {
    this.originalSchemaName = originalSchemaName;
    this.originalTableName = originalTableName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.tierColumnName = tierColumnName;
    this.blockColumnName = blockColumnName;
    this.select = select;
    this.blockCount = blockCount;
    this.actualBlockCount = actualBlockCount;
    this.columnMeta = columnMeta;
    this.overwrite = createIfNotExists;
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

  public boolean isOverwrite() {
    return overwrite;
  }

  public int getBlockCount() {
    return blockCount;
  }

  public List<Pair<String, String>> getColumnMeta() {
    return columnMeta;
  }

  public int getActualBlockCount() {
    return actualBlockCount;
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

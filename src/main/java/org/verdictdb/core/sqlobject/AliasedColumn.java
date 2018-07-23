/*
 *    Copyright 2017 University of Michigan
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

/**
 * Represents "column as attribute".
 *
 * @author Yongjoo Park
 */
public class AliasedColumn implements SelectItem {

  private static final long serialVersionUID = -8488130745600642428L;

  UnnamedColumn column;

  String aliasName;

  public AliasedColumn(UnnamedColumn column, String aliasName) {
    this.column = column;
    this.aliasName = aliasName;
  }

  public UnnamedColumn getColumn() {
    return column;
  }

  public void setColumn(UnnamedColumn column) {
    this.column = column;
  }

  public String getAliasName() {
    return aliasName;
  }

  public void setAliasName(String aliasName) {
    this.aliasName = aliasName;
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public boolean equals(Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

  @Override
  public boolean isAggregateColumn() {
    return column.isAggregateColumn();
  }

  @Override
  public SelectItem deepcopy() {
    return new AliasedColumn(column.deepcopy(), this.aliasName);
  }
}

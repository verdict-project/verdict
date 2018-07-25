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

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.google.common.base.Optional;

/**
 * Represents a relation (or a table) that can appear in the from clause.
 *
 * @author Yongjoo Park
 */
public abstract class AbstractRelation implements Serializable {

  private static final long serialVersionUID = 4819247286138983277L;

  Optional<String> aliasName = Optional.absent();

  public void setAliasName(String aliasName) {
    this.aliasName = Optional.of(aliasName);
  }

  public Optional<String> getAliasName() {
    return aliasName;
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
    return ToStringBuilder.reflectionToString(this);
  }

  public boolean isSupportedAggregate() {
    if (!(this instanceof SelectQuery)) {
      return false;
    }
    SelectQuery sel = (SelectQuery) this;
    List<SelectItem> selectList = sel.getSelectList();
    for (SelectItem item : selectList) {
      if (item instanceof AliasedColumn) {
        item = ((AliasedColumn) item).getColumn();
      }

      if (item instanceof ColumnOp) {
        ColumnOp col = (ColumnOp) item;
        if (col.isColumnOpAggregate()) {
          return true;
        }
      }
    }
    return false;
  }

  public AbstractRelation deepcopy() {
    return null;
  }
}

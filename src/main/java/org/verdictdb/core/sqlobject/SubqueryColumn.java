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

/** Subquery that may appear in the where clause. */
public class SubqueryColumn implements UnnamedColumn {

  private static final long serialVersionUID = 4046157399674779659L;

  SelectQuery subquery = new SelectQuery();

  public SubqueryColumn() {}

  public SubqueryColumn(SelectQuery relation) {
    subquery = relation;
  }

  public void setSubquery(SelectQuery relation) {
    subquery = relation;
  }

  public static SubqueryColumn getSubqueryColumn(SelectQuery relation) {
    return new SubqueryColumn(relation);
  }

  public SelectQuery getSubquery() {
    return subquery;
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

  @Override
  public boolean isAggregateColumn() {
    return false;
  }

  // not need this
  @Override
  public SubqueryColumn deepcopy() {
    return this;
  }
}

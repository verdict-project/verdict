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

import java.io.Serializable;

public class OrderbyAttribute implements Serializable {

  private static final long serialVersionUID = 735964470774655241L;

  GroupingAttribute attribute;

  String order = "asc";

  public OrderbyAttribute(String attributeName) {
    this.attribute = new AliasReference(attributeName);
  }

  public OrderbyAttribute(GroupingAttribute column) {
    this.attribute = column;
  }

  public OrderbyAttribute(String attributeName, String order) {
    this.attribute = new AliasReference(attributeName);
    this.order = order;
  }

  public OrderbyAttribute(GroupingAttribute column, String order) {
    this.attribute = column;
    this.order = order;
  }

  public String getOrder() {
    return order;
  }

  public GroupingAttribute getAttribute() {
    return attribute;
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
}

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

public class ConstantColumn implements UnnamedColumn, SelectItem {

  private static final long serialVersionUID = -4530737413387725261L;

  Object value;

  public void setValue(Object value) {
    this.value = value;
  }

  public Object getValue() {
    return value;
  }

  public static ConstantColumn valueOf(int value) {
    ConstantColumn c = new ConstantColumn();
    c.setValue(Integer.valueOf(value).toString());
    return c;
  }

  public static ConstantColumn valueOf(double value) {
    ConstantColumn c = new ConstantColumn();
    c.setValue(Double.valueOf(value).toString());
    return c;
  }

  public static ConstantColumn valueOf(String value) {
    ConstantColumn c = new ConstantColumn();
    c.setValue(value);
    return c;
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
    return false;
  }

  @Override
  public ConstantColumn deepcopy() {
    ConstantColumn c = new ConstantColumn();
    c.setValue(value);
    return c;
  }
}

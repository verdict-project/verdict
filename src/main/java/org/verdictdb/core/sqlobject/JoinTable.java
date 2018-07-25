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

public class JoinTable extends AbstractRelation {

  private static final long serialVersionUID = 4600469783922264549L;

  // May need to expand
  public enum JoinType {
    left,
    leftouter,
    right,
    rightouter,
    inner,
    outer,
    cross
  }

  List<AbstractRelation> joinList = new ArrayList<>();

  List<JoinType> joinTypeList = new ArrayList<>();

  List<UnnamedColumn> condition = new ArrayList<>();

  public static JoinTable create(
      List<AbstractRelation> joinList, List<JoinType> joinTypeList, List<UnnamedColumn> condition) {
    JoinTable join = new JoinTable();
    join.joinList = joinList;
    join.joinTypeList = joinTypeList;
    join.condition = condition;
    return join;
  }

  public static JoinTable createBase(
      AbstractRelation joinBaseTable, List<JoinType> joinTypeList, List<UnnamedColumn> condition) {
    JoinTable join = new JoinTable();
    join.joinList.add(joinBaseTable);
    join.joinTypeList = joinTypeList;
    join.condition = condition;
    return join;
  }

  public void addJoinTable(AbstractRelation joinTable, JoinType joinType, UnnamedColumn conditon) {
    this.condition.add(conditon);
    this.joinList.add(joinTable);
    this.joinTypeList.add(joinType);
  }

  public List<AbstractRelation> getJoinList() {
    return joinList;
  }

  public List<JoinType> getJoinTypeList() {
    return joinTypeList;
  }

  public List<UnnamedColumn> getCondition() {
    return condition;
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
  public JoinTable deepcopy() {
    List<AbstractRelation> newJoinlist = new ArrayList<>();
    List<UnnamedColumn> newJoinCond = new ArrayList<>();
    for (AbstractRelation j : joinList) {
      newJoinlist.add(j.deepcopy());
    }
    for (UnnamedColumn c : condition) {
      if (c != null) newJoinCond.add(c.deepcopy());
      else newJoinCond.add(null);
    }
    return JoinTable.create(newJoinlist, joinTypeList, newJoinCond);
  }
}

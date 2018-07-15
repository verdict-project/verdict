package org.verdictdb.core.sqlobject;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class JoinTable extends AbstractRelation {

  private static final long serialVersionUID = 4600469783922264549L;

  //May need to expand
  public enum JoinType {
    left, leftouter, right, rightouter, inner, outer
  }

  List<AbstractRelation> joinList = new ArrayList<>();

  List<JoinType> joinTypeList = new ArrayList<>();

  List<UnnamedColumn> condition = new ArrayList<>();

  public static JoinTable create(List<AbstractRelation> joinList, List<JoinType> joinTypeList, List<UnnamedColumn> condition) {
    JoinTable join = new JoinTable();
    join.joinList = joinList;
    join.joinTypeList = joinTypeList;
    join.condition = condition;
    return join;
  }

  public static JoinTable createBase(AbstractRelation joinBaseTable, List<JoinType> joinTypeList, List<UnnamedColumn> condition) {
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
    for (AbstractRelation j:joinList) {
      newJoinlist.add(j.deepcopy());
    }
    for (UnnamedColumn c:condition) {
      newJoinCond.add(c.deepcopy());
    }
    return JoinTable.create(newJoinlist, joinTypeList, newJoinCond);
  }
}

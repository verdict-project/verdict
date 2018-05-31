package org.verdictdb.core.logical_query;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Optional;

public class JoinTable extends AbstractRelation {

    //May need to expand
    public enum JoinType {
        left, leftouter, right, rightouter, inner, outer
    }


    List<AbstractRelation> joinList = new ArrayList<>();

    List<JoinType> joinTypeList = new ArrayList<>();

    List<UnnamedColumn> condition = new ArrayList<>();

    Optional<String> aliasName = Optional.absent();

    public static JoinTable getJoinTable(List<AbstractRelation> joinList, List<JoinType> joinTypeList, List<UnnamedColumn> condition) {
        JoinTable join = new JoinTable();
        join.joinList = joinList;
        join.joinTypeList = joinTypeList;
        join.condition = condition;
        return join;
    }

    public void addJoinTable(AbstractRelation joinTable, JoinType joinType, UnnamedColumn conditon){
        this.joinList.add(joinTable);
        this.joinTypeList.add(joinType);
        this.condition.add(conditon);
    }

    public void SetAliasName(String aliasName){
        this.aliasName = Optional.of(aliasName);
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

    public Optional<String> getAliasName() {
        return aliasName;
    }
}

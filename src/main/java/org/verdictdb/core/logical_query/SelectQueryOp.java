package org.verdictdb.core.logical_query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.google.common.base.Optional;


public class SelectQueryOp extends AbstractRelation {

  public static SelectQueryOp getSelectQueryOp(List<SelectItem> columns, AbstractRelation relation) {
    SelectQueryOp sel = new SelectQueryOp();
    for (SelectItem c : columns) {
      sel.addSelectItem(c);
    }
    sel.addTableSource(relation);
    return sel;
  }

  public static SelectQueryOp getSelectQueryOp(List<SelectItem> columns, AbstractRelation relation, UnnamedColumn predicate) {
    SelectQueryOp sel = new SelectQueryOp();
    for (SelectItem c : columns) {
      sel.addSelectItem(c);
    }
    sel.addTableSource(relation);
    sel.filter = Optional.of(predicate);
    return sel;
  }

  public static SelectQueryOp getSelectQueryOp(List<SelectItem> columns, List<AbstractRelation> relation) {
    SelectQueryOp sel = new SelectQueryOp();
    for (SelectItem c : columns) {
      sel.addSelectItem(c);
    }
    for (AbstractRelation r : relation) {
      sel.addTableSource(r);
    }
    return sel;
  }

  List<SelectItem> selectList = new ArrayList<>();

  List<AbstractRelation> fromList = new ArrayList<>();

  Optional<UnnamedColumn> filter = Optional.absent();

  List<GroupingAttribute> groupby = new ArrayList<>();

  List<OrderbyAttribute> orderby = new ArrayList<>();

  Optional<UnnamedColumn> having = Optional.absent();

  Optional<UnnamedColumn> limit = Optional.absent();

  Optional<String> aliasName = Optional.absent();

  public SelectQueryOp() {
  }

  public void addFilterByAnd(UnnamedColumn predicate) {
    if (!filter.isPresent()) {
      filter = Optional.of(predicate);
    } else {
      filter = Optional.<UnnamedColumn>of(ColumnOp.and(filter.get(), predicate));
    }
  }

  public void addGroupby(GroupingAttribute column) {
    groupby.add(column);
  }

  public void addGroupby(List<GroupingAttribute> columns) {
    groupby.addAll(columns);
  }

  public void addHavingByAnd(UnnamedColumn predicate) {
    if (!having.isPresent()) {
      having = Optional.of(predicate);
    } else {
      having = Optional.<UnnamedColumn>of(ColumnOp.and(having.get(), predicate));
    }
  }

  public void addLimit(UnnamedColumn limit) {
    this.limit = Optional.of(limit);
  }

  public void addOrderby(List<OrderbyAttribute> columns) {
    orderby.addAll(columns);
  }

  public void addOrderby(OrderbyAttribute column) {
    orderby.add(column);
  }

  public void addSelectItem(SelectItem column) {
    selectList.add(column);
  }

  public void addTableSource(AbstractRelation relation) {
    fromList.add(relation);
  }

  public void clearFilters() {
    this.filter = Optional.absent();
  }

  public void clearFromList() {
    this.fromList = new ArrayList<>();
  }

  public void clearGroupby() {
    this.groupby = new ArrayList<>();
  }

  public void clearSelectList() {
    this.selectList = new ArrayList<>();
  }

  public Optional<String> getAliasName() {
    return aliasName;
  }

  public Optional<UnnamedColumn> getFilter() {
    return filter;
  }

  public List<AbstractRelation> getFromList() {
    return fromList;
  }

  //  public void setAliasName(String aliasName) {
  //    this.aliasName = Optional.of(aliasName);
  //  }

  public List<GroupingAttribute> getGroupby() {
    return groupby;
  }

  public Optional<UnnamedColumn> getHaving() {
    return having;
  }

  public Optional<UnnamedColumn> getLimit() {
    return limit;
  }

  public List<OrderbyAttribute> getOrderby() {
    return orderby;
  }

  public List<SelectItem> getSelectList() {
    return selectList;
  }

  public void setAliasName(String aliasName) {
    this.aliasName = Optional.of(aliasName);
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
    return ToStringBuilder.reflectionToString(this);
  }

}

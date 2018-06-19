package org.verdictdb.core.query;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Subquery that may appear in the where clause.
 */
public class SubqueryColumn implements UnnamedColumn {
  
  SelectQueryOp subquery = new SelectQueryOp();

  public SubqueryColumn() {
  }

  public SubqueryColumn(SelectQueryOp relation) {
    subquery = relation;
  }

  public void setSubquery(SelectQueryOp relation) {
    subquery = relation;
  }

  public static SubqueryColumn getSubqueryColumn(SelectQueryOp relation) {
    return new SubqueryColumn(relation);
  }

  public SelectQueryOp getSubquery() {
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
  
}

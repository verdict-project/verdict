package org.verdictdb.core.logical_query;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Represents "column as aliasName".
 *
 * @author Yongjoo Park
 */
public class AliasedColumn implements SelectItem {

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
    return ToStringBuilder.reflectionToString(this);
  }

}

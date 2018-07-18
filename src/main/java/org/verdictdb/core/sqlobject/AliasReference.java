package org.verdictdb.core.sqlobject;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Represents the alias name that appears in the group-by clause or in the order-by clause.
 * This column does not include any reference to the table.
 *
 * @author Yongjoo Park
 */
public class AliasReference implements GroupingAttribute {

  private static final long serialVersionUID = 6273526004275442693L;

  String aliasName;

  String tableAlias;

  UnnamedColumn column;

  public AliasReference(String aliasName) {
    this.aliasName = aliasName;
  }

  public AliasReference(String tableAlias, String aliasName) {
    this.tableAlias = tableAlias;
    this.aliasName = aliasName;
  }

  public String getAliasName() {
    return aliasName;
  }

  public String getTableAlias() {
    return tableAlias;
  }

  public void setColumn(UnnamedColumn column) {this.column = column;}

  public UnnamedColumn getColumn() {
    return column;
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

}

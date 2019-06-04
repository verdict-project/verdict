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
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class BaseTable extends AbstractRelation {

  private static final long serialVersionUID = 8758804572670242146L;

  String catalogName;

  String schemaName;

  String tableName;

  public BaseTable(
      String catalogName, String schemaName, String tableName, String tableSourceAlias) {
    if (catalogName != null) this.catalogName = catalogName;
    if (schemaName != null) this.schemaName = schemaName;
    this.tableName = tableName;
    if (tableSourceAlias != null) super.setAliasName(tableSourceAlias);
  }

  public BaseTable(String schemaName, String tableName, String tableSourceAlias) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    super.setAliasName(tableSourceAlias);
  }

  public static BaseTable getBaseTableWithoutSchema(String tableName, String tableSourceAlias) {
    BaseTable t = new BaseTable(tableName);
    t.setAliasName(tableSourceAlias);
    return t;
  }

  public BaseTable(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  public BaseTable(String tableName) {
    this.tableName = tableName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  //    public String getTableSourceAlias() {
  //        return tableSourceAlias;
  //    }
  //
  //    public void setTableSourceAlias(String tableSourceAlias) {
  //        this.tableSourceAlias = tableSourceAlias;
  //    }

  public String toSql(SqlSyntax syntax) throws VerdictDBTypeException {
    throw new VerdictDBTypeException("A base table itself cannot be converted to a sql.");
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

  // Do not deepcopy Basetable
  @Override
  public BaseTable deepcopy() {
    return this;
  }
}

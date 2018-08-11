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

public class CreateTableQuery implements SqlConvertible {

  private static final long serialVersionUID = 7842821425389712381L;

  protected String schemaName;

  protected String tableName;

  protected boolean ifNotExists = false;
  
  public static CreateTableQuery create(String schema, String table) {
    CreateTableQuery query = new CreateTableQuery();
    query.schemaName = schema;
    query.tableName = table;
    query.ifNotExists = true;
    return query;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }
}

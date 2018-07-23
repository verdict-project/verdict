/*
 *    Copyright 2017 University of Michigan
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

public class InsertValuesQuery implements SqlConvertible {

  private static final long serialVersionUID = -1243370415019825285L;

  protected String schemaName;

  protected String tableName;

  protected List<Object> values = new ArrayList<>();

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public List<Object> getValues() {
    return values;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setValues(List<Object> values) {
    this.values = values;
  }
}

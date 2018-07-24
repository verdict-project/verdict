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

package org.verdictdb.core.querying;

import org.verdictdb.core.sqlobject.SqlConvertible;

public class CreateSchemaQuery implements SqlConvertible {

  private static final long serialVersionUID = -3471060043688549954L;

  String schemaName;

  boolean ifNotExists = false;

  public CreateSchemaQuery(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }
}

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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Not implemented yet.
 *
 * @author Yongjoo Park
 */
public class CreateTableDefinitionQuery extends CreateTableQuery {

  private static final long serialVersionUID = -3733162210722527846L;

  List<String> partitionColumns = new ArrayList<>();

  // See DataTypeConverter for types
  List<Pair<String, String>> columnNameAndTypes = new ArrayList<>();

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public List<Pair<String, String>> getColumnNameAndTypes() {
    return columnNameAndTypes;
  }

  public void setPartitionColumns(List<String> partitionColumns) {
    this.partitionColumns = partitionColumns;
  }

  public void setColumnNameAndTypes(List<Pair<String, String>> columnNameAndTypes) {
    this.columnNameAndTypes = columnNameAndTypes;
  }

  public void addColumnNameAndType(Pair<String, String> nameAndType) {
    this.columnNameAndTypes.add(nameAndType);
  }
}

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

package org.verdictdb.core.scrambling;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;

public class UniformScrambler extends BaseScrambler {

  public UniformScrambler(
      String originalSchemaName,
      String originalTableName,
      String scrambledSchemaName,
      String scrambledTableName,
      int aggregationBlockCount) {
    super(
        originalSchemaName,
        originalTableName,
        scrambledSchemaName,
        scrambledTableName,
        aggregationBlockCount);
  }

  public CreateTableAsSelectQuery createQuery() {
    SelectQuery selectQuery = scramblingQuery();
    CreateTableAsSelectQuery createQuery =
        new CreateTableAsSelectQuery(scrambledSchemaName, scrambledTableName, selectQuery);
    createQuery.addPartitionColumn(aggregationBlockColumn);
    return createQuery;
  }

  SelectQuery scramblingQuery() {
    // block agg index = cast(floor(rand() * aggBlockCount) as smallint)
    AliasedColumn aggBlockValue =
        new AliasedColumn(
            ColumnOp.cast(
                ColumnOp.floor(
                    ColumnOp.multiply(
                        ColumnOp.rand(), ConstantColumn.valueOf(aggregationBlockCount))),
                ConstantColumn.valueOf("smallint")),
            aggregationBlockColumn);

    // subsample value: random integer between 0 and 99 (inclusive)
    // = cast(floor(rand() * 100) as smallint)
    AliasedColumn subsampleValue =
        new AliasedColumn(
            ColumnOp.cast(
                ColumnOp.floor(ColumnOp.multiply(ColumnOp.rand(), ConstantColumn.valueOf(100))),
                ConstantColumn.valueOf("smallint")),
            subsampleColumn);
    AliasedColumn tierValue = new AliasedColumn(ConstantColumn.valueOf(1), tierColumn);

    List<SelectItem> newSelectList = new ArrayList<>();
    newSelectList.add(new AsteriskColumn());
    newSelectList.add(aggBlockValue);
    newSelectList.add(subsampleValue);
    newSelectList.add(tierValue);

    SelectQuery augmentedRelation =
        SelectQuery.create(newSelectList, new BaseTable(originalSchemaName, originalTableName));
    return augmentedRelation;
  }
}

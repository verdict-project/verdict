package org.verdictdb.core.scramble;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.ConstantColumn;
import org.verdictdb.core.query.CreateTableAsSelect;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQueryOp;

public class UniformScrambler extends Scrambler {

  public UniformScrambler(
      String originalSchemaName, String originalTableName,
      String scrambledSchemaName, String scrambledTableName,
      int aggregationBlockCount) {
    super(originalSchemaName, originalTableName, scrambledSchemaName, scrambledTableName, aggregationBlockCount);
  }

  public CreateTableAsSelect scrambledTableCreationQuery() {
    SelectQueryOp selectQuery = scramblingQuery();
    CreateTableAsSelect createQuery =
        new CreateTableAsSelect(scrambledSchemaName, scrambledTableName, selectQuery);
    createQuery.addPartitionColumn(aggregationBlockColumn);
    return createQuery;
  }

  SelectQueryOp scramblingQuery() {
    // block agg index = floor(rand() * aggBlockCount)
    AliasedColumn aggBlockValue = new AliasedColumn(
        ColumnOp.floor(ColumnOp.multiply(
            ColumnOp.rand(),
            ConstantColumn.valueOf(aggregationBlockCount))),
        aggregationBlockColumn);

    // inclusion probability = 1 / (aggblock count)
//    AliasedColumn incProbValue = new AliasedColumn(
//        ConstantColumn.valueOf(1.0 / aggregationBlockCount),
//        inclusionProbabilityColumn);

    // inclusion block difference = 1 / (aggblock count)
//    AliasedColumn incProbBlockDiffValue = new AliasedColumn(
//        ConstantColumn.valueOf(1.0 / aggregationBlockCount),
//        inclusionProbabilityBlockDifferenceColumn);

    // subsample value: random integer between 0 and 99 (inclusive)
    // = floor(rand() * 100)
    AliasedColumn subsampleValue = new AliasedColumn(
        ColumnOp.floor(ColumnOp.multiply(
            ColumnOp.rand(),
            ConstantColumn.valueOf(100))),
        subsampleColumn);

    AliasedColumn tierValue = new AliasedColumn(ConstantColumn.valueOf(1), tierColumn);

    List<SelectItem> newSelectList = new ArrayList<>();
    newSelectList.add(new AsteriskColumn());
    newSelectList.add(aggBlockValue);
//    newSelectList.add(incProbValue);
//    newSelectList.add(incProbBlockDiffValue);
    newSelectList.add(subsampleValue);
    newSelectList.add(tierValue);

    SelectQueryOp augmentedRelation = SelectQueryOp.getSelectQueryOp(
        newSelectList,
        new BaseTable(originalSchemaName, originalTableName));
    return augmentedRelation;
  }

}

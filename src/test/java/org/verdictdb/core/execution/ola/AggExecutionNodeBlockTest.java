package org.verdictdb.core.execution.ola;

import static org.junit.Assert.*;

import org.junit.Test;
import org.verdictdb.core.execution.AggExecutionNode;
import org.verdictdb.core.execution.QueryExecutionNode;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.scramble.Scrambler;
import org.verdictdb.exception.VerdictDBValueException;

public class AggExecutionNodeBlockTest {
  
  ScrambleMeta generateTestScrambleMeta() {
    int aggblockCount = 2;
    ScrambleMeta meta = new ScrambleMeta();
    meta.insertScrambleMetaEntry("myschema", "mytable",
        Scrambler.getAggregationBlockColumn(),
        Scrambler.getSubsampleColumn(),
        Scrambler.getTierColumn(),
        aggblockCount);
    return meta;
  }

  @Test
  public void testConvertFlatToProgressiveAgg() throws VerdictDBValueException {
    SelectQuery aggQuery = SelectQuery.create(
        ColumnOp.count(),
        new BaseTable("myschema", "mytable", "t"));
    AggExecutionNode aggnode = AggExecutionNode.create(aggQuery, "myschema");
    AggExecutionNodeBlock block = new AggExecutionNodeBlock(aggnode);
    ScrambleMeta scrambleMeta = generateTestScrambleMeta();
    QueryExecutionNode converted = block.convertToProgressiveAgg(scrambleMeta);
    converted.print();
  }

}

package org.verdictdb.core.rewriter.aggresult;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.verdictdb.core.aggresult.AggregateFrame;
import org.verdictdb.core.aggresult.AggregateGroup;
import org.verdictdb.core.aggresult.AggregateMeasures;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.exception.VerdictDBValueException;

public class AggregateFrameQueryResultTest {

  @Test
  public void testToDbmsQueryResult() throws VerdictDBValueException {
    List<String> orderedColumnNames = Arrays.asList("col1", "col2");
    AggregateFrame aggframe = new AggregateFrame(orderedColumnNames);
    AggregateGroup group = new AggregateGroup(Arrays.asList("col2"), Arrays.asList((Object) "group1"));
    AggregateMeasures measures = new AggregateMeasures(Arrays.asList("col1"), Arrays.asList((Object) "value1"));
    aggframe.addRow(group, measures);
    
    DbmsQueryResult result = aggframe.toDbmsQueryResult();
    result.printContent();
  }

}

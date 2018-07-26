package org.verdictdb.core.querying;

import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.scrambling.BaseScrambler;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.Arrays;

public class QueryExecutionNodeTest {
  
  ScrambleMetaSet generateTestScrambleMeta() {
    int aggblockCount = 2;
    ScrambleMetaSet meta = new ScrambleMetaSet();
    meta.insertScrambleMetaEntry("myschema", "mytable",
        BaseScrambler.getAggregationBlockColumn(),
        BaseScrambler.getSubsampleColumn(),
        BaseScrambler.getTierColumn(),
        aggblockCount);
    return meta;
  }

  @Test
  public void testDoesContainScrambledTableFlatQuery() throws VerdictDBValueException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery query = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new ColumnOp("count", new BaseColumn("t", "mycolumn1"))),
        base);
    
    DbmsConnection conn = null;
    String schemaName = "newschema";
    String tableName = "newtable";
    ScrambleMetaSet scrambleMeta = generateTestScrambleMeta();
    ExecutableNodeBase node = AggExecutionNode.create(QueryExecutionPlanFactory.create("newschema"), query);
//    assertTrue(node.doesContainScrambledTablesInDescendants(scrambleMeta));
  }
  
  @Test
  public void testDoesContainScrambledTableNestedQuery() throws VerdictDBValueException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery innerRelation = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("t", "mygroup"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), "myavg")),
        base);
    innerRelation.addGroupby(new AliasReference("mygroup"));
    innerRelation.setAliasName("s");
    SelectQuery query = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        innerRelation);
    
    DbmsConnection conn = null;
    String schemaName = "newschema";
    String tableName = "newtable";
    ScrambleMetaSet scrambleMeta = generateTestScrambleMeta();
    ExecutableNodeBase node = AggExecutionNode.create(QueryExecutionPlanFactory.create("newschema"), query);
//    assertFalse(node.doesContainScrambledTablesInDescendants(scrambleMeta));
  }
  
//  @Test
//  public void testIdentifyTopAggNodes() {
//    DbmsConnection conn = null;
//    
//    BaseTable base = new BaseTable("myschema", "temptable", "t");
//    SelectQuery aggQuery = SelectQuery.create(
//        Arrays.<SelectItem>asList(
//            new BaseColumn("t", "mygroup"),
//            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), "myavg")),
//        base);
//    aggQuery.addGroupby(new AliasReference("mygroup"));
//    aggQuery.setAliasName("s");
//    SelectQuery projectionQuery = SelectQuery.create(
//        Arrays.<SelectItem>asList(new AsteriskColumn()),
//        new BaseTable("myschema", "temptable2", "t"));
//    
//    QueryExecutionNode dep = AggExecutionNode.create(aggQuery, "newschema");
//    QueryExecutionNode root = ProjectionExecutionNode.create(projectionQuery, "newschema");
//    root.addDependency(dep);
//    
//    List<AggExecutionNodeBlock> topAggNodes = new ArrayList<>();
//    root.identifyTopAggBlocks(topAggNodes);
//    
//    assertEquals(dep, topAggNodes.get(0).getBlockRootNode());
//  }

}

package org.verdictdb.core.querying;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.BaseScrambler;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBValueException;

public class QueryExecutionNodeTest {
  
  ScrambleMeta generateTestScrambleMeta() {
    int aggblockCount = 2;
    ScrambleMeta meta = new ScrambleMeta();
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
    ScrambleMeta scrambleMeta = generateTestScrambleMeta();
    BaseQueryNode node = AggExecutionNode.create(new QueryExecutionPlan("newschema"), query);
    assertTrue(node.doesContainScrambledTablesInDescendants(scrambleMeta));
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
    ScrambleMeta scrambleMeta = generateTestScrambleMeta();
    BaseQueryNode node = AggExecutionNode.create(new QueryExecutionPlan("newschema"), query);
    assertFalse(node.doesContainScrambledTablesInDescendants(scrambleMeta));
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

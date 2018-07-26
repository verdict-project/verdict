package org.verdictdb.core.querying;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlsyntax.H2Syntax;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class QueryExecutionPlanSimplifierTest {

  static String originalSchema = "originalschema";

  static String originalTable = "originaltable";

  static String newSchema = "newschema";

  static String newTable  = "newtable";

  static int aggblockCount = 2;

  static DbmsConnection conn;

  @BeforeClass
  public static void setupDbConnAndScrambledTable() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:plancompresstest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = new JdbcConnection(DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD), new H2Syntax());
    conn.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", originalSchema));
    conn.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", newSchema));
    populateData(conn, originalSchema, originalTable);
  }

  static void populateData(DbmsConnection conn, String schemaName, String tableName) throws VerdictDBDbmsException {
    conn.execute(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    for (int i = 0; i < 10; i++) {
      conn.execute(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, (double) i+1));
    }
  }

  @Test
  public void simpleAggregateTest() throws VerdictDBException {
    String sql = "select avg(t.value) as a from originalschema.originaltable as t;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
    ExecutableNodeBase copy = queryExecutionPlan.root.deepcopy();
    QueryExecutionPlan plan = QueryExecutionPlanSimplifier.simplify(queryExecutionPlan);
    assertEquals(0, plan.root.getDependentNodeCount());
    selectQuery.setAliasName("t");
    assertEquals(selectQuery, ((QueryNodeBase) plan.root).selectQuery.getFromList().get(0));
    ((QueryNodeBase) copy.getExecutableNodeBaseDependent(0)).selectQuery.setAliasName("t");
    assertEquals(
        ((QueryNodeBase) copy.getExecutableNodeBaseDependent(0)).selectQuery, 
        ((QueryNodeBase) plan.root).selectQuery.getFromList().get(0));

    // queryExecutionPlan.root.execute(conn);
  }

  @Test
  public void NestedAggregateFromTest() throws VerdictDBException {
    String sql = "select avg(t.value) from (select o.value from originalschema.originaltable as o where o.value>5) as t;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
    ExecutableNodeBase copy = queryExecutionPlan.root.getExecutableNodeBaseDependent(0).getExecutableNodeBaseDependent(0).deepcopy();
    QueryExecutionPlan plan = QueryExecutionPlanSimplifier.simplify(queryExecutionPlan);
    assertEquals(0, plan.root.getDependentNodeCount());
    assertEquals(
        ((QueryNodeBase) copy).selectQuery,
        ((SelectQuery)
            ((QueryNodeBase) plan.root).selectQuery.getFromList().get(0))
        .getFromList().get(0));
    // queryExecutionPlan.root.execute(conn);
  }

  @Test
  public void NestedAggregateFilterTest() throws VerdictDBException {
    String sql = "select avg(t.value) as a from originalschema.originaltable as t where t.value > " +
        "(select avg(o.value) as avg_value from originalschema.originaltable as o);";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
    ExecutableNodeBase copy = queryExecutionPlan.root.getExecutableNodeBaseDependent(0).getExecutableNodeBaseDependent(0).deepcopy();
    QueryExecutionPlan plan = QueryExecutionPlanSimplifier.simplify(queryExecutionPlan);
    
    assertEquals(0, plan.root.getDependentNodeCount());

    assertEquals(
        ((QueryNodeBase) copy).selectQuery,
        ((SubqueryColumn)
            ((ColumnOp)
                ((SelectQuery) 
                    ((QueryNodeBase) plan.root)
                    .selectQuery.getFromList().get(0)).getFilter().get()).getOperand(1)).getSubquery());
    // queryExecutionPlan.root.execute(conn);
  }

//  @Test
//  public void SimpleAggregateWithScrambleTableTest() throws VerdictDBException {
//    String sql = "select avg(t.value) as a from originalschema.originaltable as t;";
//    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
//    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
//    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
//
//    BaseTable base = new BaseTable(originalSchema, originalTable, "t");
//    SelectQuery leftQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    leftQuery.addFilterByAnd(ColumnOp.lessequal(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    SelectQuery rightQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    rightQuery.addFilterByAnd(ColumnOp.greater(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    AggExecutionNode leftNode = AggExecutionNode.create(null, leftQuery);
//    AggExecutionNode rightNode = AggExecutionNode.create(null, rightQuery);
////    ExecutionTokenQueue queue = new ExecutionTokenQueue();
//    AggCombinerExecutionNode combiner = AggCombinerExecutionNode.create(queryExecutionPlan, leftNode, rightNode);
////    combiner.addBroadcastingQueue(queue);
//    
//    
//    AsyncAggExecutionNode asyncAggExecutionNode =
//        AsyncAggExecutionNode.create(queryExecutionPlan, Arrays.<ExecutableNodeBase>asList(leftNode, rightNode),
//            Arrays.<ExecutableNodeBase>asList(combiner));
//    queryExecutionPlan.root.getExecutableNodeBaseDependents().remove(0);
//    queryExecutionPlan.root.getListeningQueues().remove(0);
//    ExecutionTokenQueue q = new ExecutionTokenQueue();
//    queryExecutionPlan.root.getListeningQueues().add(q);
//    asyncAggExecutionNode.addBroadcastingQueue(q);
//    queryExecutionPlan.root.addDependency(asyncAggExecutionNode);
//
//    ExecutableNodeBase copy = queryExecutionPlan.root.deepcopy();
//    queryExecutionPlan.compress();
//
//    assertEquals(
//        asyncAggExecutionNode, 
//        queryExecutionPlan.root.getExecutableNodeBaseDependent(0));
//    assertEquals(copy.selectQuery, queryExecutionPlan.root.selectQuery);
//  }

//  @Test
//  public void NestedAggregateWithScrambleTableTest() throws VerdictDBException {
//    String sql = "select avg(t.value) as a from (select o.value from originalschema.originaltable as o where o.value>5) as t;";
//    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
//    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
//    
//    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
//    BaseTable base = new BaseTable(originalSchema, originalTable, "t");
//    SelectQuery leftQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    leftQuery.addFilterByAnd(ColumnOp.lessequal(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    SelectQuery rightQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    rightQuery.addFilterByAnd(ColumnOp.greater(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    AggExecutionNode leftNode = AggExecutionNode.create(null, leftQuery);
//    AggExecutionNode rightNode = AggExecutionNode.create(null, rightQuery);
//    ExecutionTokenQueue queue = new ExecutionTokenQueue();
//    AggCombinerExecutionNode combiner = AggCombinerExecutionNode.create(queryExecutionPlan, leftNode, rightNode);
//    combiner.addBroadcastingQueue(queue);
//    AsyncAggExecutionNode asyncAggExecutionNode =
//        AsyncAggExecutionNode.create(null, Arrays.<ExecutableNodeBase>asList(leftNode, rightNode),
//            Arrays.<ExecutableNodeBase>asList(combiner));
//    queryExecutionPlan.root.getExecutableNodeBaseDependent(0).getDependents().remove(0);
//    queryExecutionPlan.root.getExecutableNodeBaseDependent(0).getListeningQueues().remove(0);
//    ExecutionTokenQueue q = new ExecutionTokenQueue();
//    queryExecutionPlan.root.getExecutableNodeBaseDependent(0).getListeningQueues().add(q);
//    asyncAggExecutionNode.addBroadcastingQueue(q);
//    queryExecutionPlan.root.getExecutableNodeBaseDependent(0).addDependency(asyncAggExecutionNode);
//    ExecutableNodeBase copy = queryExecutionPlan.root.getDependent(0).deepcopy();
//    queryExecutionPlan.compress();
//
//    SelectQuery compressed = SelectQuery.create(
//        Arrays.<SelectItem>asList(
//          new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "value")), "a")
//        ), new BaseTable("placeholderSchemaName", "placeholderTableName", "t"));
//    compressed.setAliasName("t");
//    assertEquals(queryExecutionPlan.root.selectQuery.getFromList().get(0), compressed);
//    assertEquals(queryExecutionPlan.root.getExecutableNodeBaseDependent(0), asyncAggExecutionNode);
//
//    assertEquals(copy.getExecutableNodeBaseDependent(0), queryExecutionPlan.root.getExecutableNodeBaseDependent(0));
//  }

//  @Test
//  public void NestedAggregateWithScrambleTableHavingCommonChildrenTest() throws VerdictDBException {
//    String sql = "select avg(t.value) as a from (select o.value from originalschema.originaltable as o where o.value>5) as t;";
//    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
//    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
//    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
//    BaseTable base = new BaseTable(originalSchema, originalTable, "t");
//    SelectQuery leftQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    leftQuery.addFilterByAnd(ColumnOp.lessequal(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    SelectQuery rightQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    rightQuery.addFilterByAnd(ColumnOp.greater(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    AggExecutionNode leftNode = AggExecutionNode.create(null, leftQuery);
//    AggExecutionNode rightNode = AggExecutionNode.create(null, rightQuery);
//    ExecutionTokenQueue queue = new ExecutionTokenQueue();
//    AggCombinerExecutionNode combiner = AggCombinerExecutionNode.create(queryExecutionPlan, leftNode, rightNode);
//    combiner.addBroadcastingQueue(queue);
//    AsyncAggExecutionNode asyncAggExecutionNode =
//        AsyncAggExecutionNode.create(null, Arrays.<ExecutableNodeBase>asList(leftNode, rightNode),
//            Arrays.<ExecutableNodeBase>asList(combiner));
//    queryExecutionPlan.root.getExecutableNodeBaseDependent(0).getDependents().remove(0);
//    queryExecutionPlan.root.getExecutableNodeBaseDependent(0).getListeningQueues().remove(0);
//    ExecutionTokenQueue q = new ExecutionTokenQueue();
//    queryExecutionPlan.root.getExecutableNodeBaseDependent(0).getListeningQueues().add(q);
//    asyncAggExecutionNode.addBroadcastingQueue(q);
//    queryExecutionPlan.root.getExecutableNodeBaseDependent(0).addDependency(asyncAggExecutionNode);
//
//    SelectQuery commonQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    rightQuery.addFilterByAnd(ColumnOp.greater(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    AggExecutionNode common = AggExecutionNode.create(null, commonQuery);
//    leftQuery.addFilterByAnd(ColumnOp.lessequal(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    leftNode.addDependency(common);
//    common.addBroadcastingQueue(leftNode.generateListeningQueue());
//    rightNode.addDependency(common);
//    common.addBroadcastingQueue(rightNode.generateListeningQueue());
//    ExecutableNodeBase copy = queryExecutionPlan.root.getDependent(0).deepcopy();
//    queryExecutionPlan.compress();
//
//    SelectQuery compressed = SelectQuery.create(
//        Arrays.<SelectItem>asList(
//            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "value")), "a")
//        ), new BaseTable("placeholderSchemaName", "placeholderTableName", "t"));
//    compressed.setAliasName("t");
//    assertEquals(queryExecutionPlan.root.selectQuery.getFromList().get(0), compressed);
//    assertEquals(queryExecutionPlan.root.getExecutableNodeBaseDependent(0), asyncAggExecutionNode);
//
//    assertEquals(copy.getExecutableNodeBaseDependent(0), queryExecutionPlan.root.getExecutableNodeBaseDependent(0));
//  }
  
//=======
//  @Test
//  public void SimpleAggregateWithScrambleTableTest() throws VerdictDBException {
//    String sql = "select avg(t.value) as a from originalschema.originaltable as t;";
//    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
//    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
//    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
//
//    BaseTable base = new BaseTable(originalSchema, originalTable, "t");
//    SelectQuery leftQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    leftQuery.addFilterByAnd(ColumnOp.lessequal(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    SelectQuery rightQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    rightQuery.addFilterByAnd(ColumnOp.greater(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    AggExecutionNode leftNode = AggExecutionNode.create(null, leftQuery);
//    AggExecutionNode rightNode = AggExecutionNode.create(null, rightQuery);
//    ExecutionTokenQueue queue = new ExecutionTokenQueue();
//    AggCombinerExecutionNode combiner = AggCombinerExecutionNode.create(queryExecutionPlan, leftNode, rightNode);
//    combiner.addBroadcastingQueue(queue);
//    AsyncAggExecutionNode asyncAggExecutionNode =
//        AsyncAggExecutionNode.create(queryExecutionPlan, Arrays.<BaseQueryNode>asList(leftNode, rightNode),
//            Arrays.<BaseQueryNode>asList(combiner), null);
//    queryExecutionPlan.root.getDependents().remove(0);
//    queryExecutionPlan.root.getListeningQueues().remove(0);
//    ExecutionTokenQueue q = new ExecutionTokenQueue();
//    queryExecutionPlan.root.getListeningQueues().add(q);
//    asyncAggExecutionNode.addBroadcastingQueue(q);
//    queryExecutionPlan.root.addDependency(asyncAggExecutionNode);
//
//    BaseQueryNode copy = queryExecutionPlan.root.deepcopy();
//    queryExecutionPlan.compress();
//
//    assertEquals(asyncAggExecutionNode, queryExecutionPlan.root.dependents.get(0));
//    assertEquals(copy.selectQuery, queryExecutionPlan.root.selectQuery);
//  }
//
//  @Test
//  public void NestedAggregateWithScrambleTableTest() throws VerdictDBException {
//    String sql = "select avg(t.value) as a from (select o.value from originalschema.originaltable as o where o.value>5) as t;";
//    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
//    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
//    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
//    BaseTable base = new BaseTable(originalSchema, originalTable, "t");
//    SelectQuery leftQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    leftQuery.addFilterByAnd(ColumnOp.lessequal(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    SelectQuery rightQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    rightQuery.addFilterByAnd(ColumnOp.greater(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    AggExecutionNode leftNode = AggExecutionNode.create(null, leftQuery);
//    AggExecutionNode rightNode = AggExecutionNode.create(null, rightQuery);
//    ExecutionTokenQueue queue = new ExecutionTokenQueue();
//    AggCombinerExecutionNode combiner = AggCombinerExecutionNode.create(queryExecutionPlan, leftNode, rightNode);
//    combiner.addBroadcastingQueue(queue);
//    AsyncAggExecutionNode asyncAggExecutionNode =
//        AsyncAggExecutionNode.create(null, Arrays.<BaseQueryNode>asList(leftNode, rightNode),
//            Arrays.<BaseQueryNode>asList(combiner), null);
//    queryExecutionPlan.root.dependents.get(0).getDependents().remove(0);
//    queryExecutionPlan.root.dependents.get(0).getListeningQueues().remove(0);
//    ExecutionTokenQueue q = new ExecutionTokenQueue();
//    queryExecutionPlan.root.dependents.get(0).getListeningQueues().add(q);
//    asyncAggExecutionNode.addBroadcastingQueue(q);
//    queryExecutionPlan.root.dependents.get(0).addDependency(asyncAggExecutionNode);
//    BaseQueryNode copy = queryExecutionPlan.root.getDependent(0).deepcopy();
//    queryExecutionPlan.compress();
//
//    SelectQuery compressed = SelectQuery.create(
//        Arrays.<SelectItem>asList(
//          new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "value")), "a")
//        ), new BaseTable("placeholderSchemaName", "placeholderTableName", "t"));
//    compressed.setAliasName("t");
//    assertEquals(queryExecutionPlan.root.selectQuery.getFromList().get(0), compressed);
//    assertEquals(queryExecutionPlan.root.dependents.get(0), asyncAggExecutionNode);
//
//    assertEquals(copy.dependents.get(0), queryExecutionPlan.root.dependents.get(0));
//  }
//
//  @Test
//  public void NestedAggregateWithScrambleTableHavingCommonChildrenTest() throws VerdictDBException {
//    String sql = "select avg(t.value) as a from (select o.value from originalschema.originaltable as o where o.value>5) as t;";
//    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
//    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
//    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
//    BaseTable base = new BaseTable(originalSchema, originalTable, "t");
//    SelectQuery leftQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    leftQuery.addFilterByAnd(ColumnOp.lessequal(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    SelectQuery rightQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    rightQuery.addFilterByAnd(ColumnOp.greater(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    AggExecutionNode leftNode = AggExecutionNode.create(null, leftQuery);
//    AggExecutionNode rightNode = AggExecutionNode.create(null, rightQuery);
//    ExecutionTokenQueue queue = new ExecutionTokenQueue();
//    AggCombinerExecutionNode combiner = AggCombinerExecutionNode.create(queryExecutionPlan, leftNode, rightNode);
//    combiner.addBroadcastingQueue(queue);
//    AsyncAggExecutionNode asyncAggExecutionNode =
//        AsyncAggExecutionNode.create(null, Arrays.<BaseQueryNode>asList(leftNode, rightNode),
//            Arrays.<BaseQueryNode>asList(combiner), null);
//    queryExecutionPlan.root.dependents.get(0).getDependents().remove(0);
//    queryExecutionPlan.root.dependents.get(0).getListeningQueues().remove(0);
//    ExecutionTokenQueue q = new ExecutionTokenQueue();
//    queryExecutionPlan.root.dependents.get(0).getListeningQueues().add(q);
//    asyncAggExecutionNode.addBroadcastingQueue(q);
//    queryExecutionPlan.root.dependents.get(0).addDependency(asyncAggExecutionNode);
//
//    SelectQuery commonQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
//    rightQuery.addFilterByAnd(ColumnOp.greater(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    AggExecutionNode common = AggExecutionNode.create(null, commonQuery);
//    leftQuery.addFilterByAnd(ColumnOp.lessequal(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
//    leftNode.addDependency(common);
//    common.addBroadcastingQueue(leftNode.generateListeningQueue());
//    rightNode.addDependency(common);
//    common.addBroadcastingQueue(rightNode.generateListeningQueue());
//    BaseQueryNode copy = queryExecutionPlan.root.getDependent(0).deepcopy();
//    queryExecutionPlan.compress();
//
//    SelectQuery compressed = SelectQuery.create(
//        Arrays.<SelectItem>asList(
//            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "value")), "a")
//        ), new BaseTable("placeholderSchemaName", "placeholderTableName", "t"));
//    compressed.setAliasName("t");
//    assertEquals(queryExecutionPlan.root.selectQuery.getFromList().get(0), compressed);
//    assertEquals(queryExecutionPlan.root.dependents.get(0), asyncAggExecutionNode);
//
//    assertEquals(copy.dependents.get(0), queryExecutionPlan.root.dependents.get(0));
//  }
//>>>>>>> origin/joezhong-scale

}

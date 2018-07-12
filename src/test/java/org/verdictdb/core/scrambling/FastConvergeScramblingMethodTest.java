package org.verdictdb.core.scrambling;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.execution.ExecutablePlan;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.TempIdCreatorInScratchpadSchema;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.HiveSyntax;
import org.verdictdb.sqlwriter.QueryToSql;

public class FastConvergeScramblingMethodTest {
  
  static Connection h2conn;

  @BeforeClass
  public static void setupH2Database() throws SQLException {
    final String DB_CONNECTION = "jdbc:h2:mem:fastconvergemethodtest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    h2conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
    Statement stmt;

    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female", 15, 170.2, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female", 17, 156.5, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168.1, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male", 14, 178.6, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male", 18, 190.7, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Sam", "male", 18, 190.0, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Alice", "female", 18, 190.21, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Bob", "male", 18, 190.3, "CHN", "2017-10-12 21:22:23"));
    stmt = h2conn.createStatement();
    stmt.execute("CREATE SCHEMA IF NOT EXISTS \"test\"");
    stmt.execute("DROP TABLE \"test\".\"people\" IF EXISTS");
    stmt.execute("CREATE TABLE \"test\".\"people\" ("
        + "id smallint, "
        + "name varchar(255), "
        + "gender varchar(8), "
        + "age float, "
        + "height float, "
        + "nation varchar(8), "
        + "birth timestamp)");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      String age = row.get(3).toString();
      String height = row.get(4).toString();
      String nation = row.get(5).toString();
      String birth = row.get(6).toString();
      stmt.execute(String.format("INSERT INTO \"test\".\"people\" "
          + "(id, name, gender, age, height, nation, birth) "
          + "VALUES (%s, '%s', '%s', %s, %s, '%s', '%s')", 
          id, name, gender, age, height, nation, birth));
    }
  }

  @Test
  public void testLargeGroupListNodeWithoutTableSize() throws VerdictDBException {
    String scratchpadSchemaName = "verdictdbtempSchema";
    TempIdCreatorInScratchpadSchema idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    String schemaName = "oldSchema";
    String tableName = "oldTable";
    String primaryColumnName = "pcolumn";
    long blockSize = 10;
    LargeGroupListNode node = 
        new LargeGroupListNode(idCreator, schemaName, tableName, primaryColumnName, blockSize);
    
    SqlConvertible sqlobj = node.createQuery(Arrays.<ExecutionInfoToken>asList());
    String sql = QueryToSql.convert(new HiveSyntax(), sqlobj);
    String actual = sql.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    
    String expected = "create table `verdictdbtempSchema`.`verdictdbtemptable` "
        + "as select t.`pcolumn` as `verdictdbrenameprimarygroup`, "
        + "count(*) * (1.0 / 0.001) as `groupSize` "
        + "from `oldSchema`.`oldTable` as t "
        + "where rand() < 0.001 "
        + "group by `pcolumn`";
    assertEquals(expected, actual);
  }
  
  @Test
  public void testLargeGroupListNodeWithTableSize() throws VerdictDBException, SQLException {
    String scratchpadSchemaName = "verdictdbtempSchema";
    TempIdCreatorInScratchpadSchema idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    String schemaName = "oldSchema";
    String tableName = "oldTable";
    String primaryColumnName = "pcolumn";
    long blockSize = 10;
    LargeGroupListNode node = 
        new LargeGroupListNode(idCreator, schemaName, tableName, primaryColumnName, blockSize);
    
    // provision table size token
    int tableSize = 100;
    String aliasname = PercentilesAndCountNode.TOTAL_COUNT_ALIAS_NAME;
    DbmsConnection conn = JdbcConnection.create(h2conn);
    DbmsQueryResult result = conn.execute(String.format("select %d as \"%s\"", tableSize, aliasname));
    
    ExecutionInfoToken e = new ExecutionInfoToken();
    e.setKeyValue(PercentilesAndCountNode.class.getSimpleName(), result);
    
    // run the method to test
    SqlConvertible sqlobj = node.createQuery(Arrays.<ExecutionInfoToken>asList(e));
    String sql = QueryToSql.convert(new HiveSyntax(), sqlobj);
    String actual = sql.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    
    String expected = "create table `verdictdbtempSchema`.`verdictdbtemptable` "
        + "as select t.`pcolumn` as `verdictdbrenameprimarygroup`, "
        + "count(*) * (1.0 / 0.1) as `groupSize` "
        + "from `oldSchema`.`oldTable` as t "
        + "where rand() < 0.1 "
        + "group by `pcolumn`";
    assertEquals(expected, actual);
  }
  
  @Test
  public void testGetStatisticsNode() throws SQLException, VerdictDBException {
    int blockSize = 10;
    String scratchpadSchemaName = "test";
    String primaryGroupColumnName = "NAME";
    FastConvergeScramblingMethod method = 
        new FastConvergeScramblingMethod(blockSize, scratchpadSchemaName, primaryGroupColumnName);
    
    String oldSchemaName = "test";
    String oldTableName = "people";
    String columnMetaTokenKey = "columnMeta";
    String partitionMetaTokenKey = "partitionMeta";
    List<ExecutableNodeBase> nodes = 
        method.getStatisticsNode(oldSchemaName, oldTableName, columnMetaTokenKey, partitionMetaTokenKey);
    
    // create the node for passing meta information
    ExecutableNodeBase columnMetaDataNode = 
        ColumnMetadataRetrievalNode.create(oldSchemaName, oldTableName, columnMetaTokenKey);
    
    ExecutableNodeBase tableStatisticsRoot = nodes.get(0);
    tableStatisticsRoot.subscribeTo(columnMetaDataNode, 100);
//    ExecutablePlan columnMetaPlan = new SimpleTreePlan(tableStatisticsRoot);
//    DbmsConnection conn = new JdbcConnection(h2conn);
//    ExecutablePlanRunner.runTillEnd(conn, columnMetaPlan);
    
    ExecutableNodeBase groupSizeRoot = nodes.get(2);
    groupSizeRoot.subscribeTo(columnMetaDataNode, 100);
    ExecutablePlan groupSizePlan = new SimpleTreePlan(groupSizeRoot);
    DbmsConnection conn = JdbcConnection.create(h2conn);
    ExecutablePlanRunner.runTillEnd(conn, groupSizePlan);
  }
  
  @Test
  public void testGetTierExpressions() throws VerdictDBDbmsException {
    int blockSize = 10;
    String scratchpadSchemaName = "test";
    String primaryGroupColumnName = "NAME";
    FastConvergeScramblingMethod method = 
        new FastConvergeScramblingMethod(blockSize, scratchpadSchemaName, primaryGroupColumnName);
    
    // query result; preparation
    String sql = "select avg(t.\"ID\") as \"verdictdbavgID\", "
        + "stddev_pop(t.\"ID\") as \"verdictdbstddevID\", "
        + "avg(t.\"AGE\") as \"verdictdbavgAGE\", "
        + "stddev_pop(t.\"AGE\") as \"verdictdbstddevAGE\", "
        + "avg(t.\"HEIGHT\") as \"verdictdbavgHEIGHT\", "
        + "stddev_pop(t.\"HEIGHT\") as \"verdictdbstddevHEIGHT\", "
        + "count(*) as \"verdictdbtotalcount\" "
        + "from \"test\".\"people\" as t";
    DbmsConnection conn = JdbcConnection.create(h2conn);
    DbmsQueryResult queryResult = conn.execute(sql);
    
    // tests
    Map<String, Object> metaData = new HashMap<>();
    metaData.put(PercentilesAndCountNode.class.getSimpleName(), queryResult);
    List<UnnamedColumn> tiers = method.getTierExpressions(metaData);
    
    assertEquals("or", ((ColumnOp) tiers.get(0)).getOpType());
    assertEquals("isnull", ((ColumnOp) tiers.get(1)).getOpType());
  }

}

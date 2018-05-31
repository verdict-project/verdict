package org.verdictdb.core.scramble;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Random;

import org.junit.Test;
import org.verdictdb.core.logical_query.CreateTableAsSelect;
import org.verdictdb.core.logical_query.SelectQueryOp;
import org.verdictdb.core.rewriter.ScrambleMetaForTable;
import org.verdictdb.core.sql.CreateTableToSql;
import org.verdictdb.core.sql.SelectQueryToSql;
import org.verdictdb.core.sql.syntax.H2Syntax;
import org.verdictdb.core.sql.syntax.HiveSyntax;
import org.verdictdb.exception.VerdictDbException;

public class UniformScramblerTest {

  @Test
  public void testSelectQuery() throws VerdictDbException {
    String originalSchema = "originalschema";
    String originalTable = "originalschema";
    String newSchema = "newschema";
    String newTable  = "newtable";
    int aggBlockCount = 10;
    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, newSchema, newTable, aggBlockCount);
    SelectQueryOp scramblingQuery = scrambler.scramblingQuery();
    
    ScrambleMetaForTable meta = scrambler.generateMeta();
    meta.getAggregationBlockColumn();
    
    String expected = "select *"
        + String.format(", floor(rand() * %d) as %s", aggBlockCount, meta.getAggregationBlockColumn())
        + String.format(", 0.1 as %s", meta.getInclusionProbabilityColumn())
        + String.format(", 0.1 as %s", meta.getInclusionProbabilityBlockDifferenceColumn())
        + String.format(", floor(rand() * 100) as %s", meta.getSubsampleColumn())
        + String.format(" from `%s`.`%s`", originalSchema, originalTable);
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(scramblingQuery);
    assertEquals(expected, actual);
  }
  
  @Test
  public void testCreateTableQuery() throws VerdictDbException {
    String originalSchema = "originalschema";
    String originalTable = "originalschema";
    String newSchema = "newschema";
    String newTable  = "newtable";
    int aggBlockCount = 10;
    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, newSchema, newTable, aggBlockCount);
    CreateTableAsSelect createQuery = scrambler.scrambledTableCreationQuery();
    
    ScrambleMetaForTable meta = scrambler.generateMeta();
    meta.getAggregationBlockColumn();
    
    String expected = String.format("create table `%s`.`%s` ", newSchema, newTable)
        + String.format("partitioned by (`%s`) ", meta.getAggregationBlockColumn())
        + "as select *"
        + String.format(", floor(rand() * %d) as %s", aggBlockCount, meta.getAggregationBlockColumn())
        + String.format(", 0.1 as %s", meta.getInclusionProbabilityColumn())
        + String.format(", 0.1 as %s", meta.getInclusionProbabilityBlockDifferenceColumn())
        + String.format(", floor(rand() * 100) as %s", meta.getSubsampleColumn())
        + String.format(" from `%s`.`%s`", originalSchema, originalTable);
    CreateTableToSql createToSql = new CreateTableToSql(new HiveSyntax());
    String actual = createToSql.toSql(createQuery);
    assertEquals(expected, actual);
  }
  
  @Test
  public void testCreateTableQueryWithH2() throws SQLException, VerdictDbException {
    String originalSchema = "originalschema";
    String originalTable = "originalschema";
    String newSchema = "newschema";
    String newTable  = "newtable";
    
    final String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    Connection conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
    conn.createStatement().execute("CREATE SCHEMA " + originalSchema);
    conn.createStatement().execute("CREATE SCHEMA " + newSchema);
    populateRandomData(conn, originalSchema, originalTable);
    
    int aggBlockCount = 1;
    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, newSchema, newTable, aggBlockCount);
    CreateTableAsSelect createQuery = scrambler.scrambledTableCreationQuery();
    CreateTableToSql createToSql = new CreateTableToSql(new H2Syntax());
    String scrambleSql = createToSql.toSql(createQuery);
    conn.createStatement().execute(scrambleSql);
    
    // retrieve all values
//    printTableContent(conn, originalSchema, originalTable);
//    
//    printTableContent(conn, newSchema, newTable);
    conn.close();
  }
  
  @Test
  public void testCreateTableQueryCorrectnessWithH2() throws SQLException, VerdictDbException {
    String originalSchema = "originalschema";
    String originalTable = "originalschema";
    String newSchema = "newschema";
    String newTable  = "newtable";
    
    final String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    Connection conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
    conn.createStatement().execute("CREATE SCHEMA " + originalSchema);
    conn.createStatement().execute("CREATE SCHEMA " + newSchema);
    populateRandomData(conn, originalSchema, originalTable);
    
    int aggBlockCount = 5;
    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, newSchema, newTable, aggBlockCount);
    CreateTableAsSelect createQuery = scrambler.scrambledTableCreationQuery();
    CreateTableToSql createToSql = new CreateTableToSql(new H2Syntax());
    String scrambleSql = createToSql.toSql(createQuery);
    conn.createStatement().execute(scrambleSql);
    
    ResultSet rs = conn.createStatement().executeQuery(
        String.format("select min(verdictAggBlock), max(verdictAggBlock), "
            + "avg(verdictIncProb), avg(verdictIncProbBlockDiff) "
            + "from %s.%s", newSchema, newTable));
    rs.next();
    double eps = 1e-6;
    assertEquals(0, rs.getInt(1));
    assertEquals(4, rs.getInt(2));
    assertEquals(0.2, rs.getDouble(3), eps);
    assertEquals(0.2, rs.getDouble(4), eps);
    
    conn.close();
  }
  
  void populateRandomData(Connection conn, String schemaName, String tableName) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("CREATE TABLE %s.%s(id int, value double)", schemaName, tableName));
    Random r = new Random();
    for (int i = 0; i < 100; i++) {
      stmt.execute(String.format("INSERT INTO %s.%s(id, value) VALUES(%s, %f)",
          schemaName, tableName, i, r.nextDouble()));
    }
    stmt.close();
  }
  
  void printTableContent(Connection conn, String schemaName, String tableName) throws SQLException {
    ResultSet rs = conn.createStatement().executeQuery(String.format("SELECT * FROM %s.%s", schemaName, tableName));
    System.out.println(schemaName + "." + tableName);
    
    boolean isFirst = true;
    StringBuilder row = new StringBuilder();
    
    int colCount = rs.getMetaData().getColumnCount();
    for (int i = 0; i < colCount; i++) {
      if (isFirst) {
        isFirst = false;
      }
      else {
        row.append("\t");
      }
      row.append(rs.getMetaData().getColumnName(i+1));
    }
    System.out.println(row);
    
    // data
    while (rs.next()) {
      row = new StringBuilder();
      isFirst = true;
      for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
        if (isFirst) {
          isFirst = false;
        }
        else {
          row.append("\t");
        }
        row.append(rs.getObject(i+1));
      }
      System.out.println(row);
    }
    System.out.println();
  }

}

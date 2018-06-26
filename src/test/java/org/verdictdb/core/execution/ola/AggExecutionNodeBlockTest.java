package org.verdictdb.core.execution.ola;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.execution.AggExecutionNode;
import org.verdictdb.core.execution.QueryExecutionNode;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.rewriter.ScrambleMetaForTable;
import org.verdictdb.core.scramble.Scrambler;
import org.verdictdb.core.scramble.UniformScrambler;
import org.verdictdb.core.sql.QueryToSql;
import org.verdictdb.core.sql.SelectQueryToSql;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sql.syntax.H2Syntax;

public class AggExecutionNodeBlockTest {
  
  static Connection conn;
  
  static String originalSchema = "originalschema";

  static String originalTable = "originalschema";

  static String newSchema = "newschema";

  static String newTable = "newtable";
  
  static ScrambleMeta scrambleMeta = new ScrambleMeta();
  
//  ScrambleMeta generateTestScrambleMeta() {
//    int aggblockCount = 2;
//    ScrambleMeta meta = new ScrambleMeta();
//    meta.insertScrambleMetaEntry("default", "people",
//        Scrambler.getAggregationBlockColumn(),
//        Scrambler.getSubsampleColumn(),
//        Scrambler.getTierColumn(),
//        aggblockCount);
//    return meta;
//  }
  
  @BeforeClass
  public static void setupH2Database() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:aggexecnodeblocktest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
    conn.createStatement().execute(String.format("CREATE SCHEMA \"%s\"", originalSchema));
    conn.createStatement().execute(String.format("CREATE SCHEMA \"%s\"", newSchema));
    populateRandomData(conn, originalSchema, originalTable);
    
    int aggBlockCount = 2;
    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, newSchema, newTable, aggBlockCount);
    CreateTableAsSelectQuery createQuery = scrambler.scrambledTableCreationQuery();
    String scrambleSql = QueryToSql.convert(new H2Syntax(), createQuery);
    conn.createStatement().execute(scrambleSql);
    ScrambleMetaForTable metaEntry = scrambler.generateMeta();
    scrambleMeta.insertScrambleMetaEntry(metaEntry);
  }
  
  @AfterClass
  public static void closeH2Connection() throws SQLException {
    conn.close();
  }

  @Test
  public void testConvertFlatToProgressiveAgg() throws VerdictDBValueException {
    SelectQuery aggQuery = SelectQuery.create(
        new AliasedColumn(ColumnOp.count(), "agg"),
        new BaseTable(newSchema, newTable, "t"));
    AggExecutionNode aggnode = AggExecutionNode.create(aggQuery, newSchema);
    AggExecutionNodeBlock block = new AggExecutionNodeBlock(aggnode);
    QueryExecutionNode converted = block.convertToProgressiveAgg(newSchema, scrambleMeta);
    converted.print();
    converted.execute(new JdbcConnection(conn, new H2Syntax()));
  }
  
  static void populateRandomData(Connection conn, String schemaName, String tableName) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    Random r = new Random();
    for (int i = 0; i < 10; i++) {
      stmt.execute(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, r.nextDouble()));
    }
    stmt.close();
  }

}

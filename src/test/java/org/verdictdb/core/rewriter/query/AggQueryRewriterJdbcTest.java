package org.verdictdb.core.rewriter.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.scramble.Scrambler;
import org.verdictdb.core.scramble.UniformScrambler;
import org.verdictdb.core.sql.CreateTableToSql;
import org.verdictdb.core.sql.SelectQueryToSql;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sql.syntax.H2Syntax;

public class AggQueryRewriterJdbcTest {

  static String originalSchema = "originalschema";

  static String originalTable = "originalschema";

  static String newSchema = "newschema";

  static String newTable  = "newtable";

  static int aggblockCount = 2;

  static Connection conn;

  @BeforeClass
  public static void setupDbConnAndScrambledTable() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:aggrewriter;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
    conn.createStatement().execute(String.format("CREATE SCHEMA \"%s\"", originalSchema));
    conn.createStatement().execute(String.format("CREATE SCHEMA \"%s\"", newSchema));
    populateData(conn, originalSchema, originalTable);
    
    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, newSchema, newTable, aggblockCount);
    CreateTableAsSelectQuery createQuery = scrambler.createQuery();
    CreateTableToSql createToSql = new CreateTableToSql(new H2Syntax());
    String scrambleSql = createToSql.toSql(createQuery);
    conn.createStatement().execute(String.format("DROP TABLE IF EXISTS \"%s\".\"%s\"", newSchema, newTable));
    conn.createStatement().execute(scrambleSql);
  }

  @Test
  public void testSelectSumBaseTable() throws VerdictDBException {
    BaseTable base = new BaseTable(newSchema, newTable, "t");
    String aliasName = "sum1";
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("t", "value")), aliasName)),
        base);
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<Pair<AbstractRelation, AggblockMeta>> rewritten = rewriter.rewrite(relation);
    
    SelectQueryToSql relToSql = new SelectQueryToSql(new H2Syntax());
    
    for (int i = 0; i < rewritten.size(); i++) {
      String query_string = relToSql.toSql(rewritten.get(i).getLeft());
      System.out.println(query_string);
      
      JdbcConnection jdbcConn = new JdbcConnection(conn, new H2Syntax());
      DbmsQueryResult result = jdbcConn.executeQuery(query_string);
      
      result.printContent();
    }
  }

  static void populateData(Connection conn, String schemaName, String tableName) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    Random r = new Random();
    for (int i = 0; i < 100; i++) {
      stmt.execute(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, (double) i+1));
    }
    stmt.close();
  }

  ScrambleMeta generateTestScrambleMeta() {
    ScrambleMeta meta = new ScrambleMeta();
    meta.insertScrambleMetaEntry(newSchema, newTable,
        Scrambler.getAggregationBlockColumn(),
//        Scrambler.getInclusionProbabilityColumn(),
//        Scrambler.getInclusionProbabilityBlockDifferenceColumn(),
        Scrambler.getSubsampleColumn(),
        Scrambler.getTierColumn(),
        aggblockCount);
    return meta;
  }

}

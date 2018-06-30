package org.verdictdb.core.execution;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.core.ScrambleMeta;
import org.verdictdb.core.ScrambleMetaForTable;
import org.verdictdb.core.execution.ola.AsyncQueryExecutionPlan;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.scramble.UniformScrambler;
import org.verdictdb.core.sql.NonValidatingSQLParser;
import org.verdictdb.core.sql.QueryToSql;
import org.verdictdb.core.sql.RelationStandardizer;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sql.syntax.H2Syntax;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;
import static org.junit.Assert.assertEquals;

public class AsyncAggScaleTest {

  static Connection conn;

  static Statement stmt;

  static int aggBlockCount = 3;

  static ScrambleMeta meta = new ScrambleMeta();

  static StaticMetaData staticMetaData = new StaticMetaData();

  static String scrambledTable;

  String placeholderSchemaName = "placeholderSchemaName";

  String placeholderTableName = "placeholderTableName";

  static String originalSchema = "originalSchema";

  static String originalTable = "originalTable";

  static String smallTable = "smallTable";

  @BeforeClass
  public static void setupH2Database() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:aggexecnodetest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    stmt = conn.createStatement();
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", originalSchema));
    stmt.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", originalSchema, originalTable));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          originalSchema, originalTable, i, (double) i+1));
    }
    stmt.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"s_id\" int, \"s_value\" double)", originalSchema, smallTable));

    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, originalSchema, "originalTable_scrambled", aggBlockCount);
    CreateTableAsSelectQuery scramblingQuery = scrambler.createQuery();
    stmt.executeUpdate(QueryToSql.convert(new H2Syntax(), scramblingQuery));
    ScrambleMetaForTable tablemeta = scrambler.generateMeta();
    scrambledTable = tablemeta.getTableName();
    meta.insertScrambleMetaEntry(tablemeta);

    staticMetaData.setDefaultSchema(originalSchema);
    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("id", BIGINT),
        new ImmutablePair<>("value", DOUBLE)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, "originalTable_scrambled"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("s_id", BIGINT),
        new ImmutablePair<>("s_value", DOUBLE)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, smallTable), arr);
  }

  @Test
  public void nonScrambleTableTest() throws VerdictDBException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(s_value) from smallTable";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    String before = queryExecutionPlan.getRootNode().toString();
    queryExecutionPlan.setScalingNode();
    String after = queryExecutionPlan.getRootNode().toString();
    assertEquals(before, after);
  }

  @Test
  public void ScrambleTableTest() throws VerdictDBException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    //String before = queryExecutionPlan.getRootNode().toString();
    //queryExecutionPlan.setScalingNode();
    //String after = queryExecutionPlan.getRootNode().toString();
    assertEquals(2, queryExecutionPlan.getRootNode().dependents.get(0).dependents.get(0).getParents().size());
  }
}

package org.verdictdb.core.querying.ola;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.QueryExecutionPlanFactory;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.scrambling.UniformScrambler;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.H2Syntax;
import org.verdictdb.sqlwriter.QueryToSql;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;
import static org.junit.Assert.assertEquals;

public class AsyncAggJoinScaleTest {

  static Connection conn;

  static Statement stmt;

  static int aggBlockCount = 2;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static StaticMetaData staticMetaData = new StaticMetaData();

  static String scrambledTable;

  static String originalSchema = "originalSchema";

  static String originalTable1 = "originalTable1";

  static String originalTable2 = "originalTable2";

  @BeforeClass
  public static void setupH2Database() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:asyncaggscaletest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    stmt = conn.createStatement();
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", originalSchema));
    stmt.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"a_id\" int, \"a_value\" double)", originalSchema, originalTable1));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"a_id\", \"a_value\") VALUES(%s, %f)",
          originalSchema, originalTable1, i, (double) i+1));
    }
    stmt.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"b_id\" int, \"b_value\" double)", originalSchema, originalTable2));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"b_id\", \"b_value\") VALUES(%s, %f)",
          originalSchema, originalTable2, i, (double) i+1));
    }


    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable1, originalSchema, "originalTable1_scrambled", aggBlockCount);
    CreateTableAsSelectQuery scramblingQuery = scrambler.createQuery();
    stmt.executeUpdate(QueryToSql.convert(new H2Syntax(), scramblingQuery));
    ScrambleMeta tablemeta = scrambler.generateMeta();
    tablemeta.setNumberOfTiers(1);
    HashMap<Integer, List<Double>> distribution = new HashMap<>();
    distribution.put(0, Arrays.asList(0.5, 1.0));
    tablemeta.setCumulativeDistributionForTier(distribution);
    scrambledTable = tablemeta.getTableName();
    meta.addScrambleMeta(tablemeta);
    UniformScrambler scrambler2 =
        new UniformScrambler(originalSchema, originalTable2, originalSchema, "originalTable2_scrambled", aggBlockCount);
    CreateTableAsSelectQuery scramblingQuery2 = scrambler2.createQuery();
    stmt.executeUpdate(QueryToSql.convert(new H2Syntax(), scramblingQuery2));
    ScrambleMeta tablemeta2 = scrambler2.generateMeta();
    tablemeta2.setNumberOfTiers(1);
    HashMap<Integer, List<Double>> distribution2 = new HashMap<>();
    distribution2.put(0, Arrays.asList(0.5, 1.0));
    tablemeta2.setCumulativeDistributionForTier(distribution2);
    scrambledTable = tablemeta2.getTableName();
    meta.addScrambleMeta(tablemeta2);


    staticMetaData.setDefaultSchema(originalSchema);
    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("a_id", BIGINT),
        new ImmutablePair<>("a_value", DOUBLE)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, "originalTable1_scrambled"), arr);
    staticMetaData.setDefaultSchema(originalSchema);
    List<Pair<String, Integer>> arr2 = new ArrayList<>();
    arr2.addAll(Arrays.asList(new ImmutablePair<>("b_id", BIGINT),
        new ImmutablePair<>("b_value", DOUBLE)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, "originalTable2_scrambled"), arr2);
  }

  @Test
  public void ScrambleTableTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(a_value+b_value) from originalTable1_scrambled inner join originalTable2_scrambled on a_id=b_id";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable1_scrambled", 0, 1);
    Dimension d2 = new Dimension("originalSchema", "originalTable2_scrambled", 0, 0);
    Assert.assertEquals(
        new HyperTableCube(Arrays.asList(d1, d2)), 
        ((AggExecutionNode) 
            queryExecutionPlan.getRootNode()
            .getExecutableNodeBaseDependent(0)
            .getExecutableNodeBaseDependent(0)).getAggMeta().getCubes().get(0));
    
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependent(0)).setScrambleMetaSet(meta);
    stmt.execute("create schema if not exists \"verdictdb_temp\";");
//    queryExecutionPlan.getRoot().print();

    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    queryExecutionPlan.root.executeAndWaitForTermination();
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void toSqlTest() throws VerdictDBException,SQLException {
    String sql = "select sum(a_value+b_value) from originalTable1_scrambled inner join originalTable2_scrambled on a_id=b_id";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new H2Syntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    actual = actual.replaceAll("vt\\d+", "vt");
    String expected;
//    String expected = "select sum(vt4.\"a_value\" + vt5.\"b_value\") as \"agg0\" " +
//                          "from \"originalSchema\".\"originalTable1_scrambled\" as vt4 " +
//                          "inner join \"originalSchema\".\"originalTable2_scrambled\" as vt5 " +
//                          "on (vt4.\"a_id\" = vt5.\"b_id\") where ((vt4.\"verdictdbaggblock\" >= 0) " +
//                          "and (vt4.\"verdictdbaggblock\" <= 1)) and (vt5.\"verdictdbaggblock\" = 0)";
    String exepected2 = "select sum(vt.\"a_value\" + vt.\"b_value\") as \"agg0\", " +
                            "vt.\"verdictdbtier\" as \"verdictdb_tier_alias\", " +
                            "vt.\"verdictdbtier\" as \"verdictdb_tier_alias\" " +
                            "from \"originalSchema\".\"originalTable1_scrambled\" as vt " +
                            "inner join \"originalSchema\".\"originalTable2_scrambled\" as vt " +
                            "on (vt.\"a_id\" = vt.\"b_id\") " +
                            "where ((vt.\"verdictdbaggblock\" >= 0) " +
                            "and (vt.\"verdictdbaggblock\" <= 1)) and (vt.\"verdictdbaggblock\" = 0) " +
                            "group by \"verdictdb_tier_alias\", \"verdictdb_tier_alias\"";
    assertEquals(exepected2, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
//    actual = actual.replaceAll("verdictdbalias_[0-9]*_[0-9]", "alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    expected = "select sum(unionTable.\"agg0\") as \"agg0\", " +
                   "unionTable.\"verdictdb_tier_alias\" as \"verdictdb_tier_alias\", " +
                   "unionTable.\"verdictdb_tier_alias\" as \"verdictdb_tier_alias\" " +
                   "from (select * from \"verdict_temp\".\"table2\" as verdictdb_alias " +
                   "UNION ALL " +
                   "select * from \"verdict_temp\".\"table1\" as verdictdb_alias) as unionTable " +
                   "group by \"verdictdb_tier_alias\", \"verdictdb_tier_alias\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
//    actual = actual.replaceAll("verdictdbtemptable_[0-9]*_[0-9]", "alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("s\\d+", "ss");
    expected =
        "select sum(verdictdb_internal_tier_consolidated.\"agg0\") as \"ss\" " +
            "from (select 2.0000000000000000 * verdictdb_internal_before_scaling.\"agg0\" as \"agg0\", " +
                   "verdictdb_internal_before_scaling.\"verdictdb_tier_alias\" as \"verdictdb_tier_alias\", " +
            "verdictdb_internal_before_scaling.\"verdictdb_tier_alias\" as \"verdictdb_tier_alias\" " +
            "from \"verdictdb_temp\".\"verdictdbtemptable\" as verdictdb_internal_before_scaling" +
            ") as verdictdb_internal_tier_consolidated";
    assertEquals(expected, actual);
  }
}

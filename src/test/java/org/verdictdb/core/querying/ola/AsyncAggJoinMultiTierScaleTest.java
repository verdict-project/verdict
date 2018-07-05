package org.verdictdb.core.querying.ola;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.connection.StaticMetaData;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaForTable;
import org.verdictdb.core.scrambling.UniformScrambler;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.QueryToSql;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.H2Syntax;

public class AsyncAggJoinMultiTierScaleTest {


  static Connection conn;

  static Statement stmt;

  static int aggBlockCount = 2;

  static ScrambleMeta meta = new ScrambleMeta();

  static StaticMetaData staticMetaData = new StaticMetaData();

  static String scrambledTable;

  static String originalSchema = "originalSchema";

  static String originalTable1 = "originalTable1";

  static String originalTable2 = "originalTable2";

  @BeforeClass
  public static void setupH2Database() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:asyncaggjoinmultitiertest;DB_CLOSE_DELAY=-1";
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
    ScrambleMetaForTable tablemeta = scrambler.generateMeta();
    CreateTableAsSelectQuery scramblingQuery = scrambler.createQuery();
    stmt.executeUpdate(QueryToSql.convert(new H2Syntax(), scramblingQuery));
    tablemeta.setNumberOfTiers(2);
    HashMap<Integer, List<Double>> distribution = new HashMap<>();
    distribution.put(0, Arrays.asList(0.2, 1.0));
    distribution.put(1, Arrays.asList(0.5, 1.0));
    tablemeta.setCumulativeMassDistributionPerTier(distribution);
    scrambledTable = tablemeta.getTableName();
    meta.insertScrambleMetaEntry(tablemeta);
    UniformScrambler scrambler2 =
        new UniformScrambler(originalSchema, originalTable2, originalSchema, "originalTable2_scrambled", aggBlockCount);
    ScrambleMetaForTable tablemeta2 = scrambler2.generateMeta();
    CreateTableAsSelectQuery scramblingQuery2 = scrambler2.createQuery();
    stmt.executeUpdate(QueryToSql.convert(new H2Syntax(), scramblingQuery2));
    tablemeta2.setNumberOfTiers(2);
    HashMap<Integer, List<Double>> distribution2 = new HashMap<>();
    distribution2.put(0, Arrays.asList(0.5, 1.0));
    distribution2.put(1, Arrays.asList(0.2, 1.0));
    tablemeta2.setCumulativeMassDistributionPerTier(distribution2);
    scrambledTable = tablemeta2.getTableName();
    meta.insertScrambleMetaEntry(tablemeta2);


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
    String sql = "select a.verdictdbtier, b.verdictdbtier, " +
        "sum(a_value+b_value) from originalTable1_scrambled as a inner join originalTable2_scrambled as b on a_id=b_id  group by  a.verdictdbtier, b.verdictdbtier";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable1_scrambled", 0, 1);
    Dimension d2 = new Dimension("originalSchema", "originalTable2_scrambled", 0, 0);
    Assert.assertEquals(
        new HyperTableCube(Arrays.asList(d1, d2)), 
        ((AggExecutionNode)queryExecutionPlan.getRootNode().getExecutableNodeBaseDependent(0).getExecutableNodeBaseDependent(0)).getMeta().getCubes().get(0));
    
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependent(0)).setScrambleMeta(meta);
    stmt.execute("create schema if not exists \"verdictdb_temp\";");
//    queryExecutionPlan.getRoot().print();

    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    queryExecutionPlan.root.executeAndWaitForTermination();
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }
}

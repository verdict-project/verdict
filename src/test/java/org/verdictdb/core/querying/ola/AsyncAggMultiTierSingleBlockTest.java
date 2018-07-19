package org.verdictdb.core.querying.ola;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.coordinator.ScramblingCoordinator;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBException;

public class AsyncAggMultiTierSingleBlockTest {
  
  static Connection conn;

  static Statement stmt;

  static int aggBlockCount = 2;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static StaticMetaData staticMetaData = new StaticMetaData();
  
  String placeholderSchemaName = "placeholderSchemaName";

  String placeholderTableName = "placeholderTableName";

  static String originalSchema = "originalSchema";

  static String originalTable = "originalTable";

  static String smallTable = "smallTable";
  
  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "async_agg_multi_tier_single_block";

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  private static final String TABLE_NAME = "mytable";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);

    stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", originalSchema));
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", originalSchema));
    stmt.executeUpdate(String.format("CREATE TABLE `%s`.`%s` (`id` int, `value` double)", originalSchema, originalTable));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(String.format("INSERT INTO `%s`.`%s` (`id`, `value`) VALUES(%s, %f)",
          originalSchema, originalTable, i, (double) i+1));
    }
    stmt.executeUpdate(String.format("CREATE TABLE `%s`.`%s` (`s_id` int, `s_value` double)", originalSchema, smallTable));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(String.format("INSERT INTO `%s`.`%s` (`s_id`, `s_value`) VALUES(%s, %f)",
          originalSchema, smallTable, i, (double) i+1));
    }
    
    // create scrambled table
    DbmsConnection dbmsConn = JdbcConnection.create(conn);
    String scrambleSchema = MYSQL_DATABASE;
    String scratchpadSchema = MYSQL_DATABASE;
    String scrambledTable = "originalTable_scrambled";
    String primaryColumn = null;
    Map<String, String> options = new HashMap<>();
    options.put("blockColumnName", "verdictdbaggblock");
    options.put("tierColumnName", "verdictdbtier");
    long blockSize = 5;
    ScramblingCoordinator scrambler = new ScramblingCoordinator(dbmsConn, scrambleSchema, scratchpadSchema, blockSize);
    ScrambleMeta tablemeta = scrambler.scramble(originalSchema, originalTable, originalSchema, scrambledTable, "fastconverge", primaryColumn, options);

//    UniformScrambler scrambler =
//        new UniformScrambler(originalSchema, originalTable, originalSchema, "originalTable_scrambled", aggBlockCount);
//    CreateTableAsSelectQuery scramblingQuery = scrambler.createQuery();
//    stmt.executeUpdate(QueryToSql.convert(new MysqlSyntax(), scramblingQuery));
//    ScrambleMeta tablemeta = scrambler.generateMeta();
//    tablemeta.setNumberOfTiers(2);
//    HashMap<Integer, List<Double>> distribution = new HashMap<>();
//    distribution.put(0, Arrays.asList(0.5, 1.0));
//    distribution.put(1, Arrays.asList(0.2, 1.0));
//    tablemeta.setCumulativeDistributionForTier(distribution);
//    scrambledTable = tablemeta.getTableName();
    meta.addScrambleMeta(tablemeta);

    staticMetaData.setDefaultSchema(originalSchema);
    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("id", BIGINT),
        new ImmutablePair<>("value", DOUBLE),
        new ImmutablePair<>("verdictdbtier", BIGINT)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, scrambledTable), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("s_id", BIGINT),
        new ImmutablePair<>("s_value", DOUBLE)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, smallTable), arr);
    
    // scratchpad schema
    stmt.execute("create schema if not exists `verdictdb_temp`;");
  }

  @Test
  public void test() {
    fail("Not yet implemented");
  }

}

package org.verdictdb.metastore;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public class ScrambleMetaStoreTest {
  
  static Connection mysqlConn;

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "meta_store_test";

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
  public static void setupMySqlDatabase() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    mysqlConn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
    mysqlConn.createStatement().execute(
        String.format("DROP DATABASE IF EXISTS %s", MYSQL_DATABASE));
    mysqlConn.createStatement().execute(
        String.format("DROP DATABASE IF EXISTS %s", ScrambleMetaStore.getDefaultStoreSchema()));
  }
  
  @AfterClass
  public static void tearDown() throws SQLException {
    mysqlConn.createStatement().execute(
        String.format("DROP DATABASE IF EXISTS %s", MYSQL_DATABASE));
    mysqlConn.createStatement().execute(
        String.format("DROP DATABASE IF EXISTS %s", ScrambleMetaStore.getDefaultStoreSchema()));
  }

  @Test
  public void testAddScrambleMeta() throws VerdictDBException {
    DbmsConnection dbmsConnection = JdbcConnection.create(mysqlConn);
    ScrambleMeta scrambleMeta = createScrambleMeta();
    ScrambleMetaStore metaStore = new ScrambleMetaStore(dbmsConnection);
    metaStore.addToStore(scrambleMeta);
    
    // tests
    DbmsQueryResult result = dbmsConnection.execute(
        String.format("SELECT * FROM %s.%s", metaStore.getStoreSchema(), metaStore.getMetaStoreTableName()));
    assertEquals(1, result.getRowCount());
    assertEquals(4, result.getColumnCount());
    assertEquals(ScrambleMetaStore.getAddedAtColumn(), result.getColumnName(0));
    assertEquals(ScrambleMetaStore.getSchemaColumn(), result.getColumnName(1));
    assertEquals(ScrambleMetaStore.getTableColumn(), result.getColumnName(2));
    assertEquals(ScrambleMetaStore.getDataColumn(), result.getColumnName(3));
    
    result.next();
    String jsonString = result.getString(3);
    ScrambleMeta retrieved = ScrambleMeta.fromJsonString(jsonString);
    assertEquals(scrambleMeta, retrieved);
  }
  
  private ScrambleMeta createScrambleMeta() throws VerdictDBValueException {
    String scrambleSchemaName = "new_schema";
    String scrambleTableName = "New_Table";
    String originalSchemaName = "Original_Schema";
    String originalTableName = "origiNAL_TABLE";
    String blockColumn = "verdictDBblock";
    int blockCount = 3;
    String tierColumn = "VerdictTIER";
    int tierCount = 2;
    
    Map<Integer, List<Double>> cumulativeMassDistributionPerTier = new HashMap<>();
    List<Double> dist0 = Arrays.asList(0.3, 0.6, 1.0);
    List<Double> dist1 = Arrays.asList(0.2, 0.5, 1.0);
    cumulativeMassDistributionPerTier.put(0, dist0);
    cumulativeMassDistributionPerTier.put(1, dist1);

    ScrambleMeta meta = new ScrambleMeta(
        scrambleSchemaName,  scrambleTableName,
        originalSchemaName,  originalTableName,
        blockColumn,  blockCount,
        tierColumn,  tierCount,
        cumulativeMassDistributionPerTier);
    
    return meta;
  }

}

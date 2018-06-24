package org.verdictdb.core;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DataTypeConverter;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.sql.syntax.H2Syntax;
import org.verdictdb.sql.syntax.MysqlSyntax;
import org.verdictdb.sql.syntax.PostgresqlSyntax;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DbmsMetadataCacheTest {

  static Connection conn;

  private DbmsMetaDataCache metadataCache = new DbmsMetaDataCache(new JdbcConnection(conn, new H2Syntax()));

  private static Statement stmt;

  private static Connection postgresqlConn;
  
  private static Connection mysqlConn;
  
  private static final String MYSQL_HOST;
  
  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }
  
  private static final String MYSQL_DATABASE = "test";
  
  private static final String MYSQL_UESR = "root";
  
  private static final String MYSQL_PASSWORD = "";
  
  private static final String POSTGRES_HOST;
  
  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      POSTGRES_HOST = "postgres";
    } else {
      POSTGRES_HOST = "localhost";
    }
  }
  
  private static final String POSTGRES_DATABASE = "test";
  
  private static final String POSTGRES_USER = "postgres";
  
  private static final String POSTGRES_PASSWORD = "";

  @BeforeClass
  public static void setupDatabases() throws SQLException {
    final String DB_CONNECTION = "jdbc:h2:mem:metadatacachetest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female", 15, 170.2, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female", 17, 156.5, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168.1, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male", 14, 178.6, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male", 18, 190.7, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Sam", "male", 18, 190.0, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Alice", "female", 18, 190.21, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Bob", "male", 18, 190.3, "CHN", "2017-10-12 21:22:23"));
    stmt = conn.createStatement();
    stmt.execute("DROP TABLE PEOPLE IF EXISTS");
    stmt.execute("CREATE TABLE PEOPLE(id smallint, name varchar(255), gender varchar(8), age int, height float, nation varchar(8), birth timestamp)");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      String age = row.get(3).toString();
      String height = row.get(4).toString();
      String nation = row.get(5).toString();
      String birth = row.get(6).toString();
      stmt.execute(String.format("INSERT INTO PEOPLE(id, name, gender, age, height, nation, birth) VALUES(%s, '%s', '%s', %s, %s, '%s', '%s')", id, name, gender, age, height, nation, birth));
    }

    String postgresConnectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    postgresqlConn = DriverManager.getConnection(postgresConnectionString, POSTGRES_USER, POSTGRES_PASSWORD);
    
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    mysqlConn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
//    mysqlConn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "");
  }

  @Test
  public void getSchemaTest() throws SQLException {
    List<String> schemas = metadataCache.getSchemas();
    assertEquals(2, schemas.size());
    assertEquals(true, schemas.get(0).equals("PUBLIC")||schemas.get(1).equals("PUBLIC"));
    assertEquals(true, schemas.get(0).equals("INFORMATION_SCHEMA")||schemas.get(1).equals("INFORMATION_SCHEMA"));
  }

  @Test
  public void getTableTest() throws SQLException {
    List<String> tables = metadataCache.getTables("PUBLIC");
    assertEquals(1, tables.size());
    assertEquals("PEOPLE", tables.get(0));
  }

  @Test
  public void getColumnsTest() throws SQLException {
    List<Pair<String, Integer>> columns = metadataCache.getColumns("PUBLIC", "PEOPLE");
    assertEquals(7, columns.size());
    assertEquals("ID", columns.get(0).getKey());
    assertEquals(DataTypeConverter.typeInt("smallint"), (int)columns.get(0).getValue());
    assertEquals("NAME", columns.get(1).getKey());
    assertEquals(DataTypeConverter.typeInt("varchar"), (int)columns.get(1).getValue());
    assertEquals("GENDER", columns.get(2).getKey());
    assertEquals(DataTypeConverter.typeInt("varchar"), (int)columns.get(2).getValue());
    assertEquals("AGE", columns.get(3).getKey());
    assertEquals(DataTypeConverter.typeInt("integer"), (int)columns.get(3).getValue());
    assertEquals("HEIGHT", columns.get(4).getKey());
    assertEquals(DataTypeConverter.typeInt("double"), (int)columns.get(4).getValue());
    assertEquals("NATION", columns.get(5).getKey());
    assertEquals(DataTypeConverter.typeInt("varchar"), (int)columns.get(5).getValue());
    assertEquals("BIRTH", columns.get(6).getKey());
    assertEquals(DataTypeConverter.typeInt("timestamp"), (int)columns.get(6).getValue());
  }

  @Test
  public void getPartitionTestPostgres() throws SQLException {
    Statement statement = postgresqlConn.createStatement();
    DbmsMetaDataCache postgresMetadataCache = new DbmsMetaDataCache(new JdbcConnection(postgresqlConn, new PostgresqlSyntax()));
    statement.execute("DROP TABLE IF EXISTS measurement");
    statement.execute("DROP TABLE IF EXISTS measurement_y2006m02");
    statement.execute("DROP TABLE IF EXISTS measurement_y2006m03");
    statement.execute("DROP TABLE IF EXISTS measurement_y2006m04");
    statement.execute("CREATE TABLE measurement(city_id int not null, logdate date not null, peaktemp int, unitsales int) partition by range(logdate, city_id)");
    List<String> partition = postgresMetadataCache.getPartitionColumns("public", "measurement");
    assertEquals(2, partition.size());
    assertEquals("logdate", partition.get(0));
    assertEquals("city_id", partition.get(1));
  }
  
  @Test
  public void getPartitionTestMySQL() throws SQLException {
    Statement statement = mysqlConn.createStatement();
    DbmsMetaDataCache mysqlMetadataCache = new DbmsMetaDataCache(new JdbcConnection(mysqlConn, new MysqlSyntax()));
    statement.execute("DROP TABLE IF EXISTS tp");
    statement.execute("CREATE TABLE tp (c1 INT, c2 INT, c3 VARCHAR(25)) PARTITION BY HASH(c1 + c2) PARTITIONS 4;");
    List<String> partition = mysqlMetadataCache.getPartitionColumns("test", "tp");
    assertEquals(1, partition.size());
    assertEquals(true, partition.get(0).equals("c1 + c2")||partition.get(0).equals("(`c1` + `c2`)"));
  }
}

package org.verdictdb.connection;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SparkSyntax;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SparkConnectionTest {
  
  static private SparkSession spark; 
  
  @BeforeClass
  static public void setupSparkSession() {
    spark = SparkSession.builder().appName("SparkConnectionTest")
    .master("local")
    .enableHiveSupport()
    .getOrCreate();
  }

  @Test
  public void testSparkConnectionExecute() throws VerdictDBDbmsException {
    SparkConnection sparkConnection = new SparkConnection(spark, new SparkSyntax());
    sparkConnection.execute("CREATE SCHEMA IF NOT EXISTS myschema");
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju"));
    contents.add(Arrays.<Object>asList(2, "Sonia"));
    contents.add(Arrays.<Object>asList(3, "Asha"));

    sparkConnection.execute("CREATE TABLE IF NOT EXISTS myschema.PERSON(id int, name varchar(255))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      sparkConnection.execute(String.format("INSERT INTO myschema.PERSON VALUES(%s, '%s')", id, name));
    }

    sparkConnection.execute("DROP TABLE IF EXISTS myschema.PERSON");
    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema");

  }

  @Test
  public void testSparkConnection() throws VerdictDBDbmsException {
//    spark.read().format("jdbc");
    SparkConnection sparkConnection = new SparkConnection(spark, new SparkSyntax());
    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema CASCADE");
    sparkConnection.execute("CREATE SCHEMA IF NOT EXISTS myschema");
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju"));
    contents.add(Arrays.<Object>asList(2, "Sonia"));
    contents.add(Arrays.<Object>asList(3, "Asha"));

    sparkConnection.execute("CREATE TABLE IF NOT EXISTS myschema.PERSON(id int, name varchar(255))");
    List<String> schemas = sparkConnection.getSchemas();
    assertEquals("default", schemas.get(0));
    assertEquals("myschema", schemas.get(1));
    List<String> tables = sparkConnection.getTables("myschema");
    assertEquals("person", tables.get(0));
    List<Pair<String, String>> columns = sparkConnection.getColumns("myschema", "person");
    assertEquals(2, columns.size());
    assertEquals(new ImmutablePair<>("id", "int"), columns.get(0));
    assertEquals(new ImmutablePair<>("name", "string"), columns.get(1));
    
//    sparkConnection.execute("DROP TABLE IF EXISTS myschema.PERSON");
//    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema");
    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema CASCADE");
  }

  @Test
  public void testPartitionColumn() throws VerdictDBDbmsException {
//    SparkSession spark = SparkSession.builder().appName("test")
//        .master("local")
//        .config("spark.sql.catalogImplementation", "hive")
//        .getOrCreate();
    SparkConnection sparkConnection = new SparkConnection(spark, new SparkSyntax());
    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema CASCADE");
    sparkConnection.execute("CREATE SCHEMA IF NOT EXISTS myschema");
    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju"));
    contents.add(Arrays.<Object>asList(2, "Sonia"));
    contents.add(Arrays.<Object>asList(3, "Asha"));

    sparkConnection.execute("CREATE TABLE IF NOT EXISTS myschema.PERSON(id int, name varchar(255))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      sparkConnection.execute(String.format("INSERT INTO myschema.PERSON VALUES(%s, '%s')", id, name));
    }
    sparkConnection.execute("create table myschema.newtable using parquet partitioned by (id) as select * from myschema.person as t");
    
    // when there exists partitions
    List<String> partitions = sparkConnection.getPartitionColumns("myschema", "newtable");
    assertEquals(1, partitions.size());
    assertEquals("id", partitions.get(0));
//    assertEquals("id=1", partitions.get(0));
//    assertEquals("id=2", partitions.get(1));
//    assertEquals("id=3", partitions.get(2));
    
    // when there does not exist any partitions
    partitions = sparkConnection.getPartitionColumns("myschema", "person");
    assertEquals(0, partitions.size());
    
    // clean up
//    sparkConnection.execute("DROP TABLE IF EXISTS myschema.PERSON");
//    sparkConnection.execute("DROP TABLE IF EXISTS myschema.newtable");
//    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema");
    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema CASCADE");
  }

  @Test
  public void testVariousDataType() throws VerdictDBDbmsException {
//    SparkSession spark = SparkSession.builder().appName("test")
//        .master("local")
//        .config("spark.sql.catalogImplementation", "hive")
//        .getOrCreate();
    SparkConnection sparkConnection = new SparkConnection(spark, new SparkSyntax());
    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema CASCADE");
    sparkConnection.execute("CREATE SCHEMA IF NOT EXISTS myschema");
    sparkConnection.execute("CREATE TABLE IF NOT EXISTS myschema.PERSON(id int, name varchar(255), a bigint, b float, c date, d timestamp, e array<int>" +
        ", f decimal(5,2), g double, h char(5), i boolean)");
    List<Pair<String, String>> columns = sparkConnection.getColumns("myschema", "person");
    assertEquals(11, columns.size());
    assertEquals(new ImmutablePair<>("id", "int"), columns.get(0));
    assertEquals(new ImmutablePair<>("name", "string"), columns.get(1));
    assertEquals(new ImmutablePair<>("a", "bigint"), columns.get(2));
    assertEquals(new ImmutablePair<>("b", "float"), columns.get(3));
    assertEquals(new ImmutablePair<>("c", "date"), columns.get(4));
    assertEquals(new ImmutablePair<>("d", "timestamp"), columns.get(5));
    assertEquals(new ImmutablePair<>("e", "array<int>"), columns.get(6));
    assertEquals(new ImmutablePair<>("f", "decimal(5,2)"), columns.get(7));
    assertEquals(new ImmutablePair<>("g", "double"), columns.get(8));
    assertEquals(new ImmutablePair<>("h", "string"), columns.get(9));
    assertEquals(new ImmutablePair<>("i", "boolean"), columns.get(10));
//    sparkConnection.execute("DROP TABLE IF EXISTS myschema.PERSON");
//    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema");
    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema CASCADE");
  }
  
  @Test
  public void testCreateView() throws VerdictDBDbmsException {
    SparkConnection sparkConnection = new SparkConnection(spark, new SparkSyntax());
    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema CASCADE");
    sparkConnection.execute("CREATE SCHEMA IF NOT EXISTS myschema");
    sparkConnection.execute("CREATE TABLE IF NOT EXISTS myschema.PERSON(id int, name varchar(255), a bigint, b float, c date, d timestamp, e array<int>" +
        ", f decimal(5,2), g double, h char(5), i boolean)");
    
    long start = System.currentTimeMillis();
    String sql1 = "CREATE TABLE myschema.newtable AS select * FROM myschema.person";
    sparkConnection.execute(sql1);
//    System.out.println(System.currentTimeMillis() - start);
    
    start = System.currentTimeMillis();
    String sql2 = "CREATE TEMPORARY VIEW newview AS select * FROM myschema.person";
    sparkConnection.execute(sql2);
//    System.out.println(System.currentTimeMillis() - start);
    
    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema CASCADE");
  }
}

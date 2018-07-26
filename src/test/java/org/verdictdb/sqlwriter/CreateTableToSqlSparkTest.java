package org.verdictdb.sqlwriter;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.SparkSyntax;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class CreateTableToSqlSparkTest {
  
  static SparkSession spark;
  
  @BeforeClass
  public static void loadSparkSession() {
    spark = SparkSession.builder().appName("test")
                        .master("local")
                        .config("spark.sql.catalogImplementation", "hive")
                        .getOrCreate();
    
    spark.sql("CREATE SCHEMA IF NOT EXISTS tpch");
    spark.sql("CREATE TABLE  IF NOT EXISTS tpch.nation"
        + "    (n_nationkey  INT,\n" + 
        "       n_name       CHAR(25),\n" + 
        "       n_regionkey  INT,\n" + 
        "       n_comment    VARCHAR(152),\n" + 
        "       n_dummy varchar(10))");
  }
  
  @AfterClass
  public static void breakDown() {
    spark.sql("DROP TABLE IF EXISTS tpch.newtable");
    spark.sql("DROP TABLE IF EXISTS tpch.nation");
    spark.sql("DROP SCHEMA IF EXISTS tpch");
  }

  @Test
  public void testSparkContextLoading() {
    List<Row> dataset = spark.sql("select 1").collectAsList();
    System.out.println(dataset);
  }
  
  @Test
  public void createTableSelectAllWithSignlePartitionTest() throws VerdictDBException {
    BaseTable base = new BaseTable("tpch", "nation", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelectQuery create = new CreateTableAsSelectQuery("tpch", "newtable", relation);
    create.addPartitionColumn("n_nationkey");
    String expected = "create table `tpch`.`newtable` using parquet partitioned by (`n_nationkey`) as select * from `tpch`.`nation` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new SparkSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);
    
    spark.sql("drop table if exists tpch.newtable");
    spark.sql(actual);
  }

  @Test
  public void createTableSelectAllWithMultiPartitionTest() throws VerdictDBException {
    BaseTable base = new BaseTable("tpch", "nation", "t");
    SelectQuery relation = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        base);
    CreateTableAsSelectQuery create = new CreateTableAsSelectQuery("tpch", "newtable", relation);
    create.addPartitionColumn("n_nationkey");
    create.addPartitionColumn("n_name");
    String expected = "create table `tpch`.`newtable` using parquet partitioned by (`n_nationkey`, `n_name`) as select * from `tpch`.`nation` as t";
    CreateTableToSql queryToSql = new CreateTableToSql(new SparkSyntax());
    String actual = queryToSql.toSql(create);
    assertEquals(expected, actual);
    
    spark.sql("drop table if exists tpch.newtable");
    spark.sql(actual);
  }

}

package org.verdictdb.core.connection;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SparkSyntax;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SparkConnectionTest {

    @Test
    public void testSparkConnectionExecute() throws VerdictDBDbmsException {
        SparkSession spark = SparkSession.builder().appName("test")
                .master("local")
                .config("spark.sql.catalogImplementation", "hive")
                .getOrCreate();
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

        sparkConnection.execute("create table myschema.newtable using parquet partitioned by (id) as select * from myschema.person as t");
        List<String> partitions = sparkConnection.getPartitionColumns("myschema", "newtable");
        assertEquals("id=1", partitions.get(0));
        assertEquals("id=2", partitions.get(1));
        assertEquals("id=3", partitions.get(2));
        sparkConnection.execute("DROP TABLE IF EXISTS myschema.PERSON");
        sparkConnection.execute("DROP TABLE IF EXISTS myschema.newtable");
        sparkConnection.execute("DROP SCHEMA IF EXISTS myschema");

    }

    @Test
    public void testSparkConnection() throws VerdictDBDbmsException {
        SparkSession spark = SparkSession.builder().appName("test")
                .master("local")
                .config("spark.sql.catalogImplementation", "hive")
                .getOrCreate();
        spark.read().format("jdbc");
        SparkConnection sparkConnection = new SparkConnection(spark, new SparkSyntax());
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
        assertEquals(new ImmutablePair<>("id", "int"), columns.get(0));
        assertEquals(new ImmutablePair<>("name", "string"), columns.get(1));
        sparkConnection.execute("DROP TABLE IF EXISTS myschema.PERSON");
        sparkConnection.execute("DROP SCHEMA IF EXISTS myschema");
    }
}

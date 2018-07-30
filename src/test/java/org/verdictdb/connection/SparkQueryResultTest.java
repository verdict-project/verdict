package org.verdictdb.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SparkSyntax;

public class SparkQueryResultTest {

  static SparkSession spark;

  static SparkConnection sparkConnection;

  static List<List<Object>> contents = new ArrayList<>();

  @BeforeClass
  public static void setupSpark() throws VerdictDBDbmsException {
    spark = SparkSession.builder().appName("SparkConnectionResultTest")
        .master("local")
        .enableHiveSupport()
        .getOrCreate();
    sparkConnection = new SparkConnection(spark, new SparkSyntax());
    sparkConnection.execute("CREATE SCHEMA IF NOT EXISTS myschema");
    contents.add(Arrays.<Object>asList(1, "Anju"));
    contents.add(Arrays.<Object>asList(2, "Sonia"));
    contents.add(Arrays.<Object>asList(3, "Asha"));

    sparkConnection.execute("CREATE TABLE IF NOT EXISTS myschema.PERSON(id int, name varchar(255))");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      sparkConnection.execute(String.format("INSERT INTO myschema.PERSON VALUES(%s, '%s')", id, name));
    }
  }

  @AfterClass
  public static void tearDown() throws VerdictDBDbmsException {
    sparkConnection.execute("DROP TABLE IF EXISTS myschema.PERSON");
    sparkConnection.execute("DROP SCHEMA IF EXISTS myschema");
  }

  @Test
  public void testSparkQueryResultValues() throws SQLException, VerdictDBDbmsException {
    DbmsQueryResult rs = sparkConnection.execute("SELECT * FROM myschema.PERSON order by id");
    int index = 0;
    while (rs.next()) {
      int id = (Integer) rs.getValue(0);
      String name = (String) rs.getValue(1);
      assertEquals(contents.get(index).get(0), id);
      assertEquals(contents.get(index).get(1), name);
      index += 1;
    }
    assertEquals(3, index);
    assertEquals(2, rs.getColumnCount());
  }

  @Test
  public void testSparkQueryColumnName() throws VerdictDBDbmsException {
    DbmsQueryResult rs = sparkConnection.execute("SELECT name as alias1 FROM myschema.PERSON");
    assertTrue("alias1".equalsIgnoreCase(rs.getColumnName(0)));

    rs = sparkConnection.execute("SELECT `p`.name as `alias1` FROM myschema.PERSON `p`");
    assertTrue("alias1".equalsIgnoreCase(rs.getColumnName(0)));
  }
}

package confidential;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.VerdictContext;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.SparkConnection;
import org.verdictdb.coordinator.VerdictSingleResult;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.SparkSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import static org.junit.Assert.assertEquals;

public class TableauTPCHRegularQuery22Test {


  static final String TEST_SCHEMA = "tpch_flat_orc_2";

  static DbmsConnection conn;

  static SparkSession spark;

  @BeforeClass
  public static void setupSpark() throws VerdictDBException {
    String appname = "scramblingCoordinatorTest";
    spark = DatabaseConnectionHelpers.setupSpark(appname, TEST_SCHEMA);
    conn = new SparkConnection(spark);

  }

  @Test
  public void test() throws VerdictDBException {
    String sql = "SELECT `t2`.`calculation_5131031153053149` AS `calculation_5131031153053149`,\n" +
        "  `t2`.`cnt_c_custkey_ok` AS `cnt_c_custkey_ok`,\n" +
        "  `t7`.`x_measure__4` AS `sum_sum_acctbal__copy__ok`\n" +
        "FROM (\n" +
        "  SELECT CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END AS `calculation_5131031153053149`,\n" +
        "    COUNT(`customer`.`c_custkey`) AS `cnt_c_custkey_ok`\n" +
        "  FROM `tpch_flat_orc_2`.`orders` `orders`\n" +
        "    RIGHT OUTER JOIN `tpch_flat_orc_2`.`customer` `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)\n" +
        "    JOIN (\n" +
        "    SELECT AVG(`t0`.`x_measure__0`) AS `x_measure__1`\n" +
        "    FROM (\n" +
        "      SELECT `customer`.`c_custkey` AS `c_custkey`,\n" +
        "        MAX((CASE WHEN (`customer`.`c_acctbal` > 0) THEN `customer`.`c_acctbal` ELSE NULL END)) AS `x_measure__0`\n" +
        "      FROM `tpch_flat_orc_2`.`customer` `customer`\n" +
        "      WHERE (CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '13' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '17' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '18' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '23' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '29' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '30' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '31')\n" +
        "      GROUP BY `customer`.`c_custkey`\n" +
        "    ) `t0`\n" +
        "    GROUP BY `x_measure__1`\n" +
        "    HAVING (COUNT(1) > 0)\n" +
        "  ) `t1`\n" +
        "  WHERE ((CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '13' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '17' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '18' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '23' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '29' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '30' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '31') AND ((`customer`.`c_acctbal` > `t1`.`x_measure__1`) AND (`orders`.`o_orderkey` IS NULL)))\n" +
        "  GROUP BY `calculation_5131031153053149`\n" +
        ") `t2`\n" +
        "  JOIN (\n" +
        "  SELECT `t5`.`calculation_5131031153053149` AS `calculation_5131031153053149`,\n" +
        "    SUM(`t6`.`x_measure__3`) AS `x_measure__4`\n" +
        "  FROM (\n" +
        "    SELECT `customer`.`c_custkey` AS `c_custkey`,\n" +
        "      CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END AS `calculation_5131031153053149`\n" +
        "    FROM `tpch_flat_orc_2`.`orders` `orders`\n" +
        "      RIGHT OUTER JOIN `tpch_flat_orc_2`.`customer` `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)\n" +
        "      JOIN (\n" +
        "      SELECT AVG(`t3`.`x_measure__0`) AS `x_measure__1`\n" +
        "      FROM (\n" +
        "        SELECT `customer`.`c_custkey` AS `c_custkey`,\n" +
        "          MAX((CASE WHEN (`customer`.`c_acctbal` > 0) THEN `customer`.`c_acctbal` ELSE NULL END)) AS `x_measure__0`\n" +
        "        FROM `tpch_flat_orc_2`.`customer` `customer`\n" +
        "        WHERE (CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '13' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '17' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '18' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '23' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '29' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '30' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '31')\n" +
        "        GROUP BY `customer`.`c_custkey`\n" +
        "      ) `t3`\n" +
        "      GROUP BY `x_measure__1`\n" +
        "      HAVING (COUNT(1) > 0)\n" +
        "    ) `t4`\n" +
        "    WHERE ((CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '13' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '17' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '18' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '23' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '29' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '30' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '31') AND ((`customer`.`c_acctbal` > `t4`.`x_measure__1`) AND (`orders`.`o_orderkey` IS NULL)))\n" +
        "    GROUP BY `customer`.`c_custkey`,\n" +
        "      CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END\n" +
        "  ) `t5`\n" +
        "    JOIN (\n" +
        "    SELECT `customer`.`c_custkey` AS `c_custkey`,\n" +
        "      CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END AS `calculation_5131031153053149`,\n" +
        "      MAX(`customer`.`c_acctbal`) AS `x_measure__3`\n" +
        "    FROM `tpch_flat_orc_2`.`customer` `customer`\n" +
        "    WHERE (CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '13' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '17' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '18' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '23' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '29' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '30' OR CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END = '31')\n" +
        "    GROUP BY `customer`.`c_custkey`,\n" +
        "      CASE WHEN 2 >= 0 THEN SUBSTRING(`customer`.`c_phone`,1,CAST(2 AS INT)) ELSE NULL END\n" +
        "  ) `t6` ON ((`t5`.`c_custkey` = `t6`.`c_custkey`) AND (`t5`.`calculation_5131031153053149` = `t6`.`calculation_5131031153053149`))\n" +
        "  GROUP BY `t5`.`calculation_5131031153053149`\n" +
        ") `t7` ON (`t2`.`calculation_5131031153053149` = `t7`.`calculation_5131031153053149`)";
    spark.sql("drop schema if exists `verdictdb_temp` cascade");
    spark.sql("create schema if not exists `verdictdb_temp`");
    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);

    VerdictContext verdictContext = new VerdictContext(dbmsconn);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new SparkSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    Dataset<Row> rs = spark.sql(stdQuery);

    for (Row row:rs.collectAsList()) {
      result.next();
      assertEquals(row.getLong(1), result.getLong(1));
    }
  }
}

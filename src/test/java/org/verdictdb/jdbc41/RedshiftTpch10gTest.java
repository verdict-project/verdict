package org.verdictdb.jdbc41;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This test assumes that the standard TPCH tables are stored in the "tpch10g" schema, and a
 * scramble for lineitem has been created. These test cases are for benchmarking purpose, not to
 * find any bugs.
 *
 * @author Yongjoo Park
 */
public class RedshiftTpch10gTest {

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
  }

  // @Test
  public void testQ1() throws SQLException {
    String vcConnectionString =
        String.format("jdbc:verdict:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Connection vc =
        DriverManager.getConnection(vcConnectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);

    String query =
        "stream SELECT \"lineitem\".\"l_linestatus\" AS \"l_linestatus\",\n"
            + "  \"lineitem\".\"l_returnflag\" AS \"l_returnflag\",\n"
            + "  AVG(CAST(\"lineitem\".\"l_discount\" AS DOUBLE PRECISION)) AS \"avg_l_discount_ok\",\n"
            + "  AVG(CAST(\"lineitem\".\"l_extendedprice\" AS DOUBLE PRECISION)) AS \"avg_l_extendedprice_ok\",\n"
            + "  AVG(CAST(\"lineitem\".\"l_quantity\" AS DOUBLE PRECISION)) AS \"avg_l_quantity_ok\",\n"
            + "  SUM(((\"lineitem\".\"l_extendedprice\" * (1 - \"lineitem\".\"l_discount\")) * (1 + \"lineitem\".\"l_tax\"))) AS \"sum_calculation_4310711085325373_ok\",\n"
            + "  SUM((\"lineitem\".\"l_extendedprice\" * (1 - \"lineitem\".\"l_discount\"))) AS \"sum_calculation_7060711085256495_ok\",\n"
            + "  SUM(1) AS \"sum_number_of_records_ok\",\n"
            + "  SUM(\"lineitem\".\"l_extendedprice\") AS \"sum_l_extendedprice_ok\",\n"
            + "  SUM(\"lineitem\".\"l_quantity\") AS \"sum_l_quantity_ok\"\n"
            + "FROM \"tpch10g\".\"lineitem\" \"lineitem\"\n"
            + "WHERE (\"lineitem\".\"l_shipdate\" <= DATEADD(DAY,(-71),CAST((DATE '1998-12-01') AS TIMESTAMP WITHOUT TIME ZONE)))\n"
            + "GROUP BY 1, 2\n"
            + "ORDER BY 1, 2\n";

    Statement stmt = vc.createStatement();
    ResultSet rs = stmt.executeQuery(query);

    while (rs.next()) {
      for (int i = 1; i <= 6; i++) {
        System.out.print(rs.getString(i) + " ");
      }
      System.out.println();
    }

    rs.close();
    stmt.close();
    vc.close();
  }

  //  @Test
  public void testQ5() throws SQLException {
    String vcConnectionString =
        String.format("jdbc:verdict:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Connection vc =
        DriverManager.getConnection(vcConnectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);

    String query =
        "stream SELECT \"N2\".\"n_name\" AS \"n_name\",\n"
            + "  SUM((\"lineitem\".\"l_extendedprice\" * (1 - \"lineitem\".\"l_discount\"))) AS \"sum_calculation_7060711085256495_ok\"\n"
            + "FROM \"tpch10g\".\"lineitem\" \"lineitem\"\n"
            + "  INNER JOIN \"tpch10g\".\"orders\" \"orders\" ON (\"lineitem\".\"l_orderkey\" = \"orders\".\"o_orderkey\")\n"
            + "  INNER JOIN \"tpch10g\".\"supplier\" \"supplier\" ON (\"lineitem\".\"l_suppkey\" = \"supplier\".\"s_suppkey\")\n"
            + "  INNER JOIN \"tpch10g\".\"customer\" \"customer\" ON (\"orders\".\"o_custkey\" = \"customer\".\"c_custkey\")\n"
            + "  INNER JOIN \"tpch10g\".\"nation\" \"N2\" ON (\"supplier\".\"s_nationkey\" = \"N2\".\"n_nationkey\")\n"
            + "  INNER JOIN \"tpch10g\".\"nation\" \"N1\" ON (\"customer\".\"c_nationkey\" = \"N1\".\"n_nationkey\")\n"
            + "  INNER JOIN \"tpch10g\".\"region\" \"region\" ON (\"N1\".\"n_regionkey\" = \"region\".\"r_regionkey\")\n"
            + "WHERE ((\"customer\".\"c_nationkey\" = \"supplier\".\"s_nationkey\") AND ((\"orders\".\"o_orderdate\" >= DATEADD(YEAR,0,CAST(DATE_TRUNC( 'YEAR', CAST((TIMESTAMP '1994-01-01 00:00:00.000') AS TIMESTAMP WITHOUT TIME ZONE) ) AS TIMESTAMP WITHOUT TIME ZONE))) AND (\"orders\".\"o_orderdate\" < DATEADD(YEAR,1,CAST(DATE_TRUNC( 'YEAR', CAST((TIMESTAMP '1994-01-01 00:00:00.000') AS TIMESTAMP WITHOUT TIME ZONE) ) AS TIMESTAMP WITHOUT TIME ZONE)))) AND (\"region\".\"r_name\" = 'ASIA'))\n"
            + "GROUP BY 1\n"
            + "ORDER BY 1\n";

    Statement stmt = vc.createStatement();

    long start = System.currentTimeMillis();
    ResultSet rs = stmt.executeQuery(query);

    while (rs.next()) {
      for (int i = 1; i <= 2; i++) {
        System.out.print(rs.getString(i) + " ");
      }
      System.out.println();
    }

    long finish = System.currentTimeMillis();
    long timeElapsed = finish - start;
    System.out.println(String.format("elapsed time: %.3f sec", timeElapsed / 1000.0));

    rs.close();
    stmt.close();
    vc.close();
  }
}

package confidential;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.VerdictContext;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.VerdictSingleResult;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class TableauTPCHRegularQueryTest {

  static Connection conn;

  static DbmsConnection dbmsConnection;

  static Statement stmt;

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE = "tpch_flat_orc_2";

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn = DatabaseConnectionHelpers.setupMySql(
        mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    dbmsConnection = JdbcConnection.create(conn);
    dbmsConnection.setDefaultSchema(MYSQL_DATABASE);
    stmt = conn.createStatement();
  }

  @Test
  public void Query1Test() throws VerdictDBException, SQLException {
    String sql = "SELECT AVG(`lineitem`.`l_discount`) AS `avg_l_discount_ok`,\n" +
        "  AVG(`lineitem`.`l_extendedprice`) AS `avg_l_extendedprice_ok`,\n" +
        "  AVG(`lineitem`.`l_quantity`) AS `avg_l_quantity_ok`,\n" +
        "  `lineitem`.`l_linestatus` AS `l_linestatus`,\n" +
        "  `lineitem`.`l_returnflag` AS `l_returnflag`,\n" +
        "  SUM(((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`)) * (1 + `lineitem`.`l_tax`))) AS `sum_Calculation_4310711085325373_ok`,\n" +
        "  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`,\n" +
        "  SUM(1) AS `sum_Number of Records_ok`,\n" +
        "  SUM(`lineitem`.`l_extendedprice`) AS `sum_l_extendedprice_ok`,\n" +
        "  SUM(`lineitem`.`l_quantity`) AS `sum_l_quantity_ok`\n" +
        "FROM `lineitem`\n" +
        "WHERE (`lineitem`.`l_shipdate` <= TIMESTAMP('1998-09-02 00:00:00'))\n" +
        "GROUP BY 4,\n" +
        "  5;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getDouble(0), rs.getDouble(1), 1e-5);
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
      assertEquals(result.getDouble(2), rs.getDouble(3), 1e-5);
      assertEquals(result.getString(3), rs.getString(4));
      assertEquals(result.getString(4), rs.getString(5));
      assertEquals(result.getDouble(5), rs.getDouble(6), 1e-5);
      assertEquals(result.getDouble(6), rs.getDouble(7), 1e-5);
      assertEquals(result.getDouble(7), rs.getDouble(8), 1e-5);
      assertEquals(result.getDouble(8), rs.getDouble(9), 1e-5);
      assertEquals(result.getDouble(9), rs.getDouble(10), 1e-5);
    }
  }

  @Test
  public void Query2Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `nation`.`n_name` AS `n_name`,\n" +
        "  `part`.`p_mfgr` AS `p_mfgr`,\n" +
        "  `partsupp`.`ps_partkey` AS `ps_partkey`,\n" +
        "  `partsupp`.`ps_suppkey` AS `ps_suppkey`,\n" +
        "  `supplier`.`s_address` AS `s_address`,\n" +
        "  `supplier`.`s_comment` AS `s_comment`,\n" +
        "  `supplier`.`s_name` AS `s_name`,\n" +
        "  `supplier`.`s_phone` AS `s_phone`,\n" +
        "  `supplier`.`s_acctbal` AS `sum_s_acctbal_ok`\n" +
        "FROM `partsupp`\n" +
        "  INNER JOIN `part` ON (`partsupp`.`ps_partkey` = `part`.`p_partkey`)\n" +
        "  INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)\n" +
        "  INNER JOIN `nation` ON (`supplier`.`s_nationkey` = `nation`.`n_nationkey`)\n" +
        "  INNER JOIN `region` ON (`nation`.`n_regionkey` = `region`.`r_regionkey`)\n" +
        "  INNER JOIN (\n" +
        "  SELECT `partsupp`.`ps_partkey` AS `p_partkey`,\n" +
        "    MIN(`partsupp`.`ps_supplycost`) AS `__measure__0`\n" +
        "  FROM `partsupp`\n" +
        "    INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)\n" +
        "    INNER JOIN `nation` ON (`supplier`.`s_nationkey` = `nation`.`n_nationkey`)\n" +
        "    INNER JOIN `region` ON (`nation`.`n_regionkey` = `region`.`r_regionkey`)\n" +
        "  WHERE (`region`.`r_name` = 'EUROPE')\n" +
        "  GROUP BY 1\n" +
        ") `t0` ON (`part`.`p_partkey` = `t0`.`p_partkey`)\n" +
        "WHERE ((`region`.`r_name` = 'EUROPE') AND ((`partsupp`.`ps_supplycost` = `t0`.`__measure__0`) AND (`part`.`p_size` = 15) AND (RIGHT(RTRIM(`part`.`p_type`), LENGTH('BRASS')) = 'BRASS')));\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getInt(2), rs.getInt(3));
      assertEquals(result.getInt(3), rs.getInt(4));
      assertEquals(result.getString(4), rs.getString(5));
      assertEquals(result.getString(5), rs.getString(6));
      assertEquals(result.getString(6), rs.getString(7));
      assertEquals(result.getString(7), rs.getString(8));
      assertEquals(result.getDouble(8), rs.getDouble(9), 1e-5);
    }
  }

  @Test
  public void Query3Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `orders`.`o_orderdate` AS `o_orderdate`,\n" +
        "  `orders`.`o_orderkey` AS `o_orderkey`,\n" +
        "  `orders`.`o_shippriority` AS `o_shippriority`,\n" +
        "  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)\n" +
        "  INNER JOIN (\n" +
        "  SELECT `orders`.`o_orderkey` AS `o_orderkey`,\n" +
        "    SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `$__alias__0`\n" +
        "  FROM `lineitem`\n" +
        "    INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "    INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)\n" +
        "  WHERE ((`lineitem`.`l_shipdate` > DATE('1995-03-15')) AND (`orders`.`o_orderdate` < DATE('1995-03-15')) AND (`customer`.`c_mktsegment` = 'BUILDING'))\n" +
        "  GROUP BY 1\n" +
        "  ORDER BY `$__alias__0` DESC\n" +
        "  LIMIT 10\n" +
        ") `t0` ON (`orders`.`o_orderkey` = `t0`.`o_orderkey`)\n" +
        "WHERE ((`lineitem`.`l_shipdate` > DATE('1995-03-15')) AND (`orders`.`o_orderdate` < DATE('1995-03-15')) AND (`customer`.`c_mktsegment` = 'BUILDING'))\n" +
        "GROUP BY 1,\n" +
        "  2,\n" +
        "  3;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getDate(0), rs.getDate(1));
      assertEquals(result.getInt(1), rs.getInt(2));
      assertEquals(result.getInt(2), rs.getInt(3));
      assertEquals(result.getDouble(3), rs.getDouble(4), 1e-5);
    }
  }

  @Test
  public void Query4Test() throws VerdictDBException, SQLException {
    String sql = "SELECT COUNT(DISTINCT `orders`.`o_orderkey`) AS `ctd_o_orderkey_ok`,\n" +
        "  `orders`.`o_orderpriority` AS `o_orderpriority`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "WHERE ((`lineitem`.`l_commitdate` < `lineitem`.`l_receiptdate`) AND (`orders`.`o_orderdate` >= TIMESTAMP('1993-07-01 00:00:00')) AND (`orders`.`o_orderdate` < TIMESTAMP('1993-10-01 00:00:00')))\n" +
        "GROUP BY 2;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getLong(0), rs.getLong(1));
      assertEquals(result.getString(1), rs.getString(2));
    }
  }

  @Test
  public void Query5Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `N2`.`n_name` AS `n_name`,\n" +
        "  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)\n" +
        "  INNER JOIN `nation` `N1` ON (`customer`.`c_nationkey` = `N1`.`n_nationkey`)\n" +
        "  INNER JOIN `region` ON (`N1`.`n_regionkey` = `region`.`r_regionkey`)\n" +
        "  INNER JOIN `supplier` ON (`lineitem`.`l_suppkey` = `supplier`.`s_suppkey`)\n" +
        "  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)\n" +
        "WHERE ((`customer`.`c_nationkey` = `supplier`.`s_nationkey`) AND (`orders`.`o_orderdate` >= TIMESTAMP('1994-01-01 00:00:00')) AND (`orders`.`o_orderdate` < TIMESTAMP('1995-01-01 00:00:00')) AND (`region`.`r_name` = 'ASIA'))\n" +
        "GROUP BY 1;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
    }
  }

  @Test
  public void Query6Test() throws VerdictDBException, SQLException {
    String sql = "SELECT SUM((`lineitem`.`l_extendedprice` * `lineitem`.`l_discount`)) AS `sum_Calculation_8100805085146752_ok`\n" +
        "FROM `lineitem`\n" +
        "WHERE ((`lineitem`.`l_discount` >= 0.05) AND (`lineitem`.`l_discount` <= 0.07) AND (`lineitem`.`l_quantity` <= 23.00) AND (`lineitem`.`l_shipdate` >= TIMESTAMP('1994-01-01 00:00:00')) AND (`lineitem`.`l_shipdate` < TIMESTAMP('1995-01-01 00:00:00')))\n" +
        "HAVING (COUNT(1) > 0);";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getDouble(0), rs.getDouble(1), 1e-5);
    }
  }

  @Test
  public void Query7Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `N1`.`n_name` AS `n_NAME (nation1)`,\n" +
        "  `N2`.`n_name` AS `n_name`,\n" +
        "  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`,\n" +
        "  YEAR(`lineitem`.`l_shipdate`) AS `yr_l_shipdate_ok`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)\n" +
        "  INNER JOIN `nation` `N1` ON (`customer`.`c_nationkey` = `N1`.`n_nationkey`)\n" +
        "  INNER JOIN `supplier` ON (`lineitem`.`l_suppkey` = `supplier`.`s_suppkey`)\n" +
        "  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)\n" +
        "WHERE ((NOT (`customer`.`c_nationkey` = `supplier`.`s_nationkey`)) AND (`lineitem`.`l_shipdate` >= DATE('1995-01-01')) AND (`lineitem`.`l_shipdate` <= DATE('1996-12-31')) AND (`N1`.`n_name` IN ('FRANCE', 'GERMANY')) AND (`N2`.`n_name` IN ('FRANCE', 'GERMANY')))\n" +
        "GROUP BY 1,\n" +
        "  2,\n" +
        "  4;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getDouble(2), rs.getDouble(3), 1e-5);
      assertEquals(result.getDouble(3), rs.getDouble(4), 1e-5);
    }
  }

  @Test
  public void Query8Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `N2`.`n_name` AS `n_name`,\n" +
        "  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`,\n" +
        "  YEAR(`orders`.`o_orderdate`) AS `yr_o_orderdate_ok`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)\n" +
        "  INNER JOIN `nation` `N1` ON (`customer`.`c_nationkey` = `N1`.`n_nationkey`)\n" +
        "  INNER JOIN `region` ON (`N1`.`n_regionkey` = `region`.`r_regionkey`)\n" +
        "  INNER JOIN `supplier` ON (`lineitem`.`l_suppkey` = `supplier`.`s_suppkey`)\n" +
        "  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)\n" +
        "  INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)\n" +
        "WHERE ((`orders`.`o_orderdate` >= DATE('1995-01-01')) AND (`orders`.`o_orderdate` <= DATE('1996-12-31')) AND (`part`.`p_type` = 'ECONOMY ANODIZED STEEL') AND (`region`.`r_name` = 'AMERICA'))\n" +
        "GROUP BY 1,\n" +
        "  3;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
      assertEquals(result.getInt(2), rs.getInt(3));
    }
  }

  @Test
  public void Query9Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `N2`.`n_name` AS `n_name`,\n" +
        "  SUM(((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`)) - (`partsupp`.`ps_supplycost` * `lineitem`.`l_quantity`))) AS `sum_Calculation_1690805085450945_ok`,\n" +
        "  YEAR(`orders`.`o_orderdate`) AS `yr_o_orderdate_ok`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "  INNER JOIN `partsupp` ON ((`lineitem`.`l_partkey` = `partsupp`.`ps_partkey`) AND (`lineitem`.`l_suppkey` = `partsupp`.`ps_suppkey`))\n" +
        "  INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)\n" +
        "  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)\n" +
        "  INNER JOIN `part` ON (`partsupp`.`ps_partkey` = `part`.`p_partkey`)\n" +
        "WHERE (LOCATE('green',`part`.`p_name`) > 0)\n" +
        "GROUP BY 1,\n" +
        "  3;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
      assertEquals(result.getInt(2), rs.getInt(3));
    }
  }

  @Test
  public void Query10Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `customer`.`c_acctbal` AS `c_acctbal`,\n" +
        "  `customer`.`c_address` AS `c_address`,\n" +
        "  `customer`.`c_comment` AS `c_comment`,\n" +
        "  `customer`.`c_custkey` AS `c_custkey`,\n" +
        "  `customer`.`c_name` AS `c_name`,\n" +
        "  `customer`.`c_phone` AS `c_phone`,\n" +
        "  `N1`.`n_name` AS `n_NAME (nation1)`,\n" +
        "  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)\n" +
        "  INNER JOIN `nation` `N1` ON (`customer`.`c_nationkey` = `N1`.`n_nationkey`)\n" +
        "  INNER JOIN (\n" +
        "  SELECT `orders`.`o_custkey` AS `c_custkey`,\n" +
        "    SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `$__alias__0`\n" +
        "  FROM `lineitem`\n" +
        "    INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "  WHERE ((`lineitem`.`l_returnflag` = 'R') AND (`orders`.`o_orderdate` >= TIMESTAMP('1993-10-01 00:00:00')) AND (`orders`.`o_orderdate` < TIMESTAMP('1994-01-01 00:00:00')))\n" +
        "  GROUP BY 1\n" +
        "  ORDER BY `$__alias__0` DESC\n" +
        "  LIMIT 20\n" +
        ") `t0` ON (`customer`.`c_custkey` = `t0`.`c_custkey`)\n" +
        "WHERE ((`lineitem`.`l_returnflag` = 'R') AND (`orders`.`o_orderdate` >= TIMESTAMP('1993-10-01 00:00:00')) AND (`orders`.`o_orderdate` < TIMESTAMP('1994-01-01 00:00:00')))\n" +
        "GROUP BY 1,\n" +
        "  2,\n" +
        "  3,\n" +
        "  4,\n" +
        "  5,\n" +
        "  6,\n" +
        "  7;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getDouble(0), rs.getDouble(1), 1e-5);
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getString(2), rs.getString(3));
      assertEquals(result.getInt(3), rs.getInt(4));
      assertEquals(result.getString(4), rs.getString(5));
      assertEquals(result.getString(5), rs.getString(6));
      assertEquals(result.getString(6), rs.getString(7));
      assertEquals(result.getDouble(7), rs.getDouble(8), 1e-5);
    }
  }

  @Test
  public void Query11Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `partsupp`.`ps_partkey` AS `ps_partkey`,\n" +
        "  SUM((`partsupp`.`ps_supplycost` * `partsupp`.`ps_availqty`)) AS `sum_Calculation_6140711155912621_ok`\n" +
        "FROM `partsupp`\n" +
        "  INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)\n" +
        "  INNER JOIN `nation` ON (`supplier`.`s_nationkey` = `nation`.`n_nationkey`)\n" +
        "WHERE (`nation`.`n_name` = 'GERMANY')\n" +
        "GROUP BY 1;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
    }
  }

  @Test
  public void Query12Test() throws VerdictDBException, SQLException {
    String sql = "SELECT (CASE WHEN (`orders`.`o_orderpriority` IN ('1-URGENT', '2-HIGH')) THEN '1-URGENT' WHEN (`orders`.`o_orderpriority` IN ('3-MEDIUM', '4-NOT SPECIFIED', '5-LOW')) THEN '3-MEDIUM' ELSE `orders`.`o_orderpriority` END) AS `O Orderpriority (group)`,\n" +
        "  `lineitem`.`l_shipmode` AS `l_shipmode`,\n" +
        "  SUM(1) AS `sum_Number of Records_ok`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "WHERE ((`lineitem`.`l_commitdate` < `lineitem`.`l_receiptdate`) AND (`lineitem`.`l_shipdate` < `lineitem`.`l_commitdate`) AND (`lineitem`.`l_receiptdate` >= TIMESTAMP('1994-01-01 00:00:00')) AND (`lineitem`.`l_receiptdate` < TIMESTAMP('1995-01-01 00:00:00')) AND (`lineitem`.`l_shipmode` IN ('MAIL', 'SHIP')))\n" +
        "GROUP BY 1,\n" +
        "  2;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getDouble(2), rs.getDouble(3), 1e-5);
    }
  }

  @Test
  public void Query13Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `t0`.`__measure__0` AS `Calculation_1361031105746580`,\n" +
        "  COUNT(DISTINCT `customer`.`c_custkey`) AS `ctd_c_custkey_ok`\n" +
        "FROM `customer`\n" +
        "  INNER JOIN (\n" +
        "  SELECT `customer`.`c_custkey` AS `c_custkey`,\n" +
        "    COUNT((CASE WHEN (NOT ((0 < LOCATE('special',`orders`.`o_comment`)) AND (0 < IF(ISNULL(LOCATE('special',`orders`.`o_comment`)), NULL, LOCATE('requests',`orders`.`o_comment`,GREATEST(1,FLOOR(LOCATE('special',`orders`.`o_comment`)))))))) THEN `orders`.`o_orderkey` ELSE NULL END)) AS `__measure__0`\n" +
        "  FROM `orders`\n" +
        "    RIGHT JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)\n" +
        "  GROUP BY 1\n" +
        ") `t0` ON (`customer`.`c_custkey` = `t0`.`c_custkey`)\n" +
        "GROUP BY 1;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getInt(1), rs.getInt(2));
    }
  }

  @Test
  public void Query14Test() throws VerdictDBException, SQLException {
    String sql = "SELECT (NOT ISNULL(`t0`.`_Tableau_join_flag`)) AS `io_Promo Tpe_nk`,\n" +
        "  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)\n" +
        "  LEFT JOIN (\n" +
        "  SELECT `part`.`p_type` AS `p_type`,\n" +
        "    MIN(1) AS `_Tableau_join_flag`\n" +
        "  FROM `lineitem`\n" +
        "    INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)\n" +
        "  WHERE (LEFT(`part`.`p_type`, LENGTH('PROMO')) = 'PROMO')\n" +
        "  GROUP BY 1\n" +
        ") `t0` ON (`part`.`p_type` = `t0`.`p_type`)\n" +
        "WHERE ((`lineitem`.`l_shipdate` >= TIMESTAMP('1995-09-01 00:00:00')) AND (`lineitem`.`l_shipdate` < TIMESTAMP('1995-10-01 00:00:00')))\n" +
        "GROUP BY 1;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
    }
  }

  @Test
  public void Query15Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `lineitem`.`l_suppkey` AS `l_suppkey`,\n" +
        "  `supplier`.`s_address` AS `s_address`,\n" +
        "  `supplier`.`s_name` AS `s_name`,\n" +
        "  `supplier`.`s_phone` AS `s_phone`,\n" +
        "  SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `supplier` ON (`lineitem`.`l_suppkey` = `supplier`.`s_suppkey`)\n" +
        "  INNER JOIN (\n" +
        "  SELECT `lineitem`.`l_suppkey` AS `l_suppkey`,\n" +
        "    SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `__measure__0`\n" +
        "  FROM `lineitem`\n" +
        "  WHERE ((`lineitem`.`l_shipdate` >= TIMESTAMP('1996-01-01 00:00:00')) AND (`lineitem`.`l_shipdate` < TIMESTAMP('1996-04-01 00:00:00')))\n" +
        "  GROUP BY 1\n" +
        ") `t0` ON (`lineitem`.`l_suppkey` = `t0`.`l_suppkey`)\n" +
        "  CROSS JOIN (\n" +
        "  SELECT MAX(`t1`.`__measure__1`) AS `__measure__2`\n" +
        "  FROM (\n" +
        "    SELECT `lineitem`.`l_suppkey` AS `l_suppkey`,\n" +
        "      SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `__measure__1`\n" +
        "    FROM `lineitem`\n" +
        "    WHERE ((`lineitem`.`l_shipdate` >= TIMESTAMP('1996-01-01 00:00:00')) AND (`lineitem`.`l_shipdate` < TIMESTAMP('1996-04-01 00:00:00')))\n" +
        "    GROUP BY 1\n" +
        "  ) `t1`\n" +
        "  HAVING (COUNT(1) > 0)\n" +
        ") `t2`\n" +
        "WHERE (((`lineitem`.`l_shipdate` >= TIMESTAMP('1996-01-01 00:00:00')) AND (`lineitem`.`l_shipdate` < TIMESTAMP('1996-04-01 00:00:00'))) AND (`t0`.`__measure__0` = `t2`.`__measure__2`))\n" +
        "GROUP BY 1,\n" +
        "  2,\n" +
        "  3,\n" +
        "  4;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getString(2), rs.getString(3));
      assertEquals(result.getString(3), rs.getString(4));
      assertEquals(result.getDouble(4), rs.getDouble(5), 1e-5);
    }
  }

  @Test
  public void Query16Test() throws VerdictDBException, SQLException {
    String sql = "SELECT COUNT(DISTINCT `partsupp`.`ps_suppkey`) AS `ctd_ps_suppkey_ok`,\n" +
        "  `part`.`p_brand` AS `p_brand`,\n" +
        "  `part`.`p_size` AS `p_size`,\n" +
        "  `part`.`p_type` AS `p_type`\n" +
        "FROM `partsupp`\n" +
        "  INNER JOIN `part` ON (`partsupp`.`ps_partkey` = `part`.`p_partkey`)\n" +
        "  INNER JOIN (\n" +
        "  SELECT `supplier`.`s_suppkey` AS `s_suppkey`\n" +
        "  FROM `partsupp`\n" +
        "    INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)\n" +
        "  GROUP BY 1\n" +
        "  HAVING (MIN((CASE WHEN ((0 < LOCATE('Customer',`supplier`.`s_comment`)) AND (0 < IF(ISNULL(LOCATE('Customer',`supplier`.`s_comment`)), NULL, LOCATE('Complaints',`supplier`.`s_comment`,GREATEST(1,FLOOR(LOCATE('Customer',`supplier`.`s_comment`))))))) THEN 1 ELSE 0 END)) = 0)\n" +
        ") `t0` ON (`partsupp`.`ps_suppkey` = `t0`.`s_suppkey`)\n" +
        "WHERE ((NOT (`part`.`p_brand` = 'Brand#45')) AND (`part`.`p_size` IN (3, 9, 14, 19, 23, 36, 45, 49)) AND (NOT (LEFT(`part`.`p_type`, LENGTH('MEDIUM POLISHED')) = 'MEDIUM POLISHED')))\n" +
        "GROUP BY 2,\n" +
        "  3,\n" +
        "  4;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getInt(2), rs.getInt(3));
      assertEquals(result.getString(3), rs.getString(4));
    }
  }

  @Test
  public void Query17Test() throws VerdictDBException, SQLException {
    String sql = "SELECT SUM(`lineitem`.`l_extendedprice`) AS `TEMP(Calculation_7921031152254526)(1602391293)(0)`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)\n" +
        "  INNER JOIN (\n" +
        "  SELECT `lineitem`.`l_partkey` AS `l_partkey`,\n" +
        "    AVG(`lineitem`.`l_quantity`) AS `__measure__0`\n" +
        "  FROM `lineitem`\n" +
        "  GROUP BY 1\n" +
        ") `t0` ON (`lineitem`.`l_partkey` = `t0`.`l_partkey`)\n" +
        "WHERE ((((0.20000000000000001 * `t0`.`__measure__0`) - `lineitem`.`l_quantity`) >= -1.0000000000000001E-17) AND (`part`.`p_brand` = 'Brand#23') AND (`part`.`p_container` = 'MED BOX'))\n" +
        "HAVING (COUNT(1) > 0);\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getDouble(0), rs.getDouble(1), 1e-5);
    }
  }

  @Test
  public void Query18Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `customer`.`c_custkey` AS `c_custkey`,\n" +
        "  `customer`.`c_name` AS `c_name`,\n" +
        "  MIN(`orders`.`o_totalprice`) AS `min_o_totalprice_ok`,\n" +
        "  `orders`.`o_orderdate` AS `o_orderdate`,\n" +
        "  `orders`.`o_orderkey` AS `o_orderkey`,\n" +
        "  `orders`.`o_totalprice` AS `o_totalprice`,\n" +
        "  SUM(`lineitem`.`l_quantity`) AS `sum_l_quantity_ok`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "  INNER JOIN `customer` ON (`orders`.`o_custkey` = `customer`.`c_custkey`)\n" +
        "GROUP BY 1,\n" +
        "  2,\n" +
        "  4,\n" +
        "  5,\n" +
        "  6\n" +
        "HAVING (SUM(`lineitem`.`l_quantity`) >= 300.99999999999699);\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getInt(0), rs.getInt(1));
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getDouble(2), rs.getDouble(3),1e-5);
      assertEquals(result.getString(3), rs.getString(4));
      assertEquals(result.getInt(4), rs.getInt(5));
      assertEquals(result.getDouble(5), rs.getDouble(6),1e-5);
      assertEquals(result.getDouble(6), rs.getDouble(7),1e-5);
    }
  }

  @Test
  public void Query19Test() throws VerdictDBException, SQLException {
    String sql = "SELECT SUM((`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`))) AS `sum_Calculation_7060711085256495_ok`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)\n" +
        "  LEFT JOIN (\n" +
        "  SELECT `part`.`p_container` AS `p_container`,\n" +
        "    MIN(1) AS `_Tableau_join_flag`\n" +
        "  FROM `lineitem`\n" +
        "    INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)\n" +
        "  WHERE (`part`.`p_container` IN ('SM BOX', 'SM CASE', 'SM PACK', 'SM PKG'))\n" +
        "  GROUP BY 1\n" +
        ") `t0` ON (`part`.`p_container` = `t0`.`p_container`)\n" +
        "  LEFT JOIN (\n" +
        "  SELECT `part`.`p_container` AS `p_container`,\n" +
        "    MIN(1) AS `_Tableau_join_flag`\n" +
        "  FROM `lineitem`\n" +
        "    INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)\n" +
        "  WHERE (`part`.`p_container` IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG'))\n" +
        "  GROUP BY 1\n" +
        ") `t1` ON (`part`.`p_container` = `t1`.`p_container`)\n" +
        "  LEFT JOIN (\n" +
        "  SELECT `part`.`p_container` AS `p_container`,\n" +
        "    MIN(1) AS `_Tableau_join_flag`\n" +
        "  FROM `lineitem`\n" +
        "    INNER JOIN `part` ON (`lineitem`.`l_partkey` = `part`.`p_partkey`)\n" +
        "  WHERE (`part`.`p_container` IN ('LG BOX', 'LG CASE', 'LG PACK', 'LG PKG'))\n" +
        "  GROUP BY 1\n" +
        ") `t2` ON (`part`.`p_container` = `t2`.`p_container`)\n" +
        "WHERE ((((NOT ISNULL(`t0`.`_Tableau_join_flag`)) AND (`part`.`p_brand` = 'Brand#12') AND (`lineitem`.`l_quantity` >= 1) AND (`lineitem`.`l_quantity` <= 11) AND (`part`.`p_size` >= 1) AND (`part`.`p_size` <= 5)) OR ((NOT ISNULL(`t1`.`_Tableau_join_flag`)) AND (`part`.`p_brand` = 'Brand#23') AND (`lineitem`.`l_quantity` >= 10) AND (`lineitem`.`l_quantity` <= 20) AND (`part`.`p_size` >= 1) AND (`part`.`p_size` <= 10)) OR ((NOT ISNULL(`t2`.`_Tableau_join_flag`)) AND (`part`.`p_brand` = 'Brand#34') AND (`lineitem`.`l_quantity` >= 20) AND (`lineitem`.`l_quantity` <= 30) AND (`part`.`p_size` >= 1) AND (`part`.`p_size` <= 15))) AND (`lineitem`.`l_shipinstruct` = 'DELIVER IN PERSON') AND (`lineitem`.`l_shipmode` = 'AIR'))\n" +
        "HAVING (COUNT(1) > 0);\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getDouble(0), rs.getDouble(1),1e-5);
    }
  }

  @Test
  public void Query20Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `supplier`.`s_address` AS `s_address`,\n" +
        "  `supplier`.`s_name` AS `s_name`\n" +
        "FROM `partsupp`\n" +
        "  INNER JOIN `supplier` ON (`partsupp`.`ps_suppkey` = `supplier`.`s_suppkey`)\n" +
        "  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)\n" +
        "  INNER JOIN `part` ON (`partsupp`.`ps_partkey` = `part`.`p_partkey`)\n" +
        "  INNER JOIN (\n" +
        "  SELECT `partsupp`.`ps_partkey` AS `ps_partkey`,\n" +
        "    `partsupp`.`ps_suppkey` AS `ps_suppkey`\n" +
        "  FROM `lineitem`\n" +
        "    INNER JOIN `partsupp` ON ((`lineitem`.`l_partkey` = `partsupp`.`ps_partkey`) AND (`lineitem`.`l_suppkey` = `partsupp`.`ps_suppkey`))\n" +
        "  GROUP BY 1,\n" +
        "    2\n" +
        "  HAVING (MIN(`partsupp`.`ps_availqty`) > (0.5 * SUM((CASE WHEN (YEAR(`lineitem`.`l_shipdate`) = 1994) THEN `lineitem`.`l_quantity` ELSE NULL END))))\n" +
        ") `t0` ON ((`partsupp`.`ps_partkey` = `t0`.`ps_partkey`) AND (`partsupp`.`ps_suppkey` = `t0`.`ps_suppkey`))\n" +
        "WHERE ((`N2`.`n_name` = 'CANADA') AND (LEFT(`part`.`p_name`, LENGTH('forest')) = 'forest'))\n" +
        "GROUP BY 1,\n" +
        "  2;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getString(1), rs.getString(2));
    }
  }

  @Test
  public void Query21Test() throws VerdictDBException, SQLException {
    String sql = "SELECT `supplier`.`s_name` AS `s_name`,\n" +
        "  SUM(1) AS `sum_Number of Records_ok`,\n" +
        "  SUM(1) AS `$__alias__0`\n" +
        "FROM `lineitem`\n" +
        "  INNER JOIN `orders` ON (`lineitem`.`l_orderkey` = `orders`.`o_orderkey`)\n" +
        "  INNER JOIN `supplier` ON (`lineitem`.`l_suppkey` = `supplier`.`s_suppkey`)\n" +
        "  INNER JOIN `nation` `N2` ON (`supplier`.`s_nationkey` = `N2`.`n_nationkey`)\n" +
        "  INNER JOIN (\n" +
        "  SELECT `lineitem`.`l_orderkey` AS `none_L Orderkey (copy)_ok`\n" +
        "  FROM `lineitem`\n" +
        "  GROUP BY 1\n" +
        "  HAVING (COUNT(DISTINCT (CASE WHEN (`lineitem`.`l_commitdate` < `lineitem`.`l_receiptdate`) THEN `lineitem`.`l_suppkey` ELSE NULL END)) = 1)\n" +
        ") `t0` ON (`lineitem`.`l_orderkey` = `t0`.`none_L Orderkey (copy)_ok`)\n" +
        "  INNER JOIN (\n" +
        "  SELECT `lineitem`.`l_orderkey` AS `none_l_orderkey_ok`\n" +
        "  FROM `lineitem`\n" +
        "  GROUP BY 1\n" +
        "  HAVING (COUNT(DISTINCT `lineitem`.`l_suppkey`) > 1)\n" +
        ") `t1` ON (`lineitem`.`l_orderkey` = `t1`.`none_l_orderkey_ok`)\n" +
        "WHERE ((`lineitem`.`l_commitdate` < `lineitem`.`l_receiptdate`) AND (`N2`.`n_name` = 'SAUDI ARABIA') AND (`orders`.`o_orderstatus` = 'F'))\n" +
        "GROUP BY 1\n" +
        "ORDER BY `$__alias__0` DESC\n" +
        "LIMIT 100;\n";
    stmt.execute("create schema if not exists `verdictdb_temp`");
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getString(0), rs.getString(1));
      assertEquals(result.getDouble(1), rs.getDouble(2), 1e-5);
      assertEquals(result.getDouble(2), rs.getDouble(3), 1e-5);
    }
  }


}

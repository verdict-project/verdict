package org.verdictdb.core.scramblingquerying;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

public class MySqlTpchUniformScramblingQueryingTest {
  
  final int blockSize = 100;

  @BeforeClass
  public void setupMySqlDatabase() {
    // create uniform scrambled table of lineitem and orders.

  }

  @Test
  public void testTpch1() throws VerdictDBException {
    RelationStandardizer.resetItemID();
    String sql = "select " +
        " l_returnflag, " +
        " l_linestatus, " +
        " sum(l_quantity) as sum_qty, " +
        " sum(l_extendedprice) as sum_base_price, " +
        " sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, " +
        " sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, " +
        " avg(l_quantity) as avg_qty, " +
        " avg(l_extendedprice) as avg_price, " +
        " avg(l_discount) as avg_disc, " +
        " count(*) as count_order " +
        "from " +
        " lineitem_scrambled " +
        "where " +
        " l_shipdate <= date '1998-12-01'" +
        "group by " +
        " l_returnflag, " +
        " l_linestatus " +
        "order by " +
        " l_returnflag, " +
        " l_linestatus " +
        "LIMIT 1 ";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute("DROP TABLE IF EXISTS `test`.`region`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`nation`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`lineitem`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`customer`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`supplier`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`partsupp`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`part`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`orders`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`lineitem_scrambled`");
  }

}

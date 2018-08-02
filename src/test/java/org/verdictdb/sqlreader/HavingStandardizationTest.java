package org.verdictdb.sqlreader;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.sql.Types.BIGINT;
import static org.junit.Assert.assertEquals;

public class HavingStandardizationTest {

  private StaticMetaData meta = new StaticMetaData();

  @Before
  public void setupMetaData() {
    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("n_nationkey", BIGINT),
            new ImmutablePair<>("n_name", BIGINT),
            new ImmutablePair<>("n_regionkey", BIGINT),
            new ImmutablePair<>("n_comment", BIGINT)));
    meta.setDefaultSchema("tpch");
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "nation"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("r_regionkey", BIGINT),
            new ImmutablePair<>("r_name", BIGINT),
            new ImmutablePair<>("r_comment", BIGINT)));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "region"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("p_partkey", BIGINT),
            new ImmutablePair<>("p_name", BIGINT),
            new ImmutablePair<>("p_brand", BIGINT),
            new ImmutablePair<>("p_mfgr", BIGINT),
            new ImmutablePair<>("p_type", BIGINT),
            new ImmutablePair<>("p_size", BIGINT),
            new ImmutablePair<>("p_container", BIGINT),
            new ImmutablePair<>("p_retailprice", BIGINT),
            new ImmutablePair<>("p_comment", BIGINT)));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "part"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("s_suppkey", BIGINT),
            new ImmutablePair<>("s_name", BIGINT),
            new ImmutablePair<>("s_address", BIGINT),
            new ImmutablePair<>("s_nationkey", BIGINT),
            new ImmutablePair<>("s_phone", BIGINT),
            new ImmutablePair<>("s_acctbal", BIGINT),
            new ImmutablePair<>("s_comment", BIGINT)));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "supplier"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("ps_partkey", BIGINT),
            new ImmutablePair<>("ps_suppkey", BIGINT),
            new ImmutablePair<>("ps_availqty", BIGINT),
            new ImmutablePair<>("ps_supplycost", BIGINT),
            new ImmutablePair<>("ps_comment", BIGINT)));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "partsupp"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("c_custkey", BIGINT),
            new ImmutablePair<>("c_name", BIGINT),
            new ImmutablePair<>("c_address", BIGINT),
            new ImmutablePair<>("c_nationkey", BIGINT),
            new ImmutablePair<>("c_phone", BIGINT),
            new ImmutablePair<>("c_acctbal", BIGINT),
            new ImmutablePair<>("c_mktsegment", BIGINT),
            new ImmutablePair<>("c_comment", BIGINT)));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "customer"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("o_orderkey", BIGINT),
            new ImmutablePair<>("o_custkey", BIGINT),
            new ImmutablePair<>("o_orderstatus", BIGINT),
            new ImmutablePair<>("o_totalprice", BIGINT),
            new ImmutablePair<>("o_orderdate", BIGINT),
            new ImmutablePair<>("o_orderpriority", BIGINT),
            new ImmutablePair<>("o_clerk", BIGINT),
            new ImmutablePair<>("o_shippriority", BIGINT),
            new ImmutablePair<>("o_comment", BIGINT)));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "orders"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("l_orderkey", BIGINT),
            new ImmutablePair<>("l_partkey", BIGINT),
            new ImmutablePair<>("l_suppkey", BIGINT),
            new ImmutablePair<>("l_linenumber", BIGINT),
            new ImmutablePair<>("l_quantity", BIGINT),
            new ImmutablePair<>("l_extendedprice", BIGINT),
            new ImmutablePair<>("l_discount", BIGINT),
            new ImmutablePair<>("l_tax", BIGINT),
            new ImmutablePair<>("l_returnflag", BIGINT),
            new ImmutablePair<>("l_linestatus", BIGINT),
            new ImmutablePair<>("l_shipdate", BIGINT),
            new ImmutablePair<>("l_commitdate", BIGINT),
            new ImmutablePair<>("l_receiptdate", BIGINT),
            new ImmutablePair<>("l_shipinstruct", BIGINT),
            new ImmutablePair<>("l_shipmode", BIGINT),
            new ImmutablePair<>("l_comment", BIGINT)));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "lineitem"), arr);
  }

  @Test
  public void HavingAndOrderbyTest() throws VerdictDBException {
    RelationStandardizer.resetItemID();

    String sql =
        "select\n"
            + "  ps_partkey * 2 as groupkey,\n"
            + "  sum(ps_supplycost * ps_availqty) as value\n"
            + "from\n"
            + "  partsupp,\n"
            + "  supplier,\n"
            + "  nation\n"
            + "where\n"
            + "  ps_suppkey = s_suppkey\n"
            + "  and s_nationkey = n_nationkey\n"
            + "group by\n"
            + "  ps_partkey * 2\n"
            + "having\n"
            + "    sum(ps_supplycost * ps_availqty) > 10\n"
            + "order by\n"
            + "  sum(ps_supplycost * ps_availqty) desc;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = selectQueryToSql.toSql(relation);
    String expected =
        "select "
            + "vt1.`ps_partkey` * 2 as `groupkey`, "
            + "sum(vt1.`ps_supplycost` * vt1.`ps_availqty`) as `value` "
            + "from "
            + "`tpch`.`partsupp` as vt1, "
            + "`tpch`.`supplier` as vt2, "
            + "`tpch`.`nation` as vt3 "
            + "where "
            + "(vt1.`ps_suppkey` = vt2.`s_suppkey`) "
            + "and (vt2.`s_nationkey` = vt3.`n_nationkey`) "
            + "group by vt1.`ps_partkey` * 2 "
            + "having `value` > 10 "
            + "order by `value` desc";
    assertEquals(expected, actual);
  }

  @Test
  public void ReplaceGroupbyIndexTest() throws VerdictDBException {
    RelationStandardizer.resetItemID();

    String sql =
        "select ps_partkey as g, ps_supplycost as g2, count(*) as c\n"
            + "from partsupp\n"
            + "group by 1, 2";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = selectQueryToSql.toSql(relation);
    String expected =
        "select "
            + "vt1.`ps_partkey` as `g`, "
            + "vt1.`ps_supplycost` as `g2`, "
            + "count(*) as `c` "
            + "from `tpch`.`partsupp` as vt1 "
            + "group by vt1.`ps_partkey`, vt1.`ps_supplycost`";
    assertEquals(expected, actual);
  }
}

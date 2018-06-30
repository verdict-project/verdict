package org.verdictdb.core.sql;

import static java.sql.Types.BIGINT;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.AliasReference;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.ConstantColumn;
import org.verdictdb.core.query.GroupingAttribute;
import org.verdictdb.core.query.JoinTable;
import org.verdictdb.core.query.OrderbyAttribute;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.query.SubqueryColumn;
import org.verdictdb.core.query.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;

public class TpchSqlToRelationAfterAliasTest {

  private StaticMetaData meta = new StaticMetaData();

  @Before
  public void setupMetaData() {
    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("n_nationkey", BIGINT),
        new ImmutablePair<>("n_name", BIGINT),
        new ImmutablePair<>("n_regionkey", BIGINT),
        new ImmutablePair<>("n_comment", BIGINT)));
    meta.setDefaultSchema("tpch");
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "nation"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("r_regionkey", BIGINT),
        new ImmutablePair<>("r_name", BIGINT),
        new ImmutablePair<>("r_comment", BIGINT)));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "region"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("p_partkey", BIGINT),
        new ImmutablePair<>("p_name", BIGINT),
        new ImmutablePair<>("p_brand", BIGINT),
        new ImmutablePair<>("p_mfgr", BIGINT),
        new ImmutablePair<>("p_type", BIGINT),
        new ImmutablePair<>("p_size", BIGINT),
        new ImmutablePair<>("p_container", BIGINT),
        new ImmutablePair<>("p_retailprice", BIGINT),
        new ImmutablePair<>("p_comment", BIGINT)
    ));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "part"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("s_suppkey", BIGINT),
        new ImmutablePair<>("s_name", BIGINT),
        new ImmutablePair<>("s_address", BIGINT),
        new ImmutablePair<>("s_nationkey", BIGINT),
        new ImmutablePair<>("s_phone", BIGINT),
        new ImmutablePair<>("s_acctbal", BIGINT),
        new ImmutablePair<>("s_comment", BIGINT)
    ));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "supplier"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("ps_partkey", BIGINT),
        new ImmutablePair<>("ps_suppkey", BIGINT),
        new ImmutablePair<>("ps_availqty", BIGINT),
        new ImmutablePair<>("ps_supplycost", BIGINT),
        new ImmutablePair<>("ps_comment", BIGINT)));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "partsupp"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("c_custkey", BIGINT),
        new ImmutablePair<>("c_name", BIGINT),
        new ImmutablePair<>("c_address", BIGINT),
        new ImmutablePair<>("c_nationkey", BIGINT),
        new ImmutablePair<>("c_phone", BIGINT),
        new ImmutablePair<>("c_acctbal", BIGINT),
        new ImmutablePair<>("c_mktsegment", BIGINT),
        new ImmutablePair<>("c_comment", BIGINT)
    ));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "customer"), arr);
    arr = new ArrayList<>();
    arr.addAll( Arrays.asList(new ImmutablePair<>("o_orderkey", BIGINT),
        new ImmutablePair<>("o_custkey", BIGINT),
        new ImmutablePair<>("o_orderstatus", BIGINT),
        new ImmutablePair<>("o_totalprice", BIGINT),
        new ImmutablePair<>("o_orderdate", BIGINT),
        new ImmutablePair<>("o_orderpriority", BIGINT),
        new ImmutablePair<>("o_clerk", BIGINT),
        new ImmutablePair<>("o_shippriority", BIGINT),
        new ImmutablePair<>("o_comment", BIGINT)
    ));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "orders"), arr);
    arr = new ArrayList<>();
    arr.addAll( Arrays.asList(new ImmutablePair<>("l_orderkey", BIGINT),
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
        new ImmutablePair<>("l_comment", BIGINT)
    ));
    meta.addTableData(new StaticMetaData.TableInfo("tpch", "lineitem"), arr);

  }

  @Test
  public void Query1Test() throws VerdictDBException, SQLException,SQLException {
    RelationStandardizer.resetItemID();
    BaseTable base = new BaseTable("tpch", "lineitem", "vt1");
    List<UnnamedColumn> operand1 = Arrays.<UnnamedColumn>asList(
        ConstantColumn.valueOf(1),
        new BaseColumn("vt1", "l_discount"));
    List<UnnamedColumn> operand2 = Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "l_extendedprice"),
        new ColumnOp("subtract", operand1));
    List<UnnamedColumn> operand3 = Arrays.<UnnamedColumn>asList(
        ConstantColumn.valueOf(1),
        new BaseColumn("vt1", "l_tax"));
    List<UnnamedColumn> operand4 = Arrays.<UnnamedColumn>asList(
        new ColumnOp("multiply", operand2),
        new ColumnOp("add", operand3));
    List<UnnamedColumn> operand5 = Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "l_shipdate"),
        new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("'1998-12-01'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':1'"), ConstantColumn.valueOf("day")))
            )));
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt1", "l_returnflag"), "vc2"),
            new AliasedColumn(new BaseColumn("vt1", "l_linestatus"), "vc3"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("vt1", "l_quantity")), "sum_qty"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("vt1", "l_extendedprice")), "sum_base_price"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply", operand2)), "sum_disc_price"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply", operand4)), "sum_charge"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("vt1", "l_quantity")), "avg_qty"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("vt1", "l_extendedprice")), "avg_price"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("vt1", "l_discount")), "avg_disc"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "count_order")
            ),
        base, new ColumnOp("lessequal", operand5));
    expected.addGroupby(Arrays.<GroupingAttribute>asList(new AliasReference("vc2"),
        new AliasReference("vc3")));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(new OrderbyAttribute("vc2"),
        new OrderbyAttribute("vc3")));
    expected.addLimit(ConstantColumn.valueOf(1));
    
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
        " lineitem " +
        "where " +
        " l_shipdate <= date '1998-12-01' - interval ':1' day " +
        "group by " +
        " l_returnflag, " +
        " l_linestatus " +
        "order by " +
        " l_returnflag, " +
        " l_linestatus " +
        "LIMIT 1 ";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query2Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    BaseTable part = new BaseTable("tpch", "part", "vt1");
    BaseTable supplier = new BaseTable("tpch", "supplier", "vt2");
    BaseTable partsupp = new BaseTable("tpch", "partsupp", "vt3");
    BaseTable nation = new BaseTable("tpch", "nation", "vt4");
    BaseTable region = new BaseTable("tpch", "region", "vt5");
    List<AbstractRelation> from = Arrays.<AbstractRelation>asList(part, supplier, partsupp, nation, region);
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt2", "s_acctbal"), "vc6"),
            new AliasedColumn(new BaseColumn("vt2", "s_name"), "vc7"),
            new AliasedColumn(new BaseColumn("vt4", "n_name"), "vc8"),
            new AliasedColumn(new BaseColumn("vt1", "p_partkey"), "vc9"),
            new AliasedColumn(new BaseColumn("vt1", "p_mfgr"), "vc10"),
            new AliasedColumn(new BaseColumn("vt2", "s_address"), "vc11"),
            new AliasedColumn(new BaseColumn("vt2", "s_phone"), "vc12"),
            new AliasedColumn(new BaseColumn("vt2", "s_comment"), "vc13")),
        from);
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "p_partkey"),
        new BaseColumn("vt3", "ps_partkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "s_suppkey"),
        new BaseColumn("vt3", "ps_suppkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "p_size"),
        ConstantColumn.valueOf("':1'")
        )));
    expected.addFilterByAnd(new ColumnOp("like", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "p_type"),
        ConstantColumn.valueOf("'%:2'")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "s_nationkey"),
        new BaseColumn("vt4", "n_nationkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt4", "n_regionkey"),
        new BaseColumn("vt5", "r_regionkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt5", "r_name"),
        ConstantColumn.valueOf("':3'")
        )));
    List<AbstractRelation> subqueryFrom = Arrays.<AbstractRelation>asList(
        new BaseTable("tpch", "partsupp", "vt14"),
        new BaseTable("tpch", "supplier", "vt15"),
        new BaseTable("tpch", "nation", "vt16"),
        new BaseTable("tpch", "region", "vt17"));
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("min", new BaseColumn("vt14", "ps_supplycost"))
                , "vc18")),
        subqueryFrom);
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "p_partkey"),
        new BaseColumn("vt14", "ps_partkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt15", "s_suppkey"),
        new BaseColumn("vt14", "ps_suppkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt15", "s_nationkey"),
        new BaseColumn("vt16", "n_nationkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt16", "n_regionkey"),
        new BaseColumn("vt17", "r_regionkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt17", "r_name"),
        ConstantColumn.valueOf("':3'")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.asList(
        new BaseColumn("vt3", "ps_supplycost"),
        SubqueryColumn.getSubqueryColumn(subquery)
        )));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("vc6", "desc"),
        new OrderbyAttribute("vc8"),
        new OrderbyAttribute("vc7"),
        new OrderbyAttribute("vc9")
        ));
    expected.addLimit(ConstantColumn.valueOf(100));
    String sql = "select " +
        "s_acctbal, " +
        "s_name, " +
        "n_name, " +
        "p_partkey, " +
        "p_mfgr, " +
        "s_address, " +
        "s_phone, " +
        "s_comment " +
        "from " +
        "part, " +
        "supplier, " +
        "partsupp, " +
        "nation, " +
        "region " +
        "where " +
        "p_partkey = ps_partkey " +
        "and s_suppkey = ps_suppkey " +
        "and p_size = ':1' " +
        "and p_type like '%:2' " +
        "and s_nationkey = n_nationkey " +
        "and n_regionkey = r_regionkey " +
        "and r_name = ':3' " +
        "and ps_supplycost = ( " +
        "select " +
        "min(ps_supplycost) " +
        "from " +
        "partsupp, " +
        "supplier, " +
        "nation, " +
        "region " +
        "where " +
        "p_partkey = ps_partkey " +
        "and s_suppkey = ps_suppkey " +
        "and s_nationkey = n_nationkey " +
        "and n_regionkey = r_regionkey " +
        "and r_name = ':3' " +
        ") " +
        "order by " +
        "s_acctbal desc, " +
        "n_name, " +
        "s_name, " +
        "p_partkey " +
        "LIMIT 100";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query3Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select " +
        "l_orderkey, " +
        "sum(l_extendedprice * (1 - l_discount)) as revenue, " +
        "o_orderdate, " +
        "o_shippriority " +
        "from " +
        "customer, " +
        "orders, " +
        "lineitem " +
        "where " +
        "c_mktsegment = ':1' " +
        "and c_custkey = o_custkey " +
        "and l_orderkey = o_orderkey " +
        "and o_orderdate < date ':2' " +
        "and l_shipdate > date ':2' " +
        "group by " +
        "l_orderkey, " +
        "o_orderdate, " +
        "o_shippriority " +
        "order by " +
        "revenue desc, " +
        "o_orderdate " +
        "LIMIT 10";
    AbstractRelation customer = new BaseTable("tpch", "customer", "vt1");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt2");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt3");
    ColumnOp op1 = new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "l_extendedprice"),
        new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
            ConstantColumn.valueOf(1),
            new BaseColumn("vt3", "l_discount")
            ))
        ));
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt3", "l_orderkey"), "vc4"),
            new AliasedColumn(new ColumnOp("sum", op1), "revenue"),
            new AliasedColumn(new BaseColumn("vt2", "o_orderdate"), "vc5"),
            new AliasedColumn(new BaseColumn("vt2", "o_shippriority"), "vc6")
            ),
        Arrays.asList(customer, orders, lineitem));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "c_mktsegment"),
        ConstantColumn.valueOf("':1'")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "c_custkey"),
        new BaseColumn("vt2", "o_custkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "l_orderkey"),
        new BaseColumn("vt2", "o_orderkey")
        )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "o_orderdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':2'"))
        )));
    expected.addFilterByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "l_shipdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':2'"))
        )));
    expected.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("vc4"),
        new AliasReference("vc5"),
        new AliasReference("vc6")
        ));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("revenue", "desc"),
        new OrderbyAttribute("vc5")
        ));
    expected.addLimit(ConstantColumn.valueOf(10));
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query4Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt1");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt1", "o_orderpriority"), "vc2"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "order_count")
            ),
        orders);
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "o_orderdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':1'"))
        )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "o_orderdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':1'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'3'"), ConstantColumn.valueOf("month")))
            ))
        )));
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        new BaseTable("tpch", "lineitem", "vt3"));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "l_orderkey"),
        new BaseColumn("vt1", "o_orderkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "l_commitdate"),
        new BaseColumn("vt3", "l_receiptdate")
        )));
    expected.addFilterByAnd(new ColumnOp("exists", SubqueryColumn.getSubqueryColumn(subquery)));
    expected.addGroupby(new AliasReference("vc2"));
    expected.addOrderby(new OrderbyAttribute("vc2"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "o_orderpriority, " +
        "count(*) as order_count " +
        "from " +
        "orders " +
        "where " +
        "o_orderdate >= date ':1' " +
        "and o_orderdate < date ':1' + interval '3' month " +
        "and exists ( " +
        "select " +
        "* " +
        "from " +
        "lineitem " +
        "where " +
        "l_orderkey = o_orderkey " +
        "and l_commitdate < l_receiptdate " +
        ") " +
        "group by " +
        "o_orderpriority " +
        "order by " +
        "o_orderpriority " +
        "LIMIT 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query5Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation customer = new BaseTable("tpch", "customer", "vt1");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt2");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt3");
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "vt4");
    AbstractRelation nation = new BaseTable("tpch", "nation", "vt5");
    AbstractRelation region = new BaseTable("tpch", "region", "vt6");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt5", "n_name"), "vc7"),
            new AliasedColumn(new ColumnOp("sum", Arrays.<UnnamedColumn>asList(
                new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("vt3", "l_extendedprice"),
                    new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
                        ConstantColumn.valueOf(1),
                        new BaseColumn("vt3", "l_discount")
                        ))
                    ))
                )), "revenue")
            ),
        Arrays.asList(customer, orders, lineitem, supplier, nation, region));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "c_custkey"),
        new BaseColumn("vt2", "o_custkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "l_orderkey"),
        new BaseColumn("vt2", "o_orderkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "l_suppkey"),
        new BaseColumn("vt4", "s_suppkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "c_nationkey"),
        new BaseColumn("vt4", "s_nationkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt4", "s_nationkey"),
        new BaseColumn("vt5", "n_nationkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt5", "n_regionkey"),
        new BaseColumn("vt6", "r_regionkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt6", "r_name"),
        ConstantColumn.valueOf("':1'")
        )));
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "o_orderdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':2'"))
        )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "o_orderdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':2'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'1'"), ConstantColumn.valueOf("year")))
            ))
        )));
    expected.addGroupby(new AliasReference("vc7"));
    expected.addOrderby(new OrderbyAttribute("revenue", "desc"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "n_name, " +
        "sum(l_extendedprice * (1 - l_discount)) as revenue " +
        "from " +
        "customer, " +
        "orders, " +
        "lineitem, " +
        "supplier, " +
        "nation, " +
        "region " +
        "where " +
        "c_custkey = o_custkey " +
        "and l_orderkey = o_orderkey " +
        "and l_suppkey = s_suppkey " +
        "and c_nationkey = s_nationkey " +
        "and s_nationkey = n_nationkey " +
        "and n_regionkey = r_regionkey " +
        "and r_name = ':1' " +
        "and o_orderdate >= date ':2' " +
        "and o_orderdate < date ':2' + interval '1' year " +
        "group by " +
        "n_name " +
        "order by " +
        "revenue desc " +
        "LIMIT 1; ";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query6Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt1");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply",
                Arrays.<UnnamedColumn>asList(
                    new BaseColumn("vt1", "l_extendedprice"),
                    new BaseColumn("vt1", "l_discount")
                    ))), "revenue")
            ),
        lineitem);
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "l_shipdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':1'"))
        )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "l_shipdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':1'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(
                ConstantColumn.valueOf("'1'"),
                ConstantColumn.valueOf("year")
                ))
            ))
        )));
    expected.addFilterByAnd(new ColumnOp("between", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "l_discount"),
        new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':2'"), ConstantColumn.valueOf("0.01"))),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':2'"), ConstantColumn.valueOf("0.01")))
        )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "l_quantity"),
        ConstantColumn.valueOf("':3'"))
        ));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "sum(l_extendedprice * l_discount) as revenue " +
        "from " +
        "lineitem " +
        "where " +
        "l_shipdate >= date ':1' " +
        "and l_shipdate < date ':1' + interval '1' year " +
        "and l_discount between ':2' - 0.01 and ':2' + 0.01 " +
        "and l_quantity < ':3' " +
        "LIMIT 1; ";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query7Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "vt1");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt2");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt3");
    AbstractRelation customer = new BaseTable("tpch", "customer", "vt4");
    AbstractRelation nation1 = new BaseTable("tpch", "nation", "n1");
    AbstractRelation nation2 = new BaseTable("tpch", "nation", "n2");
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("n1", "n_name"), "supp_nation"),
            new AliasedColumn(new BaseColumn("n2", "n_name"), "cust_nation"),
            new AliasedColumn(new ColumnOp("substr", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "l_shipdate"), ConstantColumn.valueOf(0), ConstantColumn.valueOf(4))), "l_year"),
            new AliasedColumn(new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "l_extendedprice"),
                new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
                    ConstantColumn.valueOf(1), new BaseColumn("vt2", "l_discount")))
                )), "volume")
            ),
        Arrays.asList(supplier, lineitem, orders, customer, nation1, nation2));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "s_suppkey"),
        new BaseColumn("vt2", "l_suppkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "o_orderkey"),
        new BaseColumn("vt2", "l_orderkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt4", "c_custkey"),
        new BaseColumn("vt3", "o_custkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "s_nationkey"),
        new BaseColumn("n1", "n_nationkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt4", "c_nationkey"),
        new BaseColumn("n2", "n_nationkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("or", Arrays.<UnnamedColumn>asList(
        new ColumnOp("and", Arrays.<UnnamedColumn>asList(
            new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                new BaseColumn("n1", "n_name"),
                ConstantColumn.valueOf("':1'")
                )),
            new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                new BaseColumn("n2", "n_name"),
                ConstantColumn.valueOf("':2'")
                ))
            )),
        new ColumnOp("and", Arrays.<UnnamedColumn>asList(
            new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                new BaseColumn("n1", "n_name"),
                ConstantColumn.valueOf("':2'")
                )),
            new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                new BaseColumn("n2", "n_name"),
                ConstantColumn.valueOf("':1'")
                ))
            ))
        )));
    subquery.addFilterByAnd(new ColumnOp("between", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "l_shipdate"),
        new ColumnOp("date", ConstantColumn.valueOf("'1995-01-01'")),
        new ColumnOp("date", ConstantColumn.valueOf("'1996-12-31'")))
        ));
    subquery.setAliasName("shipping");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("shipping", "supp_nation"), "vc5"),
            new AliasedColumn(new BaseColumn("shipping", "cust_nation"), "vc6"),
            new AliasedColumn(new BaseColumn("shipping", "l_year"), "vc7"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("shipping", "volume")), "revenue")
            ),
        subquery);
    expected.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("vc5"),
        new AliasReference("vc6"),
        new AliasReference("vc7")
        ));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("vc5"),
        new OrderbyAttribute("vc6"),
        new OrderbyAttribute("vc7")
        ));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "supp_nation, " +
        "cust_nation, " +
        "l_year, " +
        "sum(volume) as revenue " +
        "from " +
        "( " +
        "select " +
        "n1.n_name as supp_nation, " +
        "n2.n_name as cust_nation, " +
        "substr(l_shipdate,0,4) as l_year, " +
        "l_extendedprice * (1 - l_discount) as volume " +
        "from " +
        "supplier, " +
        "lineitem, " +
        "orders, " +
        "customer, " +
        "nation n1, " +
        "nation n2 " +
        "where " +
        "s_suppkey = l_suppkey " +
        "and o_orderkey = l_orderkey " +
        "and c_custkey = o_custkey " +
        "and s_nationkey = n1.n_nationkey " +
        "and c_nationkey = n2.n_nationkey " +
        "and ( " +
        "(n1.n_name = ':1' and n2.n_name = ':2') " +
        "or (n1.n_name = ':2' and n2.n_name = ':1') " +
        ") " +
        "and l_shipdate between date '1995-01-01' and date '1996-12-31' " +
        ") as shipping " +
        "group by " +
        "supp_nation, " +
        "cust_nation, " +
        "l_year " +
        "order by " +
        "supp_nation, " +
        "cust_nation, " +
        "l_year " +
        "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query8Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation part = new BaseTable("tpch", "part", "vt1");
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "vt2");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt3");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt4");
    AbstractRelation customer = new BaseTable("tpch", "customer", "vt5");
    AbstractRelation nation1 = new BaseTable("tpch", "nation", "n1");
    AbstractRelation nation2 = new BaseTable("tpch", "nation", "n2");
    AbstractRelation region = new BaseTable("tpch", "region", "vt6");
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("substr", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt4", "o_orderdate"),
                ConstantColumn.valueOf(0), ConstantColumn.valueOf(4))), "o_year"),
            new AliasedColumn(new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "l_extendedprice"),
                new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf(1), new BaseColumn("vt3", "l_discount")))
                )), "volume"),
            new AliasedColumn(new BaseColumn("n2", "n_name"), "nation")
            ),
        Arrays.asList(part, supplier, lineitem, orders, customer, nation1, nation2, region));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "p_partkey"),
        new BaseColumn("vt3", "l_partkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "s_suppkey"),
        new BaseColumn("vt3", "l_suppkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "l_orderkey"),
        new BaseColumn("vt4", "o_orderkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt4", "o_custkey"),
        new BaseColumn("vt5", "c_custkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt5", "c_nationkey"),
        new BaseColumn("n1", "n_nationkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("n1", "n_regionkey"),
        new BaseColumn("vt6", "r_regionkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt6", "r_name"),
        ConstantColumn.valueOf("':2'")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "s_nationkey"),
        new BaseColumn("n2", "n_nationkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("between", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt4", "o_orderdate"),
        new ColumnOp("date", ConstantColumn.valueOf("'1995-01-01'")),
        new ColumnOp("date", ConstantColumn.valueOf("'1996-12-31'"))
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "p_type"),
        ConstantColumn.valueOf("':3'")
        )));
    subquery.setAliasName("all_nations");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("all_nations", "o_year"), "vc7"),
            new AliasedColumn(
                new ColumnOp("divide", Arrays.<UnnamedColumn>asList(
                    new ColumnOp("sum", new ColumnOp("whenthenelse", Arrays.<UnnamedColumn>asList(
                        new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                            new BaseColumn("all_nations", "nation"),
                            ConstantColumn.valueOf("':1'")
                            )), new BaseColumn("all_nations", "volume"),
                        ConstantColumn.valueOf(0)))),
                    new ColumnOp("sum", new BaseColumn("all_nations", "volume")))), "mkt_share"

                )),
        subquery);
    expected.addGroupby(new AliasReference("vc7"));
    expected.addOrderby(new OrderbyAttribute("vc7"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "o_year, " +
        "sum(case " +
        "when nation = ':1' then volume " +
        "else 0 " +
        "end) / sum(volume) as mkt_share " +
        "from " +
        "( " +
        "select " +
        "substr(o_orderdate,0,4) as o_year, " +
        "l_extendedprice * (1 - l_discount) as volume, " +
        "n2.n_name as nation " +
        "from " +
        "part, " +
        "supplier, " +
        "lineitem, " +
        "orders, " +
        "customer, " +
        "nation n1, " +
        "nation n2, " +
        "region " +
        "where " +
        "p_partkey = l_partkey " +
        "and s_suppkey = l_suppkey " +
        "and l_orderkey = o_orderkey " +
        "and o_custkey = c_custkey " +
        "and c_nationkey = n1.n_nationkey " +
        "and n1.n_regionkey = r_regionkey " +
        "and r_name = ':2' " +
        "and s_nationkey = n2.n_nationkey " +
        "and o_orderdate between date '1995-01-01' and date '1996-12-31' " +
        "and p_type = ':3' " +
        ") as all_nations " +
        "group by " +
        "o_year " +
        "order by " +
        "o_year " +
        "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query9Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation part = new BaseTable("tpch", "part", "vt1");
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "vt2");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt3");
    AbstractRelation partsupp = new BaseTable("tpch", "partsupp", "vt4");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt5");
    AbstractRelation nation = new BaseTable("tpch", "nation", "vt6");
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt6", "n_name"), "nation"),
            new AliasedColumn(new ColumnOp("substr", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt5", "o_orderdate"),
                ConstantColumn.valueOf(0),
                ConstantColumn.valueOf(4))), "o_year"),
            new AliasedColumn(new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
                new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("vt3", "l_extendedprice"),
                    new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf(1), new BaseColumn("vt3", "l_discount")))
                    )),
                new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("vt4", "ps_supplycost"),
                    new BaseColumn("vt3", "l_quantity")
                    ))
                )), "amount")
            ),
        Arrays.asList(part, supplier, lineitem, partsupp, orders, nation));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "s_suppkey"),
        new BaseColumn("vt3", "l_suppkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt4", "ps_suppkey"),
        new BaseColumn("vt3", "l_suppkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt4", "ps_partkey"),
        new BaseColumn("vt3", "l_partkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "p_partkey"),
        new BaseColumn("vt3", "l_partkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt5", "o_orderkey"),
        new BaseColumn("vt3", "l_orderkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "s_nationkey"),
        new BaseColumn("vt6", "n_nationkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("like", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "p_name"),
        ConstantColumn.valueOf("'%:1%'")
        )));
    subquery.setAliasName("profit");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("profit", "nation"), "vc7"),
            new AliasedColumn(new BaseColumn("profit", "o_year"), "vc8"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("profit", "amount")), "sum_profit")
            ),
        subquery);
    expected.addGroupby(Arrays.<GroupingAttribute>asList(new AliasReference("vc7"), new AliasReference("vc8")));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(new OrderbyAttribute("vc7"),
        new OrderbyAttribute("vc8", "desc")));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "nation, " +
        "o_year, " +
        "sum(amount) as sum_profit " +
        "from " +
        "( " +
        "select " +
        "n_name as nation, " +
        "substr(o_orderdate,0,4) as o_year, " +
        "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount " +
        "from " +
        "part, " +
        "supplier, " +
        "lineitem, " +
        "partsupp, " +
        "orders, " +
        "nation " +
        "where " +
        "s_suppkey = l_suppkey " +
        "and ps_suppkey = l_suppkey " +
        "and ps_partkey = l_partkey " +
        "and p_partkey = l_partkey " +
        "and o_orderkey = l_orderkey " +
        "and s_nationkey = n_nationkey " +
        "and p_name like '%:1%' " +
        ") as profit " +
        "group by " +
        "nation, " +
        "o_year " +
        "order by " +
        "nation, " +
        "o_year desc " +
        "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query10Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation customer = new BaseTable("tpch", "customer", "vt1");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt2");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt3");
    AbstractRelation nation = new BaseTable("tpch", "nation", "vt4");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt1", "c_custkey"), "vc5"),
            new AliasedColumn(new BaseColumn("vt1", "c_name"), "vc6"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "l_extendedprice"),
                new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
                    ConstantColumn.valueOf(1),
                    new BaseColumn("vt3", "l_discount")
                    ))
                ))), "revenue"),
            new AliasedColumn(new BaseColumn("vt1", "c_acctbal"), "vc7"),
            new AliasedColumn(new BaseColumn("vt4", "n_name"), "vc8"),
            new AliasedColumn(new BaseColumn("vt1", "c_address"), "vc9"),
            new AliasedColumn(new BaseColumn("vt1", "c_phone"), "vc10"),
            new AliasedColumn(new BaseColumn("vt1", "c_comment"), "vc11")
            ),
        Arrays.asList(customer, orders, lineitem, nation));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "c_custkey"),
        new BaseColumn("vt2", "o_custkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "l_orderkey"),
        new BaseColumn("vt2", "o_orderkey")
        )));
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "o_orderdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':1'"))
        )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "o_orderdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':1'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'3'"), ConstantColumn.valueOf("month")))
            )
            ))));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "l_returnflag"),
        ConstantColumn.valueOf("'R'")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "c_nationkey"),
        new BaseColumn("vt4", "n_nationkey")
        )));
    expected.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("vc5"),
        new AliasReference("vc6"),
        new AliasReference("vc7"),
        new AliasReference("vc10"),
        new AliasReference("vc8"),
        new AliasReference("vc9"),
        new AliasReference("vc11")
        ));
    expected.addOrderby(new OrderbyAttribute("revenue", "desc"));
    expected.addLimit(ConstantColumn.valueOf(20));
    String sql = "select " +
        "c_custkey, " +
        "c_name, " +
        "sum(l_extendedprice * (1 - l_discount)) as revenue, " +
        "c_acctbal, " +
        "n_name, " +
        "c_address, " +
        "c_phone, " +
        "c_comment " +
        "from " +
        "customer, " +
        "orders, " +
        "lineitem, " +
        "nation " +
        "where " +
        "c_custkey = o_custkey " +
        "and l_orderkey = o_orderkey " +
        "and o_orderdate >= date ':1' " +
        "and o_orderdate < date ':1' + interval '3' month " +
        "and l_returnflag = 'R' " +
        "and c_nationkey = n_nationkey " +
        "group by " +
        "c_custkey, " +
        "c_name, " +
        "c_acctbal, " +
        "c_phone, " +
        "n_name, " +
        "c_address, " +
        "c_comment " +
        "order by " +
        "revenue desc " +
        "LIMIT 20;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query11Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation partsupp = new BaseTable("tpch", "partsupp", "vt1");
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "vt2");
    AbstractRelation nation = new BaseTable("tpch", "nation", "vt3");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt1", "ps_partkey"), "vc4"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "ps_supplycost"),
                new BaseColumn("vt1", "ps_availqty")
                ))), "value")
            ),
        Arrays.asList(partsupp, supplier, nation));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "ps_suppkey"),
        new BaseColumn("vt2", "s_suppkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "s_nationkey"),
        new BaseColumn("vt3", "n_nationkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "n_name"),
        ConstantColumn.valueOf("':1'")
        )));
    expected.addGroupby(new AliasReference("vc4"));
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("vt5", "ps_supplycost"),
                    new BaseColumn("vt5", "ps_availqty")
                    ))),
                ConstantColumn.valueOf("':2'")
                )), "vc8")
            ), Arrays.<AbstractRelation>asList(new BaseTable("tpch", "partsupp", "vt5"),
                new BaseTable("tpch", "supplier", "vt6"),
                new BaseTable("tpch", "nation", "vt7")));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt5", "ps_suppkey"),
        new BaseColumn("vt6", "s_suppkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt6", "s_nationkey"),
        new BaseColumn("vt7", "n_nationkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt7", "n_name"),
        ConstantColumn.valueOf("':1'")
        )));
    expected.addHavingByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
            new BaseColumn("vt1", "ps_supplycost"),
            new BaseColumn("vt1", "ps_availqty")
            ))),
        SubqueryColumn.getSubqueryColumn(subquery)
        )));
    expected.addOrderby(new OrderbyAttribute("value", "desc"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "ps_partkey, " +
        "sum(ps_supplycost * ps_availqty) as value " +
        "from " +
        "partsupp, " +
        "supplier, " +
        "nation " +
        "where " +
        "ps_suppkey = s_suppkey " +
        "and s_nationkey = n_nationkey " +
        "and n_name = ':1' " +
        "group by " +
        "ps_partkey having " +
        "sum(ps_supplycost * ps_availqty) > ( " +
        "select " +
        "sum(ps_supplycost * ps_availqty) * ':2' " +
        "from " +
        "partsupp, " +
        "supplier, " +
        "nation " +
        "where " +
        "ps_suppkey = s_suppkey " +
        "and s_nationkey = n_nationkey " +
        "and n_name = ':1' " +
        ") " +
        "order by " +
        "value desc " +
        "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query12Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt1");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt2");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt2", "l_shipmode"), "vc3"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("whenthenelse", Arrays.<UnnamedColumn>asList(
                new ColumnOp("or", Arrays.<UnnamedColumn>asList(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("vt1", "o_orderpriority"),
                    ConstantColumn.valueOf("'1-URGENT'")
                    )),
                    new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt1", "o_orderpriority"),
                        ConstantColumn.valueOf("'2-HIGH'")
                        ))
                    )),
                ConstantColumn.valueOf(1),
                ConstantColumn.valueOf(0)
                ))), "high_line_count"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("whenthenelse", Arrays.<UnnamedColumn>asList(
                new ColumnOp("and", Arrays.<UnnamedColumn>asList(new ColumnOp("notequal", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("vt1", "o_orderpriority"),
                    ConstantColumn.valueOf("'1-URGENT'")
                    )),
                    new ColumnOp("notequal", Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt1", "o_orderpriority"),
                        ConstantColumn.valueOf("'2-HIGH'")
                        ))
                    )),
                ConstantColumn.valueOf(1),
                ConstantColumn.valueOf(0)
                ))), "low_line_count")
            ),
        Arrays.asList(orders, lineitem));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "o_orderkey"),
        new BaseColumn("vt2", "l_orderkey")
        )));
    expected.addFilterByAnd(new ColumnOp("in", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "l_shipmode"),
        ConstantColumn.valueOf("':1'"),
        ConstantColumn.valueOf("':2'")
        )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "l_commitdate"),
        new BaseColumn("vt2", "l_receiptdate")
        )));

    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "l_shipdate"),
        new BaseColumn("vt2", "l_commitdate")
        )));
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "l_receiptdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':3'"))
        )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "l_receiptdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':3'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'1'"), ConstantColumn.valueOf("year")))
            ))
        )));
    expected.addGroupby(new AliasReference("vc3"));
    expected.addOrderby(new OrderbyAttribute("vc3"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "l_shipmode, " +
        "sum(case " +
        "when o_orderpriority = '1-URGENT' " +
        "or o_orderpriority = '2-HIGH' " +
        "then 1 " +
        "else 0 " +
        "end) as high_line_count, " +
        "sum(case " +
        "when o_orderpriority <> '1-URGENT' " +
        "and o_orderpriority <> '2-HIGH' " +
        "then 1 " +
        "else 0 " +
        "end) as low_line_count " +
        "from " +
        "orders, " +
        "lineitem " +
        "where " +
        "o_orderkey = l_orderkey " +
        "and l_shipmode in (':1', ':2') " +
        "and l_commitdate < l_receiptdate " +
        "and l_shipdate < l_commitdate " +
        "and l_receiptdate >= date ':3' " +
        "and l_receiptdate < date ':3' + interval '1' year " +
        "group by " +
        "l_shipmode " +
        "order by " +
        "l_shipmode " +
        "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query13Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    BaseTable customer = new BaseTable("tpch", "customer", "vt1");
    BaseTable orders = new BaseTable("tpch", "orders", "vt2");
    JoinTable join = JoinTable.create(Arrays.<AbstractRelation>asList(customer, orders),
        Arrays.<JoinTable.JoinType>asList(JoinTable.JoinType.leftouter),
        Arrays.<UnnamedColumn>asList(new ColumnOp("and", Arrays.<UnnamedColumn>asList(
            new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "c_custkey"),
                new BaseColumn("vt2", "o_custkey")
                )),
            new ColumnOp("notlike", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "o_comment"),
                ConstantColumn.valueOf("'%:1%:2%'")
                ))
            ))));
    SelectQuery subqery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt1", "c_custkey"), "c_custkey"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "c_count")
            ),
        join);
    subqery.addGroupby(new AliasReference("c_custkey"));
    subqery.setAliasName("c_orders");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("c_orders", "c_count"), "vc3"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "custdist")
            ),
        subqery);
    expected.addGroupby(new AliasReference("vc3"));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("custdist", "desc"),
        new OrderbyAttribute("vc3", "desc")));
    expected.addLimit(ConstantColumn.valueOf(1));
    
    String sql = "select " +
        "c_count, " +
        "count(*) as custdist " +
        "from " +
        "( " +
        "select " +
        "c_custkey, " +
        "count(o_orderkey) " +
        "from " +
        "customer left outer join orders on " +
        "c_custkey = o_custkey " +
        "and o_comment not like '%:1%:2%' " +
        "group by " +
        "c_custkey " +
        ") as c_orders (c_custkey, c_count) " +
        "group by " +
        "c_count " +
        "order by " +
        "custdist desc, " +
        "c_count desc " +
        "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query14Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt1");
    AbstractRelation part = new BaseTable("tpch", "part", "vt2");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("divide", Arrays.<UnnamedColumn>asList(
                new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    ConstantColumn.valueOf("100.00"),
                    new ColumnOp("sum", new ColumnOp("whenthenelse", Arrays.<UnnamedColumn>asList(
                        new ColumnOp("like", Arrays.<UnnamedColumn>asList(
                            new BaseColumn("vt2", "p_type"),
                            ConstantColumn.valueOf("'PROMO%'")
                            )),
                        new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                            new BaseColumn("vt1", "l_extendedprice"),
                            new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf(1), new BaseColumn("vt1", "l_discount"))))),
                        ConstantColumn.valueOf(0)
                        )))
                    )),
                new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("vt1", "l_extendedprice"),
                    new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf(1), new BaseColumn("vt1", "l_discount")))
                    )))
                )), "promo_revenue")
            ),
        Arrays.asList(lineitem, part));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "l_partkey"),
        new BaseColumn("vt2", "p_partkey")
        )));
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "l_shipdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':1'"))
        )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "l_shipdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':1'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'1'"), ConstantColumn.valueOf("month")))
            ))
        )));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "100.00 * sum(case " +
        "when p_type like 'PROMO%' " +
        "then l_extendedprice * (1 - l_discount) " +
        "else 0 " +
        "end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue " +
        "from " +
        "lineitem, " +
        "part " +
        "where " +
        "l_partkey = p_partkey " +
        "and l_shipdate >= date ':1' " +
        "and l_shipdate < date ':1' + interval '1' month " +
        "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query16Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation partsupp = new BaseTable("tpch", "partsupp", "vt1");
    AbstractRelation part = new BaseTable("tpch", "part", "vt2");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt2", "p_brand"), "vc3"),
            new AliasedColumn(new BaseColumn("vt2", "p_type"), "vc4"),
            new AliasedColumn(new BaseColumn("vt2", "p_size"), "vc5"),
            new AliasedColumn(new ColumnOp("countdistinct", new BaseColumn("vt1", "ps_suppkey")), "supplier_cnt")
            ),
        Arrays.asList(partsupp, part));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "p_partkey"),
        new BaseColumn("vt1", "ps_partkey")
        )));
    expected.addFilterByAnd(new ColumnOp("notequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "p_brand"),
        ConstantColumn.valueOf("':1'")
        )));
    expected.addFilterByAnd(new ColumnOp("notlike", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "p_type"),
        ConstantColumn.valueOf("':2%'")
        )));
    expected.addFilterByAnd(new ColumnOp("in", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "p_size"),
        ConstantColumn.valueOf("':3'"), ConstantColumn.valueOf("':4'"), ConstantColumn.valueOf("':5'"), ConstantColumn.valueOf("':6'"),
        ConstantColumn.valueOf("':7'"), ConstantColumn.valueOf("':8'"), ConstantColumn.valueOf("':9'"), ConstantColumn.valueOf("':10'")
        )));
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(new AliasedColumn(new BaseColumn("vt6", "s_suppkey"), "vc7")),
        Arrays.<AbstractRelation>asList(new BaseTable("tpch", "supplier", "vt6")));
    subquery.addFilterByAnd(new ColumnOp("like", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt6", "s_comment"),
        ConstantColumn.valueOf("'%Customer%Complaints%'")
        )));
    expected.addFilterByAnd(new ColumnOp("notin", Arrays.asList(
        new BaseColumn("vt1", "ps_suppkey"),
        SubqueryColumn.getSubqueryColumn(subquery)
        )));
    expected.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("vc3"),
        new AliasReference("vc4"),
        new AliasReference("vc5")
        ));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("supplier_cnt", "desc"),
        new OrderbyAttribute("vc3"),
        new OrderbyAttribute("vc4"),
        new OrderbyAttribute("vc5")
        ));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "p_brand, " +
        "p_type, " +
        "p_size, " +
        "count(distinct ps_suppkey) as supplier_cnt " +
        "from " +
        "partsupp, " +
        "part " +
        "where " +
        "p_partkey = ps_partkey " +
        "and p_brand <> ':1' " +
        "and p_type not like ':2%' " +
        "and p_size in (':3', ':4', ':5', ':6', ':7', ':8', ':9', ':10') " +
        "and ps_suppkey not in ( " +
        "select " +
        "s_suppkey " +
        "from " +
        "supplier " +
        "where " +
        "s_comment like '%Customer%Complaints%' " +
        ") " +
        "group by " +
        "p_brand, " +
        "p_type, " +
        "p_size " +
        "order by " +
        "supplier_cnt desc, " +
        "p_brand, " +
        "p_type, " +
        "p_size " +
        "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query17Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt1");
    AbstractRelation part = new BaseTable("tpch", "part", "vt2");
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt3", "l_partkey"), "agg_partkey"),
            new AliasedColumn(new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                ConstantColumn.valueOf("0.2"),
                new ColumnOp("avg", new BaseColumn("vt3", "l_quantity"))
                )), "avg_quantity")
            ),
        new BaseTable("tpch", "lineitem", "vt3"));
    subquery.addGroupby(new AliasReference("agg_partkey"));
    subquery.setAliasName("part_agg");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("divide", Arrays.<UnnamedColumn>asList(
                new ColumnOp("sum", new BaseColumn("vt1", "l_extendedprice")),
                ConstantColumn.valueOf("7.0")
                )), "avg_yearly")
            ),
        Arrays.asList(lineitem, part, subquery));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "p_partkey"),
        new BaseColumn("vt1", "l_partkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("part_agg", "agg_partkey"),
        new BaseColumn("vt1", "l_partkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "p_brand"),
        ConstantColumn.valueOf("':1'")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "p_container"),
        ConstantColumn.valueOf("':2'")
        )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "l_quantity"),
        new BaseColumn("part_agg", "avg_quantity")
        )));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "sum(l_extendedprice) / 7.0 as avg_yearly " +
        "from " +
        "lineitem, " +
        "part, " +
        "(SELECT l_partkey AS agg_partkey, 0.2 * avg(l_quantity) AS avg_quantity FROM lineitem GROUP BY l_partkey) part_agg " +
        "where " +
        "p_partkey = l_partkey " +
        "and agg_partkey = l_partkey " +
        "and p_brand = ':1' " +
        "and p_container = ':2' " +
        "and l_quantity < avg_quantity " +
        "LIMIT 1; ";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query18Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation customer = new BaseTable("tpch", "customer", "vt1");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt2");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt3");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt1", "c_name"), "vc4"),
            new AliasedColumn(new BaseColumn("vt1", "c_custkey"), "vc5"),
            new AliasedColumn(new BaseColumn("vt2", "o_orderkey"), "vc6"),
            new AliasedColumn(new BaseColumn("vt2", "o_orderdate"), "vc7"),
            new AliasedColumn(new BaseColumn("vt2", "o_totalprice"), "vc8"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("vt3", "l_quantity")), "s9")
            ),
        Arrays.asList(customer, orders, lineitem));
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(new AliasedColumn(new BaseColumn("vt10", "l_orderkey"), "vc11")),
        new BaseTable("tpch", "lineitem", "vt10"));
    subquery.addGroupby(new AliasReference("vc11"));
    subquery.addHavingByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new ColumnOp("sum", new BaseColumn("vt10", "l_quantity")),
        ConstantColumn.valueOf("':1'")
        )));
    expected.addFilterByAnd(new ColumnOp("in", Arrays.asList(
        new BaseColumn("vt2", "o_orderkey"),
        SubqueryColumn.getSubqueryColumn(subquery)
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "c_custkey"),
        new BaseColumn("vt2", "o_custkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "o_orderkey"),
        new BaseColumn("vt3", "l_orderkey")
        )));
    expected.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("vc4"),
        new AliasReference("vc5"),
        new AliasReference("vc6"),
        new AliasReference("vc7"),
        new AliasReference("vc8")
        ));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("vc8", "desc"),
        new OrderbyAttribute("vc7")
        ));
    expected.addLimit(ConstantColumn.valueOf(100));
    String sql = "select " +
        "c_name, " +
        "c_custkey, " +
        "o_orderkey, " +
        "o_orderdate, " +
        "o_totalprice, " +
        "sum(l_quantity) " +
        "from " +
        "customer, " +
        "orders, " +
        "lineitem " +
        "where " +
        "o_orderkey in ( " +
        "select " +
        "l_orderkey " +
        "from " +
        "lineitem " +
        "group by " +
        "l_orderkey having " +
        "sum(l_quantity) > ':1' " +
        ") " +
        "and c_custkey = o_custkey " +
        "and o_orderkey = l_orderkey " +
        "group by " +
        "c_name, " +
        "c_custkey, " +
        "o_orderkey, " +
        "o_orderdate, " +
        "o_totalprice " +
        "order by " +
        "o_totalprice desc, " +
        "o_orderdate " +
        "LIMIT 100;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query19Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt1");
    AbstractRelation part = new BaseTable("tpch", "part", "vt2");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "l_extendedprice"),
                new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
                    ConstantColumn.valueOf(1),
                    new BaseColumn("vt1", "l_discount")
                    ))
                ))), "revenue")
            ),
        Arrays.asList(lineitem, part));
    ColumnOp columnOp1 = new ColumnOp("and", Arrays.<UnnamedColumn>asList(
        new ColumnOp("and", Arrays.<UnnamedColumn>asList(
            new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                    new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                        new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                            new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                                new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                                    new BaseColumn("vt2", "p_partkey"),
                                    new BaseColumn("vt1", "l_partkey")
                                    )),
                                new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                                    new BaseColumn("vt2", "p_brand"),
                                    ConstantColumn.valueOf("':1'")
                                    ))
                                )),
                            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt2", "p_container"),
                                ConstantColumn.valueOf("'SM CASE'"),
                                ConstantColumn.valueOf("'SM BOX'"),
                                ConstantColumn.valueOf("'SM PACK'"),
                                ConstantColumn.valueOf("'SM PKG'")
                                ))
                            )),
                        new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
                            new BaseColumn("vt1", "l_quantity"),
                            ConstantColumn.valueOf("':4'")
                            ))
                        )),
                    new ColumnOp("lessequal", Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt1", "l_quantity"),
                        new ColumnOp("add", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':4'"), ConstantColumn.valueOf(10)))
                        ))
                    )),
                new ColumnOp("between", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("vt2", "p_size"),
                    ConstantColumn.valueOf(1),
                    ConstantColumn.valueOf(5)
                    ))
                )),
            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "l_shipmode"),
                ConstantColumn.valueOf("'AIR'"),
                ConstantColumn.valueOf("'AIR REG'")
                ))
            )),
        new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
            new BaseColumn("vt1", "l_shipinstruct"),
            ConstantColumn.valueOf("'DELIVER IN PERSON'")
            ))
        ));
    ColumnOp columnOp2 = new ColumnOp("and", Arrays.<UnnamedColumn>asList(
        new ColumnOp("and", Arrays.<UnnamedColumn>asList(
            new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                    new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                        new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                            new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                                new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                                    new BaseColumn("vt2", "p_partkey"),
                                    new BaseColumn("vt1", "l_partkey")
                                    )),
                                new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                                    new BaseColumn("vt2", "p_brand"),
                                    ConstantColumn.valueOf("':2'")
                                    ))
                                )),
                            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt2", "p_container"),
                                ConstantColumn.valueOf("'MED BAG'"),
                                ConstantColumn.valueOf("'MED BOX'"),
                                ConstantColumn.valueOf("'MED PKG'"),
                                ConstantColumn.valueOf("'MED PACK'")
                                ))
                            )),
                        new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
                            new BaseColumn("vt1", "l_quantity"),
                            ConstantColumn.valueOf("':5'")
                            ))
                        )),
                    new ColumnOp("lessequal", Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt1", "l_quantity"),
                        new ColumnOp("add", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':5'"), ConstantColumn.valueOf(10)))
                        ))
                    )),
                new ColumnOp("between", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("vt2", "p_size"),
                    ConstantColumn.valueOf(1),
                    ConstantColumn.valueOf(10)
                    ))
                )),
            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "l_shipmode"),
                ConstantColumn.valueOf("'AIR'"),
                ConstantColumn.valueOf("'AIR REG'")
                ))
            )),
        new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
            new BaseColumn("vt1", "l_shipinstruct"),
            ConstantColumn.valueOf("'DELIVER IN PERSON'")
            ))
        ));
    ColumnOp columnOp3 = new ColumnOp("and", Arrays.<UnnamedColumn>asList(
        new ColumnOp("and", Arrays.<UnnamedColumn>asList(
            new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                    new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                        new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                            new ColumnOp("and", Arrays.<UnnamedColumn>asList(
                                new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                                    new BaseColumn("vt2", "p_partkey"),
                                    new BaseColumn("vt1", "l_partkey")
                                    )),
                                new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                                    new BaseColumn("vt2", "p_brand"),
                                    ConstantColumn.valueOf("':3'")
                                    ))
                                )),
                            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt2", "p_container"),
                                ConstantColumn.valueOf("'LG CASE'"),
                                ConstantColumn.valueOf("'LG BOX'"),
                                ConstantColumn.valueOf("'LG PACK'"),
                                ConstantColumn.valueOf("'LG PKG'")
                                ))
                            )),
                        new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
                            new BaseColumn("vt1", "l_quantity"),
                            ConstantColumn.valueOf("':6'")
                            ))
                        )),
                    new ColumnOp("lessequal", Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt1", "l_quantity"),
                        new ColumnOp("add", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':6'"), ConstantColumn.valueOf(10)))
                        ))
                    )),
                new ColumnOp("between", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("vt2", "p_size"),
                    ConstantColumn.valueOf(1),
                    ConstantColumn.valueOf(15)
                    ))
                )),
            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "l_shipmode"),
                ConstantColumn.valueOf("'AIR'"),
                ConstantColumn.valueOf("'AIR REG'")
                ))
            )),
        new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
            new BaseColumn("vt1", "l_shipinstruct"),
            ConstantColumn.valueOf("'DELIVER IN PERSON'")
            ))
        ));
    expected.addFilterByAnd(new ColumnOp("or", Arrays.<UnnamedColumn>asList(
        new ColumnOp("or", Arrays.<UnnamedColumn>asList(
            columnOp1, columnOp2
            )),
        columnOp3
        )));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "sum(l_extendedprice* (1 - l_discount)) as revenue " +
        "from " +
        "lineitem, " +
        "part " +
        "where " +
        "( " +
        "p_partkey = l_partkey " +
        "and p_brand = ':1' " +
        "and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') " +
        "and l_quantity >= ':4' and l_quantity <= ':4' + 10 " +
        "and p_size between 1 and 5 " +
        "and l_shipmode in ('AIR', 'AIR REG') " +
        "and l_shipinstruct = 'DELIVER IN PERSON' " +
        ") " +
        "or " +
        "( " +
        "p_partkey = l_partkey " +
        "and p_brand = ':2' " +
        "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') " +
        "and l_quantity >= ':5' and l_quantity <= ':5' + 10 " +
        "and p_size between 1 and 10 " +
        "and l_shipmode in ('AIR', 'AIR REG') " +
        "and l_shipinstruct = 'DELIVER IN PERSON' " +
        ") " +
        "or " +
        "( " +
        "p_partkey = l_partkey " +
        "and p_brand = ':3' " +
        "and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') " +
        "and l_quantity >= ':6' and l_quantity <= ':6' + 10 " +
        "and p_size between 1 and 15 " +
        "and l_shipmode in ('AIR', 'AIR REG') " +
        "and l_shipinstruct = 'DELIVER IN PERSON' " +
        ") " +
        "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query20Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "vt1");
    AbstractRelation nation = new BaseTable("tpch", "nation", "vt2");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt1", "s_name"), "vc3"),
            new AliasedColumn(new BaseColumn("vt1", "s_address"), "vc4")
            ),
        Arrays.asList(supplier, nation));
    SelectQuery subsubquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt6", "l_partkey"), "agg_partkey"),
            new AliasedColumn(new BaseColumn("vt6", "l_suppkey"), "agg_suppkey"),
            new AliasedColumn(new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                ConstantColumn.valueOf("0.5"), new ColumnOp("sum", new BaseColumn("vt6", "l_quantity")))), "agg_quantity")
            ),
        new BaseTable("tpch", "lineitem", "vt6"));
    subsubquery.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt6", "l_shipdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':2'"))
        )));
    subsubquery.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt6", "l_shipdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':2'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'1'"), ConstantColumn.valueOf("year")))
            ))
        )));
    subsubquery.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("agg_partkey"),
        new AliasReference("agg_suppkey")
        ));
    subsubquery.setAliasName("agg_lineitem");
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt5", "ps_suppkey"), "vc7")
            ),
        Arrays.asList(new BaseTable("tpch", "partsupp", "vt5"), subsubquery));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("agg_lineitem", "agg_partkey"),
        new BaseColumn("vt5", "ps_partkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("agg_lineitem", "agg_suppkey"),
        new BaseColumn("vt5", "ps_suppkey")
        )));
    SelectQuery subsubquery2 = SelectQuery.create(
        Arrays.<SelectItem>asList(new AliasedColumn(new BaseColumn("vt8", "p_partkey"), "vc9")),
        new BaseTable("tpch", "part", "vt8"));
    subsubquery2.addFilterByAnd(new ColumnOp("like", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt8", "p_name"), ConstantColumn.valueOf("':1%'")
        )));
    subquery.addFilterByAnd(new ColumnOp("in", Arrays.asList(
        new BaseColumn("vt5", "ps_partkey"),
        SubqueryColumn.getSubqueryColumn(subsubquery2)
        )));
    subquery.addFilterByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt5", "ps_availqty"),
        new BaseColumn("agg_lineitem", "agg_quantity")
        )));
    expected.addFilterByAnd(new ColumnOp("in", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "s_suppkey"),
        SubqueryColumn.getSubqueryColumn(subquery)
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "s_nationkey"),
        new BaseColumn("vt2", "n_nationkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "n_name"),
        ConstantColumn.valueOf("':3'")
        )));
    expected.addOrderby(new OrderbyAttribute("vc3"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "s_name, " +
        "s_address " +
        "from " +
        "supplier, " +
        "nation " +
        "where " +
        "s_suppkey in ( " +
        "select " +
        "ps_suppkey " +
        "from " +
        "partsupp, " +
        "( " +
        "select " +
        "l_partkey agg_partkey, " +
        "l_suppkey agg_suppkey, " +
        "0.5 * sum(l_quantity) AS agg_quantity " +
        "from " +
        "lineitem " +
        "where " +
        "l_shipdate >= date ':2' " +
        "and l_shipdate < date ':2' + interval '1' year " +
        "group by " +
        "l_partkey, " +
        "l_suppkey " +
        ") agg_lineitem " +
        "where " +
        "agg_partkey = ps_partkey " +
        "and agg_suppkey = ps_suppkey " +
        "and ps_partkey in ( " +
        "select " +
        "p_partkey " +
        "from " +
        "part " +
        "where " +
        "p_name like ':1%' " +
        ") " +
        "and ps_availqty > agg_quantity " +
        ") " +
        "and s_nationkey = n_nationkey " +
        "and n_name = ':3' " +
        "order by " +
        "s_name " +
        "LIMIT 1; ";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query21Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "vt1");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l1");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt2");
    AbstractRelation nation = new BaseTable("tpch", "nation", "vt3");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("vt1", "s_name"), "vc4"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "numwait")
            ),
        Arrays.asList(supplier, lineitem, orders, nation));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "s_suppkey"),
        new BaseColumn("l1", "l_suppkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "o_orderkey"),
        new BaseColumn("l1", "l_orderkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt2", "o_orderstatus"),
        ConstantColumn.valueOf("'F'")
        )));
    expected.addFilterByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l1", "l_receiptdate"),
        new BaseColumn("l1", "l_commitdate")
        )));
    SelectQuery subquery1 = SelectQuery.create(Arrays.<SelectItem>asList(
        new AsteriskColumn()
        ), new BaseTable("tpch", "lineitem", "l2"));
    subquery1.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l2", "l_orderkey"),
        new BaseColumn("l1", "l_orderkey")
        )));
    subquery1.addFilterByAnd(new ColumnOp("notequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l2", "l_suppkey"),
        new BaseColumn("l1", "l_suppkey")
        )));
    expected.addFilterByAnd(new ColumnOp("exists", SubqueryColumn.getSubqueryColumn(subquery1)));
    SelectQuery subquery2 = SelectQuery.create(Arrays.<SelectItem>asList(
        new AsteriskColumn()
        ), new BaseTable("tpch", "lineitem", "l3"));
    subquery2.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l3", "l_orderkey"),
        new BaseColumn("l1", "l_orderkey")
        )));
    subquery2.addFilterByAnd(new ColumnOp("notequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l3", "l_suppkey"),
        new BaseColumn("l1", "l_suppkey")
        )));
    subquery2.addFilterByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l3", "l_receiptdate"),
        new BaseColumn("l3", "l_commitdate")
        )));
    expected.addFilterByAnd(new ColumnOp("notexists", SubqueryColumn.getSubqueryColumn(subquery2)));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt1", "s_nationkey"),
        new BaseColumn("vt3", "n_nationkey")
        )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "n_name"),
        ConstantColumn.valueOf("':1'")
        )));
    expected.addGroupby(new AliasReference("vc4"));
    expected.addOrderby(new OrderbyAttribute("numwait", "desc"));
    expected.addOrderby(new OrderbyAttribute("vc4"));
    expected.addLimit(ConstantColumn.valueOf(100));
    String sql = "select " +
        "s_name, " +
        "count(*) as numwait " +
        "from " +
        "supplier, " +
        "lineitem l1, " +
        "orders, " +
        "nation " +
        "where " +
        "s_suppkey = l1.l_suppkey " +
        "and o_orderkey = l1.l_orderkey " +
        "and o_orderstatus = 'F' " +
        "and l1.l_receiptdate > l1.l_commitdate " +
        "and exists ( " +
        "select " +
        "* " +
        "from " +
        "lineitem l2 " +
        "where " +
        "l2.l_orderkey = l1.l_orderkey " +
        "and l2.l_suppkey <> l1.l_suppkey " +
        ") " +
        "and not exists ( " +
        "select " +
        "* " +
        "from " +
        "lineitem l3 " +
        "where " +
        "l3.l_orderkey = l1.l_orderkey " +
        "and l3.l_suppkey <> l1.l_suppkey " +
        "and l3.l_receiptdate > l3.l_commitdate " +
        ") " +
        "and s_nationkey = n_nationkey " +
        "and n_name = ':1' " +
        "group by " +
        "s_name " +
        "order by " +
        "numwait desc, " +
        "s_name " +
        "LIMIT 100;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }

  @Test
  public void Query22Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("substr", Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "c_phone"),
                ConstantColumn.valueOf(1), ConstantColumn.valueOf(2))), "cntrycode"),
            new AliasedColumn(new BaseColumn("vt1", "c_acctbal"), "vc2")
            ),
        new BaseTable("tpch", "customer", "vt1"));
    subquery.addFilterByAnd(new ColumnOp("in", Arrays.<UnnamedColumn>asList(
        new ColumnOp("substr", Arrays.<UnnamedColumn>asList(
            new BaseColumn("vt1", "c_phone"),
            ConstantColumn.valueOf(1), ConstantColumn.valueOf(2))),
        ConstantColumn.valueOf("':1'"), ConstantColumn.valueOf("':2'"), ConstantColumn.valueOf("':3'"),
        ConstantColumn.valueOf("':4'"), ConstantColumn.valueOf("':5'"), ConstantColumn.valueOf("':6'"),
        ConstantColumn.valueOf("':7'")
        )));
    SelectQuery subsubquery1 = SelectQuery.create(
        Arrays.<SelectItem>asList(new AliasedColumn(new ColumnOp("avg", new BaseColumn("vt4", "c_acctbal")), "a5")),
        new BaseTable("tpch", "customer", "vt4"));
    subsubquery1.addFilterByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt4", "c_acctbal"),
        ConstantColumn.valueOf("0.00")
        )));
    subsubquery1.addFilterByAnd(new ColumnOp("in", Arrays.<UnnamedColumn>asList(
        new ColumnOp("substr", Arrays.<UnnamedColumn>asList(
            new BaseColumn("vt4", "c_phone"),
            ConstantColumn.valueOf(1), ConstantColumn.valueOf(2))),
        ConstantColumn.valueOf("':1'"), ConstantColumn.valueOf("':2'"), ConstantColumn.valueOf("':3'"),
        ConstantColumn.valueOf("':4'"), ConstantColumn.valueOf("':5'"), ConstantColumn.valueOf("':6'"),
        ConstantColumn.valueOf("':7'")
        )));
    subquery.addFilterByAnd(new ColumnOp("greater", Arrays.asList(
        new BaseColumn("vt1", "c_acctbal"), SubqueryColumn.getSubqueryColumn(subsubquery1)
        )));
    SelectQuery subsubquery2 = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        new BaseTable("tpch", "orders", "vt3"));
    subsubquery2.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("vt3", "o_custkey"),
        new BaseColumn("vt1", "c_custkey")
        )));
    subquery.addFilterByAnd(new ColumnOp("notexists", SubqueryColumn.getSubqueryColumn(subsubquery2)));
    subquery.setAliasName("custsale");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("custsale", "cntrycode"), "vc6"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "numcust"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("custsale", "vc2")), "totacctbal")
            ),
        subquery);
    expected.addGroupby(new AliasReference("vc6"));
    expected.addOrderby(new OrderbyAttribute("vc6"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "cntrycode, " +
        "count(*) as numcust, " +
        "sum(c_acctbal) as totacctbal " +
        "from " +
        "( " +
        "select " +
        "substr(c_phone,1,2) as cntrycode, " +
        "c_acctbal " +
        "from " +
        "customer " +
        "where " +
        "substr(c_phone,1,2) in " +
        "(':1', ':2', ':3', ':4', ':5', ':6', ':7') " +
        "and c_acctbal > ( " +
        "select " +
        "avg(c_acctbal) " +
        "from " +
        "customer " +
        "where " +
        "c_acctbal > 0.00 " +
        "and substr(c_phone,1,2) in " +
        "(':1', ':2', ':3', ':4', ':5', ':6', ':7') " +
        ") " +
        "and not exists ( " +
        "select " +
        "* " +
        "from " +
        "orders " +
        "where " +
        "o_custkey = c_custkey " +
        ") " +
        ") as custsale " +
        "group by " +
        "cntrycode " +
        "order by " +
        "cntrycode " +
        "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(meta);
    relation = gen.standardize((SelectQuery) relation);
    assertEquals(expected, relation);
  }
}

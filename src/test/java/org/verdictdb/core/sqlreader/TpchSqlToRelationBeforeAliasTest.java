package org.verdictdb.core.sqlreader;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.GroupingAttribute;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.OrderbyAttribute;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;

public class TpchSqlToRelationBeforeAliasTest {

  @Test
  public void Query1Test() throws VerdictDBException {
    BaseTable base = new BaseTable("tpch", "lineitem", "t");
    List<UnnamedColumn> operand1 = Arrays.<UnnamedColumn>asList(
        ConstantColumn.valueOf(1),
        new BaseColumn("t", "l_discount"));
    List<UnnamedColumn> operand2 = Arrays.<UnnamedColumn>asList(
        new BaseColumn("t", "l_extendedprice"),
        new ColumnOp("subtract", operand1));
    List<UnnamedColumn> operand3 = Arrays.<UnnamedColumn>asList(
        ConstantColumn.valueOf(1),
        new BaseColumn("t", "l_tax"));
    List<UnnamedColumn> operand4 = Arrays.<UnnamedColumn>asList(
        new ColumnOp("multiply", operand2),
        new ColumnOp("add", operand3));
    List<UnnamedColumn> operand5 = Arrays.<UnnamedColumn>asList(
        new BaseColumn("t", "l_shipdate"),
        new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("'1998-12-01'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':1'"), ConstantColumn.valueOf("day")))
        )));
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("t", "l_returnflag"),
            new BaseColumn("t", "l_linestatus"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("t", "l_quantity")), "sum_qty"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("t", "l_extendedprice")), "sum_base_price"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply", operand2)), "sum_disc_price"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply", operand4)), "sum_charge"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "l_quantity")), "avg_qty"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "l_extendedprice")), "avg_price"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "l_discount")), "avg_disc"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "count_order")
        ),
        base, new ColumnOp("lessequal", operand5));
    expected.addGroupby(Arrays.<GroupingAttribute>asList(new AliasReference("l_returnflag"),
        new AliasReference("l_linestatus")));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(new OrderbyAttribute("l_returnflag"),
        new OrderbyAttribute("l_linestatus")));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select t.l_returnflag, " +
        "t.l_linestatus, " +
        "sum(t.l_quantity) as sum_qty, " +
        "sum(t.l_extendedprice) as sum_base_price, " +
        "sum(t.l_extendedprice * (1 - t.l_discount)) as sum_disc_price, " +
        "sum((t.l_extendedprice * (1 - t.l_discount)) * (1 + t.l_tax)) as sum_charge, " +
        "avg(t.l_quantity) as avg_qty, " +
        "avg(t.l_extendedprice) as avg_price, " +
        "avg(t.l_discount) as avg_disc, " +
        "count(*) as count_order " +
        "from " +
        "tpch.lineitem as t " +
        "where " +
        "t.l_shipdate <= ((date '1998-12-01' ) - (interval ':1' day)) " +
        "group by " +
        "l_returnflag, " +
        "l_linestatus " +
        "order by " +
        "l_returnflag asc, " +
        "l_linestatus asc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query2Test() throws VerdictDBException {
    BaseTable part = new BaseTable("tpch", "part", "p");
    BaseTable supplier = new BaseTable("tpch", "supplier", "s");
    BaseTable partsupp = new BaseTable("tpch", "partsupp", "ps");
    BaseTable nation = new BaseTable("tpch", "nation", "n");
    BaseTable region = new BaseTable("tpch", "region", "r");
    List<AbstractRelation> from = Arrays.<AbstractRelation>asList(part, supplier, partsupp, nation, region);
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("s", "s_acctbal"),
            new BaseColumn("s", "s_name"),
            new BaseColumn("n", "n_name"),
            new BaseColumn("p", "p_partkey"),
            new BaseColumn("p", "p_mfgr"),
            new BaseColumn("s", "s_address"),
            new BaseColumn("s", "s_phone"),
            new BaseColumn("s", "s_comment")),
        from);
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_partkey"),
        new BaseColumn("ps", "ps_partkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_suppkey"),
        new BaseColumn("ps", "ps_suppkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_size"),
        ConstantColumn.valueOf("':1'")
    )));
    expected.addFilterByAnd(new ColumnOp("like", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_type"),
        ConstantColumn.valueOf("'%:2'")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_nationkey"),
        new BaseColumn("n", "n_nationkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("n", "n_regionkey"),
        new BaseColumn("r", "r_regionkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("r", "r_name"),
        ConstantColumn.valueOf("':3'")
    )));
    List<AbstractRelation> subqueryFrom = Arrays.<AbstractRelation>asList(partsupp, supplier, nation, region);
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(new ColumnOp("min", new BaseColumn("ps", "ps_supplycost"))),
        subqueryFrom);
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_partkey"),
        new BaseColumn("ps", "ps_partkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_suppkey"),
        new BaseColumn("ps", "ps_suppkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_nationkey"),
        new BaseColumn("n", "n_nationkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("n", "n_regionkey"),
        new BaseColumn("r", "r_regionkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("r", "r_name"),
        ConstantColumn.valueOf("':3'")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.asList(
        new BaseColumn("ps", "ps_supplycost"),
        SubqueryColumn.getSubqueryColumn(subquery)
    )));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("s_acctbal", "desc"),
        new OrderbyAttribute("n_name"),
        new OrderbyAttribute("s_name"),
        new OrderbyAttribute("p_partkey")
    ));
    expected.addLimit(ConstantColumn.valueOf(100));
    String sql = "select " +
        "s.s_acctbal, " +
        "s.s_name, " +
        "n.n_name, " +
        "p.p_partkey, " +
        "p.p_mfgr, " +
        "s.s_address, " +
        "s.s_phone, " +
        "s.s_comment " +
        "from " +
        "tpch.part as p, " +
        "tpch.supplier as s, " +
        "tpch.partsupp as ps, " +
        "tpch.nation as n, " +
        "tpch.region as r " +
        "where " +
        "(((((((p.p_partkey = ps.ps_partkey) " +
        "and (s.s_suppkey = ps.ps_suppkey)) " +
        "and (p.p_size = ':1')) " +
        "and (p.p_type like '%:2')) " +
        "and (s.s_nationkey = n.n_nationkey)) " +
        "and (n.n_regionkey = r.r_regionkey)) " +
        "and (r.r_name = ':3')) " +
        "and (ps.ps_supplycost = (" +
        "select " +
        "min(ps.ps_supplycost) " +
        "from " +
        "tpch.partsupp as ps, " +
        "tpch.supplier as s, " +
        "tpch.nation as n, " +
        "tpch.region as r " +
        "where " +
        "((((p.p_partkey = ps.ps_partkey) " +
        "and (s.s_suppkey = ps.ps_suppkey)) " +
        "and (s.s_nationkey = n.n_nationkey)) " +
        "and (n.n_regionkey = r.r_regionkey)) " +
        "and (r.r_name = ':3'))) " +
        "order by " +
        "s_acctbal desc, " +
        "n_name asc, " +
        "s_name asc, " +
        "p_partkey asc " +
        "limit 100";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query3Test() throws VerdictDBException {
    String sql = "select " +
        "l.l_orderkey, " +
        "sum(l.l_extendedprice * (1 - l.l_discount)) as revenue, " +
        "o.o_orderdate, " +
        "o.o_shippriority " +
        "from " +
        "tpch.customer as c, " +
        "tpch.orders as o, " +
        "tpch.lineitem as l " +
        "where " +
        "((((c.c_mktsegment = ':1') " +
        "and (c.c_custkey = o.o_custkey)) " +
        "and (l.l_orderkey = o.o_orderkey)) " +
        "and (o.o_orderdate < (date ':2'))) " +
        "and (l.l_shipdate > (date ':2')) " +
        "group by " +
        "l_orderkey, " +
        "o_orderdate, " +
        "o_shippriority " +
        "order by " +
        "revenue desc, " +
        "o_orderdate asc " +
        "limit 10";
    AbstractRelation customer = new BaseTable("tpch", "customer", "c");
    AbstractRelation orders = new BaseTable("tpch", "orders", "o");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    ColumnOp op1 = new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_extendedprice"),
        new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
            ConstantColumn.valueOf(1),
            new BaseColumn("l", "l_discount")
        ))
    ));
    SelectQuery expected = SelectQuery.create(
        Arrays.asList(
            new BaseColumn("l", "l_orderkey"),
            new AliasedColumn(new ColumnOp("sum", op1), "revenue"),
            new BaseColumn("o", "o_orderdate"),
            new BaseColumn("o", "o_shippriority")
        ),
        Arrays.asList(customer, orders, lineitem));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("c", "c_mktsegment"),
        ConstantColumn.valueOf("':1'")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("c", "c_custkey"),
        new BaseColumn("o", "o_custkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_orderkey"),
        new BaseColumn("o", "o_orderkey")
    )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':2'"))
    )));
    expected.addFilterByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_shipdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':2'"))
    )));
    expected.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("l_orderkey"),
        new AliasReference("o_orderdate"),
        new AliasReference("o_shippriority")
    ));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("revenue", "desc"),
        new OrderbyAttribute("o_orderdate")
    ));
    expected.addLimit(ConstantColumn.valueOf(10));
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query4Test() throws VerdictDBException {
    AbstractRelation orders = new BaseTable("tpch", "orders", "o");
    SelectQuery expected = SelectQuery.create(
        Arrays.asList(
            new BaseColumn("o", "o_orderpriority"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "order_count")
        ),
        orders);
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':1'"))
    )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':1'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'3'"), ConstantColumn.valueOf("month")))
        ))
    )));
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        new BaseTable("tpch", "lineitem", "l"));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_orderkey"),
        new BaseColumn("o", "o_orderkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_commitdate"),
        new BaseColumn("l", "l_receiptdate")
    )));
    expected.addFilterByAnd(new ColumnOp("exists", SubqueryColumn.getSubqueryColumn(subquery)));
    expected.addGroupby(new AliasReference("o_orderpriority"));
    expected.addOrderby(new OrderbyAttribute("o_orderpriority"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "o.o_orderpriority, " +
        "count(*) as order_count " +
        "from " +
        "tpch.orders as o " +
        "where " +
        "((o.o_orderdate >= (date ':1')) " +
        "and (o.o_orderdate < ((date ':1') + (interval '3' month)))) " +
        "and (exists (" +
        "select " +
        "* " +
        "from " +
        "tpch.lineitem as l " +
        "where " +
        "(l.l_orderkey = o.o_orderkey) " +
        "and (l.l_commitdate < l.l_receiptdate)" +
        ")) " +
        "group by " +
        "o_orderpriority " +
        "order by " +
        "o_orderpriority asc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query5Test() throws VerdictDBException {
    AbstractRelation customer = new BaseTable("tpch", "customer", "c");
    AbstractRelation orders = new BaseTable("tpch", "orders", "o");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "s");
    AbstractRelation nation = new BaseTable("tpch", "nation", "n");
    AbstractRelation region = new BaseTable("tpch", "region", "r");
    SelectQuery expected = SelectQuery.create(
        Arrays.asList(
            new BaseColumn("n", "n_name"),
            new AliasedColumn(new ColumnOp("sum", Arrays.<UnnamedColumn>asList(
                new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("l", "l_extendedprice"),
                    new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
                        ConstantColumn.valueOf(1),
                        new BaseColumn("l", "l_discount")
                    ))
                ))
            )), "revenue")
        ),
        Arrays.asList(customer, orders, lineitem, supplier, nation, region));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("c", "c_custkey"),
        new BaseColumn("o", "o_custkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_orderkey"),
        new BaseColumn("o", "o_orderkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_suppkey"),
        new BaseColumn("s", "s_suppkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("c", "c_nationkey"),
        new BaseColumn("s", "s_nationkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_nationkey"),
        new BaseColumn("n", "n_nationkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("n", "n_regionkey"),
        new BaseColumn("r", "r_regionkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("r", "r_name"),
        ConstantColumn.valueOf("':1'")
    )));
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':2'"))
    )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':2'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'1'"), ConstantColumn.valueOf("year")))
        ))
    )));
    expected.addGroupby(new AliasReference("n_name"));
    expected.addOrderby(new OrderbyAttribute("revenue", "desc"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "n.n_name, " +
        "sum(l.l_extendedprice * (1 - l.l_discount)) as revenue " +
        "from " +
        "tpch.customer as c, " +
        "tpch.orders as o, " +
        "tpch.lineitem as l, " +
        "tpch.supplier as s, " +
        "tpch.nation as n, " +
        "tpch.region as r " +
        "where " +
        "((((((((c.c_custkey = o.o_custkey) " +
        "and (l.l_orderkey = o.o_orderkey)) " +
        "and (l.l_suppkey = s.s_suppkey)) " +
        "and (c.c_nationkey = s.s_nationkey)) " +
        "and (s.s_nationkey = n.n_nationkey)) " +
        "and (n.n_regionkey = r.r_regionkey)) " +
        "and (r.r_name = ':1')) " +
        "and (o.o_orderdate >= (date ':2'))) " +
        "and (o.o_orderdate < ((date ':2') + (interval '1' year))) " +
        "group by " +
        "n_name " +
        "order by " +
        "revenue desc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query6Test() throws VerdictDBException {
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply",
                Arrays.<UnnamedColumn>asList(
                    new BaseColumn("l", "l_extendedprice"),
                    new BaseColumn("l", "l_discount")
                ))), "revenue")
        ),
        lineitem);
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_shipdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':1'"))
    )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_shipdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':1'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(
                ConstantColumn.valueOf("'1'"),
                ConstantColumn.valueOf("year")
            ))
        ))
    )));
    expected.addFilterByAnd(new ColumnOp("between", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_discount"),
        new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':2'"), ConstantColumn.valueOf("0.01"))),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':2'"), ConstantColumn.valueOf("0.01")))
    )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_quantity"),
        ConstantColumn.valueOf("':3'"))
    ));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "sum(l.l_extendedprice * l.l_discount) as revenue " +
        "from " +
        "tpch.lineitem as l " +
        "where " +
        "(((l.l_shipdate >= (date ':1')) " +
        "and (l.l_shipdate < ((date ':1') + (interval '1' year)))) " +
        "and (l.l_discount between (':2' - 0.01) and (':2' + 0.01))) " +
        "and (l.l_quantity < ':3') " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query7Test() throws VerdictDBException {
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "s");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    AbstractRelation orders = new BaseTable("tpch", "orders", "o");
    AbstractRelation customer = new BaseTable("tpch", "customer", "c");
    AbstractRelation nation1 = new BaseTable("tpch", "nation", "n1");
    AbstractRelation nation2 = new BaseTable("tpch", "nation", "n2");
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("n1", "n_name"), "supp_nation"),
            new AliasedColumn(new BaseColumn("n2", "n_name"), "cust_nation"),
            new AliasedColumn(new ColumnOp("substr", Arrays.<UnnamedColumn>asList(
                new BaseColumn("l", "l_shipdate"), ConstantColumn.valueOf(0), ConstantColumn.valueOf(4))), "l_year"),
            new AliasedColumn(new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new BaseColumn("l", "l_extendedprice"),
                new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
                    ConstantColumn.valueOf(1), new BaseColumn("l", "l_discount")))
            )), "volume")
        ),
        Arrays.asList(supplier, lineitem, orders, customer, nation1, nation2));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_suppkey"),
        new BaseColumn("l", "l_suppkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderkey"),
        new BaseColumn("l", "l_orderkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("c", "c_custkey"),
        new BaseColumn("o", "o_custkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_nationkey"),
        new BaseColumn("n1", "n_nationkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("c", "c_nationkey"),
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
        new BaseColumn("l", "l_shipdate"),
        new ColumnOp("date", ConstantColumn.valueOf("'1995-01-01'")),
        new ColumnOp("date", ConstantColumn.valueOf("'1996-12-31'")))
    ));
    subquery.setAliasName("shipping");
    SelectQuery expected = SelectQuery.create(
        Arrays.asList(
            new BaseColumn("shipping", "supp_nation"),
            new BaseColumn("shipping", "cust_nation"),
            new BaseColumn("shipping", "l_year"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("shipping", "volume")), "revenue")
        ),
        subquery);
    expected.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("supp_nation"),
        new AliasReference("cust_nation"),
        new AliasReference("l_year")
    ));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("supp_nation"),
        new OrderbyAttribute("cust_nation"),
        new OrderbyAttribute("l_year")
    ));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "shipping.supp_nation, " +
        "shipping.cust_nation, " +
        "shipping.l_year, " +
        "sum(shipping.volume) as revenue " +
        "from " +
        "(" +
        "select " +
        "n1.n_name as supp_nation, " +
        "n2.n_name as cust_nation, " +
        "substr(l.l_shipdate, 0, 4) as l_year, " +
        "l.l_extendedprice * (1 - l.l_discount) as volume " +
        "from " +
        "tpch.supplier as s, " +
        "tpch.lineitem as l, " +
        "tpch.orders as o, " +
        "tpch.customer as c, " +
        "tpch.nation as n1, " +
        "tpch.nation as n2 " +
        "where " +
        "((((((s.s_suppkey = l.l_suppkey) " +
        "and (o.o_orderkey = l.l_orderkey)) " +
        "and (c.c_custkey = o.o_custkey)) " +
        "and (s.s_nationkey = n1.n_nationkey)) " +
        "and (c.c_nationkey = n2.n_nationkey)) " +
        "and (((" +
        "n1.n_name = ':1') and (n2.n_name = ':2')) " +
        "or ((n1.n_name = ':2') and (n2.n_name = ':1')))" +
        ") " +
        "and (l.l_shipdate between (date '1995-01-01') and (date '1996-12-31'))" +
        ") as shipping " +
        "group by " +
        "supp_nation, " +
        "cust_nation, " +
        "l_year " +
        "order by " +
        "supp_nation asc, " +
        "cust_nation asc, " +
        "l_year asc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query8Test() throws VerdictDBException {
    AbstractRelation part = new BaseTable("tpch", "part", "p");
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "s");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    AbstractRelation orders = new BaseTable("tpch", "orders", "o");
    AbstractRelation customer = new BaseTable("tpch", "customer", "c");
    AbstractRelation nation1 = new BaseTable("tpch", "nation", "n1");
    AbstractRelation nation2 = new BaseTable("tpch", "nation", "n2");
    AbstractRelation region = new BaseTable("tpch", "region", "r");
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("substr", Arrays.<UnnamedColumn>asList(new BaseColumn("o", "o_orderdate"), ConstantColumn.valueOf(0), ConstantColumn.valueOf(4))), "o_year"),
            new AliasedColumn(new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new BaseColumn("l", "l_extendedprice"),
                new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf(1), new BaseColumn("l", "l_discount")))
            )), "volume"),
            new AliasedColumn(new BaseColumn("n2", "n_name"), "nation")
        ),
        Arrays.asList(part, supplier, lineitem, orders, customer, nation1, nation2, region));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_partkey"),
        new BaseColumn("l", "l_partkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_suppkey"),
        new BaseColumn("l", "l_suppkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_orderkey"),
        new BaseColumn("o", "o_orderkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_custkey"),
        new BaseColumn("c", "c_custkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("c", "c_nationkey"),
        new BaseColumn("n1", "n_nationkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("n1", "n_regionkey"),
        new BaseColumn("r", "r_regionkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("r", "r_name"),
        ConstantColumn.valueOf("':2'")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_nationkey"),
        new BaseColumn("n2", "n_nationkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("between", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderdate"),
        new ColumnOp("date", ConstantColumn.valueOf("'1995-01-01'")),
        new ColumnOp("date", ConstantColumn.valueOf("'1996-12-31'"))
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_type"),
        ConstantColumn.valueOf("':3'")
    )));
    subquery.setAliasName("all_nations");
    SelectQuery expected = SelectQuery.create(
        Arrays.asList(
            new BaseColumn("all_nations", "o_year"),
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
    expected.addGroupby(new AliasReference("o_year"));
    expected.addOrderby(new OrderbyAttribute("o_year"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "all_nations.o_year, " +
        "sum(case " +
        "when (all_nations.nation = ':1') then all_nations.volume " +
        "else 0 " +
        "end) / sum(all_nations.volume) as mkt_share " +
        "from " +
        "(" +
        "select " +
        "substr(o.o_orderdate, 0, 4) as o_year, " +
        "l.l_extendedprice * (1 - l.l_discount) as volume, " +
        "n2.n_name as nation " +
        "from " +
        "tpch.part as p, " +
        "tpch.supplier as s, " +
        "tpch.lineitem as l, " +
        "tpch.orders as o, " +
        "tpch.customer as c, " +
        "tpch.nation as n1, " +
        "tpch.nation as n2, " +
        "tpch.region as r " +
        "where " +
        "(((((((((p.p_partkey = l.l_partkey) " +
        "and (s.s_suppkey = l.l_suppkey)) " +
        "and (l.l_orderkey = o.o_orderkey)) " +
        "and (o.o_custkey = c.c_custkey)) " +
        "and (c.c_nationkey = n1.n_nationkey)) " +
        "and (n1.n_regionkey = r.r_regionkey)) " +
        "and (r.r_name = ':2')) " +
        "and (s.s_nationkey = n2.n_nationkey)) " +
        "and (o.o_orderdate between (date '1995-01-01') and (date '1996-12-31'))) " +
        "and (p.p_type = ':3')" +
        ") as all_nations " +
        "group by " +
        "o_year " +
        "order by " +
        "o_year asc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query9Test() throws VerdictDBException {
    AbstractRelation part = new BaseTable("tpch", "part", "p");
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "s");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    AbstractRelation partsupp = new BaseTable("tpch", "partsupp", "ps");
    AbstractRelation orders = new BaseTable("tpch", "orders", "o");
    AbstractRelation nation = new BaseTable("tpch", "nation", "n");
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("n", "n_name"), "nation"),
            new AliasedColumn(new ColumnOp("substr", Arrays.<UnnamedColumn>asList(new BaseColumn("o", "o_orderdate"), ConstantColumn.valueOf(0), ConstantColumn.valueOf(4))), "o_year"),
            new AliasedColumn(new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
                new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("l", "l_extendedprice"),
                    new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf(1), new BaseColumn("l", "l_discount")))
                )),
                new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("ps", "ps_supplycost"),
                    new BaseColumn("l", "l_quantity")
                ))
            )), "amount")
        ),
        Arrays.asList(part, supplier, lineitem, partsupp, orders, nation));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_suppkey"),
        new BaseColumn("l", "l_suppkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("ps", "ps_suppkey"),
        new BaseColumn("l", "l_suppkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("ps", "ps_partkey"),
        new BaseColumn("l", "l_partkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_partkey"),
        new BaseColumn("l", "l_partkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderkey"),
        new BaseColumn("l", "l_orderkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_nationkey"),
        new BaseColumn("n", "n_nationkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("like", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_name"),
        ConstantColumn.valueOf("'%:1%'")
    )));
    subquery.setAliasName("profit");
    SelectQuery expected = SelectQuery.create(
        Arrays.asList(
            new BaseColumn("profit", "nation"),
            new BaseColumn("profit", "o_year"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("profit", "amount")), "sum_profit")
        ),
        subquery);
    expected.addGroupby(Arrays.<GroupingAttribute>asList(new AliasReference("nation"), new AliasReference("o_year")));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(new OrderbyAttribute("nation"),
        new OrderbyAttribute("o_year", "desc")));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "profit.nation, " +
        "profit.o_year, " +
        "sum(profit.amount) as sum_profit " +
        "from " +
        "(" +
        "select " +
        "n.n_name as nation, " +
        "substr(o.o_orderdate, 0, 4) as o_year, " +
        "(l.l_extendedprice * (1 - l.l_discount)) - (ps.ps_supplycost * l.l_quantity) as amount " +
        "from " +
        "tpch.part as p, " +
        "tpch.supplier as s, " +
        "tpch.lineitem as l, " +
        "tpch.partsupp as ps, " +
        "tpch.orders as o, " +
        "tpch.nation as n " +
        "where " +
        "((((((s.s_suppkey = l.l_suppkey) " +
        "and (ps.ps_suppkey = l.l_suppkey)) " +
        "and (ps.ps_partkey = l.l_partkey)) " +
        "and (p.p_partkey = l.l_partkey)) " +
        "and (o.o_orderkey = l.l_orderkey)) " +
        "and (s.s_nationkey = n.n_nationkey)) " +
        "and (p.p_name like '%:1%')" +
        ") as profit " +
        "group by " +
        "nation, " +
        "o_year " +
        "order by " +
        "nation asc, " +
        "o_year desc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query10Test() throws VerdictDBException {
    AbstractRelation customer = new BaseTable("tpch", "customer", "c");
    AbstractRelation orders = new BaseTable("tpch", "orders", "o");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    AbstractRelation nation = new BaseTable("tpch", "nation", "n");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("c", "c_custkey"),
            new BaseColumn("c", "c_name"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new BaseColumn("l", "l_extendedprice"),
                new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
                    ConstantColumn.valueOf(1),
                    new BaseColumn("l", "l_discount")
                ))
            ))), "revenue"),
            new BaseColumn("c", "c_acctbal"),
            new BaseColumn("n", "n_name"),
            new BaseColumn("c", "c_address"),
            new BaseColumn("c", "c_phone"),
            new BaseColumn("c", "c_comment")
        ),
        Arrays.asList(customer, orders, lineitem, nation));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("c", "c_custkey"),
        new BaseColumn("o", "o_custkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_orderkey"),
        new BaseColumn("o", "o_orderkey")
    )));
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':1'"))
    )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':1'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'3'"), ConstantColumn.valueOf("month")))
        )
        ))));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_returnflag"),
        ConstantColumn.valueOf("'R'")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("c", "c_nationkey"),
        new BaseColumn("n", "n_nationkey")
    )));
    expected.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("c_custkey"),
        new AliasReference("c_name"),
        new AliasReference("c_acctbal"),
        new AliasReference("c_phone"),
        new AliasReference("n_name"),
        new AliasReference("c_address"),
        new AliasReference("c_comment")
    ));
    expected.addOrderby(new OrderbyAttribute("revenue", "desc"));
    expected.addLimit(ConstantColumn.valueOf(20));
    String sql = "select " +
        "c.c_custkey, " +
        "c.c_name, " +
        "sum(l.l_extendedprice * (1 - l.l_discount)) as revenue, " +
        "c.c_acctbal, " +
        "n.n_name, " +
        "c.c_address, " +
        "c.c_phone, " +
        "c.c_comment " +
        "from " +
        "tpch.customer as c, " +
        "tpch.orders as o, " +
        "tpch.lineitem as l, " +
        "tpch.nation as n " +
        "where " +
        "(((((c.c_custkey = o.o_custkey) " +
        "and (l.l_orderkey = o.o_orderkey)) " +
        "and (o.o_orderdate >= (date ':1'))) " +
        "and (o.o_orderdate < ((date ':1') + (interval '3' month)))) " +
        "and (l.l_returnflag = 'R')) " +
        "and (c.c_nationkey = n.n_nationkey) " +
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
        "limit 20";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query11Test() throws VerdictDBException {
    AbstractRelation partsupp = new BaseTable("tpch", "partsupp", "ps");
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "s");
    AbstractRelation nation = new BaseTable("tpch", "nation", "n");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("ps", "ps_partkey"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new BaseColumn("ps", "ps_supplycost"),
                new BaseColumn("ps", "ps_availqty")
            ))), "value")
        ),
        Arrays.asList(partsupp, supplier, nation));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("ps", "ps_suppkey"),
        new BaseColumn("s", "s_suppkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_nationkey"),
        new BaseColumn("n", "n_nationkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("n", "n_name"),
        ConstantColumn.valueOf("':1'")
    )));
    expected.addGroupby(new AliasReference("ps_partkey"));
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("ps", "ps_supplycost"),
                    new BaseColumn("ps", "ps_availqty")
                ))),
                ConstantColumn.valueOf("':2'")
            ))
        ), Arrays.asList(partsupp, supplier, nation));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("ps", "ps_suppkey"),
        new BaseColumn("s", "s_suppkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_nationkey"),
        new BaseColumn("n", "n_nationkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("n", "n_name"),
        ConstantColumn.valueOf("':1'")
    )));
    expected.addHavingByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
            new BaseColumn("ps", "ps_supplycost"),
            new BaseColumn("ps", "ps_availqty")
        ))),
        SubqueryColumn.getSubqueryColumn(subquery)
    )));
    expected.addOrderby(new OrderbyAttribute("value", "desc"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "ps.ps_partkey, " +
        "sum(ps.ps_supplycost * ps.ps_availqty) as value " +
        "from " +
        "tpch.partsupp as ps, " +
        "tpch.supplier as s, " +
        "tpch.nation as n " +
        "where " +
        "((ps.ps_suppkey = s.s_suppkey) " +
        "and (s.s_nationkey = n.n_nationkey)) " +
        "and (n.n_name = ':1') " +
        "group by " +
        "ps_partkey having " +
        "sum(ps.ps_supplycost * ps.ps_availqty) > (" +
        "select " +
        "sum(ps.ps_supplycost * ps.ps_availqty) * ':2' " +
        "from " +
        "tpch.partsupp as ps, " +
        "tpch.supplier as s, " +
        "tpch.nation as n " +
        "where " +
        "((ps.ps_suppkey = s.s_suppkey) " +
        "and (s.s_nationkey = n.n_nationkey)) " +
        "and (n.n_name = ':1')" +
        ") " +
        "order by " +
        "value desc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query12Test() throws VerdictDBException {
    AbstractRelation orders = new BaseTable("tpch", "orders", "o");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("l", "l_shipmode"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("whenthenelse", Arrays.<UnnamedColumn>asList(
                new ColumnOp("or", Arrays.<UnnamedColumn>asList(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("o", "o_orderpriority"),
                    ConstantColumn.valueOf("'1-URGENT'")
                    )),
                    new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                        new BaseColumn("o", "o_orderpriority"),
                        ConstantColumn.valueOf("'2-HIGH'")
                    ))
                )),
                ConstantColumn.valueOf(1),
                ConstantColumn.valueOf(0)
            ))), "high_line_count"),
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("whenthenelse", Arrays.<UnnamedColumn>asList(
                new ColumnOp("and", Arrays.<UnnamedColumn>asList(new ColumnOp("notequal", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("o", "o_orderpriority"),
                    ConstantColumn.valueOf("'1-URGENT'")
                    )),
                    new ColumnOp("notequal", Arrays.<UnnamedColumn>asList(
                        new BaseColumn("o", "o_orderpriority"),
                        ConstantColumn.valueOf("'2-HIGH'")
                    ))
                )),
                ConstantColumn.valueOf(1),
                ConstantColumn.valueOf(0)
            ))), "low_line_count")
        ),
        Arrays.asList(orders, lineitem));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderkey"),
        new BaseColumn("l", "l_orderkey")
    )));
    expected.addFilterByAnd(new ColumnOp("in", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_shipmode"),
        ConstantColumn.valueOf("':1'"),
        ConstantColumn.valueOf("':2'")
    )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_commitdate"),
        new BaseColumn("l", "l_receiptdate")
    )));

    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_shipdate"),
        new BaseColumn("l", "l_commitdate")
    )));
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_receiptdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':3'"))
    )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_receiptdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':3'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'1'"), ConstantColumn.valueOf("year")))
        ))
    )));
    expected.addGroupby(new AliasReference("l_shipmode"));
    expected.addOrderby(new OrderbyAttribute("l_shipmode"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "l.l_shipmode, " +
        "sum(case " +
        "when ((o.o_orderpriority = '1-URGENT') " +
        "or (o.o_orderpriority = '2-HIGH')) " +
        "then 1 " +
        "else 0 " +
        "end) as high_line_count, " +
        "sum(case " +
        "when ((o.o_orderpriority <> '1-URGENT') " +
        "and (o.o_orderpriority <> '2-HIGH')) " +
        "then 1 " +
        "else 0 " +
        "end) as low_line_count " +
        "from " +
        "tpch.orders as o, " +
        "tpch.lineitem as l " +
        "where " +
        "(((((o.o_orderkey = l.l_orderkey) " +
        "and (l.l_shipmode in (':1', ':2'))) " +
        "and (l.l_commitdate < l.l_receiptdate)) " +
        "and (l.l_shipdate < l.l_commitdate)) " +
        "and (l.l_receiptdate >= (date ':3'))) " +
        "and (l.l_receiptdate < ((date ':3') + (interval '1' year))) " +
        "group by " +
        "l_shipmode " +
        "order by " +
        "l_shipmode asc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query13Test() throws VerdictDBException {
    BaseTable customer = new BaseTable("tpch", "customer", "c");
    BaseTable orders = new BaseTable("tpch", "orders", "o");
    JoinTable join = JoinTable.create(Arrays.<AbstractRelation>asList(customer, orders),
        Arrays.<JoinTable.JoinType>asList(JoinTable.JoinType.leftouter),
        Arrays.<UnnamedColumn>asList(new ColumnOp("and", Arrays.<UnnamedColumn>asList(
            new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                new BaseColumn("c", "c_custkey"),
                new BaseColumn("o", "o_custkey")
            )),
            new ColumnOp("notlike", Arrays.<UnnamedColumn>asList(
                new BaseColumn("o", "o_comment"),
                ConstantColumn.valueOf("'%:1%:2%'")
            ))
        ))));
    SelectQuery subqery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("c", "c_custkey"), "c_custkey"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "c_count")
        ),
        join);
    subqery.addGroupby(new AliasReference("c_custkey"));
    subqery.setAliasName("c_orders");
    SelectQuery expected = SelectQuery.create(
        Arrays.asList(
            new BaseColumn("c_orders", "c_count"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "custdist")
        ),
        subqery);
    expected.addGroupby(new AliasReference("c_count"));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("custdist", "desc"),
        new OrderbyAttribute("c_count", "desc")));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "c_orders.c_count, " +
        "count(*) as custdist " +
        "from " +
        "(" +
        "select " +
        "c.c_custkey, " +
        "count(*) " +
        "from " +
        "(tpch.customer as c left outer join tpch.orders as o on " +
        "((c.c_custkey = o.o_custkey) " +
        "and (o.o_comment not like '%:1%:2%'))) " +
        "group by " +
        "c_custkey" +
        ") as c_orders (c_custkey, c_count) " +
        "group by " +
        "c_count " +
        "order by " +
        "custdist desc, " +
        "c_count desc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query14Test() throws VerdictDBException {
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    AbstractRelation part = new BaseTable("tpch", "part", "p");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("divide", Arrays.<UnnamedColumn>asList(
                new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    ConstantColumn.valueOf("100.00"),
                    new ColumnOp("sum", new ColumnOp("whenthenelse", Arrays.<UnnamedColumn>asList(
                        new ColumnOp("like", Arrays.<UnnamedColumn>asList(
                            new BaseColumn("p", "p_type"),
                            ConstantColumn.valueOf("'PROMO%'")
                        )),
                        new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                            new BaseColumn("l", "l_extendedprice"),
                            new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf(1), new BaseColumn("l", "l_discount"))))),
                        ConstantColumn.valueOf(0)
                    )))
                )),
                new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("l", "l_extendedprice"),
                    new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf(1), new BaseColumn("l", "l_discount")))
                )))
            )), "promo_revenue")
        ),
        Arrays.asList(lineitem, part));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_partkey"),
        new BaseColumn("p", "p_partkey")
    )));
    expected.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_shipdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':1'"))
    )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_shipdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':1'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'1'"), ConstantColumn.valueOf("month")))
        ))
    )));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "(100.00 * sum(case " +
        "when (p.p_type like 'PROMO%') " +
        "then (l.l_extendedprice * (1 - l.l_discount)) " +
        "else 0 " +
        "end)) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue " +
        "from " +
        "tpch.lineitem as l, " +
        "tpch.part as p " +
        "where " +
        "((l.l_partkey = p.p_partkey) " +
        "and (l.l_shipdate >= (date ':1'))) " +
        "and (l.l_shipdate < ((date ':1') + (interval '1' month))) " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query15Test() throws VerdictDBException {
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "s");
    AbstractRelation revenue = new BaseTable("tpch", "revenue", "r");
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(new ColumnOp("max", new BaseColumn("r", "total_revenue"))),
        revenue);
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("s", "s_suppkey"),
            new BaseColumn("s", "s_name"),
            new BaseColumn("s", "s_address"),
            new BaseColumn("s", "s_phone"),
            new BaseColumn("r", "total_revenue")
        ),
        Arrays.asList(supplier, revenue));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_suppkey"),
        new BaseColumn("r", "supplier_no")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("r", "total_revenue"),
        SubqueryColumn.getSubqueryColumn(subquery)
    )));
    expected.addOrderby(new OrderbyAttribute("s_suppkey"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "s.s_suppkey, " +
        "s.s_name, " +
        "s.s_address, " +
        "s.s_phone, " +
        "r.total_revenue " +
        "from " +
        "tpch.supplier as s, " +
        "tpch.revenue as r " +
        "where " +
        "(s.s_suppkey = r.supplier_no) " +
        "and (r.total_revenue = (" +
        "select " +
        "max(r.total_revenue) " +
        "from " +
        "tpch.revenue as r" +
        ")) " +
        "order by " +
        "s_suppkey asc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query16Test() throws VerdictDBException {
    AbstractRelation partsupp = new BaseTable("tpch", "partsupp", "ps");
    AbstractRelation part = new BaseTable("tpch", "part", "p");
    SelectQuery expected = SelectQuery.create(
        Arrays.asList(
            new BaseColumn("p", "p_brand"),
            new BaseColumn("p", "p_type"),
            new BaseColumn("p", "p_size"),
            new AliasedColumn(new ColumnOp("countdistinct", new BaseColumn("ps", "ps_suppkey")), "supplier_cnt")
        ),
        Arrays.asList(partsupp, part));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_partkey"),
        new BaseColumn("ps", "ps_partkey")
    )));
    expected.addFilterByAnd(new ColumnOp("notequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_brand"),
        ConstantColumn.valueOf("':1'")
    )));
    expected.addFilterByAnd(new ColumnOp("notlike", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_type"),
        ConstantColumn.valueOf("':2%'")
    )));
    expected.addFilterByAnd(new ColumnOp("in", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_size"),
        ConstantColumn.valueOf("':3'"), ConstantColumn.valueOf("':4'"), ConstantColumn.valueOf("':5'"), ConstantColumn.valueOf("':6'"),
        ConstantColumn.valueOf("':7'"), ConstantColumn.valueOf("':8'"), ConstantColumn.valueOf("':9'"), ConstantColumn.valueOf("':10'")
    )));
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(new BaseColumn("s", "s_suppkey")),
        Arrays.<AbstractRelation>asList(new BaseTable("tpch", "supplier", "s")));
    subquery.addFilterByAnd(new ColumnOp("like", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_comment"),
        ConstantColumn.valueOf("'%Customer%Complaints%'")
    )));
    expected.addFilterByAnd(new ColumnOp("notin", Arrays.asList(
        new BaseColumn("ps", "ps_suppkey"),
        SubqueryColumn.getSubqueryColumn(subquery)
    )));
    expected.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("p_brand"),
        new AliasReference("p_type"),
        new AliasReference("p_size")
    ));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("supplier_cnt", "desc"),
        new OrderbyAttribute("p_brand"),
        new OrderbyAttribute("p_type"),
        new OrderbyAttribute("p_size")
    ));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "p.p_brand, " +
        "p.p_type, " +
        "p.p_size, " +
        "count(distinct ps.ps_suppkey) as supplier_cnt " +
        "from " +
        "tpch.partsupp as ps, " +
        "tpch.part as p " +
        "where " +
        "((((p.p_partkey = ps.ps_partkey) " +
        "and (p.p_brand <> ':1')) " +
        "and (p.p_type not like ':2%')) " +
        "and (p.p_size in (':3', ':4', ':5', ':6', ':7', ':8', ':9', ':10'))) " +
        "and (ps.ps_suppkey not in ((" +
        "select " +
        "s.s_suppkey " +
        "from " +
        "tpch.supplier as s " +
        "where " +
        "s.s_comment like '%Customer%Complaints%'" +
        "))) " +
        "group by " +
        "p_brand, " +
        "p_type, " +
        "p_size " +
        "order by " +
        "supplier_cnt desc, " +
        "p_brand asc, " +
        "p_type asc, " +
        "p_size asc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query17Test() throws VerdictDBException {
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    AbstractRelation part = new BaseTable("tpch", "part", "p");
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("l", "l_partkey"), "agg_partkey"),
            new AliasedColumn(new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                ConstantColumn.valueOf("0.2"),
                new ColumnOp("avg", new BaseColumn("l", "l_quantity"))
            )), "avg_quantity")
        ),
        lineitem);
    subquery.addGroupby(new AliasReference("l_partkey"));
    subquery.setAliasName("part_agg");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("divide", Arrays.<UnnamedColumn>asList(
                new ColumnOp("sum", new BaseColumn("l", "l_extendedprice")),
                ConstantColumn.valueOf("7.0")
            )), "avg_yearly")
        ),
        Arrays.asList(lineitem, part, subquery));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_partkey"),
        new BaseColumn("l", "l_partkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("part_agg", "agg_partkey"),
        new BaseColumn("l", "l_partkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_brand"),
        ConstantColumn.valueOf("':1'")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_container"),
        ConstantColumn.valueOf("':2'")
    )));
    expected.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_quantity"),
        new BaseColumn("part_agg", "avg_quantity")
    )));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "sum(l.l_extendedprice) / 7.0 as avg_yearly " +
        "from " +
        "tpch.lineitem as l, " +
        "tpch.part as p, " +
        "(select l.l_partkey as agg_partkey, 0.2 * avg(l.l_quantity) as avg_quantity from tpch.lineitem as l group by l_partkey) as part_agg " +
        "where " +
        "((((p.p_partkey = l.l_partkey) " +
        "and (part_agg.agg_partkey = l.l_partkey)) " +
        "and (p.p_brand = ':1')) " +
        "and (p.p_container = ':2')) " +
        "and (l.l_quantity < part_agg.avg_quantity) " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query18Test() throws VerdictDBException {
    AbstractRelation customer = new BaseTable("tpch", "customer", "c");
    AbstractRelation orders = new BaseTable("tpch", "orders", "o");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("c", "c_name"),
            new BaseColumn("c", "c_custkey"),
            new BaseColumn("o", "o_orderkey"),
            new BaseColumn("o", "o_orderdate"),
            new BaseColumn("o", "o_totalprice"),
            new ColumnOp("sum", new BaseColumn("l", "l_quantity"))
        ),
        Arrays.asList(customer, orders, lineitem));
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(new BaseColumn("l", "l_orderkey")),
        lineitem);
    subquery.addGroupby(new AliasReference("l_orderkey"));
    subquery.addHavingByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new ColumnOp("sum", new BaseColumn("l", "l_quantity")),
        ConstantColumn.valueOf("':1'")
    )));
    expected.addFilterByAnd(new ColumnOp("in", Arrays.asList(
        new BaseColumn("o", "o_orderkey"),
        SubqueryColumn.getSubqueryColumn(subquery)
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("c", "c_custkey"),
        new BaseColumn("o", "o_custkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderkey"),
        new BaseColumn("l", "l_orderkey")
    )));
    expected.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("c_name"),
        new AliasReference("c_custkey"),
        new AliasReference("o_orderkey"),
        new AliasReference("o_orderdate"),
        new AliasReference("o_totalprice")
    ));
    expected.addOrderby(Arrays.<OrderbyAttribute>asList(
        new OrderbyAttribute("o_totalprice", "desc"),
        new OrderbyAttribute("o_orderdate")
    ));
    expected.addLimit(ConstantColumn.valueOf(100));
    String sql = "select " +
        "c.c_name, " +
        "c.c_custkey, " +
        "o.o_orderkey, " +
        "o.o_orderdate, " +
        "o.o_totalprice, " +
        "sum(l.l_quantity) " +
        "from " +
        "tpch.customer as c, " +
        "tpch.orders as o, " +
        "tpch.lineitem as l " +
        "where " +
        "((o.o_orderkey in ((" +
        "select " +
        "l.l_orderkey " +
        "from " +
        "tpch.lineitem as l " +
        "group by " +
        "l_orderkey having " +
        "sum(l.l_quantity) > ':1'" +
        "))) " +
        "and (c.c_custkey = o.o_custkey)) " +
        "and (o.o_orderkey = l.l_orderkey) " +
        "group by " +
        "c_name, " +
        "c_custkey, " +
        "o_orderkey, " +
        "o_orderdate, " +
        "o_totalprice " +
        "order by " +
        "o_totalprice desc, " +
        "o_orderdate asc " +
        "limit 100";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query19Test() throws VerdictDBException {
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l");
    AbstractRelation part = new BaseTable("tpch", "part", "p");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("sum", new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                new BaseColumn("l", "l_extendedprice"),
                new ColumnOp("subtract", Arrays.<UnnamedColumn>asList(
                    ConstantColumn.valueOf(1),
                    new BaseColumn("l", "l_discount")
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
                                    new BaseColumn("p", "p_partkey"),
                                    new BaseColumn("l", "l_partkey")
                                )),
                                new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                                    new BaseColumn("p", "p_brand"),
                                    ConstantColumn.valueOf("':1'")
                                ))
                            )),
                            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                                new BaseColumn("p", "p_container"),
                                ConstantColumn.valueOf("'SM CASE'"),
                                ConstantColumn.valueOf("'SM BOX'"),
                                ConstantColumn.valueOf("'SM PACK'"),
                                ConstantColumn.valueOf("'SM PKG'")
                            ))
                        )),
                        new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
                            new BaseColumn("l", "l_quantity"),
                            ConstantColumn.valueOf("':4'")
                        ))
                    )),
                    new ColumnOp("lessequal", Arrays.<UnnamedColumn>asList(
                        new BaseColumn("l", "l_quantity"),
                        new ColumnOp("add", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':4'"), ConstantColumn.valueOf(10)))
                    ))
                )),
                new ColumnOp("between", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("p", "p_size"),
                    ConstantColumn.valueOf(1),
                    ConstantColumn.valueOf(5)
                ))
            )),
            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                new BaseColumn("l", "l_shipmode"),
                ConstantColumn.valueOf("'AIR'"),
                ConstantColumn.valueOf("'AIR REG'")
            ))
        )),
        new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
            new BaseColumn("l", "l_shipinstruct"),
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
                                    new BaseColumn("p", "p_partkey"),
                                    new BaseColumn("l", "l_partkey")
                                )),
                                new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                                    new BaseColumn("p", "p_brand"),
                                    ConstantColumn.valueOf("':2'")
                                ))
                            )),
                            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                                new BaseColumn("p", "p_container"),
                                ConstantColumn.valueOf("'MED BAG'"),
                                ConstantColumn.valueOf("'MED BOX'"),
                                ConstantColumn.valueOf("'MED PKG'"),
                                ConstantColumn.valueOf("'MED PACK'")
                            ))
                        )),
                        new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
                            new BaseColumn("l", "l_quantity"),
                            ConstantColumn.valueOf("':5'")
                        ))
                    )),
                    new ColumnOp("lessequal", Arrays.<UnnamedColumn>asList(
                        new BaseColumn("l", "l_quantity"),
                        new ColumnOp("add", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':5'"), ConstantColumn.valueOf(10)))
                    ))
                )),
                new ColumnOp("between", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("p", "p_size"),
                    ConstantColumn.valueOf(1),
                    ConstantColumn.valueOf(10)
                ))
            )),
            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                new BaseColumn("l", "l_shipmode"),
                ConstantColumn.valueOf("'AIR'"),
                ConstantColumn.valueOf("'AIR REG'")
            ))
        )),
        new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
            new BaseColumn("l", "l_shipinstruct"),
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
                                    new BaseColumn("p", "p_partkey"),
                                    new BaseColumn("l", "l_partkey")
                                )),
                                new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
                                    new BaseColumn("p", "p_brand"),
                                    ConstantColumn.valueOf("':3'")
                                ))
                            )),
                            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                                new BaseColumn("p", "p_container"),
                                ConstantColumn.valueOf("'LG CASE'"),
                                ConstantColumn.valueOf("'LG BOX'"),
                                ConstantColumn.valueOf("'LG PACK'"),
                                ConstantColumn.valueOf("'LG PKG'")
                            ))
                        )),
                        new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
                            new BaseColumn("l", "l_quantity"),
                            ConstantColumn.valueOf("':6'")
                        ))
                    )),
                    new ColumnOp("lessequal", Arrays.<UnnamedColumn>asList(
                        new BaseColumn("l", "l_quantity"),
                        new ColumnOp("add", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("':6'"), ConstantColumn.valueOf(10)))
                    ))
                )),
                new ColumnOp("between", Arrays.<UnnamedColumn>asList(
                    new BaseColumn("p", "p_size"),
                    ConstantColumn.valueOf(1),
                    ConstantColumn.valueOf(15)
                ))
            )),
            new ColumnOp("in", Arrays.<UnnamedColumn>asList(
                new BaseColumn("l", "l_shipmode"),
                ConstantColumn.valueOf("'AIR'"),
                ConstantColumn.valueOf("'AIR REG'")
            ))
        )),
        new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
            new BaseColumn("l", "l_shipinstruct"),
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
        "sum(l.l_extendedprice * (1 - l.l_discount)) as revenue " +
        "from " +
        "tpch.lineitem as l, " +
        "tpch.part as p " +
        "where " +
        "(((((((((" +
        "p.p_partkey = l.l_partkey) " +
        "and (p.p_brand = ':1')) " +
        "and (p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))) " +
        "and (l.l_quantity >= ':4')) and (l.l_quantity <= (':4' + 10))) " +
        "and (p.p_size between 1 and 5)) " +
        "and (l.l_shipmode in ('AIR', 'AIR REG'))) " +
        "and (l.l_shipinstruct = 'DELIVER IN PERSON')) " +
        "or " +
        "((((((((p.p_partkey = l.l_partkey) " +
        "and (p.p_brand = ':2')) " +
        "and (p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))) " +
        "and (l.l_quantity >= ':5')) and (l.l_quantity <= (':5' + 10))) " +
        "and (p.p_size between 1 and 10)) " +
        "and (l.l_shipmode in ('AIR', 'AIR REG'))) " +
        "and (l.l_shipinstruct = 'DELIVER IN PERSON'))" +
        ") " +
        "or " +
        "((((((((p.p_partkey = l.l_partkey) " +
        "and (p.p_brand = ':3')) " +
        "and (p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))) " +
        "and (l.l_quantity >= ':6')) and (l.l_quantity <= (':6' + 10))) " +
        "and (p.p_size between 1 and 15)) " +
        "and (l.l_shipmode in ('AIR', 'AIR REG'))) " +
        "and (l.l_shipinstruct = 'DELIVER IN PERSON')" +
        ") " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query20Test() throws VerdictDBException {
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "s");
    AbstractRelation nation = new BaseTable("tpch", "nation", "n");
    SelectQuery expected = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("s", "s_name"),
            new BaseColumn("s", "s_address")
        ),
        Arrays.asList(supplier, nation));
    SelectQuery subsubquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("l", "l_partkey"), "agg_partkey"),
            new AliasedColumn(new BaseColumn("l", "l_suppkey"), "agg_suppkey"),
            new AliasedColumn(new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                ConstantColumn.valueOf("0.5"), new ColumnOp("sum", new BaseColumn("l", "l_quantity")))), "agg_quantity")
        ),
        new BaseTable("tpch", "lineitem", "l"));
    subsubquery.addFilterByAnd(new ColumnOp("greaterequal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_shipdate"),
        new ColumnOp("date", ConstantColumn.valueOf("':2'"))
    )));
    subsubquery.addFilterByAnd(new ColumnOp("less", Arrays.<UnnamedColumn>asList(
        new BaseColumn("l", "l_shipdate"),
        new ColumnOp("add", Arrays.<UnnamedColumn>asList(
            new ColumnOp("date", ConstantColumn.valueOf("':2'")),
            new ColumnOp("interval", Arrays.<UnnamedColumn>asList(ConstantColumn.valueOf("'1'"), ConstantColumn.valueOf("year")))
        ))
    )));
    subsubquery.addGroupby(Arrays.<GroupingAttribute>asList(
        new AliasReference("l_partkey"),
        new AliasReference("l_suppkey")
    ));
    subsubquery.setAliasName("agg_lineitem");
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new BaseColumn("ps", "ps_suppkey")
        ),
        Arrays.asList(new BaseTable("tpch", "partsupp", "ps"), subsubquery));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("agg_lineitem", "agg_partkey"),
        new BaseColumn("ps", "ps_partkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("agg_lineitem", "agg_suppkey"),
        new BaseColumn("ps", "ps_suppkey")
    )));
    SelectQuery subsubquery2 = SelectQuery.create(
        Arrays.<SelectItem>asList(new BaseColumn("p", "p_partkey")),
        new BaseTable("tpch", "part", "p"));
    subsubquery2.addFilterByAnd(new ColumnOp("like", Arrays.<UnnamedColumn>asList(
        new BaseColumn("p", "p_name"), ConstantColumn.valueOf("':1%'")
    )));
    subquery.addFilterByAnd(new ColumnOp("in", Arrays.asList(
        new BaseColumn("ps", "ps_partkey"),
        SubqueryColumn.getSubqueryColumn(subsubquery2)
    )));
    subquery.addFilterByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new BaseColumn("ps", "ps_availqty"),
        new BaseColumn("agg_lineitem", "agg_quantity")
    )));
    expected.addFilterByAnd(new ColumnOp("in", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_suppkey"),
        SubqueryColumn.getSubqueryColumn(subquery)
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_nationkey"),
        new BaseColumn("n", "n_nationkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("n", "n_name"),
        ConstantColumn.valueOf("':3'")
    )));
    expected.addOrderby(new OrderbyAttribute("s_name"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "s.s_name, " +
        "s.s_address " +
        "from " +
        "tpch.supplier as s, " +
        "tpch.nation as n " +
        "where " +
        "((s.s_suppkey in ((" +
        "select " +
        "ps.ps_suppkey " +
        "from " +
        "tpch.partsupp as ps, " +
        "(" +
        "select " +
        "l.l_partkey as agg_partkey, " +
        "l.l_suppkey as agg_suppkey, " +
        "0.5 * sum(l.l_quantity) as agg_quantity " +
        "from " +
        "tpch.lineitem as l " +
        "where " +
        "(l.l_shipdate >= (date ':2')) " +
        "and (l.l_shipdate < ((date ':2') + (interval '1' year))) " +
        "group by " +
        "l_partkey, " +
        "l_suppkey" +
        ") as agg_lineitem " +
        "where " +
        "(((agg_lineitem.agg_partkey = ps.ps_partkey) " +
        "and (agg_lineitem.agg_suppkey = ps.ps_suppkey)) " +
        "and (ps.ps_partkey in ((" +
        "select " +
        "p.p_partkey " +
        "from " +
        "tpch.part as p " +
        "where " +
        "p.p_name like ':1%'" +
        ")))) " +
        "and (ps.ps_availqty > agg_lineitem.agg_quantity" +
        ")))) " +
        "and (s.s_nationkey = n.n_nationkey)) " +
        "and (n.n_name = ':3') " +
        "order by " +
        "s_name asc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query21Test() throws VerdictDBException {
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "s");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "l1");
    AbstractRelation orders = new BaseTable("tpch", "orders", "o");
    AbstractRelation nation = new BaseTable("tpch", "nation", "n");
    SelectQuery expected = SelectQuery.create(
        Arrays.asList(
            new BaseColumn("s", "s_name"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "numwait")
        ),
        Arrays.asList(supplier, lineitem, orders, nation));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("s", "s_suppkey"),
        new BaseColumn("l1", "l_suppkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderkey"),
        new BaseColumn("l1", "l_orderkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_orderstatus"),
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
        new BaseColumn("s", "s_nationkey"),
        new BaseColumn("n", "n_nationkey")
    )));
    expected.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("n", "n_name"),
        ConstantColumn.valueOf("':1'")
    )));
    expected.addGroupby(new AliasReference("s_name"));
    expected.addOrderby(new OrderbyAttribute("numwait", "desc"));
    expected.addOrderby(new OrderbyAttribute("s_name"));
    expected.addLimit(ConstantColumn.valueOf(100));
    String sql = "select " +
        "s.s_name, " +
        "count(*) as numwait " +
        "from " +
        "tpch.supplier as s, " +
        "tpch.lineitem as l1, " +
        "tpch.orders as o, " +
        "tpch.nation as n " +
        "where " +
        "(((((((s.s_suppkey = l1.l_suppkey) " +
        "and (o.o_orderkey = l1.l_orderkey)) " +
        "and (o.o_orderstatus = 'F')) " +
        "and (l1.l_receiptdate > l1.l_commitdate)) " +
        "and (exists (" +
        "select " +
        "* " +
        "from " +
        "tpch.lineitem as l2 " +
        "where " +
        "(l2.l_orderkey = l1.l_orderkey) " +
        "and (l2.l_suppkey <> l1.l_suppkey)" +
        "))) " +
        "and (not exists (" +
        "select " +
        "* " +
        "from " +
        "tpch.lineitem as l3 " +
        "where " +
        "((l3.l_orderkey = l1.l_orderkey) " +
        "and (l3.l_suppkey <> l1.l_suppkey)) " +
        "and (l3.l_receiptdate > l3.l_commitdate)" +
        "))) " +
        "and (s.s_nationkey = n.n_nationkey)) " +
        "and (n.n_name = ':1') " +
        "group by " +
        "s_name " +
        "order by " +
        "numwait desc, " +
        "s_name asc " +
        "limit 100";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }

  @Test
  public void Query22Test() throws VerdictDBException {
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("substr", Arrays.<UnnamedColumn>asList(
                new BaseColumn("c", "c_phone"),
                ConstantColumn.valueOf(1), ConstantColumn.valueOf(2))), "cntrycode"),
            new BaseColumn("c", "c_acctbal")
        ),
        new BaseTable("tpch", "customer", "c"));
    subquery.addFilterByAnd(new ColumnOp("in", Arrays.<UnnamedColumn>asList(
        new ColumnOp("substr", Arrays.<UnnamedColumn>asList(
            new BaseColumn("c", "c_phone"),
            ConstantColumn.valueOf(1), ConstantColumn.valueOf(2))),
        ConstantColumn.valueOf("':1'"), ConstantColumn.valueOf("':2'"), ConstantColumn.valueOf("':3'"),
        ConstantColumn.valueOf("':4'"), ConstantColumn.valueOf("':5'"), ConstantColumn.valueOf("':6'"),
        ConstantColumn.valueOf("':7'")
    )));
    SelectQuery subsubquery1 = SelectQuery.create(
        Arrays.<SelectItem>asList(new ColumnOp("avg", new BaseColumn("c", "c_acctbal"))),
        new BaseTable("tpch", "customer", "c"));
    subsubquery1.addFilterByAnd(new ColumnOp("greater", Arrays.<UnnamedColumn>asList(
        new BaseColumn("c", "c_acctbal"),
        ConstantColumn.valueOf("0.00")
    )));
    subsubquery1.addFilterByAnd(new ColumnOp("in", Arrays.<UnnamedColumn>asList(
        new ColumnOp("substr", Arrays.<UnnamedColumn>asList(
            new BaseColumn("c", "c_phone"),
            ConstantColumn.valueOf(1), ConstantColumn.valueOf(2))),
        ConstantColumn.valueOf("':1'"), ConstantColumn.valueOf("':2'"), ConstantColumn.valueOf("':3'"),
        ConstantColumn.valueOf("':4'"), ConstantColumn.valueOf("':5'"), ConstantColumn.valueOf("':6'"),
        ConstantColumn.valueOf("':7'")
    )));
    subquery.addFilterByAnd(new ColumnOp("greater", Arrays.asList(
        new BaseColumn("c", "c_acctbal"), SubqueryColumn.getSubqueryColumn(subsubquery1)
    )));
    SelectQuery subsubquery2 = SelectQuery.create(
        Arrays.<SelectItem>asList(new AsteriskColumn()),
        new BaseTable("tpch", "orders", "o"));
    subsubquery2.addFilterByAnd(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(
        new BaseColumn("o", "o_custkey"),
        new BaseColumn("c", "c_custkey")
    )));
    subquery.addFilterByAnd(new ColumnOp("notexists", SubqueryColumn.getSubqueryColumn(subsubquery2)));
    subquery.setAliasName("custsale");
    SelectQuery expected = SelectQuery.create(
        Arrays.asList(
            new BaseColumn("custsale", "cntrycode"),
            new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "numcust"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("custsale", "c_acctbal")), "totacctbal")
        ),
        subquery);
    expected.addGroupby(new AliasReference("cntrycode"));
    expected.addOrderby(new OrderbyAttribute("cntrycode"));
    expected.addLimit(ConstantColumn.valueOf(1));
    String sql = "select " +
        "custsale.cntrycode, " +
        "count(*) as numcust, " +
        "sum(custsale.c_acctbal) as totacctbal " +
        "from " +
        "(" +
        "select " +
        "substr(c.c_phone, 1, 2) as cntrycode, " +
        "c.c_acctbal " +
        "from " +
        "tpch.customer as c " +
        "where " +
        "(((substr(c.c_phone, 1, 2)) in " +
        "(':1', ':2', ':3', ':4', ':5', ':6', ':7')) " +
        "and (c.c_acctbal > (" +
        "select " +
        "avg(c.c_acctbal) " +
        "from " +
        "tpch.customer as c " +
        "where " +
        "(c.c_acctbal > 0.00) " +
        "and ((substr(c.c_phone, 1, 2)) in " +
        "(':1', ':2', ':3', ':4', ':5', ':6', ':7'))" +
        "))) " +
        "and (not exists (" +
        "select " +
        "* " +
        "from " +
        "tpch.orders as o " +
        "where " +
        "o.o_custkey = c.c_custkey" +
        ")" +
        ")) as custsale " +
        "group by " +
        "cntrycode " +
        "order by " +
        "cntrycode asc " +
        "limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    assertEquals(expected, relation);
  }
}

package edu.umich.verdict.redshift;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class RedshiftThroughVerdictTest {

    public static void main(String[] args) throws VerdictException {

        VerdictConf conf = new VerdictConf();
        conf.setDbms("redshift");
        conf.setHost("salat2-verdict.ctkb4oe4rzfm.us-east-1.redshift.amazonaws.com");
        conf.setPort("5439");		
        conf.setDbmsSchema("dev");
        conf.setUser("junhao");
        conf.setPassword("BTzyc1xG");
        conf.set("loglevel", "debug");
        
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        
        VerdictContext vc = VerdictJDBCContext.from(conf);        
        vc.executeJdbcQuery("set bypass=true");
        vc.executeJdbcQuery("set search_path = tpch1g,tpch1g_verdict");
        vc.executeJdbcQuery("set bypass=false");
//        vc.executeJdbcQuery("show tables in tpch1g");
//        vc.executeJdbcQuery("show tables in tpch1g_verdict");
//        vc.executeJdbcQuery("create sample of tpch1g.orders");
//        vc.executeJdbcQuery("create sample of tpch1g.lineitem");
//        vc.executeJdbcQuery("delete sample of tpch1g.orders");
//        vc.executeJdbcQuery("delete sample of tpch1g.lineitem");
        
        // ======== test sample creation ========  
        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create uniform sample of tpch1g.lineitem;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate uniform sample of tpch1g.lineitem;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create stratified sample of tpch1g.lineitem on l_suppkey;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate stratified sample of tpch1g.lineitem on l_suppkey;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create universe sample of tpch1g.lineitem on l_orderkey;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate universe sample of tpch1g.lineitem on l_orderkey;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create universe sample of tpch1g.lineitem on l_partkey;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate universe sample of tpch1g.lineitem on l_partkey;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create uniform sample of tpch1g.orders;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate uniform sample of tpch1g.orders;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create universe sample of tpch1g.o_orderkey;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate universe sample of tpch1g.o_orderkey;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        
        // ======== test Queries ========
        
        String q1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from tpch1g.lineitem where l_shipdate <= '1997-12-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;";
        String q9 = "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract (year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from tpch1g.lineitem inner join tpch1g.orders on o_orderkey = l_orderkey inner join tpch1g.partsupp on ps_suppkey = l_suppkey inner join tpch1g.part on p_partkey = ps_partkey inner join tpch1g.supplier on s_suppkey = ps_suppkey inner join tpch1g.nation on s_nationkey = n_nationkey where p_name like '%green%') as profit group by nation, o_year order by nation, o_year desc;";
        String q12 = "select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from tpch1g.orders, tpch1g.lineitem where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01' group by l_shipmode order by l_shipmode;";
        String q15 = "create view revenue_temp as select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue from tpch1g.lineitem where l_shipdate >= '1995-01-01' and l_shipdate < '1996-01-01' group by l_suppkey; select s_suppkey, s_name, s_address, s_phone, total_revenue from tpch1g.supplier, tpch1g.revenue_temp where s_suppkey = supplier_no and total_revenue = ( select max(total_revenue) from revenue_temp) order by s_suppkey; drop view revenue_temp;";
        String q15_check = "select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue from tpch1g.lineitem where l_shipdate >= '1995-01-01' and l_shipdate < '1996-01-01' group by l_suppkey order by total_revenue limit 10;";
        String q17 = "select sum(l_extendedprice) / 7.0 as avg_yearly from tpch1g.lineitem inner join ( select l_partkey as partkey, 0.2 * avg(l_quantity) as small_quantity from tpch1g.lineitem inner join tpch1g.part on l_partkey = p_partkey group by l_partkey) t on l_partkey = partkey inner join tpch1g.part on l_partkey = p_partkey where p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < small_quantity;";
        
        // ======== test Q1 ========
        
        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery(q1);
        endTime = System.currentTimeMillis();
        System.out.println("\n\n" + q1);
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        
        // ======== test Q9 ========
        
        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery(q9);
        endTime = System.currentTimeMillis();
        System.out.println("\n\n" + q9);
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        
        // ======== test Q12 ========
        
        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery(q12);
        endTime = System.currentTimeMillis();
        System.out.println("\n\n" + q12);
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        
        // ======== test Q15 ========
        
        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery(q15);
        endTime = System.currentTimeMillis();
        System.out.println("\n\n" + q15);
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
              
        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery(q15_check);
        endTime = System.currentTimeMillis();
        System.out.println("\n\n" + q15_check);
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        
        // ======== test Q17 ========
        
        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery(q17);
        endTime = System.currentTimeMillis();
        System.out.println("\n\n" + q17);
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
                
        vc.destroy();
    }

}

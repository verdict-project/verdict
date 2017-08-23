package edu.umich.verdict.redshift;

import java.sql.ResultSet;
import java.sql.Statement;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class RedshiftThroughVerdictTest {

    public static void main(String[] args) throws VerdictException {

        VerdictConf conf = new VerdictConf();
        conf.setDbms("redshift");
//        conf.setHost("salat2-verdict.ctkb4oe4rzfm.us-east-1.redshift.amazonaws.com");
        conf.setHost("verdict-vldb.crc58e3qof3k.us-east-1.redshift.amazonaws.com");
        conf.setPort("5439");		
        conf.setDbmsSchema("dev");
//        conf.setUser("junhao");
//        conf.setPassword("BTzyc1xG");
        conf.setUser("admin");
        conf.setPassword("qKUcr2CUgSP3NjHE");
//        conf.set("loglevel", "debug");
        
        
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        
        VerdictContext vc = VerdictJDBCContext.from(conf);        
        vc.executeJdbcQuery("set bypass=true");
//        vc.executeJdbcQuery("set search_path = tpch500g, tpch500g_verdict2");
        vc.executeJdbcQuery("set search_path = instacart100g, instacart100g_verdict");
        vc.executeJdbcQuery("set bypass=false");

        
        
//        // ======== test sample creation tpch ========  
//        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create 0.5% uniform sample of tpch500g.lineitem;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate uniform sample of tpch500g.lineitem;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create 0.5% stratified sample of tpch500g.lineitem on l_suppkey;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate stratified sample of tpch500g.lineitem on l_suppkey;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create 0.5% universe sample of tpch500g.lineitem on l_orderkey;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate universe sample of tpch500g.lineitem on l_orderkey;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create 0.5% universe sample of tpch500g.lineitem on l_partkey;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate universe sample of tpch500g.lineitem on l_partkey;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create 0.5% uniform sample of tpch500g.orders;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate uniform sample of tpch500g.orders;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create 0.5% universe sample of tpch500g.o_orderkey;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate universe sample of tpch500g.o_orderkey;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        
//        // ======== test sample creation instacart ========  
//        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create uniform sample of instacart100g.orders;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate uniform sample of instacart100g.orders;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create uniform sample of instacart100g.order_products;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate uniform sample of instacart100g.order_products;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create universe sample of instacart100g.orders on order_id;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate universe sample of instacart100g.orders on order_id;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery("create universe sample of instacart100g.order_products on order_id;");
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\ncreate universe sample of instacart100g.order_products on order_id;");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
              
        
//        // ======== test Queries tpch ========
//        String q0 = "select count(*) from tpch500g.lineitem;";
//        String q1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from tpch500g.lineitem where l_shipdate <= '1997-12-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;";
//        String q9 = "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract (year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from tpch500g.lineitem inner join tpch500g.orders on o_orderkey = l_orderkey inner join tpch500g.partsupp on ps_suppkey = l_suppkey inner join tpch500g.part on p_partkey = ps_partkey inner join tpch500g.supplier on s_suppkey = ps_suppkey inner join tpch500g.nation on s_nationkey = n_nationkey where p_name like '%green%') as profit group by nation, o_year order by nation, o_year desc;";
//        String q12 = "select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from tpch500g.orders, tpch500g.lineitem where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01' group by l_shipmode order by l_shipmode;";
//        String q15 = "create view revenue_temp as select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue from tpch500g.lineitem where l_shipdate >= '1995-01-01' and l_shipdate < '1996-01-01' group by l_suppkey; select s_suppkey, s_name, s_address, s_phone, total_revenue from tpch500g.supplier, tpch500g.revenue_temp where s_suppkey = supplier_no and total_revenue = ( select max(total_revenue) from revenue_temp) order by s_suppkey; drop view revenue_temp;";
//        String q15_check = "select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue from tpch500g.lineitem where l_shipdate >= '1995-01-01' and l_shipdate < '1996-01-01' group by l_suppkey order by total_revenue limit 10;";
//        String q17 = "select sum(l_extendedprice) / 7.0 as avg_yearly from tpch500g.lineitem inner join ( select l_partkey as partkey, 0.2 * avg(l_quantity) as small_quantity from tpch500g.lineitem inner join tpch500g.part on l_partkey = p_partkey group by l_partkey) t on l_partkey = partkey inner join tpch500g.part on l_partkey = p_partkey where p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < small_quantity;";
//        
//        // 	======== test Q0 ========
//        
//        startTime = System.currentTimeMillis();        
//        ResultSet rs = vc.executeJdbcQuery(q0);
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\n" + "q0");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");                
//		ResultSetConversion.printResultSet(rs);
//		
//        // ======== test Q1 ========
//        
//        startTime = System.currentTimeMillis();        
//        rs = vc.executeJdbcQuery(q1);
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\n" + "q1");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        ResultSetConversion.printResultSet(rs);
//        
//        // ======== test Q9 ========
//        
//        startTime = System.currentTimeMillis();        
//        rs = vc.executeJdbcQuery(q9);
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\n" + "q9");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        ResultSetConversion.printResultSet(rs);
//        
//        // ======== test Q12 ========
//        
//        startTime = System.currentTimeMillis();        
//        rs = vc.executeJdbcQuery(q12);
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\n" + "q12");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        ResultSetConversion.printResultSet(rs);
//        
//        // ======== test Q15 ========
//        
//        startTime = System.currentTimeMillis();        
//        vc.executeJdbcQuery(q15);
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\n" + "q15");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//              
//        startTime = System.currentTimeMillis();        
//        rs = vc.executeJdbcQuery(q15_check);
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\n" + "q15_check");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        ResultSetConversion.printResultSet(rs);
//        
//        // ======== test Q17 ========
//        
//        startTime = System.currentTimeMillis();        
//        rs = vc.executeJdbcQuery(q17);
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\n" + "q17");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        ResultSetConversion.printResultSet(rs);
//        vc.destroy();
        
     // ======== test Queries instacart ========
        String q2 = "select order_hour_of_day, count(*) as c from instacart100g.orders group by order_hour_of_day order by order_hour_of_day;";
        String q3 = "SELECT product_name, count(*) as order_count FROM instacart100g.order_products, instacart100g.orders, instacart100g.products WHERE orders.order_id = order_products.order_id AND order_products.product_id = products.product_id AND (order_dow = 0 OR order_dow = 1) GROUP BY product_name ORDER BY order_count DESC LIMIT 5;";
        String q4 = "SELECT departments.department_id, department, count(*) as order_count FROM instacart100g.order_products, instacart100g.orders, instacart100g.products, instacart100g.departments WHERE orders.order_id = order_products.order_id AND order_products.product_id = products.product_id AND products.department_id = departments.department_id GROUP BY departments.department_id, department ORDER BY order_count DESC LIMIT 5;";
        
//        // 	======== test Q2 ========
//        
//        startTime = System.currentTimeMillis();        
//        ResultSet rs = vc.executeJdbcQuery(q2);
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\n" + "q2");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");                
//		ResultSetConversion.printResultSet(rs);
//		
//        // ======== test Q3 ========
//        
//        startTime = System.currentTimeMillis();        
//        rs = vc.executeJdbcQuery(q3);
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\n" + "q3");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        ResultSetConversion.printResultSet(rs);
        
        // ======== test Q4 ========
        
        startTime = System.currentTimeMillis();        
        ResultSet rs = vc.executeJdbcQuery(q4);
        endTime = System.currentTimeMillis();
        System.out.println("\n\n" + "q4");
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        ResultSetConversion.printResultSet(rs);

               
    }

}

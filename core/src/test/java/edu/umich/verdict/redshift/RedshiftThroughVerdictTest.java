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
        vc.executeJdbcQuery("set search_path = public");
        vc.executeJdbcQuery("set bypass=false");
        
        // create sample
        
        vc.executeJdbcQuery("create uniform sample of public.s");
        
     // ======== test Queries instacart ========
        String q2 = "select order_hour_of_day, count(*) as c from public.orders group by order_hour_of_day order by order_hour_of_day;";
        String q3 = "SELECT product_name, count(*) as order_count FROM public.order_products, public.orders, public.products WHERE orders.order_id = order_products.order_id AND order_products.product_id = products.product_id AND (order_dow = 0 OR order_dow = 1) GROUP BY product_name ORDER BY order_count DESC LIMIT 5;";
        String q4 = "SELECT departments.department_id, department, count(*) as order_count FROM public.order_products, public.orders, public.products, public.departments WHERE orders.order_id = order_products.order_id AND order_products.product_id = products.product_id AND products.department_id = departments.department_id GROUP BY departments.department_id, department ORDER BY order_count DESC LIMIT 5;";
//        String q4_alias = "SELECT d.department_id, d.department, count(*) as order_count FROM instacart100g.order_products as op, instacart100g.orders as ord, instacart100g.products as p, instacart100g.departments as d WHERE ord.order_id = op.order_id AND op.product_id = p.product_id AND p.department_id = d.department_id GROUP BY d.department_id, d.department ORDER BY order_count DESC LIMIT 5;";
        
        // 	======== test Q2 ========
        
        startTime = System.currentTimeMillis();        
        ResultSet rs = vc.executeJdbcQuery(q2);
        endTime = System.currentTimeMillis();
        System.out.println("\n\n" + "q2");
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");                
		ResultSetConversion.printResultSet(rs);
		
        // ======== test Q3 ========
        
        startTime = System.currentTimeMillis();        
        rs = vc.executeJdbcQuery(q3);
        endTime = System.currentTimeMillis();
        System.out.println("\n\n" + "q3");
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        ResultSetConversion.printResultSet(rs);
        
//        // ======== test Q4 ========
//        
//        startTime = System.currentTimeMillis();        
//        rs = vc.executeJdbcQuery(q4_alias);
//        endTime = System.currentTimeMillis();
//        System.out.println("\n\n" + "q4");
//        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
//        ResultSetConversion.printResultSet(rs);

    }
}

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
//        conf.set("loglevel", "debug");
        
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
        
        // test sample creation
        
        long startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery("create uniform sample of tpch1g.lineitem;");
        long endTime = System.currentTimeMillis();
        System.out.println("\n\ncreate uniform sample of tpch1g.lineitem;");
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");

        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery("create stratified sample of tpch1g.lineitem on l_suppkey;");
        endTime = System.currentTimeMillis();
        System.out.println("\n\ncreate stratified sample of tpch1g.lineitem on l_suppkey;");
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        
        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery("create universe sample of tpch1g.lineitem on l_orderkey;");
        endTime = System.currentTimeMillis();
        System.out.println("\n\ncreate universe sample of tpch1g.lineitem on l_orderkey;");
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        
        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery("create universe sample of tpch1g.lineitem on l_partkey;");
        endTime = System.currentTimeMillis();
        System.out.println("\n\ncreate universe sample of tpch1g.lineitem on l_partkey;");
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        
        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery("create uniform sample of tpch1g.orders;");
        endTime = System.currentTimeMillis();
        System.out.println("\n\ncreate uniform sample of tpch1g.orders;");
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");

        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery("create universe sample of tpch1g.o_orderkey;");
        endTime = System.currentTimeMillis();
        System.out.println("\n\ncreate universe sample of tpch1g.o_orderkey;");
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");

        startTime = System.currentTimeMillis();        
        vc.executeJdbcQuery("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= '1997-12-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;");
        endTime = System.currentTimeMillis();
        System.out.println("\n\nselect l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= '1997-12-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;");
        System.out.println("That took " + (endTime - startTime) + " milliseconds \n\n");
        
        vc.destroy();
    }

}

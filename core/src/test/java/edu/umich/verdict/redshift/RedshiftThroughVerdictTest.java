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
        
        VerdictContext vc = VerdictJDBCContext.from(conf);
        
        vc.executeJdbcQuery("set bypass=true");
        vc.executeJdbcQuery("set search_path = tpch1g,tpch1g_verdict");
        vc.executeJdbcQuery("set bypass=false");    
        
//        long startTime = System.currentTimeMillis();                
//        vc.executeJdbcQuery("create sample of tpch1g.orders");        
//        long endTime   = System.currentTimeMillis();
//        long totalTime = endTime - startTime;
//        System.out.println(totalTime);
//
//        vc.executeJdbcQuery("create stratified sample of tpch1g.lineitem on l_suppkey");
//
//        vc.executeJdbcQuery("create universe sample of lineitem on l_orderkey");
//
//        vc.executeJdbcQuery("create universe sample of lineitem on l_partkey");
//
//        vc.executeJdbcQuery("create uniform sample of orders");
//
//        vc.executeJdbcQuery("create universe sample of o_orderkey");

        vc.destroy();
    }

}

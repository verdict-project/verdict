package edu.umich.verdict.impala;

import java.io.FileNotFoundException;

import edu.umich.verdict.BaseIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ImpalaThroughVerdictTest {

    public static void main(String[] args) throws VerdictException, FileNotFoundException {

        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(BaseIT.readHost());
        conf.setPort("21050");
        conf.set("loglevel", "debug");
        conf.set("verdict.meta_data.meta_database_suffix", "_verdict_tpchq15");
        conf.set("verdict.jdbc.schema", "tpch1g");

        VerdictContext vc = VerdictJDBCContext.from(conf);
//        vc.executeJdbcQuery("set verdict.meta_catalog_suffix=_verdict_impala");
//        vc.executeJdbcQuery("use tpch1g");
        //vc.executeJdbcQuery("select count(*) from instacart1g.orders");
//        vc.executeJdbcQuery("select count(*) from instacart1g.orders group by order_hour_of_day");
        vc.executeJdbcQuery("select l_suppkey as supplier_no,\n" + 
                "       sum(l_extendedprice * (1 - l_discount)) as total_revenue\n" + 
                "from lineitem\n" + 
                "where l_shipdate >= '1995-01-01' and l_shipdate < '1996-01-01'\n" + 
                "group by l_suppkey\n" + 
                "order by total_revenue desc\n" + 
                "limit 10;");

        vc.destroy();
    }

}

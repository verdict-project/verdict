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

        VerdictContext vc = VerdictJDBCContext.from(conf);
        vc.executeJdbcQuery("set verdict.meta_catalog_suffix=_verdict_impala");
        vc.executeJdbcQuery("use tpch1g");
        //vc.executeJdbcQuery("select count(*) from instacart1g.orders");
        vc.executeJdbcQuery("select count(*) from lineitem;");

        vc.destroy();
    }
}

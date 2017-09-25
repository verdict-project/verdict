package edu.umich.verdict.impala;

import java.io.FileNotFoundException;

import edu.umich.verdict.BaseIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;
import java.sql.ResultSet;

public class ImpalaThroughVerdictTest {


    public static void main(String[] args) throws VerdictException, FileNotFoundException {


        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost("salat2.eecs.umich.edu");
        conf.setPort("21050");
        conf.set("loglevel", "debug");
        conf.set("verdict.meta_data.meta_database_suffix", "_verdict");
//        conf.set("verdict.jdbc.schema", "instacart100g");

        VerdictContext vc = VerdictJDBCContext.from(conf);

        ResultSet rs = vc.executeJdbcQuery("show databases;");
//        vc.executeJdbcQuery("create uniform sample of instacart1g.orders");
        rs = vc.executeJdbcQuery("select count(*) from instacart1g.orders left join instacart1g.order_products on instacart1g.orders.order_id = instacart1g.order_products.order_id limit 10");
//        rs = vc.executeJdbcQuery("select count(*) from instacart1g.orders, instacart1g.order_products where instacart1g.orders.order_id = instacart1g.order_products.order_id limit 10");
        ResultSetConversion.printResultSet(rs);

        vc.destroy();
    }
}

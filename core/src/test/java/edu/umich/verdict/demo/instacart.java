package edu.umich.verdict.demo;

import java.io.FileNotFoundException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class instacart {

    static VerdictJDBCContext vc;

    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setHost("salat1.eecs.umich.edu");
        conf.setPort("21050");
        conf.setDbms("impala");
        conf.setDbmsSchema("instacart100g");
        conf.set("verdict.meta_catalog_suffix", "_verdict");
        vc = VerdictJDBCContext.from(conf);
    }

    @Test
    public void test() throws VerdictException {

        String sql = "SELECT product_name, count(*) as order_count " +
                "FROM instacart100g.order_products, instacart100g.orders, instacart100g.products " +
                "WHERE orders.order_id = order_products.order_id " +
                "  AND order_products.product_id = products.product_id " +
                "  AND (order_dow = 0 OR order_dow = 1) " +
                "GROUP BY product_name " +
                "ORDER BY order_count DESC " +
                "LIMIT 5";
        vc.sql(sql);
    }

}

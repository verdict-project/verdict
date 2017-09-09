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
        conf.set("verdict.meta_data.meta_database_suffix", "_verdict");
        conf.set("verdict.jdbc.schema", "instacart100g");

        VerdictContext vc = VerdictJDBCContext.from(conf);
//        vc.executeJdbcQuery("set verdict.meta_catalog_suffix=_verdict_impala");
//        vc.executeJdbcQuery("use tpch1g");
        //vc.executeJdbcQuery("select count(*) from instacart1g.orders");
//        vc.executeJdbcQuery("select count(*) from instacart1g.orders group by order_hour_of_day");
        vc.executeJdbcQuery("SELECT 5*round(d1/5) AS reorder_after_days, count(*) as c\n" + 
                "FROM (SELECT user_id, avg(days_since_prior) AS d1, count(*) AS order_size\n" + 
                "      FROM order_products INNER JOIN orders\n" + 
                "        ON orders.order_id = order_products.order_id\n" + 
                "      GROUP BY user_id) t2\n" + 
                "     CROSS JOIN\n" + 
                "     (SELECT AVG(c1) AS avg_order_size\n" + 
                "      FROM (SELECT count(*) AS c1\n" + 
                "            FROM order_products INNER JOIN orders\n" + 
                "              ON orders.order_id = order_products.order_id\n" + 
                "            GROUP BY user_id) t1) t3\n" + 
                "WHERE order_size >= avg_order_size\n" + 
                "GROUP BY reorder_after_days\n" + 
                "order by reorder_after_days;\n");

        vc.destroy();
    }

}

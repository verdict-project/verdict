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
        vc.executeJdbcQuery("select order_hour_of_day, t1.department, c / total as ratio\n" + 
                "from\n" + 
                "    (select order_hour_of_day, department, count(*) as c\n" + 
                "    from order_products op, orders o, products p, departments d\n" + 
                "    where op.order_id = o.order_id\n" + 
                "      and op.product_id = p.product_id\n" + 
                "      and p.department_id = d.department_id\n" + 
                "    group by order_hour_of_day, d.department) t1,\n" +
                "    (select department, count(*) as total\n" + 
                "    from order_products op, orders o, products p, departments d\n" + 
                "    where op.order_id = o.order_id\n" + 
                "      and op.product_id = p.product_id\n" + 
                "      and p.department_id = d.department_id\n" + 
                "    group by department) t2\n" + 
                "where t1.department = t2.department\n" + 
                "order by order_hour_of_day, ratio desc;\n");

        vc.destroy();
    }

}

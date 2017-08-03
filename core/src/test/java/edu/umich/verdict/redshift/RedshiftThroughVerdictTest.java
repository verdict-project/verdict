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
		vc.executeJdbcQuery("SELECT product_name, count(*) as order_count "+
							"FROM order_products, orders, products "+
							"WHERE orders.order_id = order_products.order_id "+
							  "AND order_products.product_id = products.product_id "+
							  "AND (order_dow = 0 OR order_dow = 1) "+
							"GROUP BY product_name "+
							"ORDER BY order_count DESC "+
							"LIMIT 5;");
		
		vc.destroy();
	}

}

package edu.umich.verdict.relation;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class Sql2ExactRelationTest {

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setHost("salat1.eecs.umich.edu");
		conf.setPort("21050");
		conf.setDbmsSchema("instacart1g");
		conf.set("no_user_password", "true");
		VerdictContext vc = new VerdictContext(conf);
		
		String sql = "select order_dow, count(*) as c"
				+ " from order_products, orders"
				+ " where order_products.order_id = orders.order_id"
				+ "   AND order_dow = 0 or order_dow = 1"
				+ " group by order_dow";
		String sql2 = "select count(*)"
				+ " from order_products inner join orders on order_products.order_id = orders.order_id"
				+ " where order_dow = 0 or order_dow = 1"
				+ " group by order_dow";
		ExactRelation r = ExactRelation.from(vc, sql2);
		System.out.println(Relation.prettyPrintSql(r.toSql()));
		
		vc.destroy();
	}

}

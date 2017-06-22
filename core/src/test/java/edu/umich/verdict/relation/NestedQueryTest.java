package edu.umich.verdict.relation;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.CompCond;

public class NestedQueryTest {

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setHost("salat1.eecs.umich.edu");
		conf.setPort("21050");
		conf.setDbmsSchema("instacart1g");
		conf.set("no_user_password", "true");
		VerdictContext vc = new VerdictContext(conf);
		
		SingleRelation r1 = SingleRelation.from(vc, "order_products");
		SingleRelation r2 = SingleRelation.from(vc, "orders");
		SingleRelation r3 = SingleRelation.from(vc, "products");
		System.out.println(
				r2.where(CompCond.from("order_number", ">", r2.avg("order_number")))
				.groupby("order_dow")
				.approxAgg("count(distinct user_id) as user_count")
				.orderby("user_count desc")
				.limit(10)
				.collectAsString());
		
		vc.destroy();
	}

}

package edu.umich.verdict.relation;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class BasicAggregationTest {

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setHost("salat1.eecs.umich.edu");
		conf.setPort("21050");
		conf.setDbmsSchema("instacart1g");
		conf.set("no_user_password", "true");
		VerdictContext vc = new VerdictContext(conf);
		
//		TableUniqueName t = TableUniqueName.uname(vc, "orders");
//		SampleParam p = new SampleParam(t, "uniform", 0.01, null);
//		Relation r = SampleRelation.from(vc, p);
//		System.out.println(r.count());
		
		SingleRelation r = SingleRelation.from(vc, "orders");
		System.out.println(r.filter("order_dow = 1").approxCount());
//		ResultSet rs = r.approxAgg(count(), countDistinct("user_id")).collectResultSet();
//		ResultSet rs = r.agg(count()).approx().collectResultSet();
//		ResultSetConversion.printResultSet(rs);
		
		vc.destroy();
	}

}

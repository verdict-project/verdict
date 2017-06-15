package edu.umich.verdict.relation;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;

public class BasicAggregationTest {

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setHost("ec2-54-174-219-128.compute-1.amazonaws.com");
		conf.setPort("21050");
		conf.setDbmsSchema("instacart1g");
		conf.set("no_user_password", "true");
		VerdictContext vc = new VerdictContext(conf);
		
		TableUniqueName t = TableUniqueName.uname(vc, "orders");
		SampleParam p = new SampleParam(t, "uniform", 0.01, null);
		Relation r = SampleRelation.from(vc, p);
		System.out.println(r.count());
		
		vc.destroy();
	}

}

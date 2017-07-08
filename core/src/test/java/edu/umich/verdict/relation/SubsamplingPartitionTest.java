package edu.umich.verdict.relation;

import java.util.Arrays;

import org.junit.Test;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;

public class SubsamplingPartitionTest {

	@Test
	public void SingleRelationTest() throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setHost("salat1.eecs.umich.edu");
		conf.setDbms("impala");
		conf.setPort("21050");
		conf.setDbmsSchema("instacart1g");
		conf.set("no_user_password", "true");
		VerdictContext vc = VerdictContext.from(conf);
		
		TableUniqueName orders = TableUniqueName.uname(vc, "orders");
		SampleParam param = new SampleParam(vc, orders, "uniform", 0.01, Arrays.<String>asList());
		ExactRelation r = ApproxSingleRelation.from(vc, param).rewriteWithPartition();
		
		System.out.println(r.toSql());
		
	}

}

package edu.umich.verdict.relation;

import java.util.Arrays;
import java.util.List;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;

public class ApproxSqlTest {

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setHost("salat1.eecs.umich.edu");
		conf.setDbms("hive2");
		conf.setPort("10000");
		conf.setDbmsSchema("instacart1g");
		conf.set("no_user_password", "true");
		VerdictJDBCContext vc = VerdictJDBCContext.from(conf);
		
//		testSimpleAvgFor("orders", "days_since_prior", "universe", Arrays.<String>asList("order_id"));
		String tableName = "orders";
		String sampleType = "universe";
		String aggCol = "days_since_prior";
		List<String> sampleColumns = Arrays.<String>asList("order_id");
		double samplingRatio = 0.1;
		
		TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
		ApproxRelation r = ApproxSingleRelation.from(vc, new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
		r.avg(aggCol).collectResultSet();
		
		vc.destroy();
	}

}

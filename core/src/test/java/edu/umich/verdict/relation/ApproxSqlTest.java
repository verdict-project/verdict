package edu.umich.verdict.relation;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class ApproxSqlTest {

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setHost("salat1.eecs.umich.edu");
		conf.setDbms("impala");
		conf.setPort("21050");
		conf.setDbmsSchema("instacart1g");
		conf.set("no_user_password", "true");
		VerdictContext vc = new VerdictContext(conf);
		
		ResultSet rs = vc.executeQuery("select count(*) from orders");
		ResultSetConversion.printResultSet(rs);
		
//		testSimpleAvgFor("orders", "days_since_prior", "universe", Arrays.<String>asList("order_id"));
//		String tableName = "orders";
//		String sampleType = "universe";
//		String aggCol = "days_since_prior";
//		List<String> sampleColumns = Arrays.<String>asList("order_id");
//		double samplingRatio = 0.1;
//		
//		TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
//		ApproxRelation r = ApproxSingleRelation.from(vc, new SampleParam(originalTable, sampleType, samplingRatio, sampleColumns));
//		r.avg(aggCol).collectResultSet();
		
		vc.destroy();
	}

}

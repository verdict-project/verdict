package edu.umich.verdict.impala;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class ImpalaCatalogTest {

	public ImpalaCatalogTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws VerdictException {
		
		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setHost("ec2-34-202-126-188.compute-1.amazonaws.com");
		conf.setPort("21050");
		conf.setDbmsSchema("instacart1g");
		VerdictContext vc = new VerdictContext(conf);
		
		ResultSet rs5 = vc.executeQuery("show databases");
		ResultSetConversion.printResultSet(rs5);
		
		ResultSet rs = vc.executeQuery("show tables");
		ResultSetConversion.printResultSet(rs);
		
		ResultSet rs2 = vc.executeQuery("describe aisles");
		ResultSetConversion.printResultSet(rs2);
		
//		vc.executeQuery("use instacart1g");
//		ResultSet rs2 = vc.executeQuery("show tables");
//		ResultSetConversion.printResultSet(rs2);
//		
//		ResultSet rs3 = vc.executeQuery("set meta=\"mymeta\"");
//		ResultSetConversion.printResultSet(rs3);
//		
//		ResultSet rs4 = vc.executeQuery("get meta");
//		ResultSetConversion.printResultSet(rs4);
		
		vc.destroy();

	}

}

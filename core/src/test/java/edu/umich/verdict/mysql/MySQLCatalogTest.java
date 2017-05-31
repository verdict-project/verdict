package edu.umich.verdict.mysql;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class MySQLCatalogTest {

	public MySQLCatalogTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws VerdictException {
		
		VerdictConf conf = new VerdictConf();
		conf.setDbms("mysql");
		conf.setHost("localhost");
		conf.setPort("3306");
		conf.setDbmsSchema("tpch1G");
		conf.setUser("verdict");
		conf.setPassword("verdict");
		VerdictContext vc = new VerdictContext(conf);
//		vc.setLogLevel("DEBUG");
		
		ResultSet rs5 = vc.executeQuery("show databases");
		ResultSetConversion.printResultSet(rs5);
		
		ResultSet rs = vc.executeQuery("show tables");
		ResultSetConversion.printResultSet(rs);
		
//		vc.executeQuery("use instacart1g");
//		ResultSet rs2 = vc.executeQuery("show tables");
//		ResultSetConversion.printResultSet(rs2);
//		
//		ResultSet rs3 = vc.executeQuery("set meta=\"mymeta\"");
//		ResultSetConversion.printResultSet(rs3);
//		
//		ResultSet rs4 = vc.executeQuery("get meta");
//		ResultSetConversion.printResultSet(rs4);
//		
		vc.destroy();

	}

}

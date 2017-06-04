package edu.umich.verdict.mysql;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class MySQLBootstrappingTest {

	public MySQLBootstrappingTest() {
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
		
		String sql = "select l_shipmode, count(*) from lineitem group by l_shipmode order by count(*) desc";
//		String sql = "select count(*) from nation";
//		String sql = "SELECT *, rand() AS verdict_rand FROM tpch1G.sample_tpch1G_nation";
		
		ResultSet rs1 = vc.executeQuery(sql);
		ResultSetConversion.printResultSet(rs1);

	}

}

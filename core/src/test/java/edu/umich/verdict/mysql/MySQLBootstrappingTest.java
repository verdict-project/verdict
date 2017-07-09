package edu.umich.verdict.mysql;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
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
		VerdictJDBCContext vc = VerdictJDBCContext.from(conf);
		
		String sql = "select l_shipmode, count(*) from lineitem group by l_shipmode order by count(*) desc";
		
		String sql2 = "select l_shipmode, avg(l_discount) / avg(l_tax) from lineitem group by l_shipmode";
		
		String sql3 = "select l_shipdate, count(*) as R from lineitem group by l_shipdate order by R desc limit 10";
		
		
		ResultSet rs1 = vc.executeQuery(sql3);
		ResultSetConversion.printResultSet(rs1);

	}

}

package edu.umich.verdict.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class MysqlBasicAggregationTest {

	public static void main(String[] args) throws VerdictException, SQLException {
		
		VerdictConf conf = new VerdictConf();
		conf.setDbms("mysql");
		conf.setHost("localhost");
		conf.setPort("3306");
		conf.setDbmsSchema("instacart1g");
		conf.setUser("verdict");
		conf.setPassword("verdict");
		
		
		VerdictContext vc = new VerdictContext(conf);
		
		ResultSet rs0 = vc.executeQuery("select count(distinct user_id) from orders");
		ResultSetConversion.printResultSet(rs0);

//		ResultSet rs1 = vc.executeQuery("select l_shipmode, avg(l_discount) / avg(l_tax) from lineitem group by l_shipmode");
//		ResultSetConversion.printResultSet(rs1);
		
//		ResultSet rs2 = vc.executeQuery("select c_nationkey, c_name, 1/2+(2+1)*2 from customer group by c_nationkey, c_name limit 5");
//		ResultSetConversion.printResultSet(rs2);
//		
//		ResultSet rs3 = vc.executeQuery("select l_shipdate, count(*) from lineitem group by l_shipdate order by count(*) desc limit 10");
//		ResultSetConversion.printResultSet(rs3);
		
//		ResultSet rs4 = vc.executeQuery("select round(c_nationkey), AVG(round(c_acctbal)) as avg_acctbal, COUNT(*) as ccount from customer group by c_nationkey order by AVG(c_acctbal) DESC");
//		ResultSetConversion.printResultSet(rs4);
		
//		System.out.println("hello2");
//		SQLWarning warning = vc.getDbms().getDbmsConnection().getWarnings();
//		while (warning != null) {
//			System.out.println(String.format("State: %s, Error: %s, Msg: %s",
//					warning.getSQLState(),
//					warning.getErrorCode(),
//					warning.getMessage()));
//			warning = warning.getNextWarning();
//		}
		
		vc.destroy();

	}

}

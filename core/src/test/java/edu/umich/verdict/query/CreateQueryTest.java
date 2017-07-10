package edu.umich.verdict.query;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Scanner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.umich.verdict.BaseIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class CreateQueryTest {
	
	protected static VerdictJDBCContext vc;
	
	@BeforeClass
	public static void connect() throws VerdictException, SQLException, FileNotFoundException {
		final String host = readHost();
		final String port = "21050";
		final String schema = "instacart1g";
		
		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setHost(host);
		conf.setPort(port);
		conf.setDbmsSchema(schema);
		conf.set("no_user_password", "true");
		vc = VerdictJDBCContext.from(conf);
	}
	
	@AfterClass
	public static void destroy() throws VerdictException {
		vc.destroy();
	}

	@Test
	public void externalTablTest() throws VerdictException {
		String sql = "create table mytable (col1 string, col2 double) "
				+ "location hdfs://localhost:1000/data "
				+ "fields separated by \",\" "
				+ "escaped by \"\\\" ";
//				+ "type csv "
//				+ "quoted by \"\\\"\"";
		
		System.out.println(sql);
		vc.executeJdbcQuery(sql);
		
	}
	
	public static String readHost() throws FileNotFoundException {
		ClassLoader classLoader = BaseIT.class.getClassLoader();
		File file = new File(classLoader.getResource("integration_test_host.test").getFile());
		
		Scanner scanner = new Scanner(file);
		String line = scanner.nextLine();
		scanner.close();
		return line;
	}

}

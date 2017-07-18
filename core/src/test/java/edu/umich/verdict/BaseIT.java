package edu.umich.verdict;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Scanner;

public class BaseIT {
	
	protected double error = 0.05;
	
	protected double samplingRatio = 0.1;
	
	protected static Connection conn;
	
	protected static Statement stmt;
	
	protected static VerdictJDBCContext vc;
	
	public static String readHost() throws FileNotFoundException {
		ClassLoader classLoader = BaseIT.class.getClassLoader();
		File file = new File(classLoader.getResource("integration_test_host.test").getFile());
		
		Scanner scanner = new Scanner(file);
		String line = scanner.nextLine();
		scanner.close();
		return line;
	}
}

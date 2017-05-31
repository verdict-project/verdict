package edu.umich.verdict.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ImpalaJdbcTest {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub

		Class.forName("com.cloudera.impala.jdbc4.Driver");

		String url = "jdbc:impala://ec2-34-202-126-188.compute-1.amazonaws.com:21050/instacart1g";
		Connection conn = DriverManager.getConnection(url);
		Statement stmt = conn.createStatement();

		String sql = "select * from orders limit 10";
		ResultSet rs = stmt.executeQuery(sql);
		
		int cols = rs.getMetaData().getColumnCount();
		
		// print column name
		for (int i = 1; i <= cols; i++) {
			System.out.print(rs.getMetaData().getColumnName(i) + " ("
							 + rs.getMetaData().getColumnTypeName(i) + ")\t");
		}
		System.out.println();;
		
		// print table
		while(rs.next()) {
			for (int i = 1; i <= cols; i++) {
				System.out.print(rs.getString(i) + "\t");
			}
			System.out.println();
		}

		rs.close();
		stmt.close();
		conn.close();
	}

}

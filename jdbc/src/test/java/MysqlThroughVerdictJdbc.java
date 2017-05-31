import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MysqlThroughVerdictJdbc {

	public static void main(String args[]) throws ClassNotFoundException, SQLException {
		
		Class.forName("edu.umich.verdict.jdbc.Driver");

		String url = "jdbc:verdict:mysql://localhost:3306/mysql";
		Properties properties = new Properties();
		properties.setProperty("user", "root");
		Connection conn = DriverManager.getConnection(url, properties);
		Statement stmt = conn.createStatement();

		String sql = "show databases;";
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

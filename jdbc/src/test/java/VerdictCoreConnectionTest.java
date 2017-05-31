import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class VerdictCoreConnectionTest {
	
	public static void main(String args[]) throws ClassNotFoundException, SQLException {
		
		Class.forName("edu.umich.verdict.jdbc.Driver");
		
		String url = "jdbc:verdict:impala://localhost:3306/hue";
		Connection conn = DriverManager.getConnection(url);
		Statement stmt = conn.createStatement();
		
		String sql = "select * from table";
		ResultSet rs = stmt.executeQuery(sql);
		
		while(rs.next()) {
			int cols = rs.getMetaData().getColumnCount();

			for (int i = 0; i < cols; i++) {
				System.out.print(rs.getString(i) + " ");
			}
			System.out.println();
		}

		rs.close();
		stmt.close();
		conn.close();
	}

}

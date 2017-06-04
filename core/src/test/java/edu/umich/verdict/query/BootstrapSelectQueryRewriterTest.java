package edu.umich.verdict.query;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;

public class BootstrapSelectQueryRewriterTest {

	public BootstrapSelectQueryRewriterTest() {
	}

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("mysql");
		conf.setHost("localhost");
		conf.setPort("3306");
		conf.setDbmsSchema("instacart1g");
		conf.setUser("verdict");
		conf.setPassword("verdict");
		VerdictContext vc = new VerdictContext(conf);
		
		String sql = "select l_shipmode, count(*) from lineitem, orders"
				+ " where l_orderkey = o_orderkey"
				+ " group by l_shipmode";
		
		String sql1 = "select order_hour_of_day, count(*) as c from orders"
				+ " group by order_hour_of_day"
				+ " order by order_hour_of_day";
		
		String sql3 = "SELECT product_name, count(*) as order_count"
				+ " FROM order_products, orders, products"
				+ " WHERE orders.order_id = order_products.order_id"
				+ "   AND order_products.product_id = products.product_id"
				+ "   AND (order_dow = 0 OR order_dow = 1)"
				+ " GROUP BY product_name"
				+ " ORDER BY order_count DESC"
				+ " LIMIT 5";
		
		String sql4 = "SELECT departments.department_id, department, count(*) as order_count"
				+ " FROM order_products, orders, products, departments"
				+ " WHERE orders.order_id = order_products.order_id"
				+ "   AND order_products.product_id = products.product_id"
				+ "   AND products.department_id = departments.department_id"
				+ " GROUP BY department_id, department"
				+ " ORDER BY order_count DESC"
				+ " LIMIT 5;";
		
		String sql5 = "SELECT 5*round(d1/5) as reorder_after_days, COUNT(*)"
				+ " FROM (SELECT user_id, AVG(days_since_prior) AS d1, COUNT(*) AS c2"
				+ "       FROM order_products, orders"
				+ "       WHERE orders.order_id = order_products.order_id"
				+ "         AND days_since_prior IS NOT NULL"
				+ "       GROUP BY user_id) t2"
				+ " WHERE c2 > (SELECT AVG(c1) AS a1"
				+ "             FROM (SELECT user_id, COUNT(*) AS c1"
				+ "                   FROM orders, order_products"
				+ "                   WHERE orders.order_id = order_products.order_id"
				+ "                   GROUP BY user_id) t1)"
				+ " group by reorder_after_days"
				+ " order by reorder_after_days";
		
		String sql6 = "SELECT product_name, count(*) as freq_order_count"
				+ " FROM order_products, orders, products, departments"
				+ " WHERE orders.order_id = order_products.order_id"
				+ "   AND order_products.product_id = products.product_id"
				+ "   AND products.department_id = departments.department_id"
				+ "   AND user_id IN ("
				+ "           SELECT user_id"
				+ "           FROM (SELECT user_id, count(*) as order_count"
				+ "                 FROM orders"
				+ "                 GROUP BY user_id) t1"
				+ "           WHERE order_count > (SELECT AVG(order_count) as large_order_count"
				+ "                                FROM (SELECT user_id, count(*) as order_count"
				+ "                                      FROM orders"
				+ "                                      GROUP BY user_id) t2))"
				+ " GROUP BY product_name"
				+ " ORDER BY freq_order_count DESC"
				+ " LIMIT 10;"; 

		System.out.println(getRewritten(vc, sql1));
	}
	
	static String getRewritten(VerdictContext vc, String sql) {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(sql));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		
//		VerdictApproximateSelectStatementVisitor visitor = new VerdictApproximateSelectStatementVisitor(vc, sql);
		
		BootstrapSelectStatementRewriter visitor = new BootstrapSelectStatementRewriter(vc, sql);
		String rewritten = visitor.visit(p.select_statement());
		
		return rewritten;
	}

}

package edu.umich.verdict.query;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.query.VerdictApproximateSelectQuery;

public class VerdictApproximateSelectQueryTest {

	public VerdictApproximateSelectQueryTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("mysql");
		conf.setHost("localhost:3306");
		conf.setDbmsSchema("instacart1g");
		conf.set("user", "verdict");
		conf.set("password", "verdict");
		VerdictContext vc = new VerdictContext(conf);
		
		
		String sql1 = "select count(*) from orders";
		VerdictApproximateSelectQuery sq = new VerdictApproximateSelectQuery(sql1, vc);
		System.out.println(sq.rewriteQuery());
		
		String sql2 = "select user_id, count(*) from orders group by user_id";
		sq = new VerdictApproximateSelectQuery(sql2, vc);
		System.out.println(sq.rewriteQuery());
		
		String sql3 = "select order_hour_of_day, count(*) as c from orders"
				+ " group by order_hour_of_day"
				+ " order by order_hour_of_day";
		sq = new VerdictApproximateSelectQuery(sql3, vc);
		System.out.println(sq.rewriteQuery());
		
		String sql4 = "SELECT product_name, count(*) as order_count"
				+ "				FROM orders, order_products, products"
				+ "				WHERE orders.order_id = order_products.order_id"
				+ "				  AND order_products.product_id = products.product_id"
				+ "				  AND (order_dow = 0 OR order_dow = 1)"
				+ "				GROUP BY product_name"
				+ "				ORDER BY order_count DESC"
				+ "				LIMIT 5";
		sq = new VerdictApproximateSelectQuery(sql4, vc);
		System.out.println(sq.rewriteQuery());
		
		String sql5 = "SELECT departments.department_id, department, count(*) as order_count"
				+ " FROM orders, order_products, products, departments"
				+ " WHERE orders.order_id = order_products.order_id"
				+ "   AND order_products.product_id = products.product_id"
				+ "   AND products.department_id = departments.department_id"
				+ " GROUP BY department_id, department"
				+ " ORDER BY order_count DESC";
		sq = new VerdictApproximateSelectQuery(sql5, vc);
		System.out.println(sq.rewriteQuery());
		
		String sql6 = "SELECT 5*round(d1/5) as reorder_after_days, COUNT(*)"
				+ " FROM (SELECT user_id, AVG(days_since_prior) AS d1, COUNT(*) AS c2"
				+ "       FROM orders, order_products"
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
		sq = new VerdictApproximateSelectQuery(sql6, vc);
		System.out.println(sq.rewriteQuery());
		
		String sql7 = "SELECT product_name, count(*) as freq_order_count"
		+ " FROM orders, order_products, products, departments"
		+ " WHERE orders.order_id = order_products.order_id"
		+ "	  AND order_products.product_id = products.product_id"
		+ "	  AND products.department_id = departments.department_id"
		+ "	  AND user_id IN ("
		+ "		          SELECT user_id"
		+ "		          FROM (SELECT user_id, count(*) as order_count"
		+ "						FROM orders"
		+ "						GROUP BY user_id) t1"
		+ "		          WHERE order_count > (SELECT AVG(order_count) * 5 as large_order_count"
		+ "		                               FROM (SELECT user_id, count(*) as order_count"
		+ "											 FROM orders"
		+ "											 GROUP BY user_id) t2 ))"
		+ "		GROUP BY product_name"
		+ "		ORDER BY freq_order_count DESC"
		+ "		LIMIT 10";
		sq = new VerdictApproximateSelectQuery(sql7, vc);
		System.out.println(sq.rewriteQuery());
	}

}

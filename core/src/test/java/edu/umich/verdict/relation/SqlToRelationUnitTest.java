package edu.umich.verdict.relation;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.util.StackTraceReader;

public class SqlToRelationUnitTest {
	
	static VerdictContext vc;
	
	@BeforeClass
	public static void connect() throws VerdictException, SQLException, FileNotFoundException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("dummy");
		vc = new VerdictContext(conf);
	}

	@Test
	public void singleAggregationTest() {
		String sql = "SELECT COUNT(*) FROM orders";

		ExactRelation r = ExactRelation.from(vc, sql);
		assertTrue(r instanceof ProjectedRelation);

		ProjectedRelation r1 = (ProjectedRelation) r; 
		assertTrue(r1.getSource() instanceof AggregatedRelation);

		AggregatedRelation r2 = (AggregatedRelation) r1.getSource();
		assertTrue(r2.getSource() instanceof SingleRelation);
	}
	
	@Test
	public void nestedRelationTest() {
		String sql = "select a, b, s.c, t.c "
				+ "from (select a, b, c from mytable) s, "
				+ "     t "
				+ "where s.col = t.col";

		ExactRelation r = ExactRelation.from(vc, sql);
		System.out.println(r.toSql());
	}
	
	@Test
	public void nestedRelationTest2() {
		String sql = "select col1, col2, s.col3, t.col3 "
				+ "from (select col1, col2, col3 from mytable) s, "
				+ "     t AS t1 "
				+ "where s.col = t1.col";

		ExactRelation r = ExactRelation.from(vc, sql);
		System.out.println(r);
	}
	
	@Test
	public void nestedRelationWithSubqueryTest() {
		String sql = "select col1, col2, s.col3, t.col3 "
				+ "from (select col1, col2, col3 from mytable) s, "
				+ "     t AS t1 "
				+ "where s.col = t1.col AND "
				+ "      t.col1 > (select avg(col1) from t)";

		ExactRelation r = ExactRelation.from(vc, sql);
		System.out.println(r);
		System.out.println(r.toSql());
		System.out.println(Relation.prettyfySql(r.toSql()));
	}
	
	@Test
	public void multipleJoinTest() {
		String sql = "select g, count(*) "
			 	   + "from t1, t2, t3, t4 "
			 	   + "where t1.col = t2.col AND "
			 	   + "      t1.col2 = t3.col2 AND "
			 	   + "      t1.col3 = t4.col3 AND "
			 	   + "      t1.col4 = 'air'";

		ExactRelation r = ExactRelation.from(vc, sql);
		System.out.println(r);
		System.out.println(r.toSql());
		System.out.println(Relation.prettyfySql(r.toSql()));
	}
	
	@Test
	public void selectFunctionsWithCommaTest() {
		ExactRelation r = SingleRelation.from(vc, "mytable");
		r = r.select("a, b, c, pmod(cast(user_id as string), 100) AS __vpart");
		System.out.println(r.toSql());
	}
	
	@Test
	public void selectFunctionsWithCommaTest2() {
		ExactRelation r = SingleRelation.from(vc, "mytable");
		r = r.select("*, 1 AS one, count(*) OVER () AS `__total_size`");
		System.out.println(r.toSql());
	}
	
	@Test
	public void complexTest1() {
		String sql = " SELECT `vt12`.`order_hour_of_day`, " +
					"        round(avg(`vt12`.`expr1`)) AS `expr1`, " +
					"        ((stddev(`vt12`.`expr1`) * sqrt(avg(`vt12`.`__vpsize`))) / sqrt(sum(`vt12`.`__vpsize`))) AS `expr1_err`  " +
					" FROM ( " +
					"   SELECT `vt5`.`order_hour_of_day`, " +
					"          `vt5`.`__vpart`, " +
					"          (count(distinct `user_id`) * (1.0 / 0.1)) AS `expr1`, " +
					"          count(*) AS `__vpsize`  " +
					"   FROM ( " +
					"     SELECT `order_id`, " +
					"            `user_id`, " +
					"            `eval_set`, " +
					"            `order_number`, " +
					"            `order_dow`, " +
					"            `order_hour_of_day`, " +
					"            `days_since_prior`, " +
					"            `__vprob`, " +
					"            pmod(crc32(`user_id`), 100) AS `__vpart`  " +
					"     FROM vs_orders_uv_0_1000_user_id vt9) vt5 " +
					"   GROUP BY `vt5`.`order_hour_of_day`, `vt5`.`__vpart`) vt12 " +
					" GROUP BY `vt12`.`order_hour_of_day` " +
					" ORDER BY `order_hour_of_day`";
		ExactRelation r = ExactRelation.from(vc, sql);
		System.out.println(r.toSql());
		System.out.println(Relation.prettyfySql(r.toSql()));
	}

}

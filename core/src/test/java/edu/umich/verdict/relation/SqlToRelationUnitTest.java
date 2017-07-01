package edu.umich.verdict.relation;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StackTraceReader;

public class SqlToRelationUnitTest {

	@Test
	public void singleAggregation() {
		try {
			VerdictConf conf = new VerdictConf();
			conf.setDbms("dummy");
			VerdictContext vc = new VerdictContext(conf);
			
			String sql = "SELECT COUNT(*) FROM orders";
			
			ExactRelation r = ExactRelation.from(vc, sql);
			assertTrue(r instanceof ProjectedRelation);
			
			ProjectedRelation r1 = (ProjectedRelation) r; 
			assertTrue(r1.getSource() instanceof AggregatedRelation);
			
			AggregatedRelation r2 = (AggregatedRelation) r1.getSource();
			assertTrue(r2.getSource() instanceof SingleRelation);
			
		} catch (VerdictException e) {
			System.out.println(StackTraceReader.stackTrace2String(e));
			assert(false);
		}
	}
	
	@Test
	public void test2() throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("dummy");
		VerdictContext vc = new VerdictContext(conf);
		
		Relation tableWithRand = SingleRelation.from(vc, "table1")
				 .join(SingleRelation.from(vc, "table2"),
				 	   Joiner.on(" AND ").join(Arrays.asList("col1 = col1")))
				 .select("a, b, c, verdict_grp_size, rand(unix_timestamp()) as verdict_rand");
		
		System.out.println(tableWithRand.toSql());
	}

}

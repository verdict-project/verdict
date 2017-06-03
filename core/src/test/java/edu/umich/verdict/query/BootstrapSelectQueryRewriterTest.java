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
		conf.setDbmsSchema("tpch1G");
		conf.setUser("verdict");
		conf.setPassword("verdict");
		VerdictContext vc = new VerdictContext(conf);
		
		String sql = "select l_shipmode, count(*) from lineitem, orders"
				+ " where l_orderkey = o_orderkey"
				+ " group by l_shipmode";
		
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(sql));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		
//		VerdictApproximateSelectStatementVisitor visitor = new VerdictApproximateSelectStatementVisitor(vc, sql);
		
		BootstrapSelectStatementRewriter visitor = new BootstrapSelectStatementRewriter(vc, sql);
		String rewritten = visitor.visit(p.select_statement().query_expression().query_specification());
		
		System.out.println(rewritten);
	}

}

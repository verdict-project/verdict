package edu.umich.verdict.query;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;

public class VerdictBootstrappingSelectStatementVisitorTest {

	public VerdictBootstrappingSelectStatementVisitorTest() {
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
		
		String sql = "select count(*) from lineitem, orders where lineitem.l_orderkey = orders.o_orderkey";
		
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(sql));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		
//		VerdictApproximateSelectStatementVisitor visitor = new VerdictApproximateSelectStatementVisitor(vc, sql);
		
		VerdictBootstrappingSelectStatementVisitor visitor = new VerdictBootstrappingSelectStatementVisitor(vc, sql);
		String rewritten = visitor.visit(p.select_statement().query_expression().query_specification());
		
		System.out.println(rewritten);
	}

}

package edu.umich.verdict.query;

import java.sql.ResultSet;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

/**
 * This class is responsible to choose the best method to handle a given SQL query.
 * 
 * Currently, we have only one method that relies on analytic computations as much as possible.
 * 
 * @author Yongjoo Park
 *
 */
public class VerdictQuery {

	protected final String queryString;

	protected final VerdictContext vc;

	public enum Type {
		SELECT, CREATE_SAMPLE, DROP_SAMPLE, SHOW_SAMPLE, CONFIG, DESCRIBE_TABLE,
		OTHER_USE, OTHER_SHOW_TABLES, OTHER_SHOW_DATABASES, NOSUPPORT;
	}

	/**
	 * This class should be instantiated by Query.
	 * @param q query string
	 * @param vc
	 */
	public VerdictQuery(String q, VerdictContext vc) {
		queryString = q;
		this.vc = vc;
	}

	public String getQueryString() {
		return queryString;
	}

	public ResultSet compute() throws VerdictException {
		VerdictQuery query = null;
		Type queryType = getStatementType();
		VerdictLogger.debug(this, String.format("A query type: %s", queryType.toString()));
		
		if (queryType.equals(Type.CONFIG)) {
			query = new VerdictConfigQuery(this);
		} else {
			if (vc.getConf().doesContain("bypass") && vc.getConf().getBoolean("bypass")) {
				VerdictLogger.info("Verdict bypasses this query. Run \"set bypass=\'false\'\""
						+ " to enable Verdict's approximate query processing.");
				query = new ByPassVerdictQuery(this);
			} else {
				if (queryType.equals(Type.SELECT)) {
					query = new VerdictSelectQuery(this);
				} else if (queryType.equals(Type.CREATE_SAMPLE)) {
					query = new VerdictCreateSampleQuery(this);
				} else if (queryType.equals(Type.DROP_SAMPLE)) {
					query = new VerdictDropSampleQuery(this);
				} else if (queryType.equals(Type.SHOW_SAMPLE)) {
					query = new VerdictShowSampleQuery(this);
				} else if (queryType.equals(Type.DESCRIBE_TABLE)) {
					query = new VerdictDescribeTableQuery(this);
				} else if (queryType.equals(Type.OTHER_USE)) {
					query = new VerdictOtherUseQuery(this);
				} else if (queryType.equals(Type.OTHER_SHOW_TABLES)) {
					query = new VerdictOtherShowTablesQuery(this);
				} else if (queryType.equals(Type.OTHER_SHOW_DATABASES)) {
					query = new VerdictOtherShowDatabasesQuery(this);
				} else {
					VerdictLogger.error(this, "Unsupported query: " + queryString);
					throw new VerdictException("Unsupported query.");
				}				
			}
		}

		return query.compute();
	}

	protected Type getStatementType() {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(queryString));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));

		VerdictSQLBaseVisitor<Type> visitor = new VerdictSQLBaseVisitor<Type>() {
			private Type type = Type.NOSUPPORT;

			protected Type defaultResult() { return type; }

			@Override
			public Type visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {
				type = Type.SELECT;
				return type;
			}

			@Override
			public Type visitCreate_sample_statement(VerdictSQLParser.Create_sample_statementContext ctx) {
				type = Type.CREATE_SAMPLE;
				return type;
			} 

			@Override
			public Type visitDelete_sample_statement(VerdictSQLParser.Delete_sample_statementContext ctx) {
				type = Type.DROP_SAMPLE;
				return type;
			}
			
			@Override
			public Type visitShow_samples_statement(VerdictSQLParser.Show_samples_statementContext ctx) {
				type = Type.SHOW_SAMPLE;
				return type;
			}
			
			@Override
			public Type visitDescribe_table_statement(VerdictSQLParser.Describe_table_statementContext ctx) {
				type = Type.DESCRIBE_TABLE;
				return type;
			}

			@Override
			public Type visitUse_statement(VerdictSQLParser.Use_statementContext ctx) {
				type = Type.OTHER_USE;
				return type;
			}

			@Override
			public Type visitShow_tables_statement(VerdictSQLParser.Show_tables_statementContext ctx) {
				type = Type.OTHER_SHOW_TABLES;
				return type;
			}

			@Override
			public Type visitConfig_statement(VerdictSQLParser.Config_statementContext ctx) {
				type = Type.CONFIG;
				return type;
			}
			
			@Override
			public Type visitShow_databases_statement(VerdictSQLParser.Show_databases_statementContext ctx) {
				type = Type.OTHER_SHOW_DATABASES;
				return type;
			}
		};

		return visitor.visit(p.verdict_statement());
	}

}

class VerdictSQLBaseVisitorWithMsg<T> extends VerdictSQLBaseVisitor<T> {
	public String err_msg = null;

	protected String getOriginalText(ParserRuleContext ctx, String queryString) {
		int a = ctx.start.getStartIndex();
		int b = ctx.stop.getStopIndex();
		Interval interval = new Interval(a,b);
		return CharStreams.fromString(queryString).getText(interval);
	}
}

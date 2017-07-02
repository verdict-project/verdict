package edu.umich.verdict.query;

import java.sql.ResultSet;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.Alias;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

/**
 * This class is responsible to choose the best method to handle a given SQL query.
 * 
 * @author Yongjoo Park
 *
 */
public abstract class Query {

	protected final String queryString;

	protected final VerdictContext vc;

	public enum Type {
		SELECT, CREATE_SAMPLE, DROP_SAMPLE, SHOW_SAMPLE, CONFIG, DESCRIBE_TABLE,
		OTHER_USE, OTHER_SHOW_TABLES, OTHER_SHOW_DATABASES, NOSUPPORT, OTHER_REFRESH,
		CREATE_TABLE, CREATE_TABLE_AS_SELECT, DROP_TABLE, DELETE_FROM, CREATE_VIEW, DROP_VIEW
	}

	/**
	 * This class should be instantiated by Query.
	 * @param q query string
	 * @param vc
	 */
	public Query(VerdictContext vc, String q) {
		queryString = q;
		this.vc = vc;
		vc.incrementQid();
//		vc.getMeta().clearSampleInfo();
		Alias.resetAliasIndex();
	}

	public String getQueryString() {
		return queryString;
	}

	/**
	 * This is the main entry point for all Verdict queries. Whether a given query expects a result set to be returned
	 * will be determined by analyzing the query itself. For instance, "CREATE TABLE" query should not expect any result
	 * set to be returned, while "SELECT ..." query expects a result set.
	 * 
	 * Verdict recognizes the following types of queries.
	 * A. Types of the queries that expect a result set:
	 *   1. SELECT ...
	 *   2. DESCRIBE TABLE
	 *   3. USE DATABASE (empty result set)
	 *   4. SHOW TABLES
	 *   5. SHOW DATABASES
	 *   6. SHOW SAMPLE
	 * 
	 * B. Types of the queries that expect updates.
	 * 1. CREAET SAMPLE
	 * 2. DROP SAMPLE
	 * 3. CREATE TABLE (AS SELECT) (TODO)
	 * 4. DROP TABLE  (TODO)
	 * 5. DELETE FROM (TODO)
	 * 6. CREATE VIEW (TODO)
	 * 7. DROP VIEW   (TODO)
	 * 
	 * @return ResultSet if the query belongs to the type A, otherwise return null.
	 * @throws VerdictException
	 */
	public abstract ResultSet compute() throws VerdictException;
	
	public static Query getInstance(VerdictContext vc, String queryString) throws VerdictException {
		Query query = null;
		Type queryType = getStatementType(queryString);
		VerdictLogger.debug(Query.class, String.format("[%d] A query type: %s", vc.getQid(), queryType.toString()));
		
		if (queryType.equals(Type.CONFIG)) {
			query = new ConfigQuery(vc, queryString);
		} else {
			if (vc.getConf().doesContain("bypass") && vc.getConf().getBoolean("bypass")) {
				VerdictLogger.info("Verdict bypasses this query. Run \"set bypass=\'false\'\""
						+ " to enable Verdict's approximate query processing.");
				if (isUpdateType(queryType)) {
					query = new ByPassVerdictUpdateQuery(vc, queryString);
				} else {
					query = new ByPassSelectQuery(vc, queryString);
				}
			} else {
				if (queryType.equals(Type.SELECT)) {
//					query = SelectQuery.getInstance(vc, queryString);
					query = new SelectQuery(vc, queryString);
				} else if (queryType.equals(Type.CREATE_SAMPLE)) {
					query = new CreateSampleQuery(vc, queryString);
				} else if (queryType.equals(Type.DROP_SAMPLE)) {
					query = new DropSampleQuery(vc, queryString);
				} else if (queryType.equals(Type.SHOW_SAMPLE)) {
					query = new ShowSamplesQuery(vc, queryString);
				} else if (queryType.equals(Type.DESCRIBE_TABLE)) {
					query = new DescribeTableQuery(vc, queryString);
				} else if (queryType.equals(Type.OTHER_USE)) {
					query = new UseDatabaseQuery(vc, queryString);
				} else if (queryType.equals(Type.OTHER_SHOW_TABLES)) {
					query = new ShowTablesQuery(vc, queryString);
				} else if (queryType.equals(Type.OTHER_SHOW_DATABASES)) {
					query = new ShowDatabasesQuery(vc, queryString);
				} else if (queryType.equals(Type.OTHER_REFRESH)) {
					query = new RefreshQuery(vc, queryString);
//				} else if (queryType.equals(Type.CREATE_TABLE)) {
//					query = new CreateTableQuery(vc, queryString);
				} else if (queryType.equals(Type.CREATE_TABLE) ||
						   queryType.equals(Type.DROP_TABLE) ||
						   queryType.equals(Type.DELETE_FROM) ||
						   queryType.equals(Type.DROP_VIEW)) {
					query = new ByPassVerdictUpdateQuery(vc, queryString);
				} else if (queryType.equals(Type.CREATE_TABLE_AS_SELECT)) {
					query = new CreateTableAsSelectQuery(vc, queryString);
				} else if (queryType.equals(Type.CREATE_VIEW)) {
					query = new CreateViewAsSelectQuery(vc, queryString);
				} else {
					VerdictLogger.error("Unsupported query: " + queryString);
					throw new VerdictException("Unsupported query.");
				}				
			}
		}

		return query;
	}
	
	protected static boolean isUpdateType(Type type) {
		if (type.equals(Type.SELECT) || type.equals(Type.SHOW_SAMPLE) || type.equals(Type.DESCRIBE_TABLE)
		 || type.equals(Type.OTHER_USE) || type.equals(Type.OTHER_SHOW_TABLES) || type.equals(Type.OTHER_SHOW_DATABASES)) {
			return false;
		} else {
			return true;
		}
	}

	protected static Type getStatementType(String queryString) {
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
			public Type visitRefresh_statement(VerdictSQLParser.Refresh_statementContext ctx) {
				type = Type.OTHER_REFRESH;
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
			
			@Override
			public Type visitCreate_table(VerdictSQLParser.Create_tableContext ctx) {
				type = Type.CREATE_TABLE;
				return type;
			}
			
			@Override
			public Type visitCreate_table_as_select(VerdictSQLParser.Create_table_as_selectContext ctx) {
				type = Type.CREATE_TABLE_AS_SELECT;
				return type;
			}
			
			@Override
			public Type visitDrop_table(VerdictSQLParser.Drop_tableContext ctx) {
				type = Type.DROP_TABLE;
				return type;
			}
			
			@Override
			public Type visitDelete_statement(VerdictSQLParser.Delete_statementContext ctx) {
				type = Type.DELETE_FROM;
				return type;
			}
			
			@Override
			public Type visitCreate_view(VerdictSQLParser.Create_viewContext ctx) {
				type = Type.CREATE_VIEW;
				return type;
			}
			
			@Override
			public Type visitDrop_view(VerdictSQLParser.Drop_viewContext ctx) {
				type = Type.DROP_VIEW;
				return type;
			}
		};

		return visitor.visit(p.verdict_statement());
	}

}


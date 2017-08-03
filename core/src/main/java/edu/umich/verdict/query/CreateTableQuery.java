package edu.umich.verdict.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.VerdictLogger;

public class CreateTableQuery extends Query {

	public CreateTableQuery(VerdictJDBCContext vc, String q) {
		super(vc, q);
	}

	@Override
	public void compute() throws VerdictException {
		VerdictLogger.error(this, "Not supported.");
//		VerdictSQLLexer l = new VerdictSQLLexer(new ANTLRInputStream(queryString));
//		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
//		CreateTableQueryParser parser = new CreateTableQueryParser();
//		parser.visit(p.create_table());
//		
//		// read parameters and print them for test
//		System.out.println("table name: " + parser.getTableName());
//		
//		for (Pair<String, String> colDef : parser.getColumnDefinitions()) {
//			System.out.println(String.format("col name: %s, col type: %s", colDef.getKey(), colDef.getValue()));
//		}
//		
//		for (Map.Entry<String, String> e : parser.getOptions().entrySet()) {
//			System.out.println(String.format("option: %s, value: %s", e.getKey(), e.getValue()));
//		}
//		
//		return null;
	}

}

class CreateTableQueryParser extends VerdictSQLBaseVisitor<Void> {
	
	private String tableName;
	
	private List<Pair<String, String>> columnDefinitions;
	
	private Map<String, String> options;
	
	public CreateTableQueryParser() {
		columnDefinitions = new ArrayList<Pair<String, String>>();
		options = new HashMap<String, String>();
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public List<Pair<String, String>> getColumnDefinitions() {
		return columnDefinitions;
	}
	
	public Map<String, String> getOptions() {
		return options;
	}
	
	@Override
	public Void visitCreate_table(VerdictSQLParser.Create_tableContext ctx) {
		tableName = ctx.table_name().getText();
		System.out.println(ctx.getText());
		return visitChildren(ctx);
	}
	
	@Override
	public Void visitColumn_definition(VerdictSQLParser.Column_definitionContext ctx) {
		String colName = ctx.column_name().getText();
		String type = ctx.data_type().getText();
		columnDefinitions.add(Pair.of(colName, type));
		return visitChildren(ctx);
	}
	
//	@Override
//	public Void visitCreate_table_option(VerdictSQLParser.Create_table_optionContext ctx) {
//		String key = ctx.table_option_key.getText();
//		String value = ctx.table_option_value.getText();
//		options.put(key, value);
//		return visitChildren(ctx);
//	}
	
}
package edu.umich.verdict.query;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.VerdictResultSet;
import edu.umich.verdict.exceptions.VerdictException;


public class ConfigQuery extends SelectQuery {

	public ConfigQuery(String q, VerdictContext vc) {
		super(q, vc);
		// TODO Auto-generated constructor stub
	}

	public ConfigQuery(Query parent) {
		super(parent.queryString, parent.vc);
	}

	@Override
	public ResultSet compute() throws VerdictException {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(queryString));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));

		VerdictSQLBaseVisitor<Pair<String, String>> visitor = new VerdictSQLBaseVisitor<Pair<String, String>>() {
			private Pair<String, String> keyValue;

			protected Pair<String, String> defaultResult() { return keyValue; }
			
			@Override
			public Pair<String, String> visitConfig_get_statement(VerdictSQLParser.Config_get_statementContext ctx) {
				keyValue = Pair.of(ctx.config_key().getText(), null);
				return keyValue;
			}
			
			@Override
			public Pair<String, String> visitConfig_set_statement(VerdictSQLParser.Config_set_statementContext ctx) {
				keyValue = Pair.of(ctx.config_key().getText(),
						ctx.config_value().getText().replaceAll("^\"|\"$|^\'|\'$", ""));
				return keyValue;
			}
		};
		
		Pair<String, String> keyValue = visitor.visit(p.config_statement());
		List<String> row = new ArrayList<String>();
		
		if (keyValue.getRight() == null) {
			// get statement
			String value = vc.getConf().get(keyValue.getLeft());
			row.add(keyValue.getLeft());
			row.add(value);
		} else {
			// set statement
			vc.getConf().set(keyValue.getKey(), keyValue.getValue());
			row.add(keyValue.getLeft());
			row.add(keyValue.getRight());
		}
		
		// To get a ResultSet, we temporarily create a table
		List<List<String>> data = new ArrayList<List<String>>();
		data.add(row);
		
		List<String> meta = new ArrayList<String>();
		meta.add("conf_key");
		meta.add("conf_value");
		
		return VerdictResultSet.fromList(data, meta);
	}
}

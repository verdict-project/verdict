package edu.umich.verdict.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.datatypes.VerdictResultSet;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;


public class ConfigQuery extends SelectQuery {

	public ConfigQuery(VerdictContext vc, String q) {
		super(vc, q);
	}

	@Override
	public void compute() throws VerdictException {
		VerdictSQLParser p = StringManipulations.parserOf(queryString);

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
			
			if (vc.getDbms().isJDBC()) {
				// To get a ResultSet, we temporarily create a table
				List<List<String>> data = new ArrayList<List<String>>();
				data.add(row);
				
				List<String> meta = new ArrayList<String>();
				meta.add("conf_key");
				meta.add("conf_value");
				
				rs = VerdictResultSet.fromList(data, meta);
			} else if (vc.getDbms().isSpark()) {
				VerdictLogger.warn(this, "DataFrame is not generated for Spark yet. The key-value pair is " + keyValue.toString());
			}
		} else {
			// set statement
			vc.getConf().set(keyValue.getKey(), keyValue.getValue());
			row.add(keyValue.getLeft());
			row.add(keyValue.getRight());
		}
	}
}

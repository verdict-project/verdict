package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class ShowSamplesQuery extends SelectQuery {

	public ShowSamplesQuery(VerdictContext vc, String q) {
		super(vc, q);
	}

	@Override
	public void compute() throws VerdictException {
		VerdictSQLParser p = StringManipulations.parserOf(queryString);
		VerdictSQLBaseVisitor<String> visitor = new VerdictSQLBaseVisitor<String>() {
			@Override public String visitShow_samples_statement(VerdictSQLParser.Show_samples_statementContext ctx) {
				String database = null;
				if (ctx.database != null) {
					database = ctx.database.getText();
				}
				return database;
			}
		};
		String database = visitor.visit(p.show_samples_statement());
		database = (database != null)? database : ( (vc.getCurrentSchema().isPresent())? vc.getCurrentSchema().get() : null );
		
		if (database == null) {
			VerdictLogger.info("No table specified; cannot show samples");
		} else {
			ExactRelation nameTable = SingleRelation.from(vc, vc.getMeta().getMetaNameTableForOriginalSchema(database));
			nameTable.setAlias("s");
			ExactRelation sizeTable = SingleRelation.from(vc, vc.getMeta().getMetaSizeTableForOriginalSchema(database));
			sizeTable.setAlias("t");
			
			Relation info = nameTable.join(sizeTable, "s.sampleschemaaname = t.schemaname AND s.sampletablename = t.tablename")
								     .select("s.originaltablename AS \"Original Table\","
									 	   + " s.sampletype AS \"Sample Type\","
									 	   + " t.schemaname AS \"Sample Schema Name\","
									 	   + " s.sampletablename AS \"Sample Table Name\","
									 	   + " s.samplingratio AS \"Sampling Ratio\","
									 	   + " s.columnnames AS \"On columns\","
									 	   + " t.originaltablesize AS \"Original Table Size\","
									 	   + " t.samplesize AS \"Sample Table Size\"")
								     .orderby("s.originaltablename, s.sampletype, s.samplingratio, s.columnnames");
		
			if (!vc.getMeta().getDatabases().contains(database)) {
				return;
			}
				
			if (vc.getDbms().isJDBC()) {
				rs = info.collectResultSet();
			} else if (vc.getDbms().isSpark()) {
				df = info.collectDataFrame();
			}
		}
	}
	
}

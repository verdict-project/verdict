package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.util.StringManupulations;
import edu.umich.verdict.util.VerdictLogger;

public class ShowSamplesQuery extends SelectQuery {

	public ShowSamplesQuery(VerdictContext vc, String q) {
		super(vc, q);
	}

	@Override
	public void compute() throws VerdictException {
		VerdictSQLParser p = StringManupulations.parserOf(queryString);
		VerdictSQLBaseVisitor<TableUniqueName> visitor = new VerdictSQLBaseVisitor<TableUniqueName>() {
			private TableUniqueName tableName;

			protected TableUniqueName defaultResult() { return tableName; }
			
			@Override public TableUniqueName visitShow_samples_statement(VerdictSQLParser.Show_samples_statementContext ctx) {
				String schema = null;
				String table = null;
				if (ctx.table != null) {
					if (ctx.table.schema != null) {
						schema = ctx.table.schema.getText();
					} 
					if (ctx.table.table != null) {
						table = ctx.table.table.getText();
					}
				}
				return TableUniqueName.uname(schema, table);
			}
		};
		TableUniqueName tableName = visitor.visit(p.show_samples_statement());
		TableUniqueName validTableName = (tableName.getSchemaName() != null)? tableName : TableUniqueName.uname(vc, tableName.getTableName());
		String effectiveSchema = validTableName.getSchemaName();
		
		if (effectiveSchema == null || validTableName.getTableName() == null) {
			VerdictLogger.info("No table specified; cannot show samples");
		} else {
			ExactRelation nameTable = SingleRelation.from(vc, vc.getMeta().getMetaNameTableForOriginalSchema(effectiveSchema));
			nameTable.setAliasName("s");
			ExactRelation sizeTable = SingleRelation.from(vc, vc.getMeta().getMetaSizeTableForOriginalSchema(effectiveSchema));
			sizeTable.setAliasName("t");
			
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
		
			if (!vc.getDbms().doesMetaTablesExist(effectiveSchema)) {
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

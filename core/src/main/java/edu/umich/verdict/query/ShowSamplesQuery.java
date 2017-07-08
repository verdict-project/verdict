package edu.umich.verdict.query;

import java.sql.ResultSet;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.util.VerdictLogger;

public class ShowSamplesQuery extends SelectQuery {

	public ShowSamplesQuery(VerdictContext vc, String q) {
		super(vc, q);
	}

	@Override
	public ResultSet compute() throws VerdictException {
		Optional<String> currentSchema = vc.getCurrentSchema();
		
		if (!currentSchema.isPresent()) {
			VerdictLogger.info("No database schema selected; cannot show samples");
			return null;
		} else {
			// check if there's a verdict meta table.
			if (!vc.getDbms().doesMetaTablesExist(currentSchema.get())) return null;
			
			ExactRelation nameTable = SingleRelation.from(vc, vc.getMeta().getMetaNameTableForOriginalSchema(currentSchema.get()));
			nameTable.setAliasName("s");
			ExactRelation sizeTable = SingleRelation.from(vc, vc.getMeta().getMetaSizeTableForOriginalSchema(currentSchema.get()));
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
			ResultSet rs = info.collectResultSet();
			return rs;
		}
	}
	
}

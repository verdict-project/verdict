package edu.umich.verdict.query;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class ShowSamplesQuery extends SelectQuery {

	public ShowSamplesQuery(String q, VerdictContext vc) {
		super(q, vc);
	}

	public ShowSamplesQuery(Query parent) {
		super(parent.queryString, parent.vc);
	}

	@Override
	public ResultSet compute() throws VerdictException {
		Optional<String> currentSchema = vc.getCurrentSchema();
		
		if (!currentSchema.isPresent()) {
			VerdictLogger.info("No database schema selected; cannot show samples");
			return null;
		} else {
			ResultSet rs;
			// check if there's a verdict meta table.
			if (!vc.getDbms().doesMetaTablesExist(currentSchema.get())) return null;
			
			Dbms dbms = vc.getDbms();
			rs = dbms.executeQuery(
					String.format("SELECT s.sampleschemaaname AS \"Table Schema\","
							+ " s.originaltablename AS \"Original Table Name\","
							+ " t.originaltablesize AS \"Original Table Size\","
							+ " s.sampletablename AS \"Sample Table Name\","
							+ " t.samplesize AS \"Sample Table Size\""
							+ " FROM %s AS s, %s AS t"
							+ " WHERE s.sampleschemaaname = t.schemaname AND s.sampletablename = t.tablename",
							vc.getMeta().getMetaNameTableName(currentSchema.get()),
							vc.getMeta().getMetaSizeTableName(currentSchema.get())
							));
			return rs;
		}
	}
	
}

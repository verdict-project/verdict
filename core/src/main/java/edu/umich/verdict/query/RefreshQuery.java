package edu.umich.verdict.query;

import java.sql.ResultSet;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class RefreshQuery extends Query {

	public RefreshQuery(VerdictJDBCContext vc, String q) {
		super(vc, q);
	}

	@Override
	public void compute() throws VerdictException {
		Optional<String> schema = vc.getCurrentSchema();
		if (schema.isPresent()) {
			vc.getMeta().refreshSampleInfo(schema.get());
		} else {
			String msg = "No schema selected. No refresh done.";
			VerdictLogger.error(msg);
			throw new VerdictException(msg);
		}
	}

}

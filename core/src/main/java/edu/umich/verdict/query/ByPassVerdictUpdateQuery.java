package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ByPassVerdictUpdateQuery extends Query {

	public ByPassVerdictUpdateQuery(VerdictContext vc, String q) {
		super(vc, q);
	}
	
	@Override
	public void compute() throws VerdictException {
		vc.getDbms().executeUpdate(queryString);
	}

}

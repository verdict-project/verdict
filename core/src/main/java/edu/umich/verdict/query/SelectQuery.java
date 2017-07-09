package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.dbms.DbmsJDBC;
import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ApproxRelation;
import edu.umich.verdict.relation.ExactRelation;

public class SelectQuery extends Query {
	
	
	public SelectQuery(VerdictContext vc, String queryString) {
		super(vc, queryString);
	}

	@Override
	public void compute() throws VerdictException {
		super.compute();
		ExactRelation r = ExactRelation.from(vc, queryString);
		ApproxRelation a = r.approx();
		
		if (vc.getDbms() instanceof DbmsJDBC) {
			rs = a.collectResultSet();
		} else if (vc.getDbms() instanceof DbmsSpark) {
			df = a.collectDataFrame();
		}
	}
	
}

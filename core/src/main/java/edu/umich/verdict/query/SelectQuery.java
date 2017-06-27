package edu.umich.verdict.query;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ApproxRelation;
import edu.umich.verdict.relation.ExactRelation;

public class SelectQuery extends Query {

	public SelectQuery(VerdictContext vc, String queryString) {
		super(vc, queryString);
	}

	@Override
	public ResultSet compute() throws VerdictException {
		ExactRelation r = ExactRelation.from(vc, queryString);
		ApproxRelation a = r.approx();
		ResultSet rs = a.collectResultSet();
		return rs;
	}
	
//	public static SelectQuery getInstance(VerdictContext vc, String queryString) {
//		SelectQuery query = null;
//		if (ApproximateSelectQuery.doesSupport(queryString)) {
//			query = new ApproximateSelectQuery(vc, queryString);
//		} else {
//			query = new ByPassSelectQuery(vc, queryString);
//		}
//		return query;
//	}
}

package edu.umich.verdict.query;

import java.sql.ResultSet;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class SelectQuery extends Query {

	public SelectQuery(String queryString, VerdictContext vc) {
		super(queryString, vc);
	}
	
	public SelectQuery(Query parent) {
		super(parent.queryString, parent.vc);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		SelectQuery query = null;
		if (ApproximateSelectQuery.doesSupport(queryString)) {
			query = new ApproximateSelectQuery(queryString, vc);
		} else {
			query = new ByPassSelectQuery(this);
		}
		return query.compute();
	}
}

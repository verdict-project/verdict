package edu.umich.verdict.query;

import java.sql.ResultSet;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public abstract class SelectQuery extends Query {

	public SelectQuery(VerdictContext vc, String queryString) {
		super(vc, queryString);
	}
	
	public static SelectQuery getInstance(VerdictContext vc, String queryString) {
		SelectQuery query = null;
		if (ApproximateSelectQuery.doesSupport(queryString)) {
			query = new ApproximateSelectQuery(vc, queryString);
		} else {
			query = new ByPassSelectQuery(vc, queryString);
		}
		return query;
	}
}

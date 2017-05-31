package edu.umich.verdict.query;

import java.sql.ResultSet;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class VerdictSelectQuery extends VerdictQuery {

	public VerdictSelectQuery(String queryString, VerdictContext vc) {
		super(queryString, vc);
	}
	
	public VerdictSelectQuery(VerdictQuery parent) {
		super(parent.queryString, parent.vc);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		VerdictApproximateSelectQuery query = new VerdictApproximateSelectQuery(queryString, vc);
		return query.compute();
	}
}

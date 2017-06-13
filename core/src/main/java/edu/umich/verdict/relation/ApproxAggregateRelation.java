package edu.umich.verdict.relation;

import java.sql.ResultSet;
import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.relation.functions.AggFunction;

/**
 * Query result including aggregate functions. The query may include a group-by clause.
 * @author Yongjoo Park
 *
 */
public class ApproxAggregateRelation extends ApproxRelation {
	
	protected ApproxRelation prev;
	
	protected List<AggFunction> functions;

	public ApproxAggregateRelation(VerdictContext vc, ApproxRelation prev, List<AggFunction> functions) {
		super(vc, prev.getOriginalTable(), prev.param);
		this.prev = prev;
		this.functions = functions;
	}
	
	@Override
	public ResultSet collectResultSet() {
		return null;
	}
	
	public String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT");
		for (int i = 0; i < functions.size(); i++) {
			if (i == 0) {
				sql.append(" " + functions.get(i).call());
			}
		}
		
		sql.append("FROM " + prev.sampleTableName);
		return sql.toString();
	}
	
}

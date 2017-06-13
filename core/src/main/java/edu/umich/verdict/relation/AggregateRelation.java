package edu.umich.verdict.relation;

import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.relation.functions.AggFunction;

public class AggregateRelation extends Relation {

	protected Relation prev;
	
	protected List<AggFunction> functions;
	
	public AggregateRelation(VerdictContext vc, Relation prev, List<AggFunction> functions) {
		super(vc, prev.tableName);
		this.prev = prev;
		this.functions = functions;
	}
	
	public String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT");
		for (int i = 0; i < functions.size(); i++) {
			if (i == 0) {
				sql.append(" " + functions.get(i).call());
			} else {
				sql.append(", " + functions.get(i).call());
			}
		}
		
		sql.append("FROM " + prev.tableName);
		return sql.toString();
	}
}

package edu.umich.verdict.relation.expr;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.parser.VerdictSQLParser.ExpressionContext;
import edu.umich.verdict.parser.VerdictSQLParser.Over_clauseContext;
import edu.umich.verdict.parser.VerdictSQLParser.Partition_by_clauseContext;
import edu.umich.verdict.util.StringManipulations;

public class OverClause {

	protected List<Expr> partitionBy;
	
	public OverClause() {
		this.partitionBy = new ArrayList<Expr>();
	}
	
	public OverClause(List<Expr> partitionBy) {
		this.partitionBy = partitionBy;
	}
	
	public static OverClause from(String partitionByInString) {
		VerdictSQLParser p = StringManipulations.parserOf(partitionByInString);
		return from(p.over_clause());
	}
	
	@Override
	public String toString() {
		if (partitionBy.size() > 0) {
			return String.format("OVER (partition by %s)", Joiner.on(", ").join(partitionBy));
		} else {
			return "OVER ()";
		}
	}

	public static OverClause from(Over_clauseContext over_clause) {
		List<Expr> exprs = new ArrayList<Expr>();
		if (over_clause.partition_by_clause() != null) {
			Partition_by_clauseContext pctx = over_clause.partition_by_clause();
			for (ExpressionContext ectx : pctx.expression_list().expression()) {
				exprs.add(Expr.from(ectx));
			}
		}
		return new OverClause(exprs);
	}
	
}

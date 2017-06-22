package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;

public class ApproxGroupedRelation extends ApproxRelation {
	
	private ApproxRelation source;
	
	private List<ColNameExpr> groupby;
	
	public ApproxGroupedRelation(VerdictContext vc, ApproxRelation source, List<ColNameExpr> groupby) {
		super(vc);
		this.source = source;
		this.groupby = groupby;
	}
	
	public ApproxRelation getSource() {
		return source;
	}
	
	public List<ColNameExpr> getGroupby() {
		return groupby;
	}

	@Override
	public ExactRelation rewrite() {
		Map<String, String> sub = tableSubstitution();
		List<ColNameExpr> replaced = new ArrayList<ColNameExpr>();
		for (ColNameExpr e : groupby) {
			replaced.add((ColNameExpr) exprWithTableNamesSubstituted(e, sub));
		}
		ExactRelation r = new GroupedRelation(vc, source.rewrite(), replaced);
		r.setAliasName(r.getAliasName());
		return r;
	}

	@Override
	protected double samplingProbabilityFor(FuncExpr f) {
		return source.samplingProbabilityFor(f);
	}

	@Override
	protected Map<String, String> tableSubstitution() {
		return source.tableSubstitution();
	}

}

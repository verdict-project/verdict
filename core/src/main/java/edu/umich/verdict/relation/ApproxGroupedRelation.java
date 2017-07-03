package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
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
	public ExactRelation rewriteForPointEstimate() {
		List<ColNameExpr> newGroupby = groupbyWithTablesSubstituted();
		ExactRelation r = new GroupedRelation(vc, source.rewriteForPointEstimate(), newGroupby);
		r.setAliasName(r.getAliasName());
		return r;
	}
	
	@Override
	public ExactRelation rewriteWithPartition() {
		List<ColNameExpr> newGroupby = groupbyWithTablesSubstituted();
		newGroupby.add((ColNameExpr) exprWithTableNamesSubstituted(partitionColumn(), tableSubstitution()));
		ExactRelation r = new GroupedRelation(vc, source.rewriteWithPartition(), newGroupby);
		r.setAliasName(r.getAliasName());
		return r;
	}
	
	@Override
	protected ColNameExpr partitionColumn() {
		return source.partitionColumn();
	}

	@Override
	// TODO: make this more accurate for handling IN and EXISTS predicates.
	protected double samplingProbabilityFor(FuncExpr f) {
		return source.samplingProbabilityFor(f);
	}

	@Override
	protected Map<String, String> tableSubstitution() {
		return source.tableSubstitution();
	}
	
	protected List<ColNameExpr> groupbyWithTablesSubstituted() {
		Map<String, String> sub = tableSubstitution();
		List<ColNameExpr> replaced = new ArrayList<ColNameExpr>();
		for (ColNameExpr e : groupby) {
			replaced.add((ColNameExpr) exprWithTableNamesSubstituted(e, sub));
		}
		return replaced;
	}

	@Override
	protected String sampleType() {
		return source.sampleType();
	}
	
	@Override
	protected List<TableUniqueName> accumulateStratifiedSamples() {
		return source.accumulateStratifiedSamples();
	}

	@Override
	protected List<String> sampleColumns() {
		return source.sampleColumns();
	}

}

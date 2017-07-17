package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SelectElem;

public class GroupedRelation extends ExactRelation {

	protected ExactRelation source;
	
	protected List<Expr> groupby;

	public GroupedRelation(VerdictContext vc, ExactRelation source, List<Expr> groupby) {
		super(vc);
		this.source = source;
		this.groupby = groupby;
//		List<ColNameExpr> groupbyWithSource = new ArrayList<ColNameExpr>();
//		for (ColNameExpr expr : groupby) {
//			if (expr.getTab() == null) {
//				groupbyWithSource.add(new ColNameExpr(expr.getCol(), source.getAliasName()));
//			} else {
//				groupbyWithSource.add(expr);
//			}
//		}
//		this.groupby = groupbyWithSource;
		this.alias = source.alias;
	}
	
	public ExactRelation getSource() {
		return source;
	}

	@Override
	protected String getSourceName() {
		return getAliasName();
	}
	
	/*
	 * Approx
	 */

	@Override
	public ApproxRelation approx() throws VerdictException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		ApproxRelation a = new ApproxGroupedRelation(vc, source.approxWith(replace), groupby);
		a.setAliasName(getAliasName());
		return a;
	}
	
	@Override
	protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
		List<ApproxRelation> ofSources = source.nBestSamples(elem, n);
		List<ApproxRelation> grouped = new ArrayList<ApproxRelation>();
		for (ApproxRelation a : ofSources) {
			grouped.add(new ApproxGroupedRelation(vc, a, groupby));
		}
		return grouped;
	}

//	@Override
//	protected List<SampleGroup> findSample(Expr elem) {
//		// if the groupby coincides with the stratified sample's columns, we scale its sampling probability heuristically
//		List<SampleGroup> scaled = new ArrayList<SampleGroup>();
//		
//		for (SampleGroup sg : source.findSample(elem)) {
//			boolean includeSTsamplesWithMatchingGroup = false;
//			for (SampleParam param : sg.sampleSet()) {				
//				if (param.sampleType.equals("stratified")) {
//					List<String> group = new ArrayList<String>();
//					for (Expr e : groupby) {
//						group.addAll(e.extractColNames());
//					}
//					if (group.equals(param.columnNames)) {
//						includeSTsamplesWithMatchingGroup = true;
//					}
//				}
//			}
//			
//			if (includeSTsamplesWithMatchingGroup) {
//				scaled.add(new SampleGroup(sg.sampleSet(), sg.getElems(), sg.samplingProb()*2, sg.cost()));
//			} else if (sg.sampleType().equals("stratified")) {
//				// don't support yet.
//			} else {
//				// regular case
//				scaled.add(sg);
//			}
//		}
//		
//		return scaled;
//	}
	
	/*
	 * Sql
	 */
	
	@Override
	public String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * ");
		
		Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
		String gsql = Joiner.on(", ").join(groupsAndNextR.getLeft());
		
		Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(groupsAndNextR.getRight());
		String csql = (filtersAndNextR.getLeft().isPresent())? filtersAndNextR.getLeft().get().toString() : "";
		
		sql.append(String.format(" FROM %s", sourceExpr(source)));
		if (csql.length() > 0) { sql.append(" WHERE "); sql.append(csql); }
		if (gsql.length() > 0) { sql.append(" GROUP BY "); sql.append(gsql); }
		return sql.toString();
	}

//	@Override
//	public List<SelectElem> getSelectList() {
//		return source.getSelectList();
//	}

	@Override
	public ColNameExpr partitionColumn() {
		ColNameExpr col = source.partitionColumn();
//		col.setTab(getAliasName());
		return col;
	}

	@Override
	public List<ColNameExpr> accumulateSamplingProbColumns() {
		return source.accumulateSamplingProbColumns();
	}
	
	@Override
	protected String toStringWithIndent(String indent) {
		StringBuilder s = new StringBuilder(1000);
		s.append(indent);
		s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAliasName(), Joiner.on(", ").join(groupby)));
		s.append(source.toStringWithIndent(indent + "  "));
		return s.toString();
	}

}

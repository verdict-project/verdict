package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.SelectElem;

public class GroupedRelation extends ExactRelation {

	protected ExactRelation source;
	
	protected List<ColNameExpr> groupby;

	public GroupedRelation(VerdictContext vc, ExactRelation source, List<ColNameExpr> groupby) {
		super(vc);
		this.source = source;
		this.groupby = groupby;
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
	protected List<SampleGroup> findSample(SelectElem elem) {
		// if the groupby coincides with the stratified sample's columns, we scale its sampling probability heuristically
		List<SampleGroup> scaled = new ArrayList<SampleGroup>();
		
		for (SampleGroup sg : source.findSample(elem)) {
			boolean includeSTsamplesWithMatchingGroup = false;
			for (SampleParam param : sg.sampleSet()) {				
				if (param.sampleType.equals("stratified")) {
					List<String> group = new ArrayList<String>();
					for (ColNameExpr ce : groupby) {
						group.add(ce.getCol());
					}
					if (group.equals(param.columnNames)) {
						includeSTsamplesWithMatchingGroup = true;
					}
				}
			}
			
			if (includeSTsamplesWithMatchingGroup) {
				scaled.add(new SampleGroup(sg.sampleSet(), sg.getElems(), sg.samplingProb()*2, sg.cost()));
			} else if (sg.sampleType().equals("stratified")) {
				// don't support yet.
			} else {
				// regular case
				scaled.add(sg);
			}
		}
		
		return scaled;
	}
	
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

	@Override
	public List<SelectElem> getSelectList() {
		return source.getSelectList();
	}

}

package edu.umich.verdict.relation;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.SelectElem;

public class FilteredRelation extends ExactRelation {
	
	private ExactRelation source;
	
	private Cond cond;

	public FilteredRelation(VerdictContext vc, ExactRelation source, Cond cond) {
		super(vc);
		this.source = source;
		this.cond = cond;
		this.alias = source.alias;
	}

	public ExactRelation getSource() {
		return source;
	}
	
	public Cond getFilter() {
		return cond;
	}

	@Override
	protected String getSourceName() {
		return getAliasName();
	}	
	
	/*
	 * Approx
	 */
	
	public ApproxRelation approx() throws VerdictException {
		return null;
	}
	
	protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		ApproxRelation a = new ApproxFilteredRelation(vc, source.approxWith(replace), cond);
		a.setAliasName(getAliasName());
		return a;
	}

	protected List<SampleGroup> findSample(SelectElem elem) {
		return source.findSample(elem);
	}
	
	/*
	 * sql
	 */
	
	public String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * ");
		
		Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(this);
		String csql = (filtersAndNextR.getLeft().isPresent())? filtersAndNextR.getLeft().get().toString() : "";
		
		sql.append(String.format(" FROM %s", sourceExpr(source)));
		if (csql.length() > 0) { sql.append(" WHERE "); sql.append(csql); }
		return sql.toString();
	}

	@Override
	public List<SelectElem> getSelectList() {
		return source.getSelectList();
	}

	@Override
	public ColNameExpr partitionColumn() {
		ColNameExpr col = source.partitionColumn();
		col.setTab(getAliasName());
		return col;
	}

	@Override
	public List<ColNameExpr> accumulateSamplingProbColumns() {
		return source.accumulateSamplingProbColumns();
	}

}

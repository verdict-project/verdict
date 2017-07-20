package edu.umich.verdict.relation;

import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.OrderByExpr;

public class OrderedRelation extends ExactRelation {
	
	protected ExactRelation source;
	
	protected List<OrderByExpr> orderby;
	
	protected OrderedRelation(VerdictContext vc, ExactRelation source, List<OrderByExpr> orderby) {
		super(vc);
		this.source = source;
		this.orderby = orderby;
		subquery = true;
		this.alias = source.alias;
	}

	@Override
	protected String getSourceName() {
		return getAlias();
	}
	
	@Override
	protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
		return source.nBestSamples(elem, n);
	}

	@Override
	public ApproxRelation approx() throws VerdictException {
		ApproxRelation a = new ApproxOrderedRelation(vc, source.approx(), orderby);
		a.setAlias(getAlias());
		return a;
	}

	@Override
	protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		return null;
	}

	@Override
	public String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append(source.toSql());
		sql.append(" ORDER BY ");
		sql.append(Joiner.on(", ").join(orderby));
		return sql.toString();
	}

//	@Override
//	public List<SelectElem> getSelectList() {
//		return source.getSelectList();
//	}

	@Override
	public ColNameExpr partitionColumn() {
		ColNameExpr col = source.partitionColumn();
		col.setTab(getAlias());
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
		s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAlias(), Joiner.on(", ").join(orderby)));
		s.append(source.toStringWithIndent(indent + "  "));
		return s.toString();
	}

}

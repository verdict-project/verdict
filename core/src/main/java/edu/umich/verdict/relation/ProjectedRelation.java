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

public class ProjectedRelation extends ExactRelation {

    private ExactRelation source; 

    private List<SelectElem> elems;

    public ProjectedRelation(VerdictContext vc, ExactRelation source, List<SelectElem> elems) {
        super(vc);
        this.source = source;
        this.elems = elems;
    }

//    public static ProjectedRelation from(VerdictContext vc, AggregatedRelation r) {
//        List<SelectElem> selectElems = new ArrayList<SelectElem>();
//
//        // groupby expressions
//        if (r.source != null) {
//            Pair<List<Expr>, ExactRelation> groupbyAndPreceding = allPrecedingGroupbys(r.source);
//            List<Expr> groupby = groupbyAndPreceding.getLeft();
//            for (Expr e : groupby) {
//                selectElems.add(new SelectElem(vc, e));
//            }
//        }
//
//        // aggregate expressions
//        for (Expr e : r.getAggList()) {
//            selectElems.add(new SelectElem(vc, e));		// automatically aliased
//        }
//        ProjectedRelation rel = new ProjectedRelation(vc, r, selectElems);
//        return rel;
//    }

    public ExactRelation getSource() {
        return source;
    }

    public List<SelectElem> getSelectElems() {
        return elems;
    }

    @Override
    protected String getSourceName() {
        return getAlias();
    }

//    public List<SelectElem> getAggElems() {
//        List<SelectElem> elems = new ArrayList<SelectElem>();
//        for (SelectElem e : this.elems) {
//            if (e.getExpr().isagg()) {
//                elems.add(e);
//            }
//        }
//        return elems;
//    }

    @Override
    public ApproxRelation approx() throws VerdictException {
        ApproxRelation a = new ApproxProjectedRelation(vc, source.approx(), elems);
        a.setAlias(getAlias());
        a.setOriginalRelation(this);
        return a;
    }

    @Override
    protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
        return null;
    }

    @Override
    protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
        // TODO: should insert __vprob column.

        List<ApproxRelation> ofSources = source.nBestSamples(elem, n);
        List<ApproxRelation> projected = new ArrayList<ApproxRelation>();
        for (ApproxRelation a : ofSources) {
            projected.add(new ApproxProjectedRelation(vc, a, elems));
        }
        return projected;
    }

    //	@Override
    //	protected List<SampleGroup> findSample(Expr elem) {
    //		return new ArrayList<SampleGroup>();
    //	}

    protected String selectSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append(Joiner.on(", ").join(elems));
        return sql.toString();
    }

    @Override
    public String toSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(selectSql());

        ExactRelation t = this.source;
        if (t instanceof AggregatedRelation) {
            t = ((AggregatedRelation) t).getSource();
        }

        // collect groupby
        Pair<List<Expr>, ExactRelation> groupbyAndNextR = allPrecedingGroupbys(t);
        List<Expr> groupby = groupbyAndNextR.getLeft();
        t = groupbyAndNextR.getRight();

        // search conditions (or filters in the where clause)
        Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(t);
        String csql = (filtersAndNextR.getLeft().isPresent())? filtersAndNextR.getLeft().get().toSql() : "";

        sql.append(String.format(" FROM %s", sourceExpr(filtersAndNextR.getRight())));
        if (csql.length() > 0) { sql.append(" WHERE "); sql.append(csql); }

        if (groupby.size() > 0) {
            sql.append(" GROUP BY ");
            sql.append(Joiner.on(", ").join(groupby));
        }

        return sql.toString();
    }

    //	@Override
    //	public List<SelectElem> getSelectList() {
    //		return elems;
    //	}

    //	@Override
    //	public List<SelectElem> selectElemsWithAggregateSource() {
    //		List<SelectElem> sourceAggElems = source.selectElemsWithAggregateSource();
    //		final Set<String> sourceAggAliases = new HashSet<String>();
    //		for (SelectElem e : sourceAggElems) {
    //			sourceAggAliases.add(e.getAlias());
    //		}
    //		
    //		ExprVisitor<Boolean> v = new ExprVisitor<Boolean>() {
    //			private boolean aggSourceObserved = false;
    //
    //			@Override
    //			public Boolean call(Expr expr) {
    //				if (expr instanceof ColNameExpr) {
    //					if (sourceAggAliases.contains(((ColNameExpr) expr).getCol())) {
    //						aggSourceObserved = true;
    //					}
    //				}
    //				return aggSourceObserved;
    //			}
    //		};
    //		
    //		// now examine each select list elem
    //		List<SelectElem> aggElems = new ArrayList<SelectElem>();
    //		for (SelectElem e : elems) {
    //			if (v.visit(e.getExpr())) {
    //				aggElems.add(e);
    //			}
    //		}
    //		
    //		return aggElems;
    //	}

    @Override
    public ColNameExpr partitionColumn() {
        String pcol = partitionColumnName();
        ColNameExpr col = null;

        // first inspect if there is a randomly generated partition column in this instance's projection list
        for (SelectElem elem : elems) {
            String alias = elem.getAlias();
            if (alias != null && alias.equals(pcol)) {
                col = new ColNameExpr(vc, pcol, getAlias());
            }
        }

        if (col == null) {
            col = source.partitionColumn();
        }

        return col;
    }

    @Override
    public List<ColNameExpr> accumulateSamplingProbColumns() {
        List<ColNameExpr> exprs = source.accumulateSamplingProbColumns();
        List<ColNameExpr> exprsInNewTable = new ArrayList<ColNameExpr>(); 
        for (ColNameExpr c : exprs) {
            exprsInNewTable.add(new ColNameExpr(vc, c.getCol(), getAlias()));
        }
        return exprsInNewTable;
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAlias(), Joiner.on(", ").join(elems)));
        s.append(source.toStringWithIndent(indent + "  "));
        return s.toString();
    }

}

package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.BinaryOpExpr;
import edu.umich.verdict.relation.expr.ConstantExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprVisitor;
import edu.umich.verdict.relation.expr.FuncExpr;

/**
 * Query result including aggregate functions. The query may include a group-by clause.
 * @author Yongjoo Park
 *
 */
public class AggregatedSampleRelation extends AggregatedRelation implements Relation, ApproxRelation {
	
	protected SampleRelation source;
	
	protected List<Expr> aggExprs;

	protected AggregatedSampleRelation(VerdictContext vc, SampleRelation source, List<Expr> aggExpr) {
		super(vc, source.sampleAsExactRelation(), aggExpr);
		this.source = source;
		this.aggExprs = aggExpr;
	}
	
	/**
	 * Transforms to a relation that computes an approximate answer using sample queries. This method is used internally
	 * for obtaining a computable unit.
	 * @return Any subclass of ExactRelation. 
	 * @throws VerdictException 
	 */
	protected AggregatedRelation transform() throws VerdictException {
		List<Expr> t = new ArrayList<Expr>();
		for (Expr e : aggExprs) {
			t.add(transformForSingleFunction(e));
		}
		return new AggregatedRelation(vc, source.sampleAsExactRelation(), t);
	}
	
	private Expr transformForSingleFunction(Expr f) throws VerdictException {
		ExprVisitor v = new ExprVisitor() {
			public Expr call(Expr expr) throws VerdictException {
				if (expr instanceof FuncExpr) {
					FuncExpr f = (FuncExpr) expr;
					if (f.getFuncName().equals(FuncExpr.FuncName.COUNT)) {
						if (source.getSampleType().equals("uniform")) {
							return new BinaryOpExpr(f, ConstantExpr.from(source.getOriginalTableSize() / source.getSampleSize()), "*");
						} else if (source.getSampleType().equals("universe")) {
							return new BinaryOpExpr(f, ConstantExpr.from(source.getOriginalTableSize() / source.getSampleSize()), "*");
						} else {
							throw new VerdictException("unsupported.");
						}
					} else if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
						if (source.getSampleType().equals("universe")) {
							return new BinaryOpExpr(f, ConstantExpr.from(source.getSamplingRatio()), "*");
						} else if (source.getSampleType().equals("stratified")) {
							return f;
						} else {
							throw new VerdictException("unsupported.");
						}
					} else {
						throw new VerdictException("unsupported.");
					}
				} else {
					return expr;
				}
			}
		};
		
		return v.visit(f);
	}
	
	@Override
	protected String sourceSql() throws VerdictException {
		StringBuilder sql = new StringBuilder();
		sql.append("FROM ");
		sql.append(tableSourceExpr(source));
		return sql.toString();
	}
	
//	private String scaledFunctionExpr(AggFunction function, double scale) {
//		return String.format("(%s * %f)", function.toString(vc), scale);
//	}
	
	@Override
	protected String selectSql() throws VerdictException {
		throw new VerdictException("this method must not be called on this class.");
	}
	
}

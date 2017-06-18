package edu.umich.verdict.relation.condition;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.util.VerdictLogger;

public abstract class Cond {

	public Cond() {}
	
	public static Cond from(String cond) {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(cond));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		CondGen g = new CondGen();
		return g.visit(p.search_condition());
	}
	
	public abstract String toString(VerdictContext vc);
	
	@Override
	public String toString() {
		VerdictLogger.error(this, "Calling toString() method without VerdictContext is not allowed.");
		return "toString called without VerdictContext";
	}

	public Cond accept(CondModifier v) {
		return v.call(this);
	}

}


class CondGen extends VerdictSQLBaseVisitor<Cond> {
	
	@Override
	public Cond visitComp_expr_predicate(VerdictSQLParser.Comp_expr_predicateContext ctx) {
		Expr e1 = Expr.from(ctx.expression(0));
		Expr e2 = Expr.from(ctx.expression(1));
		return CompCond.from(e1, e2, ctx.comparison_operator().getText());
	}
	
	@Override
	public Cond visitSearch_condition_or(VerdictSQLParser.Search_condition_orContext ctx) {
		Cond concat = null;
		for (VerdictSQLParser.Search_condition_notContext nctx : ctx.search_condition_not()) {
			if (concat == null) {
				concat = visit(nctx);
			} else {
				concat = OrCond.from(concat, visit(nctx));
			}
		}
		return concat;
	}
	
	@Override
	public Cond visitSearch_condition(VerdictSQLParser.Search_conditionContext ctx) {
		Cond concat = null;
		for (VerdictSQLParser.Search_condition_orContext octx : ctx.search_condition_or()) {
			if (concat == null) {
				concat = visit(octx);
			} else {
				concat = AndCond.from(concat, visit(octx));
			}
		}
		return concat;
	}
	
}
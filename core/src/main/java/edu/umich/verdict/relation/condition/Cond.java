package edu.umich.verdict.relation.condition;

import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.parser.VerdictSQLParser.Search_conditionContext;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.util.StringManipulations;

public abstract class Cond {

	public Cond() {}
	
	public static Cond from(String cond) {
		VerdictSQLParser p = StringManipulations.parserOf(cond);
		return from(p.search_condition());
	}
	
	public static Cond from(Search_conditionContext ctx) {
		CondGen g = new CondGen();
		return g.visit(ctx);
	}
	
	public static Cond from(VerdictContext vc, Search_conditionContext ctx) {
		CondGen g = new CondGen(vc);
		return g.visit(ctx);
	}

	public Cond accept(CondModifier v) {
		return v.call(this);
	}

	public Cond searchForJoinCondition(List<String> joinedTableName, String rightTableName) {
		return null;
	}

	public Cond remove(Cond j) {
		return this;
	}
	
	public abstract Cond withTableSubstituted(String newTab);

	public <T> T accept(CondVisitor<T> condVisitor) {
		return condVisitor.call(this);
	}

	/**
	 * Generates a sql expression.
	 * @return
	 */
	public abstract String toSql();
}


class CondGen extends VerdictSQLBaseVisitor<Cond> {
	
	private VerdictContext vc;
	
	public CondGen() {
		
	}
	
	public CondGen(VerdictContext vc) {
		this.vc = vc;
	}
	
	@Override
	public Cond visitComp_expr_predicate(VerdictSQLParser.Comp_expr_predicateContext ctx) {
		Expr e1 = Expr.from(vc, ctx.expression(0));
		Expr e2 = Expr.from(vc, ctx.expression(1));
		return CompCond.from(e1, ctx.comparison_operator().getText(), e2);
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
	
	@Override
	public Cond visitBracket_predicate(VerdictSQLParser.Bracket_predicateContext ctx) {
		return visit(ctx.search_condition());
	}
	
	@Override
	public Cond visitSearch_condition_not(VerdictSQLParser.Search_condition_notContext ctx) {
		if (ctx.NOT() == null) {
			return visit(ctx.predicate());
		} else {
			return NotCond.from(visit(ctx.predicate()));
		}
	}
	
	@Override
	public Cond visitIs_predicate(VerdictSQLParser.Is_predicateContext ctx) {
		Expr left = Expr.from(ctx.expression());
		Cond right = visit(ctx.null_notnull());
		return new IsCond(left, right);
	}
	
	@Override
	public Cond visitNull_notnull(VerdictSQLParser.Null_notnullContext ctx) {
		if (ctx.NOT() == null) {
			return new NullCond();
		} else {
			return new NotCond(new NullCond());
		}
	}
	
}

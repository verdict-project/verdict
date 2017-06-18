package edu.umich.verdict.relation.expr;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;

public class OrderByExpr {
	
	private Expr expr;
	
	private Optional<String> direction;

	public OrderByExpr(Expr expr, String direction) {
		this.expr = expr;
		this.direction = Optional.fromNullable(direction);
	}
	
	public OrderByExpr(Expr expr) {
		this(expr, null);
	}
	
	public static OrderByExpr from(String expr) {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(expr));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		VerdictSQLBaseVisitor<OrderByExpr> v = new VerdictSQLBaseVisitor<OrderByExpr>() {
			@Override
			public OrderByExpr visitOrder_by_expression(VerdictSQLParser.Order_by_expressionContext ctx) {
				String dir = (ctx.ASC() != null)? "ASC" : ((ctx.DESC() != null)? "DESC" : null);
				return new OrderByExpr(Expr.from(ctx.expression()), dir);
			}
		};
		return v.visit(p.order_by_expression());
	}
	
	@Override
	public String toString() {
		if (direction.isPresent()) {
			return expr.toString() + " " + direction.get();
		} else {
			return expr.toString();
		}
	}

}

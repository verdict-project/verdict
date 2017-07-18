package edu.umich.verdict.relation.expr;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Optional;


public abstract class ExprModifier {

	public ExprModifier() {}

	public Expr visit(Expr expr) {
		return expr.accept(this);
	}
	
	public Expr call(Expr expr) {
		return expr;
	}
	
	public Expr visitExpr(Expr expr) {
		if (expr instanceof BinaryOpExpr) {
			return visitBinaryOpExpr((BinaryOpExpr) expr);
		} else if (expr instanceof CaseExpr) {
			return visitCaseExpr((CaseExpr) expr);
		} else if (expr instanceof ColNameExpr) {
			return visitColNameExpr((ColNameExpr) expr);
		} else if (expr instanceof ConstantExpr) {
			return visitConstantExpr((ConstantExpr) expr);
		} else if (expr instanceof FuncExpr) {
			return visitFuncExpr((FuncExpr) expr);
		} else if (expr instanceof OrderByExpr) {
			return visitOrderByExpr((OrderByExpr) expr);
		} else if (expr instanceof SubqueryExpr) {
			return visitSubqueryExpr((SubqueryExpr) expr);
		}
		
		return expr;
	}
	
	public BinaryOpExpr visitBinaryOpExpr(BinaryOpExpr expr) {
		Expr left = visitExpr(expr.getLeft());
		Expr right = visitExpr(expr.getRight());
		return new BinaryOpExpr(left, right, expr.getOp());
	}
	
	public CaseExpr visitCaseExpr(CaseExpr expr) {
		List<Expr> newExprs = new ArrayList<Expr>();
		List<Expr> exprs = expr.getExpressions();
		for (Expr e : exprs) {
			newExprs.add(visitExpr(e));
		}
		return new CaseExpr(expr.getConditions(), newExprs);
	}
	
	public ColNameExpr visitColNameExpr(ColNameExpr expr) {
		return expr;
	}
	
	public ConstantExpr visitConstantExpr(ConstantExpr expr) {
		return expr;
	}
	
	public FuncExpr visitFuncExpr(FuncExpr expr) {
		List<Expr> newExprs = new ArrayList<Expr>();
		List<Expr> exprs = expr.getExpressions();
		for (Expr e : exprs) {
			newExprs.add(visitExpr(e));
		}
		return new FuncExpr(expr.getFuncName(), newExprs, expr.getOverClause());
	}
	
	public OrderByExpr visitOrderByExpr(OrderByExpr expr) {
		Expr newExpr = visitExpr(expr.getExpression());
		Optional<String> dir = expr.getDirection();
		
		if (dir.isPresent()) {
			return new OrderByExpr(newExpr, dir.get());
		} else {
			return new OrderByExpr(newExpr);
		}
	}
	
	public SubqueryExpr visitSubqueryExpr(SubqueryExpr expr) {
		return expr;
	}
	
//	public Relation call(Relation r) {
//		return r;
//	}
	
}

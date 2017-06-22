package edu.umich.verdict.relation.expr;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;

public class SelectElem {

	private Expr expr;
	
	private Optional<String> alias;
	
	public SelectElem(Expr expr, String alias) {
		this.expr = expr;
		this.alias = Optional.fromNullable(alias);
	}
	
	public SelectElem(Expr expr) {
		this(expr, null);
	}
	
	public static SelectElem from(String elem) {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(elem));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		
		VerdictSQLBaseVisitor<SelectElem> v = new VerdictSQLBaseVisitor<SelectElem>() {
			@Override
			public SelectElem visitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
				if (ctx.column_alias() == null) {
					return new SelectElem(Expr.from(ctx.expression()));
				} else {
					return new SelectElem(Expr.from(ctx.expression()), ctx.column_alias().getText());
				}
			}	
		};
		
		return v.visit(p.select_list_elem());
	}
	
	public Expr getExpr() {
		return expr;
	}
	
	public boolean aliasPresent() {
		return alias.isPresent();
	}
	
	public String getAlias() {
		if (alias.isPresent()) {
			return alias.get();
		} else {
			return null;
		}
	}
	
	public boolean isagg() {
		return expr.isagg();
	}
	
	@Override
	public String toString() {
		if (alias.isPresent()) {
			return String.format("%s AS %s", expr.toString(), alias.get());
		} else {
			return expr.toString();
		}
	}
	
}

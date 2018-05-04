package edu.umich.verdict.relation.condition;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLParser.Comp_between_exprContext;
import edu.umich.verdict.relation.expr.Expr;

public class BetweenCond extends Cond {
    
    private Expr col;
    
    private Expr left;
    
    private Expr right;
    
    public Expr getCol() {
        return col;
    }
    
    public void setCol(Expr col) {
        this.col = col;
    }
    
    public Expr getLeft() {
        return left;
    }

    public void setLeft(Expr left) {
        this.left = left;
    }

    public Expr getRight() {
        return right;
    }

    public void setRight(Expr right) {
        this.right = right;
    }
    
    public BetweenCond(Expr col, Expr left, Expr right) {
        this.col = col;
        this.left = left;
        this.right = right;
    }
    
    public static BetweenCond from(VerdictContext vc, Comp_between_exprContext ctx) {
        Expr col = Expr.from(vc, ctx.expression(0));
        Expr left = Expr.from(vc, ctx.expression(1));
        Expr right = Expr.from(vc, ctx.expression(2));
        return new BetweenCond(col, left, right);
    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        return new BetweenCond(getCol().withTableSubstituted(newTab),
                               getLeft().withTableSubstituted(newTab),
                               getRight().withTableSubstituted(newTab));
    }

    @Override
    public Cond withNewTablePrefix(String newPrefix) {
        return new BetweenCond(getCol().withNewTablePrefix(newPrefix),
                getLeft().withNewTablePrefix(newPrefix),
                getRight().withNewTablePrefix(newPrefix));
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(col.toSql());
        sql.append(" BETWEEN ");
        sql.append(left.toSql());
        sql.append(" AND ");
        sql.append(right.toSql());
        return sql.toString();
    }

    @Override
    public boolean equals(Cond o) {
        if (o instanceof BetweenCond) {
            return getCol().equals(((BetweenCond) o).getCol()) &&
                   getLeft().equals(((BetweenCond) o).getLeft()) &&
                   getRight().equals(((BetweenCond) o).getRight());
        }
        return false;
    }

}

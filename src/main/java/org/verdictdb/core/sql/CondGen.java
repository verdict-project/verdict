package org.verdictdb.core.sql;

import org.verdictdb.core.logical_query.*;
import org.verdictdb.parser.VerdictSQLBaseVisitor;
import org.verdictdb.parser.VerdictSQLParser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CondGen extends VerdictSQLBaseVisitor<UnnamedColumn> {

    private MetaData meta;

    public CondGen() {

    }

    public CondGen(MetaData meta) {
        this.meta = meta;
    }

    @Override
    public UnnamedColumn visitComp_expr_predicate(VerdictSQLParser.Comp_expr_predicateContext ctx) {
        ExpressionGen g = new ExpressionGen(meta);
        UnnamedColumn e1 = g.visit(ctx.expression(0));
        UnnamedColumn e2 = g.visit(ctx.expression(1));
        if (ctx.comparison_operator().getText()=="=") {
            return new ColumnOp("equal", Arrays.asList(e1, e2));
        }
        else if (ctx.comparison_operator().getText()==">") {
            return new ColumnOp("greater", Arrays.asList(e1, e2));
        }
        else if (ctx.comparison_operator().getText()==">=") {
            return new ColumnOp("greaterequal", Arrays.asList(e1, e2));
        }
        else if (ctx.comparison_operator().getText()=="<") {
            return new ColumnOp("less", Arrays.asList(e1, e2));
        }
        else if (ctx.comparison_operator().getText()=="<=") {
            return new ColumnOp("lessequal", Arrays.asList(e1, e2));
        }
        else if (ctx.comparison_operator().getText()=="<>" || ctx.comparison_operator().getText()=="!=") {
            return new ColumnOp("notequal", Arrays.asList(e1, e2));
        }
        else {
            return null;
        }
    }

    @Override
    public UnnamedColumn visitSearch_condition_or(VerdictSQLParser.Search_condition_orContext ctx) {
        UnnamedColumn concat = null;
        for (VerdictSQLParser.Search_condition_notContext nctx : ctx.search_condition_not()) {
            if (concat == null) {
                concat = visit(nctx);
            } else {
                concat = new ColumnOp("or", Arrays.asList(
                        concat,
                        visit(nctx)
                ));
            }
        }
        return concat;
    }

    @Override
    public UnnamedColumn visitSearch_condition(VerdictSQLParser.Search_conditionContext ctx) {
        UnnamedColumn concat = null;
        for (VerdictSQLParser.Search_condition_orContext octx : ctx.search_condition_or()) {
            if (concat == null) {
                concat = visit(octx);
            } else {
                concat = new ColumnOp("and", Arrays.asList(
                        concat,
                        visit(octx)
                ));
            }
        }
        return concat;
    }

    @Override
    public UnnamedColumn visitBracket_predicate(VerdictSQLParser.Bracket_predicateContext ctx) {
        return visit(ctx.search_condition());
    }

    @Override
    public UnnamedColumn visitSearch_condition_not(VerdictSQLParser.Search_condition_notContext ctx) {
        if (ctx.NOT() == null) {
            return visit(ctx.predicate());
        } else {
            UnnamedColumn predicate = visit(ctx.predicate());
            if (predicate instanceof ColumnOp) {
                ((ColumnOp) predicate).setOpType("not"+((ColumnOp) predicate).getOpType());
                return predicate;
            } else {
                return null;
            }
        }
    }

    @Override
    public UnnamedColumn visitIs_predicate(VerdictSQLParser.Is_predicateContext ctx) {
        ExpressionGen g = new ExpressionGen(meta);
        UnnamedColumn left = g.visit(ctx.expression());
        UnnamedColumn right = visit(ctx.null_notnull());
        return new ColumnOp("is", Arrays.asList(left, right));
    }

    @Override
    public UnnamedColumn visitIn_predicate(VerdictSQLParser.In_predicateContext ctx) {
        ExpressionGen g1 = new ExpressionGen(meta);
        if (ctx.subquery() != null) {
            // VerdictLogger.error("Verdict currently does not support IN + subquery condition.");
            UnnamedColumn left = g1.visit(ctx.expression());
            boolean not = (ctx.NOT() != null) ? true : false;
            RelationGen g2 = new RelationGen(meta);
            UnnamedColumn subquery = SubqueryColumn.getSubqueryColumn((SelectQueryOp) g2.visit(ctx.subquery()));
            return not ? new ColumnOp("notin", Arrays.asList(left, subquery)) :
                    new ColumnOp("in", Arrays.asList(left, subquery));
        } else {
            UnnamedColumn left = g1.visit(ctx.expression());
            boolean not = (ctx.NOT() != null) ? true : false;
            List<UnnamedColumn> expressionList = new ArrayList<UnnamedColumn>();
            expressionList.add(left);
            for (VerdictSQLParser.ExpressionContext ectx : ctx.expression_list().expression()) {
                expressionList.add(g1.visit(ectx));
            }
            return not ? new ColumnOp("notin", expressionList) :
                    new ColumnOp("in", expressionList);
        }
    }

    @Override
    public UnnamedColumn visitExists_predicate(VerdictSQLParser.Exists_predicateContext ctx) {
        if(ctx.subquery() == null) {
            // VerdictLogger.error("Exists should be followed by a subquery");
            return null;
        }
        RelationGen g = new RelationGen(meta);
        UnnamedColumn subquery = SubqueryColumn.getSubqueryColumn((SelectQueryOp) g.visit(ctx.subquery()));
        return new ColumnOp("exists", subquery);
    }

    @Override
    public UnnamedColumn visitLike_predicate(VerdictSQLParser.Like_predicateContext ctx) {
        ExpressionGen g = new ExpressionGen(meta);
        UnnamedColumn left = g.visit(ctx.expression(0));
        UnnamedColumn right = g.visit(ctx.expression(1));
        boolean not = (ctx.NOT() != null) ? true : false;
        return not ? new ColumnOp("notlike", Arrays.asList(left, right)) :
                new ColumnOp("like", Arrays.asList(left, right));
    }

    @Override
    public UnnamedColumn visitComp_between_expr(VerdictSQLParser.Comp_between_exprContext ctx) {
        ExpressionGen g = new ExpressionGen(meta);
        UnnamedColumn col = g.visit(ctx.expression(0));
        UnnamedColumn left = g.visit(ctx.expression(1));
        UnnamedColumn right = g.visit(ctx.expression(2));
        return new ColumnOp("between", Arrays.asList(col, left, right));
    }

    @Override
    public UnnamedColumn visitNull_notnull(VerdictSQLParser.Null_notnullContext ctx) {
        if (ctx.NOT() == null) {
            return ConstantColumn.valueOf("NULL");
        } else {
            return ConstantColumn.valueOf("NOT NULL");
        }
    }

    @Override
    public UnnamedColumn visitTrue_orfalse(VerdictSQLParser.True_orfalseContext ctx) {
        UnnamedColumn c = null;
        if (ctx.TRUE() != null) {
            c = ConstantColumn.valueOf("TRUE");
        } else {
            c = ConstantColumn.valueOf("FALSE");
        }
        return c;
    }
}

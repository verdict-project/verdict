package org.verdictdb.core.sql;

import com.sun.org.apache.bcel.internal.classfile.ConstantValue;
import com.sun.tools.internal.jxc.ap.Const;
import org.verdictdb.core.logical_query.*;
import org.verdictdb.parser.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExpressionGen extends VerdictSQLBaseVisitor<UnnamedColumn> {

    private MetaData meta;

    public ExpressionGen(MetaData meta) {
        this.meta = meta;
    }

    @Override
    public ColumnOp visitInterval(VerdictSQLParser.IntervalContext ctx) {
        String unit = "day";
        if (ctx.DAY() != null || ctx.DAYS() != null) {
            unit = "day";
        } else if (ctx.MONTH() != null || ctx.MONTHS() != null) {
            unit = "month";
        } else if (ctx.YEAR() != null || ctx.YEARS() != null) {
            unit = "year";
        }
        return new ColumnOp("interval", Arrays.<UnnamedColumn>asList(
                ConstantColumn.valueOf(ctx.constant_expression().getText()),
                ConstantColumn.valueOf(unit)
        ));
    }

    @Override
    public ConstantColumn visitPrimitive_expression(VerdictSQLParser.Primitive_expressionContext ctx) {
        return ConstantColumn.valueOf(ctx.getText());
    }

    @Override
    public BaseColumn visitColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx) {
        String[] t = ctx.getText().split("\\.");
        if (t.length >= 2) {
            return new BaseColumn(t[1], t[0]);
        } else {
            return new BaseColumn(meta.columnAlias.get(t[0]), t[0]);
        }
    }

    @Override
    public ColumnOp visitBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx) {
        return new ColumnOp(ctx.op.getText(), Arrays.asList(
                visit(ctx.expression(0)),
                visit(ctx.expression(1))
        ));
    }

    @Override
    public ColumnOp visitFunction_call_expression(VerdictSQLParser.Function_call_expressionContext ctx) {
        VerdictSQLBaseVisitor<ColumnOp> v = new VerdictSQLBaseVisitor<ColumnOp>() {

            @Override
            public ColumnOp visitAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
                String fname;
                UnnamedColumn col = null;
                if (ctx.all_distinct_expression() != null) {
                    ExpressionGen g = new ExpressionGen(meta);
                    col = g.visit(ctx);
                }

                //OverClause overClause = null;

                if (ctx.AVG() != null) {
                    fname = "avg";
                } else if (ctx.SUM() != null) {
                    fname = "sum";
                } else if (ctx.COUNT() != null) {
                    if (ctx.all_distinct_expression() != null && ctx.all_distinct_expression().DISTINCT() != null) {
                        fname = "countdistinct";
                    } else {
                        fname = "count";
                        col = new AsteriskColumn();
                    }
                } //else if (ctx.NDV() != null) {
                  //  fname = FuncName.IMPALA_APPROX_COUNT_DISTINCT;}
                  else if (ctx.MIN() != null) {
                    fname = "min";
                } else if (ctx.MAX() != null) {
                    fname = "max";
                } //else if (ctx.STDDEV_SAMP() != null) {
                  //  fname = FuncName.STDDEV_SAMP; }
                  else {
                    fname = "UNKNOWN";
                }

              //  if (ctx.over_clause() != null) {
              //      overClause = OverClause.from(vc, ctx.over_clause());
              //  }

                return new ColumnOp(fname, col);
            }

            /*
            @Override
            public FuncExpr visitUnary_manipulation_function(VerdictSQLParser.Unary_manipulation_functionContext ctx) {
                String fname = ctx.function_name.getText().toUpperCase();
                FuncName funcName = string2FunctionType.containsKey(fname) ? string2FunctionType.get(fname)
                        : FuncName.UNKNOWN;
                if (fname.equals("CAST")) {
                    return new FuncExpr(funcName, Expr.from(vc, ctx.cast_as_expression().expression()),
                            ConstantExpr.from(vc, ctx.cast_as_expression().data_type().getText()));
                } else {
                    return new FuncExpr(funcName, Expr.from(vc, ctx.expression()));
                }
            }

            @Override
            public FuncExpr visitNoparam_manipulation_function(
                    VerdictSQLParser.Noparam_manipulation_functionContext ctx) {
                String fname = ctx.function_name.getText().toUpperCase();
                FuncName funcName = string2FunctionType.containsKey(fname) ? string2FunctionType.get(fname)
                        : FuncName.UNKNOWN;
                return new FuncExpr(funcName, null);
            }

            */

            @Override
            public ColumnOp visitBinary_manipulation_function(
                    VerdictSQLParser.Binary_manipulation_functionContext ctx) {
                String fname = ctx.function_name.getText().toLowerCase();
                return new ColumnOp(fname, Arrays.<UnnamedColumn>asList(
                        visit(ctx.expression(0)),
                        visit(ctx.expression(1))
                ));
            }

            @Override
            public ColumnOp visitTernary_manipulation_function(
                    VerdictSQLParser.Ternary_manipulation_functionContext ctx) {
                String fname = ctx.function_name.getText().toLowerCase();
                return new ColumnOp(fname, Arrays.<UnnamedColumn>asList(
                        visit(ctx.expression(0)),
                        visit(ctx.expression(1)),
                        visit(ctx.expression(2))
                ));
            }

            /*
            @Override
            public FuncExpr visitNary_manipulation_function(VerdictSQLParser.Nary_manipulation_functionContext ctx) {
                String fname = ctx.function_name.getText().toUpperCase();
                FuncName funcName = string2FunctionType.containsKey(fname) ? string2FunctionType.get(fname)
                        : FuncName.UNKNOWN;
                List<Expr> exprList = new ArrayList<>();
                for (VerdictSQLParser.ExpressionContext context : ctx.expression()) {
                    exprList.add(Expr.from(vc, context));
                }
                return new FuncExpr(funcName, exprList, null);
            }

            @Override
            public FuncExpr visitExtract_time_function(VerdictSQLParser.Extract_time_functionContext ctx) {
                String fname = ctx.function_name.getText().toUpperCase();
                FuncName funcName = string2FunctionType.containsKey(fname) ? string2FunctionType.get(fname)
                        : FuncName.UNKNOWN;
                return new FuncExpr(funcName, ConstantExpr.from(vc, ctx.extract_unit()),
                        Expr.from(vc, ctx.expression()));
            }
            */
        };
        return v.visit(ctx);
    }

    @Override
    public ColumnOp visitCase_expr(VerdictSQLParser.Case_exprContext ctx) {
        if (ctx.search_condition()!=null ){
            if (ctx.expression(1)!=null) {
                return new ColumnOp("whenthenelse", Arrays.asList(
                        getSearch_condition(ctx.search_condition()),
                        visit(ctx.expression(0)),
                        visit(ctx.expression(1))
                ));
            } else {
                return new ColumnOp("whenthenelse", Arrays.asList(
                        getSearch_condition(ctx.search_condition()),
                        visit(ctx.expression(0))
                ));
            }
        } else {
            if (ctx.expression(3)!=null) {
                return new ColumnOp("casethenelse", Arrays.asList(
                        getSearch_condition(ctx.search_condition()),
                        visit(ctx.expression(0)),
                        visit(ctx.expression(1)),
                        visit(ctx.expression(2)),
                        visit(ctx.expression(3))
                ));
            } else {
                return new ColumnOp("casethenelse", Arrays.asList(
                        getSearch_condition(ctx.search_condition()),
                        visit(ctx.expression(0)),
                        visit(ctx.expression(1)),
                        visit(ctx.expression(2))
                ));
            }
        }


    }

    @Override
    public UnnamedColumn visitBracket_expression(VerdictSQLParser.Bracket_expressionContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public SubqueryColumn visitSubquery_expression(VerdictSQLParser.Subquery_expressionContext ctx) {
        RelationGen g = new RelationGen(meta);
        return SubqueryColumn.getSubqueryColumn((SelectQueryOp) g.visitSubquery_expression(ctx));
    }

    public UnnamedColumn getSearch_condition(List<VerdictSQLParser.Search_conditionContext> ctx) {
        if (ctx.size()==1) {
            return visit(ctx.get(0));
        } else {
            UnnamedColumn col = visit(ctx.get(0));
            for (int i=0;i<ctx.size();i++) {
                col = new ColumnOp("and", Arrays.asList(
                        col, visit(ctx.get(i))
                ));
            }
            return col;
        }
    }
}

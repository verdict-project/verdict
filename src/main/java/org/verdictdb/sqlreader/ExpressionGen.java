package org.verdictdb.sqlreader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.parser.VerdictSQLBaseVisitor;
import org.verdictdb.parser.VerdictSQLParser;
import org.verdictdb.parser.VerdictSQLParser.Column_nameContext;
import org.verdictdb.parser.VerdictSQLParser.Full_column_nameContext;

public class ExpressionGen extends VerdictSQLBaseVisitor<UnnamedColumn> {

  //    private MetaData meta;

  //    public ExpressionGen(MetaData meta) {
  //        this.meta = meta;
  //    }

  public ExpressionGen() {
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
  public ColumnOp visitDate(VerdictSQLParser.DateContext ctx) {
    return new ColumnOp("date", Arrays.<UnnamedColumn>asList(
        ConstantColumn.valueOf(ctx.constant_expression().getText())
    ));
  }

  @Override
  public ConstantColumn visitPrimitive_expression(VerdictSQLParser.Primitive_expressionContext ctx) {
    return ConstantColumn.valueOf(ctx.getText());
  }

  @Override
  public BaseColumn visitColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx) {
    Full_column_nameContext fullColName = ctx.full_column_name();
    if (fullColName == null) {
      return null;
    }

    Column_nameContext columnName = fullColName.column_name();
    String colName = stripQuote(columnName.getText());
    if (fullColName.table_name() == null) {
      return new BaseColumn(colName);
    }
    String tableName = stripQuote(fullColName.table_name().table.getText());
    if (fullColName.table_name().schema == null) {
      return new BaseColumn(tableName, colName);
    } else {
      return new BaseColumn(stripQuote(fullColName.table_name().schema.getText()), tableName, colName);
    }
  }

  private String stripQuote(String expr) {
    return expr.replace("\"", "").replace("`", "");
  }

  @Override
  public ColumnOp visitBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx) {
    String opType = null;
    if (ctx.op.getText().equals("+")) {
      opType = "add";
    } else if (ctx.op.getText().equals("-")) {
      opType = "subtract";
    } else if (ctx.op.getText().equals("*")) {
      opType = "multiply";
    } else if (ctx.op.getText().equals("/")) {
      opType = "divide";
    } else if (ctx.op.getText().equals("||")) {
      opType = "||";
    }
    return new ColumnOp(opType, Arrays.asList(
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
          ExpressionGen g = new ExpressionGen();
          col = g.visit(ctx.all_distinct_expression());
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

      @Override
      public ColumnOp visitUnary_manipulation_function(VerdictSQLParser.Unary_manipulation_functionContext ctx) {
        String fname = ctx.function_name.getText().toLowerCase();
        ExpressionGen g = new ExpressionGen();
        return new ColumnOp(fname, g.visit(ctx.expression()));
      }

      @Override
      public ColumnOp visitNoparam_manipulation_function(
          VerdictSQLParser.Noparam_manipulation_functionContext ctx) {
        String fname = ctx.function_name.getText().toLowerCase();
        return new ColumnOp(fname);
      }

      @Override
      public ColumnOp visitBinary_manipulation_function(
          VerdictSQLParser.Binary_manipulation_functionContext ctx) {
        String fname = ctx.function_name.getText().toLowerCase();
        ExpressionGen g = new ExpressionGen();
        return new ColumnOp(fname, Arrays.<UnnamedColumn>asList(
            g.visit(ctx.expression(0)),
            g.visit(ctx.expression(1))
        ));
      }

      @Override
      public ColumnOp visitTernary_manipulation_function(
          VerdictSQLParser.Ternary_manipulation_functionContext ctx) {
        String fname = ctx.function_name.getText().toLowerCase();
        ExpressionGen g = new ExpressionGen();
        return new ColumnOp(fname, Arrays.<UnnamedColumn>asList(
            g.visit(ctx.expression(0)),
            g.visit(ctx.expression(1)),
            g.visit(ctx.expression(2))
        ));
      }

      @Override
      public ColumnOp visitNary_manipulation_function(VerdictSQLParser.Nary_manipulation_functionContext ctx) {
        String fname = ctx.function_name.getText().toLowerCase();
        ExpressionGen g = new ExpressionGen();
        List<UnnamedColumn> columns = new ArrayList<>();
        for (VerdictSQLParser.ExpressionContext expressionContext : ctx.expression()) {
          columns.add(g.visit(expressionContext));
        }
        return new ColumnOp(fname, columns);
      }

      @Override
      public ColumnOp visitExtract_time_function(VerdictSQLParser.Extract_time_functionContext ctx) {
        String fname = "extract";
        ExpressionGen g = new ExpressionGen();
        return new ColumnOp(fname, Arrays.<UnnamedColumn>asList(
            ConstantColumn.valueOf(ctx.extract_unit().getText()),
            g.visit(ctx.expression())
        ));
      }

      @Override
      public ColumnOp visitOverlay_string_function(VerdictSQLParser.Overlay_string_functionContext ctx) {
        String fname = "overlay";
        ExpressionGen g = new ExpressionGen();
        List<UnnamedColumn> operands = new ArrayList<>();
        operands.add(g.visit(ctx.expression(0)));
        operands.add(g.visit(ctx.expression(1)));
        operands.add(g.visit(ctx.expression(2)));
        if (ctx.expression().size() == 4) {
          operands.add(g.visit(ctx.expression(3)));
        }
        return new ColumnOp(fname, operands);
      }

      @Override
      public ColumnOp visitSubstring_string_function(VerdictSQLParser.Substring_string_functionContext ctx) {
        String fname = "substring";
        ExpressionGen g = new ExpressionGen();
        List<UnnamedColumn> operands = new ArrayList<>();
        operands.add(g.visit(ctx.expression(0)));
        operands.add(g.visit(ctx.expression(1)));
        if (ctx.expression().size() == 3) {
          operands.add(g.visit(ctx.expression(2)));
        }
        return new ColumnOp(fname, operands);
      }

    };
    return v.visit(ctx);
  }

  @Override
  public ColumnOp visitCase_expr(VerdictSQLParser.Case_exprContext ctx) {
    if (ctx.search_condition() != null) {
      if (ctx.expression(1) != null) {
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
      if (ctx.expression(3) != null) {
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
    RelationGen g = new RelationGen();
    return SubqueryColumn.getSubqueryColumn((SelectQuery) g.visit(ctx.subquery().select_statement()));
  }

  public UnnamedColumn getSearch_condition(List<VerdictSQLParser.Search_conditionContext> ctx) {
    CondGen g = new CondGen();
    if (ctx.size() == 1) {
      return g.visit(ctx.get(0));
    } else {
      UnnamedColumn col = visit(ctx.get(0));
      for (int i = 0; i < ctx.size(); i++) {
        col = new ColumnOp("and", Arrays.asList(
            col, g.visit(ctx.get(i))
        ));
      }
      return col;
    }
  }
}

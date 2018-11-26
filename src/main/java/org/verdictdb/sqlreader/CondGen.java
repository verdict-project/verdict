/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.sqlreader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.parser.VerdictSQLParser;
import org.verdictdb.parser.VerdictSQLParserBaseVisitor;

public class CondGen extends VerdictSQLParserBaseVisitor<UnnamedColumn> {

  //    private MetaData meta;

  public CondGen() {}

  //    public CondGen(MetaData meta) {
  //        this.meta = meta;
  //    }

  @Override
  public UnnamedColumn visitComp_expr_predicate(VerdictSQLParser.Comp_expr_predicateContext ctx) {
    ExpressionGen g = new ExpressionGen();
    UnnamedColumn e1 = g.visit(ctx.expression(0));
    UnnamedColumn e2 = g.visit(ctx.expression(1));
    if (ctx.comparison_operator().getText().equals("=")) {
      return new ColumnOp("equal", Arrays.asList(e1, e2));
    } else if (ctx.comparison_operator().getText().equals(">")) {
      return new ColumnOp("greater", Arrays.asList(e1, e2));
    } else if (ctx.comparison_operator().getText().equals(">=")) {
      return new ColumnOp("greaterequal", Arrays.asList(e1, e2));
    } else if (ctx.comparison_operator().getText().equals("<")) {
      return new ColumnOp("less", Arrays.asList(e1, e2));
    } else if (ctx.comparison_operator().getText().equals("<=")) {
      return new ColumnOp("lessequal", Arrays.asList(e1, e2));
    } else if (ctx.comparison_operator().getText().equals("<>")
        || ctx.comparison_operator().getText().equals("!=")) {
      return new ColumnOp("notequal", Arrays.asList(e1, e2));
    } else {
      return null;
    }
  }

  @Override
  public UnnamedColumn visitComp_pred_expr_predicate(
      VerdictSQLParser.Comp_pred_expr_predicateContext ctx) {
    ExpressionGen g = new ExpressionGen();
    UnnamedColumn e1 = visit(ctx.predicate());
    UnnamedColumn e2 = g.visit(ctx.expression());
    if (ctx.comparison_operator().getText().equals("=")) {
      return new ColumnOp("equal", Arrays.asList(e1, e2));
    } else if (ctx.comparison_operator().getText().equals(">")) {
      return new ColumnOp("greater", Arrays.asList(e1, e2));
    } else if (ctx.comparison_operator().getText().equals(">=")) {
      return new ColumnOp("greaterequal", Arrays.asList(e1, e2));
    } else if (ctx.comparison_operator().getText().equals("<")) {
      return new ColumnOp("less", Arrays.asList(e1, e2));
    } else if (ctx.comparison_operator().getText().equals("<=")) {
      return new ColumnOp("lessequal", Arrays.asList(e1, e2));
    } else if (ctx.comparison_operator().getText().equals("<>")
        || ctx.comparison_operator().getText().equals("!=")) {
      return new ColumnOp("notequal", Arrays.asList(e1, e2));
    } else {
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
        concat = new ColumnOp("or", Arrays.asList(concat, visit(nctx)));
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
        concat = new ColumnOp("and", Arrays.asList(concat, visit(octx)));
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
      return ColumnOp.not(predicate);
      //      if (predicate instanceof ColumnOp) {
      //        ((ColumnOp) predicate).setOpType("not" + ((ColumnOp) predicate).getOpType());
      //        return predicate;
      //      } else {
      //        return null;
      //      }
    }
  }

  @Override
  public UnnamedColumn visitIs_predicate(VerdictSQLParser.Is_predicateContext ctx) {
    ExpressionGen g = new ExpressionGen();
    UnnamedColumn left = g.visit(ctx.expression());

    if (ctx.null_notnull().NOT() == null) {
      return ColumnOp.rightisnull(left); // ?? is null
    } else {
      return ColumnOp.rightisnotnull(left); // ?? is not null
    }
    //        UnnamedColumn right = visit(ctx.null_notnull());
    //        return new ColumnOp("is", Arrays.asList(left, right));
  }

  @Override
  public UnnamedColumn visitIn_predicate(VerdictSQLParser.In_predicateContext ctx) {
    ExpressionGen g1 = new ExpressionGen();
    if (ctx.subquery() != null) {
      // VerdictLogger.error("Verdict currently does not support IN + subquery condition.");
      UnnamedColumn left = g1.visit(ctx.expression());
      boolean not = (ctx.NOT() != null) ? true : false;
      RelationGen g2 = new RelationGen();
      UnnamedColumn subquery =
          SubqueryColumn.getSubqueryColumn((SelectQuery) g2.visit(ctx.subquery()));
      return not
          ? ColumnOp.notin(Arrays.asList(left, subquery))
          : ColumnOp.in(Arrays.asList(left, subquery));
    } else {
      UnnamedColumn left = g1.visit(ctx.expression());
      boolean not = (ctx.NOT() != null) ? true : false;
      List<UnnamedColumn> expressionList = new ArrayList<UnnamedColumn>();
      expressionList.add(left);
      for (VerdictSQLParser.ExpressionContext ectx : ctx.expression_list().expression()) {
        expressionList.add(g1.visit(ectx));
      }
      return not ? ColumnOp.notin(expressionList) : ColumnOp.in(expressionList);
    }
  }

  @Override
  public UnnamedColumn visitExists_predicate(VerdictSQLParser.Exists_predicateContext ctx) {
    if (ctx.subquery() == null) {
      // VerdictLogger.error("Exists should be followed by a subquery");
      return null;
    }
    RelationGen g = new RelationGen();
    UnnamedColumn subquery =
        SubqueryColumn.getSubqueryColumn((SelectQuery) g.visit(ctx.subquery()));
    return ColumnOp.exists(subquery);
  }

  @Override
  public UnnamedColumn visitLike_predicate(VerdictSQLParser.Like_predicateContext ctx) {
    ExpressionGen g = new ExpressionGen();
    UnnamedColumn left = g.visit(ctx.expression(0));
    UnnamedColumn right = g.visit(ctx.expression(1));
    boolean not = (ctx.NOT() != null) ? true : false;
    if (ctx.LIKE() != null) {
      return not ? ColumnOp.notlike(left, right) : ColumnOp.like(left, right);
    } else if (ctx.RLIKE() != null) {
      return not ? ColumnOp.notrlike(left, right) : ColumnOp.rlike(left, right);
    }
    return null;
  }

  @Override
  public UnnamedColumn visitUnary_func_predicate(VerdictSQLParser.Unary_func_predicateContext ctx) {
    UnnamedColumn pred = visit(ctx.unary_predicate_function());
    return pred;
  }

  @Override
  public UnnamedColumn visitUnary_predicate_function(
      VerdictSQLParser.Unary_predicate_functionContext ctx) {
    ExpressionGen g = new ExpressionGen();
    UnnamedColumn col = g.visit(ctx.expression());
    return col;
  }

  @Override
  public UnnamedColumn visitBinary_func_predicate(
      VerdictSQLParser.Binary_func_predicateContext ctx) {
    UnnamedColumn pred = visit(ctx.binary_predicate_function());
    return pred;
  }

  @Override
  public UnnamedColumn visitBinary_predicate_function(
      VerdictSQLParser.Binary_predicate_functionContext ctx) {
    ExpressionGen g = new ExpressionGen();
    String fname = ctx.function_name.getText().toLowerCase();
    return new ColumnOp(
        fname, Arrays.asList(g.visit(ctx.expression(0)), g.visit(ctx.expression(1))));
  }

  //  @Override
  //  public UnnamedColumn visitFunc_predicate(VerdictSQLParser.Func_predicateContext ctx) {
  //    UnnamedColumn pred = visit(ctx.predicate_function());
  //    return pred;
  //  }
  //
  //  @Override
  //  public UnnamedColumn visitPredicate_function(VerdictSQLParser.Predicate_functionContext ctx) {
  //    ExpressionGen g = new ExpressionGen();
  //    UnnamedColumn col = g.visit(ctx.expression());
  //    return col;
  //  }

  @Override
  public UnnamedColumn visitComp_between_expr(VerdictSQLParser.Comp_between_exprContext ctx) {
    ExpressionGen g = new ExpressionGen();
    UnnamedColumn col = g.visit(ctx.expression(0));
    UnnamedColumn left = g.visit(ctx.expression(1));
    UnnamedColumn right = g.visit(ctx.expression(2));
    return new ColumnOp("between", Arrays.asList(col, left, right));
  }

  //    @Override
  //    public UnnamedColumn visitNull_notnull(VerdictSQLParser.Null_notnullContext ctx) {
  //        if (ctx.NOT() == null) {
  //            return ConstantColumn.valueOf("NULL");
  //        } else {
  //            return ConstantColumn.valueOf("NOT NULL");
  //        }
  //    }

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

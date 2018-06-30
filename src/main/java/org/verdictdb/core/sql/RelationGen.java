package org.verdictdb.core.sql;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.AliasReference;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.ConstantColumn;
import org.verdictdb.core.query.GroupingAttribute;
import org.verdictdb.core.query.JoinTable;
import org.verdictdb.core.query.OrderbyAttribute;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.query.UnnamedColumn;
import org.verdictdb.parser.VerdictSQLBaseVisitor;
import org.verdictdb.parser.VerdictSQLParser;

public class RelationGen extends VerdictSQLBaseVisitor<AbstractRelation> {

//  private MetaData meta;

  private List<SelectItem> selectElems = new ArrayList<>();

//  public RelationGen(MetaData meta) {
//    this.meta = meta;
//  }
  
  public RelationGen() {}

  @Override
  public SelectQuery visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
    SelectQuery sel = (SelectQuery) visit(ctx.query_expression());

    if (ctx.order_by_clause() != null) {
      for (VerdictSQLParser.Order_by_expressionContext o : ctx.order_by_clause().order_by_expression()) {
        ExpressionGen g = new ExpressionGen();
        UnnamedColumn c = g.visit(o.expression());
        OrderbyAttribute orderbyCol = null;
        if (c instanceof BaseColumn) {
          orderbyCol = new OrderbyAttribute(((BaseColumn) c).getColumnName(), (o.DESC() == null) ? "asc" : "desc");
        }
        sel.addOrderby(orderbyCol);
      }
    }

    if (ctx.limit_clause() != null) {
      sel.addLimit(ConstantColumn.valueOf(ctx.limit_clause().number().getText()));
    }
    return sel;
  }

  @Override
  public AbstractRelation visitQuery_expression(VerdictSQLParser.Query_expressionContext ctx) {
    AbstractRelation r = null;
    if (ctx.query_specification() != null) {
      r = this.visit(ctx.query_specification());
    } else if (ctx.query_expression() != null) {
      r = this.visit(ctx.query_expression());
    }
    /*
        for (VerdictSQLParser.UnionContext union : ctx.union()) {
            AbstractRelation other = this.visit(union);
            SetRelation.SetType type;
            if (union.UNION() != null) {
                type = SetRelation.SetType.UNION;
                if (union.ALL() != null) {
                    type = SetRelation.SetType.UNION_ALL;
                }
            } else if (union.EXCEPT() != null) {
                type = SetRelation.SetType.EXCEPT;
            } else if (union.INTERSECT() != null) {
                type = SetRelation.SetType.INTERSECT;
            } else {
                type = SetRelation.SetType.UNKNOWN;
            }
            r = new SetRelation(vc, r, other, type);
        }
     */
    return r;
  }

  /**
   * Parses a depth-one select statement. If there exist subqueries, this function
   * will be called recursively.
   */
  @Override
  public AbstractRelation visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {

    class SelectListExtractor extends VerdictSQLBaseVisitor<List<SelectItem>> {
      @Override
      public List<SelectItem> visitSelect_list(
          VerdictSQLParser.Select_listContext ctx) {
        List<SelectItem> selectList = new ArrayList<>();
        for (VerdictSQLParser.Select_list_elemContext a : ctx.select_list_elem()) {
          VerdictSQLBaseVisitor<SelectItem> v = new VerdictSQLBaseVisitor<SelectItem>() {
            @Override
            public SelectItem visitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
              SelectItem elem = null;
              if (ctx.STAR() != null) {
                if (ctx.table_name() == null) {
                  elem = new AsteriskColumn();
                } else {
                  elem = new AsteriskColumn(ctx.table_name().getText());
                }
              } else {
                ExpressionGen g = new ExpressionGen();
                if (g.visit(ctx.expression()) instanceof BaseColumn) {
                  elem = g.visit(ctx.expression());
                  if (ctx.column_alias() != null) {
                    elem = new AliasedColumn((BaseColumn) elem, ctx.column_alias().getText());
                  }
                } else if (g.visit(ctx.expression()) instanceof ColumnOp) {
                  elem = g.visit(ctx.expression());
                  if (ctx.column_alias() != null) {
                    elem = new AliasedColumn((ColumnOp) elem, ctx.column_alias().getText());
                  }
                } else if (g.visit(ctx.expression()) instanceof ConstantColumn) {
                  elem = g.visit(ctx.expression());
                  if (ctx.column_alias() != null) {
                    elem = new AliasedColumn((ConstantColumn) elem, ctx.column_alias().getText());
                  }
                }
              }
              return elem;
            }
          };
          SelectItem e = v.visit(a);
          selectList.add(e);
        }
        return selectList;
      }
    }

    List<AbstractRelation> tableSources = new ArrayList<>(); // assume that only the first entry can be JoinedRelation
    for (VerdictSQLParser.Table_sourceContext s : ctx.table_source()) {
      AbstractRelation r1 = this.visit(s);
      tableSources.add(r1);
    }
    // parse the where clause;
    UnnamedColumn where = null;
    if (ctx.WHERE() != null) {
      CondGen g = new CondGen();
      where = g.visit(ctx.where);
    }

    // parse select list
    SelectListExtractor select = new SelectListExtractor();
    List<SelectItem> elems = select.visit(ctx.select_list());

    selectElems = elems; // used in visitSelect_statement()

    SelectQuery sel = SelectQuery.create(
        selectElems,
        tableSources);
    if (where != null) {
      sel.addFilterByAnd(where);
    }

    if (ctx.GROUP() != null) {
      List<GroupingAttribute> groupby = new ArrayList<GroupingAttribute>();
      for (VerdictSQLParser.Group_by_itemContext g : ctx.group_by_item()) {
        
        class GroupbyGen extends VerdictSQLBaseVisitor<GroupingAttribute> {
          
//          MetaData meta;
//          public GroupbyGen(MetaData meta) {this.meta = meta; }
          public GroupbyGen() {}
          
          @Override
          public GroupingAttribute visitColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx) {
            String[] t = ctx.getText().split("\\.");
            if (t.length >= 2) {
              return new AliasReference(t[1]);
            } else {
              return new AliasReference(t[0]);
            }
          }
        }
        GroupbyGen expg = new GroupbyGen();
        GroupingAttribute gexpr = expg.visit(g);
        boolean aliasFound = false;
        if (!aliasFound) {
          groupby.add(gexpr);
        }
      }
      if (!groupby.isEmpty()) {
        sel.addGroupby(groupby);
      }
    }

    UnnamedColumn having = null;
    if (ctx.HAVING() != null){
      CondGen g = new CondGen();
      having = g.visit(ctx.having);
    }
    if (having != null){
      sel.addHavingByAnd(having);
    }
    return sel;
  }

  private UnnamedColumn joinCond = null;

  private JoinTable.JoinType joinType = null;

  @Override
  public AbstractRelation visitTable_source(VerdictSQLParser.Table_sourceContext ctx) {
    return visitTable_source_item_joined(ctx.table_source_item_joined());
  }

  @Override
  public AbstractRelation visitTable_source_item_joined(VerdictSQLParser.Table_source_item_joinedContext ctx) {
    AbstractRelation r = this.visit(ctx.table_source_item());
    if (ctx.join_part().isEmpty()){
      return r;
    }
    JoinTable jr = JoinTable.createBase(r, new ArrayList<JoinTable.JoinType>(), new ArrayList<UnnamedColumn>());
    //join error location: r2 is null
    for (VerdictSQLParser.Join_partContext j : ctx.join_part()) {
      AbstractRelation r2 = visit(j);
      jr.addJoinTable(r2, joinType, joinCond);
    }
    return jr;
  }

  @Override
  public AbstractRelation visitJoin_part(VerdictSQLParser.Join_partContext ctx) {
    if (ctx.INNER() != null) {
      AbstractRelation r = this.visit(ctx.table_source());
      CondGen g = new CondGen();
      UnnamedColumn cond = g.visit(ctx.search_condition());
      joinType = JoinTable.JoinType.inner;
      joinCond = cond;
      return r;
    }
    else if (ctx.LEFT() != null) {
      AbstractRelation r = this.visit(ctx.table_source());
      CondGen g = new CondGen();
      UnnamedColumn cond = g.visit(ctx.search_condition());
      joinType = JoinTable.JoinType.leftouter;
      joinCond = cond;
      return r;
    }
    else if (ctx.RIGHT() != null) {
      AbstractRelation r = this.visit(ctx.table_source());
      CondGen g = new CondGen();
      UnnamedColumn cond = g.visit(ctx.search_condition());
      joinType = JoinTable.JoinType.rightouter;
      joinCond = cond;
      return r;
    }
    else {
      AbstractRelation r = this.visit(ctx.table_source());
      CondGen g = new CondGen();
      UnnamedColumn cond = g.visit(ctx.search_condition());
      joinType = JoinTable.JoinType.inner;
      joinCond = cond;
      return r;
    }
  }

  @Override
  public AbstractRelation visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx) {
    AbstractRelation r = null;
    if (ctx.as_table_alias()!=null) {
      if (ctx.table_name_with_hint().table_name().schema!=null) {
        r = new BaseTable(ctx.table_name_with_hint().table_name().schema.getText(),
            ctx.table_name_with_hint().table_name().table.getText(),
            ctx.as_table_alias().table_alias().getText());
      }
      else {
        r = BaseTable.getBaseTableWithoutSchema(ctx.table_name_with_hint().table_name().table.getText(),
            ctx.as_table_alias().table_alias().getText());

      }
    }
    else {
      if (ctx.table_name_with_hint().table_name().schema!=null) {
        r = new BaseTable(ctx.table_name_with_hint().table_name().schema.getText(),
            ctx.table_name_with_hint().table_name().table.getText());
      }
      else {
        r = new BaseTable(ctx.table_name_with_hint().table_name().table.getText());
      }
    }
    return r;
  }

  @Override
  public AbstractRelation visitDerived_table_source_item(VerdictSQLParser.Derived_table_source_itemContext ctx) {
    RelationGen gen = new RelationGen();
    SelectQuery r = (SelectQuery) gen.visit(ctx.derived_table().subquery().select_statement());
    if (ctx.as_table_alias() != null) {
      r.setAliasName(ctx.as_table_alias().table_alias().getText());
    }
    if (ctx.column_alias_list() != null) {
      for (int i=0;i<ctx.column_alias_list().column_alias().size();i++){
        r.getSelectList().set(i, new AliasedColumn((UnnamedColumn) r.getSelectList().get(i), ctx.column_alias_list().column_alias(i).getText()));
      }
    }
    return r;
  }
}


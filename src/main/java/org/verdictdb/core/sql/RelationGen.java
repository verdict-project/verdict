package org.verdictdb.core.sql;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.core.logical_query.*;
import org.verdictdb.parser.*;

import java.util.*;

public class RelationGen extends VerdictSQLBaseVisitor<AbstractRelation> {

    private MetaData meta;

    private List<SelectItem> selectElems = null;

    public RelationGen(MetaData meta) {
        this.meta = meta;
    }

    @Override
    public SelectQueryOp visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
        SelectQueryOp sel = (SelectQueryOp) visit(ctx.query_expression());

        // If the raw select elements are present in order-by or group-by clauses, we replace them
        // with their aliases.
        Map<UnnamedColumn, String> selectExprToAlias = new HashMap<>();
        for (SelectItem elem : selectElems) {
            selectExprToAlias.put(((AliasedColumn)elem).getColumn(), ((AliasedColumn)elem).getAliasName());
        }

        // use the same resolver as for the select list elements to attach the same tables to the columns
        TableSourceResolver resolver = new TableSourceResolver(vc, tableAliasAndColNames);

        if (ctx.order_by_clause() != null) {
            for (VerdictSQLParser.Order_by_expressionContext o : ctx.order_by_clause().order_by_expression()) {
                ExpressionGen g = new ExpressionGen(meta);
                UnnamedColumn c = g.visit(o);
                AliasColumn orderbyCol = null;
                if (c instanceof BaseColumn){
                    orderbyCol = new AliasColumn(selectExprToAlias.get(c), (o.DESC()==null));
                }
                if (selectExprToAlias.containsKey(c)) {
                     orderbyCol = new AliasColumn(selectExprToAlias.get(c), (o.DESC()==null));
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
        // 1. extract all tables objects for creating joined table sources later.
        // the complete INNER JOIN / CROSS JOIN / LATERAL VIEW expressions are converted to a single ExactRelation
        // object.
        // 2. extract all base table names for column name resolution.
        // a. if a user defines an alias for a table, we expect he is using the alias
        // for in column names in place of
        // of the original table names. If no table name is specified, we find relevant
        // tables using the base table
        // names and insert aliases for those column names..
        // b. if a user doesn't define an alias, we generate a random alias, performs
        // the same process using the
        // auto-generated alias.
        // c. if a user defines an alias for a derived table (he must do so), we extract
        // the column names for the derived table.
        List<AbstractRelation> tableSources = new ArrayList<>(); // assume that only the first entry can be
        // JoinedRelation
        for (VerdictSQLParser.Table_sourceContext s : ctx.table_source()) {
            AbstractRelation r1 = this.visit(s);
            tableSources.add(r1);

        // parse the where clause; we also replace all base table names with their alias
        // names.
        UnnamedColumn where = null;
        if (ctx.WHERE() != null) {
            CondGen g = new CondGen(meta);
            where = g.visit(ctx.where);
            ColNameResolver resolver = new ColNameResolver(tableAliasAndColNames);
            where = resolver.visit(where);
            // at this point, all table names in the where clause are all aliased.
        }

        // parse select list
        SelectListExtractor select = new SelectListExtractor();
        Triple<List<SelectElem>, List<SelectElem>, List<SelectElem>> elems = select.visit(ctx.select_list());
        List<SelectElem> nonaggs = elems.getLeft();
        List<SelectElem> aggs = elems.getMiddle();
        List<SelectElem> bothInOrder = elems.getRight();

        // replace all base tables with their aliases
        TableSourceResolver resolver = new TableSourceResolver(vc, tableAliasAndColNames);
        nonaggs = replaceTableNamesWithAliasesIn(nonaggs, resolver);
        aggs = replaceTableNamesWithAliasesIn(aggs, resolver);
        bothInOrder = replaceTableNamesWithAliasesIn(bothInOrder, resolver);
        selectElems = bothInOrder; // used in visitSelect_statement()

        if (aggs.size() == 0) {
            // simple projection
            joinedTableSource = new ProjectedRelation(vc, joinedTableSource, bothInOrder);
        } else {
            // aggregate relation

            // step 1: obtains groupby expressions
            // groupby expressions must be fully resolved from the table sources (without
            // referring to the select list)
            // resolving groupby names from alises is currently disabled.
            // if the groupby expression includes base table names, we replace them with
            // their aliases.
            if (ctx.GROUP() != null) {
                List<Expr> groupby = new ArrayList<Expr>();
                for (Group_by_itemContext g : ctx.group_by_item()) {
                    Expr gexpr = resolver.visit(Expr.from(vc, g.expression()));
                    boolean aliasFound = false;
                    //
                    // // search in alises
                    // for (SelectElem s : bothInOrder) {
                    // if (s.aliasPresent() && gexpr.toStringWithoutQuote().equals(s.getAlias())) {
                    // groupby.add(s.getExpr());
                    // aliasFound = true;
                    // break;
                    // }
                    // }

                    if (!aliasFound) {
                        groupby.add(gexpr);
                    }
                }
                if (!groupby.isEmpty()) {
                    boolean isRollUp = (ctx.ROLLUP() != null);
                    joinedTableSource = new GroupedRelation(vc, joinedTableSource, groupby, isRollUp);
                }
            }

            joinedTableSource = new AggregatedRelation(vc, joinedTableSource, bothInOrder);
        }
        SelectQueryOp sel = SelectQueryOp.getSelectQueryOp(
                Arrays.asList(),
        tableSources);
        sel.addFilterByAnd(where);
        return joinedTableSource;
    }

        class SelectListExtractor
                extends VerdictSQLBaseVisitor<Triple<List<SelectItem>, List<SelectItem>, List<SelectItem>>> {
            @Override
            public Triple<List<SelectItem>, List<SelectItem>, List<SelectItem>> visitSelect_list(
                    VerdictSQLParser.Select_listContext ctx) {
                List<SelectItem> nonagg = new ArrayList<>();
                List<SelectItem> agg = new ArrayList<>();
                List<SelectItem> both = new ArrayList<>();
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
                                ExpressionGen g = new ExpressionGen(meta);
                                if (g.visit(ctx.expression()) instanceof BaseColumn) {
                                    elem = (BaseColumn) g.visit(ctx.expression());
                                }
                                else if (g.visit(ctx.expression()) instanceof ColumnOp) {
                                    elem = (ColumnOp) g.visit(ctx.expression());
                                }
                                else if (g.visit(ctx.expression()) instanceof ConstantColumn) {
                                    elem = (ConstantColumn) g.visit(ctx.expression());
                                }
                            }

                            if (ctx.column_alias() != null) {
                                elem.setAlias(ctx.column_alias().getText());
                            }
                            return elem;
                        }
                    };
                    SelectItem e = SelectItem.from(vc, a);
                    if (e.isagg()) {
                        agg.add(e);
                    } else {
                        nonagg.add(e);
                    }
                    both.add(e);
                }
                return Triple.of(nonagg, agg, both);
            }
        }
}

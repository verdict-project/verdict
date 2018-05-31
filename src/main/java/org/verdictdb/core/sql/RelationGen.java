package org.verdictdb.core.sql;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.math3.analysis.function.Abs;
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
           // selectExprToAlias.put(((AliasedColumn) elem).getColumn(), ((AliasedColumn) elem).getAliasName());
        }

        // use the same resolver as for the select list elements to attach the same tables to the columns
        //TableSourceResolver resolver = new TableSourceResolver(vc, tableAliasAndColNames);

        if (ctx.order_by_clause() != null) {
            for (VerdictSQLParser.Order_by_expressionContext o : ctx.order_by_clause().order_by_expression()) {
                ExpressionGen g = new ExpressionGen(meta);
                UnnamedColumn c = g.visit(o);
                OrderbyAttribute orderbyCol = null;
                if (c instanceof BaseColumn) {
                    orderbyCol = new OrderbyAttribute(selectExprToAlias.get(c), (o.DESC() == null) ? "asc" : "desc");
                }
                if (selectExprToAlias.containsKey(c)) {
                    orderbyCol = new OrderbyAttribute(selectExprToAlias.get(c), (o.DESC() == null) ? "asc" : "desc");
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
                                ExpressionGen g = new ExpressionGen(meta);
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

        List<AbstractRelation> tableSources = new ArrayList<>(); // assume that only the first entry can be
        // JoinedRelation
        for (VerdictSQLParser.Table_sourceContext s : ctx.table_source()) {
            AbstractRelation r1 = this.visit(s);
            tableSources.add(r1);
        }
        // parse the where clause; we also replace all base table names with their alias
        // names.
        UnnamedColumn where = null;
        if (ctx.WHERE() != null) {
            CondGen g = new CondGen(meta);
            where = g.visit(ctx.where);
            //ColNameResolver resolver = new ColNameResolver(tableAliasAndColNames);
            //where = resolver.visit(where);
            // at this point, all table names in the where clause are all aliased.
        }

        // parse select list
        SelectListExtractor select = new SelectListExtractor();
        List<SelectItem> elems = select.visit(ctx.select_list());

        // replace all base tables with their aliases
        //TableSourceResolver resolver = new TableSourceResolver(vc, tableAliasAndColNames);
        //elems = replaceTableNamesWithAliasesIn(elems, resolver);
        selectElems = elems; // used in visitSelect_statement()

        SelectQueryOp sel = SelectQueryOp.getSelectQueryOp(
                selectElems,
                tableSources);
        if (where != null) {
            sel.addFilterByAnd(where);
        }

        if (ctx.GROUP() != null) {
            List<GroupingAttribute> groupby = new ArrayList<GroupingAttribute>();
            for (VerdictSQLParser.Group_by_itemContext g : ctx.group_by_item()) {
                class GroupbyGen extends VerdictSQLBaseVisitor<GroupingAttribute> {
                    MetaData meta;
                    public GroupbyGen(MetaData meta) {this.meta = meta; }
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
                GroupbyGen expg = new GroupbyGen(meta);
                //GroupingAttribute gexpr = resolver.visit(expg.visit(g));
                GroupingAttribute gexpr = expg.visit(g);
                boolean aliasFound = false;
                if (!aliasFound) {
                    groupby.add(gexpr);
                }
            }
            if (!groupby.isEmpty()) {
                sel.addGroupby(groupby);
                //boolean isRollUp = (ctx.ROLLUP() != null);
            }
        }
        return sel;
    }

    // The tableSource returned from this class is supposed to include all
    // necessary join conditions; thus, we do not
    // need to search for their join conditions in the where clause.

    // dyoon : Apr 02, 2018
    // Removed this nested class because 1) it seemed necessary; and 2) RelationGen class now
    // requires to access the information gathered from methods previously resided in this
    // nested class.
//    class TableSourceExtractor extends VerdictSQLBaseVisitor<ExactRelation> {

    private List<AbstractRelation> relations = new ArrayList<>();

    private UnnamedColumn joinCond = null;

    private JoinTable.JoinType joinType = null;

    @Override
    public AbstractRelation visitTable_source_item_joined(VerdictSQLParser.Table_source_item_joinedContext ctx) {
        AbstractRelation r = this.visit(ctx.table_source_item());
        if (ctx.join_part().isEmpty()){
            return r;
        }
        JoinTable jr = JoinTable.getJoinTable(Arrays.asList(r), null, null);
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
            AbstractRelation r = (AbstractRelation) this.visit(ctx.table_source());
            CondGen g = new CondGen(meta);
            UnnamedColumn cond = g.visit(ctx.search_condition());
            //ColNameResolver resolver = new ColNameResolver(tableAliasAndColNames);
            //Cond resolved = resolver.visit(cond);

            joinType = JoinTable.JoinType.inner;
            joinCond = cond;
            return r;
        }
        else if (ctx.LEFT() != null) {
            AbstractRelation r = this.visit(ctx.table_source());
            CondGen g = new CondGen(meta);
            UnnamedColumn cond = g.visit(ctx.search_condition());
            //ColNameResolver resolver = new ColNameResolver(tableAliasAndColNames);
            //Cond resolved = resolver.visit(cond);


            joinType = JoinTable.JoinType.leftouter;
            //if (ctx.SEMI() != null) {
            //    joinType = JoinType.LEFT_SEMI;
            //}

            joinCond = cond;
            return r;
        }
        else if (ctx.RIGHT() != null) {
            AbstractRelation r = this.visit(ctx.table_source());
            CondGen g = new CondGen(meta);
            UnnamedColumn cond = g.visit(ctx.search_condition());
            //ColNameResolver resolver = new ColNameResolver(tableAliasAndColNames);
            //Cond resolved = resolver.visit(cond);

            joinType = JoinTable.JoinType.rightouter;
            joinCond = cond;
            return r;
        }
        /*
        else if (ctx.CROSS() != null) {
//                TableSourceExtractor ext = new TableSourceExtractor();
            ExactRelation r = this.visit(ctx.table_source());
            joinType = JoinType.CROSS;
            joinCond = null;
            return r;
        }
        else if (ctx.LATERAL() != null) {
            LateralFunc lf = LateralFunc.from(vc, ctx.lateral_view_function());
            String tableAlias = (ctx.table_alias() == null)? null : ctx.table_alias().getText();
            String columnAlias = (ctx.column_alias() == null)? null : ctx.column_alias().getText();
            LateralViewRelation r = new LateralViewRelation(vc, lf, tableAlias, columnAlias);
            joinType = JoinType.LATERAL;
            joinCond = null;

            // used later to update the select list
            TableUniqueName tabName = new TableUniqueName(null, r.getAlias());
            Set<String> colNames = new HashSet<String>();
            colNames.add(r.getColumnAlias());
            tableAliasAndColNames.put(tabName, Pair.of(r.getAlias(), colNames));

            return r;
        }*/
        else {
//            VerdictLogger.error(this, "Unsupported join condition: " + ctx.getText());
//            return null;
            AbstractRelation r = this.visit(ctx.table_source());
            CondGen g = new CondGen(meta);
            UnnamedColumn cond = g.visit(ctx.search_condition());
            //ColNameResolver resolver = new ColNameResolver(tableAliasAndColNames);
            //Cond resolved = resolver.visit(cond);
            joinType = JoinTable.JoinType.inner;
            joinCond = cond;
            return r;
        }
    }

    @Override
    public AbstractRelation visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx) {
        String tableName = ctx.table_name_with_hint().table_name().getText();
        AbstractRelation r = null;
        if (ctx.as_table_alias()!=null) {
            //todo
            r = new BaseTable(ctx.table_name_with_hint().table_name().schema.getText(),
                    ctx.table_name_with_hint().table_name().table.getText(), ctx.as_table_alias().table_alias().getText());
        }
        //TableUniqueName tabName = ((SingleRelation) r).getTableName();
        //Set<String> colNames = vc.getMeta().getColumns(tabName);
        //tableAliasAndColNames.put(tabName, Pair.of(r.getAlias(), colNames));
        return r;
    }

    @Override
    public AbstractRelation visitDerived_table_source_item(VerdictSQLParser.Derived_table_source_itemContext ctx) {
        RelationGen gen = new RelationGen(meta);
        SelectQueryOp r = (SelectQueryOp) gen.visit(ctx.derived_table().subquery().select_statement());
        if (ctx.as_table_alias() != null) {
            r.setAliasName(ctx.as_table_alias().table_alias().getText());
        }
    /*
        Set<String> colNames = new HashSet<String>();
        if (r instanceof AggregatedRelation) {
            List<SelectElem> elems = ((AggregatedRelation) r).getElemList();
            for (SelectElem elem : elems) {
                if (elem.aliasPresent()) {
                    colNames.add(elem.getAlias());
                } else {
                    colNames.add(elem.getExpr().toSql());
                }
            }
        }
        else if (r instanceof ProjectedRelation) {
            List<SelectElem> elems = ((ProjectedRelation) r).getSelectElems();
            for (SelectElem elem : elems) {
                if (elem.aliasPresent()) {
                    colNames.add(elem.getAlias());
                } else {
                    colNames.add(elem.getExpr().toSql());   // I don't think this should be called, since all elements are aliased.
                }
            }
        }

        TableUniqueName tabName = new TableUniqueName(null, r.getAlias());
        tableAliasAndColNames.put(tabName, Pair.of(r.getAlias(), colNames));
    */
        return r;
    }
}


package org.verdictdb.core.sql;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.logical_query.*;
import org.verdictdb.parser.*;

import java.util.*;

public class RelationGen extends VerdictSQLBaseVisitor<AbstractRelation> {

    private MetaData meta;

    private List<SelectItem> selectElems = null;

    private int itemID = 1;

    //key is the column name and value is table alias name
    private HashMap<String, String> tableAliasAndColNames = new HashMap<>();

    //key is the schema name and table name and the value is table alias name
    private HashMap<Pair<String, String>, String> tableInfoAndAlias = new HashMap<>();

    //key is the select column name, value is their alias
    private HashMap<String, String> columnAlias = new HashMap<>();

    public RelationGen(MetaData meta) {
        this.meta = meta;
    }

    @Override
    public SelectQueryOp visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
        SelectQueryOp sel = (SelectQueryOp) visit(ctx.query_expression());

        // If the raw select elements are present in order-by or group-by clauses, we replace them
        // with their aliases.
        Map<UnnamedColumn, String> selectExprToAlias = new HashMap<>();

        // use the same resolver as for the select list elements to attach the same tables to the columns
        //TableSourceResolver resolver = new TableSourceResolver(vc, tableAliasAndColNames);

        if (ctx.order_by_clause() != null) {
            for (VerdictSQLParser.Order_by_expressionContext o : ctx.order_by_clause().order_by_expression()) {
                ExpressionGen g = new ExpressionGen(tableAliasAndColNames, tableInfoAndAlias, meta);
                UnnamedColumn c = g.visit(o.expression());
                OrderbyAttribute orderbyCol = null;
                if (c instanceof BaseColumn) {
                    if (columnAlias.containsKey(((BaseColumn)c).getColumnName())){
                        orderbyCol = new OrderbyAttribute(columnAlias.get(((BaseColumn)c).getColumnName()), (o.DESC() == null) ? "asc" : "desc");
                    }
                    else orderbyCol = new OrderbyAttribute(((BaseColumn) c).getColumnName(), (o.DESC() == null) ? "asc" : "desc");
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
                                ExpressionGen g = new ExpressionGen(tableAliasAndColNames, tableInfoAndAlias, meta);
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

        // assume that only the first entry can be JoinedRelation
        List<AbstractRelation> tableSources = new ArrayList<>();

        for (VerdictSQLParser.Table_sourceContext s : ctx.table_source()) {
            AbstractRelation r1 = this.visit(s);
            tableSources.add(r1);
        }
        // parse the where clause; we also replace all base table names with their alias
        // names.
        UnnamedColumn where = null;
        if (ctx.WHERE() != null) {
            CondGen g = new CondGen(tableAliasAndColNames, tableInfoAndAlias, meta);
            where = g.visit(ctx.where);
            // at this point, all table names in the where clause are all aliased.
        }

        // parse select list
        SelectListExtractor select = new SelectListExtractor();
        List<SelectItem> elems = select.visit(ctx.select_list());

        selectElems = elems;

        for (int i=0;i<selectElems.size();i++){
            if (!(selectElems.get(i) instanceof AliasedColumn)&&!(selectElems.get(i) instanceof AsteriskColumn)){
                if (selectElems.get(i) instanceof BaseColumn){
                    columnAlias.put(((BaseColumn) selectElems.get(i)).getColumnName(), "vc"+itemID);
                    selectElems.set(i, new AliasedColumn((BaseColumn)selectElems.get(i), "vc"+itemID++));
                }
                else if (selectElems.get(i) instanceof ColumnOp){
                    if (((ColumnOp)selectElems.get(i)).getOpType().equals("count")) {
                        selectElems.set(i, new AliasedColumn((BaseColumn)selectElems.get(i), "c"+itemID++));
                    }
                    else if (((ColumnOp)selectElems.get(i)).getOpType().equals("sum")) {
                        selectElems.set(i, new AliasedColumn((BaseColumn)selectElems.get(i), "s"+itemID++));
                    }
                    else if (((ColumnOp)selectElems.get(i)).getOpType().equals("avg")) {
                        selectElems.set(i, new AliasedColumn((BaseColumn)selectElems.get(i), "a"+itemID++));
                    }
                    else selectElems.set(i, new AliasedColumn((BaseColumn)selectElems.get(i), "vc"+itemID++));
                }
            }
            else if (selectElems.get(i) instanceof AliasedColumn && ((AliasedColumn)selectElems.get(i)).getColumn() instanceof BaseColumn){
                columnAlias.put(((BaseColumn) ((AliasedColumn)selectElems.get(i)).getColumn()).getColumnName(),
                        ((AliasedColumn)selectElems.get(i)).getAliasName());
            }
        }

        SelectQueryOp sel = SelectQueryOp.getSelectQueryOp(
                selectElems,
                tableSources);
        if (where != null) {
            sel.addFilterByAnd(where);
        }

        if (ctx.GROUP() != null) {
            List<GroupingAttribute> groupby = new ArrayList<>();
            for (VerdictSQLParser.Group_by_itemContext g : ctx.group_by_item()) {
                class GroupbyGen extends VerdictSQLBaseVisitor<GroupingAttribute> {

                    MetaData meta;

                    public GroupbyGen(MetaData meta) {
                        this.meta = meta;
                    }

                    @Override
                    public GroupingAttribute visitColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx) {
                        String[] t = ctx.getText().split("\\.");
                        if (t.length == 3) {
                            if (columnAlias.containsKey(t[2])) return new AliasReference(columnAlias.get(t[2]));
                            else return new AliasReference(t[2]);
                        }
                        if (t.length == 2) {
                            if (columnAlias.containsKey(t[1])) return new AliasReference(columnAlias.get(t[1]));
                            else return new AliasReference(t[1]);
                        } else {
                            if (columnAlias.containsKey(t[0])) return new AliasReference(columnAlias.get(t[0]));
                            else return new AliasReference(t[0]);
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

        UnnamedColumn having = null;
        if (ctx.HAVING() != null){
            CondGen g = new CondGen(tableAliasAndColNames, tableInfoAndAlias, meta);
            having = g.visit(ctx.having);
        }
        if (having != null){
            sel.addHavingByAnd(having);
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

    private UnnamedColumn joinCond = null;

    private JoinTable.JoinType joinType = null;

    // Setup the ColNameResolver
    public void setupColNameResolver(AbstractRelation t){
        if (!t.getAliasName().isPresent()){
            t.setAliasName("vt"+itemID++);
        }
        if (t instanceof BaseTable){
            if (((BaseTable) t).getSchemaName()==""){
                ((BaseTable) t).setSchemaName(meta.getDefaultSchema());
            }
            HashMap<MetaData.tableInfo, List<ImmutablePair<String, MetaData.dataType>>> tablesData = meta.getTablesData();
            List<ImmutablePair<String, MetaData.dataType>> cols = tablesData.get(MetaData.tableInfo.getTableInfo(
                    ((BaseTable) t).getSchemaName(), ((BaseTable) t).getTableName()));
            for (Pair<String, MetaData.dataType> c:cols){
                tableAliasAndColNames.put(c.getKey(),t.getAliasName().toString());
            }
            tableInfoAndAlias.put(new ImmutablePair<>(((BaseTable) t).getSchemaName(), ((BaseTable) t).getTableName()),
                    t.getAliasName().toString());
        }
        else if (t instanceof JoinTable){
            for (AbstractRelation table:((JoinTable) t).getJoinList()){
                setupColNameResolver(table);
            }
        }
        else if (t instanceof SelectQueryOp){
            // Invariant: Only Aliased Column or Asterisk Column should appear in the subquery
            for (SelectItem sel:((SelectQueryOp) t).getSelectList()){
                if (sel instanceof AliasedColumn){
                    tableAliasAndColNames.put(((AliasedColumn) sel).getAliasName(), t.getAliasName().toString());
                    if (((AliasedColumn) sel).getColumn() instanceof BaseColumn){
                        tableAliasAndColNames.put(((BaseColumn) ((AliasedColumn) sel).getColumn()).getColumnName(),
                                t.getAliasName().toString());
                    }
                }
                else if (sel instanceof AsteriskColumn){
                    for (AbstractRelation from:((SelectQueryOp) t).getFromList()){
                        from.setAliasName(t.getAliasName().toString());
                        setupColNameResolver(from);
                        if (from instanceof BaseTable){
                            tableInfoAndAlias.put(new ImmutablePair<>(((BaseTable) t).getSchemaName(), ((BaseTable) t).getTableName()),
                                    t.getAliasName().toString());
                        }
                    }
                }
            }
        }
    }

    @Override
    public AbstractRelation visitTable_source(VerdictSQLParser.Table_sourceContext ctx) {
        return visitTable_source_item_joined(ctx.table_source_item_joined());
    }

    @Override
    public AbstractRelation visitTable_source_item_joined(VerdictSQLParser.Table_source_item_joinedContext ctx) {
        AbstractRelation r = this.visit(ctx.table_source_item());
        setupColNameResolver(r);
        if (ctx.join_part().isEmpty()){
            return r;
        }
        JoinTable jr = JoinTable.getBaseJoinTable(r, new ArrayList<JoinTable.JoinType>(), new ArrayList<UnnamedColumn>());
        //join error location: r2 is null
        for (VerdictSQLParser.Join_partContext j : ctx.join_part()) {
            AbstractRelation r2 = visit(j);
            setupColNameResolver(r2);
            jr.addJoinTable(r2, joinType, joinCond);
        }
        return jr;
    }

    @Override
    public AbstractRelation visitJoin_part(VerdictSQLParser.Join_partContext ctx) {
        if (ctx.INNER() != null) {
            AbstractRelation r =  this.visit(ctx.table_source());
            CondGen g = new CondGen(tableAliasAndColNames, tableInfoAndAlias, meta);
            UnnamedColumn cond = g.visit(ctx.search_condition());
            //ColNameResolver resolver = new ColNameResolver(tableAliasAndColNames);
            //Cond resolved = resolver.visit(cond);

            joinType = JoinTable.JoinType.inner;
            joinCond = cond;
            return r;
        }
        else if (ctx.LEFT() != null) {
            AbstractRelation r = this.visit(ctx.table_source());
            CondGen g = new CondGen(tableAliasAndColNames, tableInfoAndAlias, meta);
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
            CondGen g = new CondGen(tableAliasAndColNames, tableInfoAndAlias, meta);
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
            CondGen g = new CondGen(tableAliasAndColNames, tableInfoAndAlias, meta);
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


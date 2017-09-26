/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.datatypes.Alias;
import edu.umich.verdict.datatypes.ColumnName;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.exceptions.VerdictQuerySyntaxException;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.TypeCasting;
import edu.umich.verdict.util.VerdictLogger;

@Deprecated
class AnalyticSelectStatementRewriter extends SelectStatementBaseRewriter {

    protected VerdictJDBCContext vc;

    protected VerdictQuerySyntaxException e;

    protected List<Boolean> aggColumnIndicator;

    // Alias propagation
    // 1. we declare aliases for all columns.
    // 2. the alias of a non-aggregate column can simply be the column name itself.
    // 3. the alias of an aggregate column is an automatically generalized name
    // (using genAlias()) unless its alias
    // is explicitly specified.
    // 4. all declared aliases are stored in this field so that an outer query can
    // access.
    // 5. the displayed column labels should be properly handled by VerdictResultSet
    // class.
    protected List<Pair<String, Alias>> colName2Aliases;

    // Records the alias information from the derived tables; we will need this to
    // replace
    // The key is the alias name of a derived table.
    // The value is the map of original column name and its Aliases (i.e., the
    // original col name and its alias).
    protected Map<String, Map<String, Alias>> derivedTableColName2Aliases;

    // This field stores the tables sources of only current level. That is, does not
    // store the table sources
    // of the subqueries.
    // Note: currently, this field does not contain derived fields.
    // map of original table to a sample table.
    // Key: original table name
    // Value: pair of a sample name and its sample creation parameter
    protected Map<TableUniqueName, Pair<SampleParam, TableUniqueName>> replacedTableSources;

    // This field stores the replaced table sources of all levels.
    // left is the original table name, and the right is the replaced table name.
    protected final Map<TableUniqueName, TableUniqueName> cumulativeReplacedTableSources = new TreeMap<TableUniqueName, TableUniqueName>();

    // For every table source, we remember the table name and its alias.
    // This info is used in the other clauses (such as where clause and select list)
    // for replacing the table names with
    // their proper aliases.
    protected Map<TableUniqueName, Alias> tableAliases = new HashMap<TableUniqueName, Alias>();

    // Records the column index of a mean aggregation and the column index for its
    // error.
    // If the right value is 0, it means there's no error info column.
    // All valid column indexes are not smaller than 1 (namely, one-indexed).
    protected Map<Integer, Integer> meanColIndex2ErrColIndex;

    public AnalyticSelectStatementRewriter(VerdictJDBCContext vc, String queryString) {
        super(queryString);
        this.vc = vc;
        this.e = null;
        aggColumnIndicator = new ArrayList<Boolean>();
        colName2Aliases = new ArrayList<Pair<String, Alias>>();
        meanColIndex2ErrColIndex = new HashMap<Integer, Integer>();
        derivedTableColName2Aliases = new HashMap<String, Map<String, Alias>>();
    }

    public VerdictQuerySyntaxException getException() {
        return e;
    }

    public List<Boolean> getAggregateColumnIndicator() {
        return aggColumnIndicator;
    }

    public List<Pair<String, Alias>> getColName2Aliases() {
        return colName2Aliases;
    }

    public Map<TableUniqueName, TableUniqueName> getCumulativeSampleTables() {
        return cumulativeReplacedTableSources;
    }

    /**
     * Returns if columnIndex-th select list element includes an aggregate function.
     * 
     * @param columnIndex
     * @return
     */
    public boolean isAggregateColumn(int columnIndex) {
        if (aggColumnIndicator.size() >= columnIndex && aggColumnIndicator.get(columnIndex - 1)) {
            return aggColumnIndicator.get(columnIndex - 1);
        } else {
            return false;
        }
    }

    public Map<Integer, Integer> getMean2ErrorColumnMap() {
        return meanColIndex2ErrColIndex;
    }

    protected double sampleSizeToOriginalTableSizeRatio = -1;

    protected double getSampleSizeToOriginalTableSizeRatio() {
        if (sampleSizeToOriginalTableSizeRatio == -1) {
            double sampleToOriginalRatio = 1.0; // originalTableSize / sampleSize
            for (Map.Entry<TableUniqueName, Pair<SampleParam, TableUniqueName>> e : replacedTableSources.entrySet()) {
                SampleParam p = e.getValue().getLeft();
                sampleToOriginalRatio = 1 / p.getSamplingRatio();
                TableUniqueName sampleTable = e.getValue().getRight();
                SampleSizeInfo sizeInfo = vc.getMeta().getSampleSizeOf(sampleTable);
                VerdictLogger.debug(this, String.format("%s size ratio. sample size: %d, original size: %d",
                        sampleTable, sizeInfo.sampleSize, sizeInfo.originalTableSize));
            }
            sampleSizeToOriginalTableSizeRatio = sampleToOriginalRatio;
        }
        return sampleSizeToOriginalTableSizeRatio;
    }

    @Override
    public String visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx) {
        String tableNameItem = visit(ctx.table_name_with_hint());
        Alias alias = null;
        if (ctx.as_table_alias() == null) {
            alias = Alias.genAlias(depth, tableNameItem);
        } else {
            alias = new Alias(tableNameItem, ctx.as_table_alias().getText());
        }
        tableAliases.put(TableUniqueName.uname(vc, tableNameItem), alias);
        return tableNameItem + " " + alias;
    }

    @Override
    protected String tableSourceReplacer(String originalTableName) {
        TableUniqueName originalTable = TableUniqueName.uname(vc, originalTableName);
        if (replacedTableSources.containsKey(originalTable)) {
            Pair<SampleParam, TableUniqueName> chosen = replacedTableSources.get(originalTable);
            cumulativeReplacedTableSources.put(originalTable, chosen.getRight());
            return chosen.getRight().toString();
        } else {
            return originalTable.toString();
        }
    }

    @Override
    protected String tableNameReplacer(String originalTableName) {
        if (tableAliases.values().contains(originalTableName)) {
            // we don't have to replace the table name if aliases were used.
            return originalTableName;
        } else {
            // find the name of the effective table (whether an original table or a sample
            // table), then find the
            // proper alias we used for the table source.
            TableUniqueName originalTable = TableUniqueName.uname(vc, originalTableName);
            if (replacedTableSources.containsKey(originalTable)) {
                Pair<SampleParam, TableUniqueName> sampleParamAndTable = replacedTableSources.get(originalTable);
                return tableAliases.get(sampleParamAndTable.getRight()).toString();
            } else {
                return tableAliases.get(originalTable).toString();
            }
        }
    }

    @Override
    public String visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
        StringBuilder query = new StringBuilder(1000);
        query.append(visit(ctx.query_expression()));

        if (ctx.order_by_clause() != null) {
            query.append(String.format("\n%s", indentString + visit(ctx.order_by_clause())));
        }

        if (ctx.limit_clause() != null) {
            query.append(String.format("\n%s", indentString + visit(ctx.limit_clause())));
        }
        return query.toString();
    }

    protected int select_list_elem_num = 0; // 1 for the first column, 2 for the second column, and so on.

    protected String quoteString() {
        return vc.getDbms().getQuoteString();
    }

    @Override
    public String visitFull_column_name(VerdictSQLParser.Full_column_nameContext ctx) {
        String tabName = null;
        if (ctx.table_name() != null) {
            tabName = visit(ctx.table_name());
        }
        TableUniqueName tabUniqueName = StringManipulations.tabUniqueNameOfColName(vc, ctx.getText());
        String colName = ctx.column_name().getText();

        // if a table name was specified, we change it to its alias name.
        if (tableAliases.containsKey(tabUniqueName)) {
            tabName = tableAliases.get(tabUniqueName).toString();
        }

        // if there was derived table(s), we may need to substitute aliased name for the
        // colName.
        for (Map.Entry<String, Map<String, Alias>> e : derivedTableColName2Aliases.entrySet()) {
            String derivedTabName = e.getKey();

            if (tabName != null && !tabName.equals(derivedTabName)) {
                // this is the case where there are more than one derived tables, and a user
                // specifically referencing
                // a column in one of those derived tables.
                continue;
            }
            if (e.getValue().containsKey(colName)) {
                Alias alias = e.getValue().get(colName);
                if (alias.autoGenerated()) {
                    colName = alias.toString();
                }
            }
        }

        if (tabName != null && tabName.length() > 0) {
            return String.format("%s.%s", tabName, colName);
        } else {
            return colName;
        }
    }

    @Override
    public String visitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
        select_list_elem_num++;
        String newSelectListElem = null;
        Pair<String, Alias> colName2Alias = null;

        if (ctx.getText().equals("*")) {
            // TODO: replace * with all columns in the (joined) source table.
            newSelectListElem = "*";
        } else {
            StringBuilder elem = new StringBuilder();
            String colName = visit(ctx.expression());
            elem.append(colName);

            // We use a baseRewriter to prevent that "COUNT(*)" is rewritten to "COUNT(*) *
            // (1/sample_ratio)"
            // This baseRewriter is only used to remember the original column name
            // expression.
            SelectStatementBaseRewriter baseRewriter = new SelectStatementBaseRewriter(queryString);
            String originalColName = baseRewriter.visit(ctx.expression());

            if (ctx.column_alias() != null) {
                Alias alias = new Alias(originalColName, ctx.column_alias().getText());
                elem.append(String.format(" AS %s", alias));
                colName2Alias = Pair.of(colName, alias);
            } else {
                // We add a pseudo column alias
                Alias alias = Alias.genAlias(depth, originalColName);
                elem.append(String.format(" AS %s", alias));

                colName2Alias = Pair.of(colName, alias);
            }

            newSelectListElem = elem.toString();
        }

        colName2Aliases.add(Pair.of(colName2Alias.getKey(), colName2Alias.getValue()));
        return newSelectListElem;
    }

    protected boolean withinQuerySpecification = false;

    @Override
    public String visitAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
        if (withinQuerySpecification)
            return visitAggregate_function_within_query_specification(ctx);
        else
            // used for orderby clause, groupby clause, etc.
            return visitAggregate_function_outside_query_specification(ctx);
    }

    protected String visitAggregate_function_within_query_specification(
            VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
        while (aggColumnIndicator.size() < select_list_elem_num - 1) {
            aggColumnIndicator.add(false); // pad zero
        }
        aggColumnIndicator.add(true);

        if (ctx.AVG() != null) {
            return String.format("AVG(%s)", visit(ctx.all_distinct_expression()));
        } else if (ctx.SUM() != null) {
            return String.format("(SUM(%s) * %f)", visit(ctx.all_distinct_expression()),
                    getSampleSizeToOriginalTableSizeRatio());
        } else if (ctx.COUNT() != null) {
            if (ctx.all_distinct_expression() != null && ctx.all_distinct_expression().DISTINCT() != null) {
                String colName = ctx.all_distinct_expression().expression().getText();
                return String.format("ROUND((COUNT(DISTINCT %s) * %f))", colName,
                        getSampleSizeToOriginalTableSizeRatio());
            } else {
                return String.format("ROUND((COUNT(*) * %f))", getSampleSizeToOriginalTableSizeRatio());
            }
        }
        VerdictLogger.error(this, String.format("Unexpected aggregate function expression: %s", ctx.getText()));
        return null; // we don't handle other aggregate functions for now.
    }

    protected String visitAggregate_function_outside_query_specification(
            VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
        return super.visitAggregate_windowed_function(ctx);
    }

    protected String extraIndentBeforeTableSourceName(int sourceIndex) {
        return (sourceIndex == 1) ? "" : " ";
    }

    @Override
    public String visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {
        ProperSampleAnalyzer sampleAnalyzer = new ProperSampleAnalyzer(vc, queryString);
        sampleAnalyzer.visit(ctx);
        replacedTableSources = sampleAnalyzer.getOriginalToSampleTable();

        withinQuerySpecification = true;
        // We process the FROM clause first to get the ratio between the samples (if we
        // use them) and the
        // original tables.
        // As we process the FROM clause, we need to get alias information as well.
        String fromClause = rewrittenFromClause(ctx);

        String selectList = rewrittenSelectList(ctx);

        String whereClause = rewrittenWhereClause(ctx);

        String groupbyClause = rewrittenGroupbyClause(ctx);

        StringBuilder query = new StringBuilder(1000);
        query.append(indentString);
        query.append(selectList.toString());
        query.append("\n" + indentString);
        query.append(fromClause.toString());
        if (whereClause.length() > 0) {
            query.append("\n");
            query.append(indentString);
            query.append(whereClause.toString());
        }
        if (groupbyClause.length() > 0) {
            query.append("\n");
            query.append(indentString);
            query.append(groupbyClause.toString());
        }

        withinQuerySpecification = false;
        return query.toString();
    }

    protected String rewrittenFromClause(VerdictSQLParser.Query_specificationContext ctx) {
        StringBuilder fromClause = new StringBuilder(200);
        fromClause.append("FROM ");
        int sourceIndex = 1;
        for (VerdictSQLParser.Table_sourceContext tctx : ctx.table_source()) {
            if (sourceIndex == 1) {
                fromClause.append(extraIndentBeforeTableSourceName(sourceIndex) + visit(tctx));
            } else {
                fromClause.append(String.format(",%s%s", extraIndentBeforeTableSourceName(sourceIndex), visit(tctx)));
            }
            sourceIndex++;
        }
        return fromClause.toString();
    }

    protected String rewrittenSelectList(VerdictSQLParser.Query_specificationContext ctx) {
        StringBuilder selectList = new StringBuilder(200);
        selectList.append("SELECT ");
        selectList.append(visit(ctx.select_list()));
        return selectList.toString();
    }

    protected String rewrittenWhereClause(VerdictSQLParser.Query_specificationContext ctx) {
        StringBuilder whereClause = new StringBuilder(200);
        if (ctx.where != null) {
            whereClause.append("WHERE ");
            whereClause.append(visit(ctx.where));
        }
        return whereClause.toString();
    }

    protected String rewrittenGroupbyClause(VerdictSQLParser.Query_specificationContext ctx) {
        StringBuilder otherClause = new StringBuilder(200);
        if (ctx.group_by_item() != null && ctx.group_by_item().size() > 0) {
            otherClause.append("GROUP BY ");
            for (VerdictSQLParser.Group_by_itemContext gctx : ctx.group_by_item()) {
                otherClause.append(visit(gctx));
            }
        }
        return otherClause.toString();
    }

    /**
     * We have a separate visitor for derived table (rather than automatically
     * handling using {@link AnalyticSelectStatementRewriter#visitSubquery
     * visitSubquery}) to access the derived table's colName2Alias information.
     */
    @Override
    public String visitDerived_table_source_item(VerdictSQLParser.Derived_table_source_itemContext ctx) {
        // visit subquery
        AnalyticSelectStatementRewriter subqueryVisitor = new AnalyticSelectStatementRewriter(vc, queryString);
        subqueryVisitor.setIndentLevel(defaultIndent + 4);
        subqueryVisitor.setDepth(depth + 1);
        String derivedTable = subqueryVisitor
                .visit(ctx.derived_table().subquery().select_statement().query_expression());
        cumulativeReplacedTableSources.putAll(subqueryVisitor.getCumulativeSampleTables());

        // set alias
        Alias alias = new Alias("subquery", ctx.as_table_alias().getText());
        derivedTableColName2Aliases.put(alias.toString(), TypeCasting.listToMap(subqueryVisitor.getColName2Aliases()));

        return String.format("(\n%s) %s ", derivedTable, alias);
    }

    @Override
    public String visitSubquery(VerdictSQLParser.SubqueryContext ctx) {
        AnalyticSelectStatementRewriter subqueryVisitor = new AnalyticSelectStatementRewriter(vc, queryString);
        subqueryVisitor.setIndentLevel(defaultIndent + 4);
        subqueryVisitor.setDepth(depth + 1);
        String ret = subqueryVisitor.visit(ctx.select_statement().query_expression());
        cumulativeReplacedTableSources.putAll(subqueryVisitor.getCumulativeSampleTables());
        return ret;
    }

    @Override
    public String visitGroup_by_item(VerdictSQLParser.Group_by_itemContext ctx) {
        String groupName = ctx.getText();
        Alias alias = new Alias(groupName, groupName);

        for (Pair<String, Alias> e : colName2Aliases) {
            if (StringManipulations.colNameOfColName(e.getKey()).equals(groupName)) {
                alias = e.getValue();
                break;
            }
        }

        if (isFirstGroup) {
            isFirstGroup = false;
            return alias.toString();
        } else {
            return ", " + alias;
        }
    }

    @Override
    public String visitOrder_by_clause(VerdictSQLParser.Order_by_clauseContext ctx) {
        StringBuilder orderby = new StringBuilder();
        orderby.append("ORDER BY");
        boolean isFirst = true;
        for (VerdictSQLParser.Order_by_expressionContext octx : ctx.order_by_expression()) {
            Alias alias = findAliasedExpression(visit(octx));

            if (isFirst)
                orderby.append(" ");
            else
                orderby.append(", ");

            orderby.append(alias);
            if (octx.DESC() != null) {
                orderby.append(" DESC");
            }
            if (octx.ASC() != null) {
                orderby.append(" ASC");
            }
            isFirst = false;
        }
        return orderby.toString();
    }

    /**
     * @param colExpr
     *            The original text
     * @return An alias we declared
     */
    private Alias findAliasedExpression(String colExpr) {
        for (Pair<String, Alias> e : colName2Aliases) {
            if (colExpr.equalsIgnoreCase(e.getLeft())) {
                return e.getRight();
            }
        }
        return new Alias(colExpr, colExpr);
    }

}

/**
 * Obtain pairs of original table names and the names of their proper samples.
 * Here, a proper sample means is a sample table that can be used in place of an
 * original table without much affecting
 * 
 * @author Yongjoo Park
 *
 */
class ProperSampleAnalyzer extends SelectStatementBaseRewriter {

    private boolean withinQuerySpecification = false;

    private List<ColumnName> distinctColumns = new ArrayList<ColumnName>();

    private VerdictJDBCContext vc;

    private List<Pair<TableUniqueName, String>> tableSourcesWithAlias = new ArrayList<Pair<TableUniqueName, String>>();

    private VerdictException e;

    /**
     * Map from the original table name to its proper sample name.
     */
    private Map<TableUniqueName, Pair<SampleParam, TableUniqueName>> originalToSampleTable = new HashMap<TableUniqueName, Pair<SampleParam, TableUniqueName>>();

    public Map<TableUniqueName, Pair<SampleParam, TableUniqueName>> getOriginalToSampleTable() {
        return originalToSampleTable;
    }

    public ProperSampleAnalyzer(VerdictJDBCContext vc, String queryString) {
        super(queryString);
        this.vc = vc;
    }

    @Override
    public String visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {
        withinQuerySpecification = true;
        for (VerdictSQLParser.Table_sourceContext tctx : ctx.table_source()) {
            visit(tctx);
        }
        visit(ctx.select_list());
        withinQuerySpecification = false;
        analyzePossibleSamples();
        return null;
    }

    private ColumnName distinctColumnInTable(TableUniqueName tableName) {
        for (ColumnName c : distinctColumns) {
            if (c.getTableSource().equals(tableName)) {
                return c;
            }
        }
        return null;
    }

    private void analyzePossibleSamples() {
        for (Pair<TableUniqueName, String> e : tableSourcesWithAlias) {
            TableUniqueName originalTable = e.getLeft();
            List<Pair<SampleParam, TableUniqueName>> samples = vc.getMeta().getSampleInfoFor(originalTable);
            Pair<SampleParam, TableUniqueName> bestCandidateForSample = null;
            ColumnName dc = distinctColumnInTable(originalTable); // columns for which distinct-count is specified

            for (Pair<SampleParam, TableUniqueName> e2 : samples) {
                SampleParam param = e2.getLeft();

                if (bestCandidateForSample == null) {
                    bestCandidateForSample = e2;
                } else if (param.getSampleType().equals("universal") && dc != null
                        && param.getColumnNames().contains(dc.localColumnName())) {
                    bestCandidateForSample = e2;
                }
            }

            if (bestCandidateForSample != null) {
                originalToSampleTable.put(originalTable, bestCandidateForSample);
            }
        }
    }

    @Override
    public String visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx) {
        String tableNameItem = visit(ctx.table_name_with_hint());
        if (ctx.as_table_alias() == null) {
            if (tableNameItem != null) {
                TableUniqueName originalTable = TableUniqueName.uname(vc, tableNameItem);
                tableSourcesWithAlias.add(Pair.of(originalTable, (String) null));
            }
        } else {
            TableUniqueName originalTable = TableUniqueName.uname(vc, tableNameItem);
            String alias = ctx.as_table_alias().getText();
            tableSourcesWithAlias.add(Pair.of(originalTable, alias));
        }
        return null;
    }

    @Override
    public String visitAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
        if (!withinQuerySpecification)
            return null;

        if (ctx.AVG() != null) {
        } else if (ctx.SUM() != null) {
        } else if (ctx.COUNT() != null) {
            if (ctx.all_distinct_expression() != null && ctx.all_distinct_expression().DISTINCT() != null) {
                String colName = ctx.all_distinct_expression().expression().getText();
                try {
                    distinctColumns.add(ColumnName.uname(vc, tableSourcesWithAlias, colName));
                } catch (VerdictException e) {
                    this.e = e;
                }
            }
        }
        return null; // we don't handle other aggregate functions for now.
    }

    @Override
    public String visitSubquery(VerdictSQLParser.SubqueryContext ctx) {
        return null;
    }
}

// Generated from edu\u005Cumich\verdict\parser\VerdictSQL.g4 by ANTLR 4.5.3
package edu.umich.verdict.parser;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link VerdictSQLParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface VerdictSQLVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#verdict_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVerdict_statement(VerdictSQLParser.Verdict_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#create_sample_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_sample_statement(VerdictSQLParser.Create_sample_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#sample_type}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSample_type(VerdictSQLParser.Sample_typeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#on_columns}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOn_columns(VerdictSQLParser.On_columnsContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#delete_sample_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDelete_sample_statement(VerdictSQLParser.Delete_sample_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#show_samples_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShow_samples_statement(VerdictSQLParser.Show_samples_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#config_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConfig_statement(VerdictSQLParser.Config_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#other_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOther_statement(VerdictSQLParser.Other_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#create_database}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_database(VerdictSQLParser.Create_databaseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#drop_database}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDrop_database(VerdictSQLParser.Drop_databaseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#config_set_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConfig_set_statement(VerdictSQLParser.Config_set_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#config_get_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConfig_get_statement(VerdictSQLParser.Config_get_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#config_key}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConfig_key(VerdictSQLParser.Config_keyContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#config_value}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConfig_value(VerdictSQLParser.Config_valueContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#confidence_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConfidence_clause(VerdictSQLParser.Confidence_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#trials_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTrials_clause(VerdictSQLParser.Trials_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#table_name_with_sample}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_name_with_sample(VerdictSQLParser.Table_name_with_sampleContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#tsql_file}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTsql_file(VerdictSQLParser.Tsql_fileContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#sql_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSql_clause(VerdictSQLParser.Sql_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#ddl_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDdl_clause(VerdictSQLParser.Ddl_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#select_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect_statement(VerdictSQLParser.Select_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#output_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOutput_clause(VerdictSQLParser.Output_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#output_dml_list_elem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOutput_dml_list_elem(VerdictSQLParser.Output_dml_list_elemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#output_column_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOutput_column_name(VerdictSQLParser.Output_column_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#create_table}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_table(VerdictSQLParser.Create_tableContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#create_table_as_select}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_table_as_select(VerdictSQLParser.Create_table_as_selectContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#create_view}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_view(VerdictSQLParser.Create_viewContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#alter_table}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlter_table(VerdictSQLParser.Alter_tableContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#alter_database}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlter_database(VerdictSQLParser.Alter_databaseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#drop_table}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDrop_table(VerdictSQLParser.Drop_tableContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#drop_view}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDrop_view(VerdictSQLParser.Drop_viewContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#set_statment}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSet_statment(VerdictSQLParser.Set_statmentContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#use_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUse_statement(VerdictSQLParser.Use_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#show_tables_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShow_tables_statement(VerdictSQLParser.Show_tables_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#show_databases_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShow_databases_statement(VerdictSQLParser.Show_databases_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#describe_table_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribe_table_statement(VerdictSQLParser.Describe_table_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#refresh_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRefresh_statement(VerdictSQLParser.Refresh_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#show_config_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShow_config_statement(VerdictSQLParser.Show_config_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#table_type_definition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_type_definition(VerdictSQLParser.Table_type_definitionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#column_def_table_constraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_def_table_constraint(VerdictSQLParser.Column_def_table_constraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#column_definition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_definition(VerdictSQLParser.Column_definitionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#column_constraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_constraint(VerdictSQLParser.Column_constraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#table_constraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_constraint(VerdictSQLParser.Table_constraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#set_special}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSet_special(VerdictSQLParser.Set_specialContext ctx);
	/**
	 * Visit a parse tree produced by the {@code binary_operator_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code primitive_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimitive_expression(VerdictSQLParser.Primitive_expressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bracket_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBracket_expression(VerdictSQLParser.Bracket_expressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unary_operator_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnary_operator_expression(VerdictSQLParser.Unary_operator_expressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code interval_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInterval_expression(VerdictSQLParser.Interval_expressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code function_call_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunction_call_expression(VerdictSQLParser.Function_call_expressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code case_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCase_expression(VerdictSQLParser.Case_expressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code column_ref_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subquery_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubquery_expression(VerdictSQLParser.Subquery_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#interval}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInterval(VerdictSQLParser.IntervalContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#constant_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstant_expression(VerdictSQLParser.Constant_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#subquery}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubquery(VerdictSQLParser.SubqueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#dml_table_source}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDml_table_source(VerdictSQLParser.Dml_table_sourceContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#with_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWith_expression(VerdictSQLParser.With_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#common_table_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCommon_table_expression(VerdictSQLParser.Common_table_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#update_elem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUpdate_elem(VerdictSQLParser.Update_elemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#search_condition_list}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearch_condition_list(VerdictSQLParser.Search_condition_listContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#search_condition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearch_condition(VerdictSQLParser.Search_conditionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#search_condition_or}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearch_condition_or(VerdictSQLParser.Search_condition_orContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#search_condition_not}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearch_condition_not(VerdictSQLParser.Search_condition_notContext ctx);
	/**
	 * Visit a parse tree produced by the {@code exists_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExists_predicate(VerdictSQLParser.Exists_predicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code comp_expr_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComp_expr_predicate(VerdictSQLParser.Comp_expr_predicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setcomp_expr_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetcomp_expr_predicate(VerdictSQLParser.Setcomp_expr_predicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code comp_between_expr}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComp_between_expr(VerdictSQLParser.Comp_between_exprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code in_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIn_predicate(VerdictSQLParser.In_predicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code like_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLike_predicate(VerdictSQLParser.Like_predicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code is_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIs_predicate(VerdictSQLParser.Is_predicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bracket_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBracket_predicate(VerdictSQLParser.Bracket_predicateContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#query_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery_expression(VerdictSQLParser.Query_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#union}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnion(VerdictSQLParser.UnionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#query_specification}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#limit_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLimit_clause(VerdictSQLParser.Limit_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#order_by_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrder_by_clause(VerdictSQLParser.Order_by_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#for_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFor_clause(VerdictSQLParser.For_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#xml_common_directives}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitXml_common_directives(VerdictSQLParser.Xml_common_directivesContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#order_by_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrder_by_expression(VerdictSQLParser.Order_by_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#group_by_item}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroup_by_item(VerdictSQLParser.Group_by_itemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#option_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOption_clause(VerdictSQLParser.Option_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#option}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOption(VerdictSQLParser.OptionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#optimize_for_arg}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOptimize_for_arg(VerdictSQLParser.Optimize_for_argContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#select_list}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect_list(VerdictSQLParser.Select_listContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#select_list_elem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#partition_by_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPartition_by_clause(VerdictSQLParser.Partition_by_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#table_source}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_source(VerdictSQLParser.Table_sourceContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#table_source_item_joined}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_source_item_joined(VerdictSQLParser.Table_source_item_joinedContext ctx);
	/**
	 * Visit a parse tree produced by the {@code sample_table_name_item}
	 * labeled alternative in {@link VerdictSQLParser#table_source_item}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSample_table_name_item(VerdictSQLParser.Sample_table_name_itemContext ctx);
	/**
	 * Visit a parse tree produced by the {@code hinted_table_name_item}
	 * labeled alternative in {@link VerdictSQLParser#table_source_item}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx);
	/**
	 * Visit a parse tree produced by the {@code derived_table_source_item}
	 * labeled alternative in {@link VerdictSQLParser#table_source_item}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDerived_table_source_item(VerdictSQLParser.Derived_table_source_itemContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#change_table}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitChange_table(VerdictSQLParser.Change_tableContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#join_part}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoin_part(VerdictSQLParser.Join_partContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#table_name_with_hint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_name_with_hint(VerdictSQLParser.Table_name_with_hintContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#rowset_function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRowset_function(VerdictSQLParser.Rowset_functionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#bulk_option}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBulk_option(VerdictSQLParser.Bulk_optionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#derived_table}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDerived_table(VerdictSQLParser.Derived_tableContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#function_call}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunction_call(VerdictSQLParser.Function_callContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#datepart}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDatepart(VerdictSQLParser.DatepartContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#as_table_alias}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAs_table_alias(VerdictSQLParser.As_table_aliasContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#table_alias}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_alias(VerdictSQLParser.Table_aliasContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#with_table_hints}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWith_table_hints(VerdictSQLParser.With_table_hintsContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#table_hint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_hint(VerdictSQLParser.Table_hintContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#index_column_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIndex_column_name(VerdictSQLParser.Index_column_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#index_value}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIndex_value(VerdictSQLParser.Index_valueContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#column_alias_list}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_alias_list(VerdictSQLParser.Column_alias_listContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#column_alias}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_alias(VerdictSQLParser.Column_aliasContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#table_value_constructor}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_value_constructor(VerdictSQLParser.Table_value_constructorContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#expression_list}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression_list(VerdictSQLParser.Expression_listContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#case_expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCase_expr(VerdictSQLParser.Case_exprContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#ranking_windowed_function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRanking_windowed_function(VerdictSQLParser.Ranking_windowed_functionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#value_manipulation_function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValue_manipulation_function(VerdictSQLParser.Value_manipulation_functionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#nary_manipulation_function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNary_manipulation_function(VerdictSQLParser.Nary_manipulation_functionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#ternary_manipulation_function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTernary_manipulation_function(VerdictSQLParser.Ternary_manipulation_functionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#binary_manipulation_function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBinary_manipulation_function(VerdictSQLParser.Binary_manipulation_functionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#extract_time_function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExtract_time_function(VerdictSQLParser.Extract_time_functionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#extract_unit}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExtract_unit(VerdictSQLParser.Extract_unitContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#unary_manipulation_function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnary_manipulation_function(VerdictSQLParser.Unary_manipulation_functionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#noparam_manipulation_function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNoparam_manipulation_function(VerdictSQLParser.Noparam_manipulation_functionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#lateral_view_function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLateral_view_function(VerdictSQLParser.Lateral_view_functionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#aggregate_windowed_function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#all_distinct_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAll_distinct_expression(VerdictSQLParser.All_distinct_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#cast_as_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCast_as_expression(VerdictSQLParser.Cast_as_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#over_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOver_clause(VerdictSQLParser.Over_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#row_or_range_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRow_or_range_clause(VerdictSQLParser.Row_or_range_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#window_frame_extent}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindow_frame_extent(VerdictSQLParser.Window_frame_extentContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#window_frame_bound}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindow_frame_bound(VerdictSQLParser.Window_frame_boundContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#window_frame_preceding}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindow_frame_preceding(VerdictSQLParser.Window_frame_precedingContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#window_frame_following}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindow_frame_following(VerdictSQLParser.Window_frame_followingContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#full_table_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFull_table_name(VerdictSQLParser.Full_table_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#table_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_name(VerdictSQLParser.Table_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#view_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitView_name(VerdictSQLParser.View_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#func_proc_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunc_proc_name(VerdictSQLParser.Func_proc_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#ddl_object}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDdl_object(VerdictSQLParser.Ddl_objectContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#full_column_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFull_column_name(VerdictSQLParser.Full_column_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#column_name_list}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_name_list(VerdictSQLParser.Column_name_listContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#column_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_name(VerdictSQLParser.Column_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#cursor_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCursor_name(VerdictSQLParser.Cursor_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#on_off}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOn_off(VerdictSQLParser.On_offContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#clustered}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitClustered(VerdictSQLParser.ClusteredContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#null_notnull}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNull_notnull(VerdictSQLParser.Null_notnullContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#true_orfalse}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTrue_orfalse(VerdictSQLParser.True_orfalseContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#scalar_function_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitScalar_function_name(VerdictSQLParser.Scalar_function_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#data_type}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitData_type(VerdictSQLParser.Data_typeContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#default_value}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDefault_value(VerdictSQLParser.Default_valueContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstant(VerdictSQLParser.ConstantContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumber(VerdictSQLParser.NumberContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#sign}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSign(VerdictSQLParser.SignContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#id}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitId(VerdictSQLParser.IdContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#simple_id}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimple_id(VerdictSQLParser.Simple_idContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#comparison_operator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparison_operator(VerdictSQLParser.Comparison_operatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link VerdictSQLParser#assignment_operator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAssignment_operator(VerdictSQLParser.Assignment_operatorContext ctx);
}
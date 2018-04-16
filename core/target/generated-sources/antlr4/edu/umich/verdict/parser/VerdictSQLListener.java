// Generated from edu\u005Cumich\verdict\parser\VerdictSQL.g4 by ANTLR 4.5.3
package edu.umich.verdict.parser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link VerdictSQLParser}.
 */
public interface VerdictSQLListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#verdict_statement}.
	 * @param ctx the parse tree
	 */
	void enterVerdict_statement(VerdictSQLParser.Verdict_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#verdict_statement}.
	 * @param ctx the parse tree
	 */
	void exitVerdict_statement(VerdictSQLParser.Verdict_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#create_sample_statement}.
	 * @param ctx the parse tree
	 */
	void enterCreate_sample_statement(VerdictSQLParser.Create_sample_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#create_sample_statement}.
	 * @param ctx the parse tree
	 */
	void exitCreate_sample_statement(VerdictSQLParser.Create_sample_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#sample_type}.
	 * @param ctx the parse tree
	 */
	void enterSample_type(VerdictSQLParser.Sample_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#sample_type}.
	 * @param ctx the parse tree
	 */
	void exitSample_type(VerdictSQLParser.Sample_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#on_columns}.
	 * @param ctx the parse tree
	 */
	void enterOn_columns(VerdictSQLParser.On_columnsContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#on_columns}.
	 * @param ctx the parse tree
	 */
	void exitOn_columns(VerdictSQLParser.On_columnsContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#delete_sample_statement}.
	 * @param ctx the parse tree
	 */
	void enterDelete_sample_statement(VerdictSQLParser.Delete_sample_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#delete_sample_statement}.
	 * @param ctx the parse tree
	 */
	void exitDelete_sample_statement(VerdictSQLParser.Delete_sample_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#show_samples_statement}.
	 * @param ctx the parse tree
	 */
	void enterShow_samples_statement(VerdictSQLParser.Show_samples_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#show_samples_statement}.
	 * @param ctx the parse tree
	 */
	void exitShow_samples_statement(VerdictSQLParser.Show_samples_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#config_statement}.
	 * @param ctx the parse tree
	 */
	void enterConfig_statement(VerdictSQLParser.Config_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#config_statement}.
	 * @param ctx the parse tree
	 */
	void exitConfig_statement(VerdictSQLParser.Config_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#other_statement}.
	 * @param ctx the parse tree
	 */
	void enterOther_statement(VerdictSQLParser.Other_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#other_statement}.
	 * @param ctx the parse tree
	 */
	void exitOther_statement(VerdictSQLParser.Other_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#create_database}.
	 * @param ctx the parse tree
	 */
	void enterCreate_database(VerdictSQLParser.Create_databaseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#create_database}.
	 * @param ctx the parse tree
	 */
	void exitCreate_database(VerdictSQLParser.Create_databaseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#drop_database}.
	 * @param ctx the parse tree
	 */
	void enterDrop_database(VerdictSQLParser.Drop_databaseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#drop_database}.
	 * @param ctx the parse tree
	 */
	void exitDrop_database(VerdictSQLParser.Drop_databaseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#config_set_statement}.
	 * @param ctx the parse tree
	 */
	void enterConfig_set_statement(VerdictSQLParser.Config_set_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#config_set_statement}.
	 * @param ctx the parse tree
	 */
	void exitConfig_set_statement(VerdictSQLParser.Config_set_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#config_get_statement}.
	 * @param ctx the parse tree
	 */
	void enterConfig_get_statement(VerdictSQLParser.Config_get_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#config_get_statement}.
	 * @param ctx the parse tree
	 */
	void exitConfig_get_statement(VerdictSQLParser.Config_get_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#config_key}.
	 * @param ctx the parse tree
	 */
	void enterConfig_key(VerdictSQLParser.Config_keyContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#config_key}.
	 * @param ctx the parse tree
	 */
	void exitConfig_key(VerdictSQLParser.Config_keyContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#config_value}.
	 * @param ctx the parse tree
	 */
	void enterConfig_value(VerdictSQLParser.Config_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#config_value}.
	 * @param ctx the parse tree
	 */
	void exitConfig_value(VerdictSQLParser.Config_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#confidence_clause}.
	 * @param ctx the parse tree
	 */
	void enterConfidence_clause(VerdictSQLParser.Confidence_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#confidence_clause}.
	 * @param ctx the parse tree
	 */
	void exitConfidence_clause(VerdictSQLParser.Confidence_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#trials_clause}.
	 * @param ctx the parse tree
	 */
	void enterTrials_clause(VerdictSQLParser.Trials_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#trials_clause}.
	 * @param ctx the parse tree
	 */
	void exitTrials_clause(VerdictSQLParser.Trials_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#table_name_with_sample}.
	 * @param ctx the parse tree
	 */
	void enterTable_name_with_sample(VerdictSQLParser.Table_name_with_sampleContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#table_name_with_sample}.
	 * @param ctx the parse tree
	 */
	void exitTable_name_with_sample(VerdictSQLParser.Table_name_with_sampleContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#tsql_file}.
	 * @param ctx the parse tree
	 */
	void enterTsql_file(VerdictSQLParser.Tsql_fileContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#tsql_file}.
	 * @param ctx the parse tree
	 */
	void exitTsql_file(VerdictSQLParser.Tsql_fileContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#sql_clause}.
	 * @param ctx the parse tree
	 */
	void enterSql_clause(VerdictSQLParser.Sql_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#sql_clause}.
	 * @param ctx the parse tree
	 */
	void exitSql_clause(VerdictSQLParser.Sql_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#ddl_clause}.
	 * @param ctx the parse tree
	 */
	void enterDdl_clause(VerdictSQLParser.Ddl_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#ddl_clause}.
	 * @param ctx the parse tree
	 */
	void exitDdl_clause(VerdictSQLParser.Ddl_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#select_statement}.
	 * @param ctx the parse tree
	 */
	void enterSelect_statement(VerdictSQLParser.Select_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#select_statement}.
	 * @param ctx the parse tree
	 */
	void exitSelect_statement(VerdictSQLParser.Select_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#output_clause}.
	 * @param ctx the parse tree
	 */
	void enterOutput_clause(VerdictSQLParser.Output_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#output_clause}.
	 * @param ctx the parse tree
	 */
	void exitOutput_clause(VerdictSQLParser.Output_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#output_dml_list_elem}.
	 * @param ctx the parse tree
	 */
	void enterOutput_dml_list_elem(VerdictSQLParser.Output_dml_list_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#output_dml_list_elem}.
	 * @param ctx the parse tree
	 */
	void exitOutput_dml_list_elem(VerdictSQLParser.Output_dml_list_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#output_column_name}.
	 * @param ctx the parse tree
	 */
	void enterOutput_column_name(VerdictSQLParser.Output_column_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#output_column_name}.
	 * @param ctx the parse tree
	 */
	void exitOutput_column_name(VerdictSQLParser.Output_column_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#create_table}.
	 * @param ctx the parse tree
	 */
	void enterCreate_table(VerdictSQLParser.Create_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#create_table}.
	 * @param ctx the parse tree
	 */
	void exitCreate_table(VerdictSQLParser.Create_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#create_table_as_select}.
	 * @param ctx the parse tree
	 */
	void enterCreate_table_as_select(VerdictSQLParser.Create_table_as_selectContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#create_table_as_select}.
	 * @param ctx the parse tree
	 */
	void exitCreate_table_as_select(VerdictSQLParser.Create_table_as_selectContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#create_view}.
	 * @param ctx the parse tree
	 */
	void enterCreate_view(VerdictSQLParser.Create_viewContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#create_view}.
	 * @param ctx the parse tree
	 */
	void exitCreate_view(VerdictSQLParser.Create_viewContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#alter_table}.
	 * @param ctx the parse tree
	 */
	void enterAlter_table(VerdictSQLParser.Alter_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#alter_table}.
	 * @param ctx the parse tree
	 */
	void exitAlter_table(VerdictSQLParser.Alter_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#alter_database}.
	 * @param ctx the parse tree
	 */
	void enterAlter_database(VerdictSQLParser.Alter_databaseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#alter_database}.
	 * @param ctx the parse tree
	 */
	void exitAlter_database(VerdictSQLParser.Alter_databaseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#drop_table}.
	 * @param ctx the parse tree
	 */
	void enterDrop_table(VerdictSQLParser.Drop_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#drop_table}.
	 * @param ctx the parse tree
	 */
	void exitDrop_table(VerdictSQLParser.Drop_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#drop_view}.
	 * @param ctx the parse tree
	 */
	void enterDrop_view(VerdictSQLParser.Drop_viewContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#drop_view}.
	 * @param ctx the parse tree
	 */
	void exitDrop_view(VerdictSQLParser.Drop_viewContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#set_statment}.
	 * @param ctx the parse tree
	 */
	void enterSet_statment(VerdictSQLParser.Set_statmentContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#set_statment}.
	 * @param ctx the parse tree
	 */
	void exitSet_statment(VerdictSQLParser.Set_statmentContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#use_statement}.
	 * @param ctx the parse tree
	 */
	void enterUse_statement(VerdictSQLParser.Use_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#use_statement}.
	 * @param ctx the parse tree
	 */
	void exitUse_statement(VerdictSQLParser.Use_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#show_tables_statement}.
	 * @param ctx the parse tree
	 */
	void enterShow_tables_statement(VerdictSQLParser.Show_tables_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#show_tables_statement}.
	 * @param ctx the parse tree
	 */
	void exitShow_tables_statement(VerdictSQLParser.Show_tables_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#show_databases_statement}.
	 * @param ctx the parse tree
	 */
	void enterShow_databases_statement(VerdictSQLParser.Show_databases_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#show_databases_statement}.
	 * @param ctx the parse tree
	 */
	void exitShow_databases_statement(VerdictSQLParser.Show_databases_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#describe_table_statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribe_table_statement(VerdictSQLParser.Describe_table_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#describe_table_statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribe_table_statement(VerdictSQLParser.Describe_table_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#refresh_statement}.
	 * @param ctx the parse tree
	 */
	void enterRefresh_statement(VerdictSQLParser.Refresh_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#refresh_statement}.
	 * @param ctx the parse tree
	 */
	void exitRefresh_statement(VerdictSQLParser.Refresh_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#show_config_statement}.
	 * @param ctx the parse tree
	 */
	void enterShow_config_statement(VerdictSQLParser.Show_config_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#show_config_statement}.
	 * @param ctx the parse tree
	 */
	void exitShow_config_statement(VerdictSQLParser.Show_config_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#table_type_definition}.
	 * @param ctx the parse tree
	 */
	void enterTable_type_definition(VerdictSQLParser.Table_type_definitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#table_type_definition}.
	 * @param ctx the parse tree
	 */
	void exitTable_type_definition(VerdictSQLParser.Table_type_definitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#column_def_table_constraint}.
	 * @param ctx the parse tree
	 */
	void enterColumn_def_table_constraint(VerdictSQLParser.Column_def_table_constraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#column_def_table_constraint}.
	 * @param ctx the parse tree
	 */
	void exitColumn_def_table_constraint(VerdictSQLParser.Column_def_table_constraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#column_definition}.
	 * @param ctx the parse tree
	 */
	void enterColumn_definition(VerdictSQLParser.Column_definitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#column_definition}.
	 * @param ctx the parse tree
	 */
	void exitColumn_definition(VerdictSQLParser.Column_definitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#column_constraint}.
	 * @param ctx the parse tree
	 */
	void enterColumn_constraint(VerdictSQLParser.Column_constraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#column_constraint}.
	 * @param ctx the parse tree
	 */
	void exitColumn_constraint(VerdictSQLParser.Column_constraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#table_constraint}.
	 * @param ctx the parse tree
	 */
	void enterTable_constraint(VerdictSQLParser.Table_constraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#table_constraint}.
	 * @param ctx the parse tree
	 */
	void exitTable_constraint(VerdictSQLParser.Table_constraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#set_special}.
	 * @param ctx the parse tree
	 */
	void enterSet_special(VerdictSQLParser.Set_specialContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#set_special}.
	 * @param ctx the parse tree
	 */
	void exitSet_special(VerdictSQLParser.Set_specialContext ctx);
	/**
	 * Enter a parse tree produced by the {@code binary_operator_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code binary_operator_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code primitive_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterPrimitive_expression(VerdictSQLParser.Primitive_expressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code primitive_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitPrimitive_expression(VerdictSQLParser.Primitive_expressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bracket_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterBracket_expression(VerdictSQLParser.Bracket_expressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bracket_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitBracket_expression(VerdictSQLParser.Bracket_expressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unary_operator_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterUnary_operator_expression(VerdictSQLParser.Unary_operator_expressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unary_operator_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitUnary_operator_expression(VerdictSQLParser.Unary_operator_expressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code interval_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterInterval_expression(VerdictSQLParser.Interval_expressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code interval_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitInterval_expression(VerdictSQLParser.Interval_expressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code function_call_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterFunction_call_expression(VerdictSQLParser.Function_call_expressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code function_call_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitFunction_call_expression(VerdictSQLParser.Function_call_expressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code case_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterCase_expression(VerdictSQLParser.Case_expressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code case_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitCase_expression(VerdictSQLParser.Case_expressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code column_ref_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code column_ref_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subquery_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterSubquery_expression(VerdictSQLParser.Subquery_expressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subquery_expression}
	 * labeled alternative in {@link VerdictSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitSubquery_expression(VerdictSQLParser.Subquery_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#interval}.
	 * @param ctx the parse tree
	 */
	void enterInterval(VerdictSQLParser.IntervalContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#interval}.
	 * @param ctx the parse tree
	 */
	void exitInterval(VerdictSQLParser.IntervalContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#constant_expression}.
	 * @param ctx the parse tree
	 */
	void enterConstant_expression(VerdictSQLParser.Constant_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#constant_expression}.
	 * @param ctx the parse tree
	 */
	void exitConstant_expression(VerdictSQLParser.Constant_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#subquery}.
	 * @param ctx the parse tree
	 */
	void enterSubquery(VerdictSQLParser.SubqueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#subquery}.
	 * @param ctx the parse tree
	 */
	void exitSubquery(VerdictSQLParser.SubqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#dml_table_source}.
	 * @param ctx the parse tree
	 */
	void enterDml_table_source(VerdictSQLParser.Dml_table_sourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#dml_table_source}.
	 * @param ctx the parse tree
	 */
	void exitDml_table_source(VerdictSQLParser.Dml_table_sourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#with_expression}.
	 * @param ctx the parse tree
	 */
	void enterWith_expression(VerdictSQLParser.With_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#with_expression}.
	 * @param ctx the parse tree
	 */
	void exitWith_expression(VerdictSQLParser.With_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#common_table_expression}.
	 * @param ctx the parse tree
	 */
	void enterCommon_table_expression(VerdictSQLParser.Common_table_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#common_table_expression}.
	 * @param ctx the parse tree
	 */
	void exitCommon_table_expression(VerdictSQLParser.Common_table_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#update_elem}.
	 * @param ctx the parse tree
	 */
	void enterUpdate_elem(VerdictSQLParser.Update_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#update_elem}.
	 * @param ctx the parse tree
	 */
	void exitUpdate_elem(VerdictSQLParser.Update_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#search_condition_list}.
	 * @param ctx the parse tree
	 */
	void enterSearch_condition_list(VerdictSQLParser.Search_condition_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#search_condition_list}.
	 * @param ctx the parse tree
	 */
	void exitSearch_condition_list(VerdictSQLParser.Search_condition_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#search_condition}.
	 * @param ctx the parse tree
	 */
	void enterSearch_condition(VerdictSQLParser.Search_conditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#search_condition}.
	 * @param ctx the parse tree
	 */
	void exitSearch_condition(VerdictSQLParser.Search_conditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#search_condition_or}.
	 * @param ctx the parse tree
	 */
	void enterSearch_condition_or(VerdictSQLParser.Search_condition_orContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#search_condition_or}.
	 * @param ctx the parse tree
	 */
	void exitSearch_condition_or(VerdictSQLParser.Search_condition_orContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#search_condition_not}.
	 * @param ctx the parse tree
	 */
	void enterSearch_condition_not(VerdictSQLParser.Search_condition_notContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#search_condition_not}.
	 * @param ctx the parse tree
	 */
	void exitSearch_condition_not(VerdictSQLParser.Search_condition_notContext ctx);
	/**
	 * Enter a parse tree produced by the {@code exists_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterExists_predicate(VerdictSQLParser.Exists_predicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code exists_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitExists_predicate(VerdictSQLParser.Exists_predicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code comp_expr_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterComp_expr_predicate(VerdictSQLParser.Comp_expr_predicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code comp_expr_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitComp_expr_predicate(VerdictSQLParser.Comp_expr_predicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setcomp_expr_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterSetcomp_expr_predicate(VerdictSQLParser.Setcomp_expr_predicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setcomp_expr_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitSetcomp_expr_predicate(VerdictSQLParser.Setcomp_expr_predicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code comp_between_expr}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterComp_between_expr(VerdictSQLParser.Comp_between_exprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code comp_between_expr}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitComp_between_expr(VerdictSQLParser.Comp_between_exprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code in_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterIn_predicate(VerdictSQLParser.In_predicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code in_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitIn_predicate(VerdictSQLParser.In_predicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code like_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterLike_predicate(VerdictSQLParser.Like_predicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code like_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitLike_predicate(VerdictSQLParser.Like_predicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code is_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterIs_predicate(VerdictSQLParser.Is_predicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code is_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitIs_predicate(VerdictSQLParser.Is_predicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bracket_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterBracket_predicate(VerdictSQLParser.Bracket_predicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bracket_predicate}
	 * labeled alternative in {@link VerdictSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitBracket_predicate(VerdictSQLParser.Bracket_predicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#query_expression}.
	 * @param ctx the parse tree
	 */
	void enterQuery_expression(VerdictSQLParser.Query_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#query_expression}.
	 * @param ctx the parse tree
	 */
	void exitQuery_expression(VerdictSQLParser.Query_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#union}.
	 * @param ctx the parse tree
	 */
	void enterUnion(VerdictSQLParser.UnionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#union}.
	 * @param ctx the parse tree
	 */
	void exitUnion(VerdictSQLParser.UnionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#query_specification}.
	 * @param ctx the parse tree
	 */
	void enterQuery_specification(VerdictSQLParser.Query_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#query_specification}.
	 * @param ctx the parse tree
	 */
	void exitQuery_specification(VerdictSQLParser.Query_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#limit_clause}.
	 * @param ctx the parse tree
	 */
	void enterLimit_clause(VerdictSQLParser.Limit_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#limit_clause}.
	 * @param ctx the parse tree
	 */
	void exitLimit_clause(VerdictSQLParser.Limit_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#order_by_clause}.
	 * @param ctx the parse tree
	 */
	void enterOrder_by_clause(VerdictSQLParser.Order_by_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#order_by_clause}.
	 * @param ctx the parse tree
	 */
	void exitOrder_by_clause(VerdictSQLParser.Order_by_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#for_clause}.
	 * @param ctx the parse tree
	 */
	void enterFor_clause(VerdictSQLParser.For_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#for_clause}.
	 * @param ctx the parse tree
	 */
	void exitFor_clause(VerdictSQLParser.For_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#xml_common_directives}.
	 * @param ctx the parse tree
	 */
	void enterXml_common_directives(VerdictSQLParser.Xml_common_directivesContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#xml_common_directives}.
	 * @param ctx the parse tree
	 */
	void exitXml_common_directives(VerdictSQLParser.Xml_common_directivesContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#order_by_expression}.
	 * @param ctx the parse tree
	 */
	void enterOrder_by_expression(VerdictSQLParser.Order_by_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#order_by_expression}.
	 * @param ctx the parse tree
	 */
	void exitOrder_by_expression(VerdictSQLParser.Order_by_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#group_by_item}.
	 * @param ctx the parse tree
	 */
	void enterGroup_by_item(VerdictSQLParser.Group_by_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#group_by_item}.
	 * @param ctx the parse tree
	 */
	void exitGroup_by_item(VerdictSQLParser.Group_by_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#option_clause}.
	 * @param ctx the parse tree
	 */
	void enterOption_clause(VerdictSQLParser.Option_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#option_clause}.
	 * @param ctx the parse tree
	 */
	void exitOption_clause(VerdictSQLParser.Option_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#option}.
	 * @param ctx the parse tree
	 */
	void enterOption(VerdictSQLParser.OptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#option}.
	 * @param ctx the parse tree
	 */
	void exitOption(VerdictSQLParser.OptionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#optimize_for_arg}.
	 * @param ctx the parse tree
	 */
	void enterOptimize_for_arg(VerdictSQLParser.Optimize_for_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#optimize_for_arg}.
	 * @param ctx the parse tree
	 */
	void exitOptimize_for_arg(VerdictSQLParser.Optimize_for_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#select_list}.
	 * @param ctx the parse tree
	 */
	void enterSelect_list(VerdictSQLParser.Select_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#select_list}.
	 * @param ctx the parse tree
	 */
	void exitSelect_list(VerdictSQLParser.Select_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#select_list_elem}.
	 * @param ctx the parse tree
	 */
	void enterSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#select_list_elem}.
	 * @param ctx the parse tree
	 */
	void exitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#partition_by_clause}.
	 * @param ctx the parse tree
	 */
	void enterPartition_by_clause(VerdictSQLParser.Partition_by_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#partition_by_clause}.
	 * @param ctx the parse tree
	 */
	void exitPartition_by_clause(VerdictSQLParser.Partition_by_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#table_source}.
	 * @param ctx the parse tree
	 */
	void enterTable_source(VerdictSQLParser.Table_sourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#table_source}.
	 * @param ctx the parse tree
	 */
	void exitTable_source(VerdictSQLParser.Table_sourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#table_source_item_joined}.
	 * @param ctx the parse tree
	 */
	void enterTable_source_item_joined(VerdictSQLParser.Table_source_item_joinedContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#table_source_item_joined}.
	 * @param ctx the parse tree
	 */
	void exitTable_source_item_joined(VerdictSQLParser.Table_source_item_joinedContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sample_table_name_item}
	 * labeled alternative in {@link VerdictSQLParser#table_source_item}.
	 * @param ctx the parse tree
	 */
	void enterSample_table_name_item(VerdictSQLParser.Sample_table_name_itemContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sample_table_name_item}
	 * labeled alternative in {@link VerdictSQLParser#table_source_item}.
	 * @param ctx the parse tree
	 */
	void exitSample_table_name_item(VerdictSQLParser.Sample_table_name_itemContext ctx);
	/**
	 * Enter a parse tree produced by the {@code hinted_table_name_item}
	 * labeled alternative in {@link VerdictSQLParser#table_source_item}.
	 * @param ctx the parse tree
	 */
	void enterHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx);
	/**
	 * Exit a parse tree produced by the {@code hinted_table_name_item}
	 * labeled alternative in {@link VerdictSQLParser#table_source_item}.
	 * @param ctx the parse tree
	 */
	void exitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx);
	/**
	 * Enter a parse tree produced by the {@code derived_table_source_item}
	 * labeled alternative in {@link VerdictSQLParser#table_source_item}.
	 * @param ctx the parse tree
	 */
	void enterDerived_table_source_item(VerdictSQLParser.Derived_table_source_itemContext ctx);
	/**
	 * Exit a parse tree produced by the {@code derived_table_source_item}
	 * labeled alternative in {@link VerdictSQLParser#table_source_item}.
	 * @param ctx the parse tree
	 */
	void exitDerived_table_source_item(VerdictSQLParser.Derived_table_source_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#change_table}.
	 * @param ctx the parse tree
	 */
	void enterChange_table(VerdictSQLParser.Change_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#change_table}.
	 * @param ctx the parse tree
	 */
	void exitChange_table(VerdictSQLParser.Change_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#join_part}.
	 * @param ctx the parse tree
	 */
	void enterJoin_part(VerdictSQLParser.Join_partContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#join_part}.
	 * @param ctx the parse tree
	 */
	void exitJoin_part(VerdictSQLParser.Join_partContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#table_name_with_hint}.
	 * @param ctx the parse tree
	 */
	void enterTable_name_with_hint(VerdictSQLParser.Table_name_with_hintContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#table_name_with_hint}.
	 * @param ctx the parse tree
	 */
	void exitTable_name_with_hint(VerdictSQLParser.Table_name_with_hintContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#rowset_function}.
	 * @param ctx the parse tree
	 */
	void enterRowset_function(VerdictSQLParser.Rowset_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#rowset_function}.
	 * @param ctx the parse tree
	 */
	void exitRowset_function(VerdictSQLParser.Rowset_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#bulk_option}.
	 * @param ctx the parse tree
	 */
	void enterBulk_option(VerdictSQLParser.Bulk_optionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#bulk_option}.
	 * @param ctx the parse tree
	 */
	void exitBulk_option(VerdictSQLParser.Bulk_optionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#derived_table}.
	 * @param ctx the parse tree
	 */
	void enterDerived_table(VerdictSQLParser.Derived_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#derived_table}.
	 * @param ctx the parse tree
	 */
	void exitDerived_table(VerdictSQLParser.Derived_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#function_call}.
	 * @param ctx the parse tree
	 */
	void enterFunction_call(VerdictSQLParser.Function_callContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#function_call}.
	 * @param ctx the parse tree
	 */
	void exitFunction_call(VerdictSQLParser.Function_callContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#datepart}.
	 * @param ctx the parse tree
	 */
	void enterDatepart(VerdictSQLParser.DatepartContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#datepart}.
	 * @param ctx the parse tree
	 */
	void exitDatepart(VerdictSQLParser.DatepartContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#as_table_alias}.
	 * @param ctx the parse tree
	 */
	void enterAs_table_alias(VerdictSQLParser.As_table_aliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#as_table_alias}.
	 * @param ctx the parse tree
	 */
	void exitAs_table_alias(VerdictSQLParser.As_table_aliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#table_alias}.
	 * @param ctx the parse tree
	 */
	void enterTable_alias(VerdictSQLParser.Table_aliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#table_alias}.
	 * @param ctx the parse tree
	 */
	void exitTable_alias(VerdictSQLParser.Table_aliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#with_table_hints}.
	 * @param ctx the parse tree
	 */
	void enterWith_table_hints(VerdictSQLParser.With_table_hintsContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#with_table_hints}.
	 * @param ctx the parse tree
	 */
	void exitWith_table_hints(VerdictSQLParser.With_table_hintsContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#table_hint}.
	 * @param ctx the parse tree
	 */
	void enterTable_hint(VerdictSQLParser.Table_hintContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#table_hint}.
	 * @param ctx the parse tree
	 */
	void exitTable_hint(VerdictSQLParser.Table_hintContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#index_column_name}.
	 * @param ctx the parse tree
	 */
	void enterIndex_column_name(VerdictSQLParser.Index_column_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#index_column_name}.
	 * @param ctx the parse tree
	 */
	void exitIndex_column_name(VerdictSQLParser.Index_column_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#index_value}.
	 * @param ctx the parse tree
	 */
	void enterIndex_value(VerdictSQLParser.Index_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#index_value}.
	 * @param ctx the parse tree
	 */
	void exitIndex_value(VerdictSQLParser.Index_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#column_alias_list}.
	 * @param ctx the parse tree
	 */
	void enterColumn_alias_list(VerdictSQLParser.Column_alias_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#column_alias_list}.
	 * @param ctx the parse tree
	 */
	void exitColumn_alias_list(VerdictSQLParser.Column_alias_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#column_alias}.
	 * @param ctx the parse tree
	 */
	void enterColumn_alias(VerdictSQLParser.Column_aliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#column_alias}.
	 * @param ctx the parse tree
	 */
	void exitColumn_alias(VerdictSQLParser.Column_aliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#table_value_constructor}.
	 * @param ctx the parse tree
	 */
	void enterTable_value_constructor(VerdictSQLParser.Table_value_constructorContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#table_value_constructor}.
	 * @param ctx the parse tree
	 */
	void exitTable_value_constructor(VerdictSQLParser.Table_value_constructorContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#expression_list}.
	 * @param ctx the parse tree
	 */
	void enterExpression_list(VerdictSQLParser.Expression_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#expression_list}.
	 * @param ctx the parse tree
	 */
	void exitExpression_list(VerdictSQLParser.Expression_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#case_expr}.
	 * @param ctx the parse tree
	 */
	void enterCase_expr(VerdictSQLParser.Case_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#case_expr}.
	 * @param ctx the parse tree
	 */
	void exitCase_expr(VerdictSQLParser.Case_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#ranking_windowed_function}.
	 * @param ctx the parse tree
	 */
	void enterRanking_windowed_function(VerdictSQLParser.Ranking_windowed_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#ranking_windowed_function}.
	 * @param ctx the parse tree
	 */
	void exitRanking_windowed_function(VerdictSQLParser.Ranking_windowed_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#value_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void enterValue_manipulation_function(VerdictSQLParser.Value_manipulation_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#value_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void exitValue_manipulation_function(VerdictSQLParser.Value_manipulation_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#nary_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void enterNary_manipulation_function(VerdictSQLParser.Nary_manipulation_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#nary_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void exitNary_manipulation_function(VerdictSQLParser.Nary_manipulation_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#ternary_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void enterTernary_manipulation_function(VerdictSQLParser.Ternary_manipulation_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#ternary_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void exitTernary_manipulation_function(VerdictSQLParser.Ternary_manipulation_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#binary_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void enterBinary_manipulation_function(VerdictSQLParser.Binary_manipulation_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#binary_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void exitBinary_manipulation_function(VerdictSQLParser.Binary_manipulation_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#extract_time_function}.
	 * @param ctx the parse tree
	 */
	void enterExtract_time_function(VerdictSQLParser.Extract_time_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#extract_time_function}.
	 * @param ctx the parse tree
	 */
	void exitExtract_time_function(VerdictSQLParser.Extract_time_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#extract_unit}.
	 * @param ctx the parse tree
	 */
	void enterExtract_unit(VerdictSQLParser.Extract_unitContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#extract_unit}.
	 * @param ctx the parse tree
	 */
	void exitExtract_unit(VerdictSQLParser.Extract_unitContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#unary_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void enterUnary_manipulation_function(VerdictSQLParser.Unary_manipulation_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#unary_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void exitUnary_manipulation_function(VerdictSQLParser.Unary_manipulation_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#noparam_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void enterNoparam_manipulation_function(VerdictSQLParser.Noparam_manipulation_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#noparam_manipulation_function}.
	 * @param ctx the parse tree
	 */
	void exitNoparam_manipulation_function(VerdictSQLParser.Noparam_manipulation_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#lateral_view_function}.
	 * @param ctx the parse tree
	 */
	void enterLateral_view_function(VerdictSQLParser.Lateral_view_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#lateral_view_function}.
	 * @param ctx the parse tree
	 */
	void exitLateral_view_function(VerdictSQLParser.Lateral_view_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#aggregate_windowed_function}.
	 * @param ctx the parse tree
	 */
	void enterAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#aggregate_windowed_function}.
	 * @param ctx the parse tree
	 */
	void exitAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#all_distinct_expression}.
	 * @param ctx the parse tree
	 */
	void enterAll_distinct_expression(VerdictSQLParser.All_distinct_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#all_distinct_expression}.
	 * @param ctx the parse tree
	 */
	void exitAll_distinct_expression(VerdictSQLParser.All_distinct_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#cast_as_expression}.
	 * @param ctx the parse tree
	 */
	void enterCast_as_expression(VerdictSQLParser.Cast_as_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#cast_as_expression}.
	 * @param ctx the parse tree
	 */
	void exitCast_as_expression(VerdictSQLParser.Cast_as_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#over_clause}.
	 * @param ctx the parse tree
	 */
	void enterOver_clause(VerdictSQLParser.Over_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#over_clause}.
	 * @param ctx the parse tree
	 */
	void exitOver_clause(VerdictSQLParser.Over_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#row_or_range_clause}.
	 * @param ctx the parse tree
	 */
	void enterRow_or_range_clause(VerdictSQLParser.Row_or_range_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#row_or_range_clause}.
	 * @param ctx the parse tree
	 */
	void exitRow_or_range_clause(VerdictSQLParser.Row_or_range_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#window_frame_extent}.
	 * @param ctx the parse tree
	 */
	void enterWindow_frame_extent(VerdictSQLParser.Window_frame_extentContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#window_frame_extent}.
	 * @param ctx the parse tree
	 */
	void exitWindow_frame_extent(VerdictSQLParser.Window_frame_extentContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#window_frame_bound}.
	 * @param ctx the parse tree
	 */
	void enterWindow_frame_bound(VerdictSQLParser.Window_frame_boundContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#window_frame_bound}.
	 * @param ctx the parse tree
	 */
	void exitWindow_frame_bound(VerdictSQLParser.Window_frame_boundContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#window_frame_preceding}.
	 * @param ctx the parse tree
	 */
	void enterWindow_frame_preceding(VerdictSQLParser.Window_frame_precedingContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#window_frame_preceding}.
	 * @param ctx the parse tree
	 */
	void exitWindow_frame_preceding(VerdictSQLParser.Window_frame_precedingContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#window_frame_following}.
	 * @param ctx the parse tree
	 */
	void enterWindow_frame_following(VerdictSQLParser.Window_frame_followingContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#window_frame_following}.
	 * @param ctx the parse tree
	 */
	void exitWindow_frame_following(VerdictSQLParser.Window_frame_followingContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#full_table_name}.
	 * @param ctx the parse tree
	 */
	void enterFull_table_name(VerdictSQLParser.Full_table_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#full_table_name}.
	 * @param ctx the parse tree
	 */
	void exitFull_table_name(VerdictSQLParser.Full_table_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#table_name}.
	 * @param ctx the parse tree
	 */
	void enterTable_name(VerdictSQLParser.Table_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#table_name}.
	 * @param ctx the parse tree
	 */
	void exitTable_name(VerdictSQLParser.Table_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#view_name}.
	 * @param ctx the parse tree
	 */
	void enterView_name(VerdictSQLParser.View_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#view_name}.
	 * @param ctx the parse tree
	 */
	void exitView_name(VerdictSQLParser.View_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#func_proc_name}.
	 * @param ctx the parse tree
	 */
	void enterFunc_proc_name(VerdictSQLParser.Func_proc_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#func_proc_name}.
	 * @param ctx the parse tree
	 */
	void exitFunc_proc_name(VerdictSQLParser.Func_proc_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#ddl_object}.
	 * @param ctx the parse tree
	 */
	void enterDdl_object(VerdictSQLParser.Ddl_objectContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#ddl_object}.
	 * @param ctx the parse tree
	 */
	void exitDdl_object(VerdictSQLParser.Ddl_objectContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#full_column_name}.
	 * @param ctx the parse tree
	 */
	void enterFull_column_name(VerdictSQLParser.Full_column_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#full_column_name}.
	 * @param ctx the parse tree
	 */
	void exitFull_column_name(VerdictSQLParser.Full_column_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#column_name_list}.
	 * @param ctx the parse tree
	 */
	void enterColumn_name_list(VerdictSQLParser.Column_name_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#column_name_list}.
	 * @param ctx the parse tree
	 */
	void exitColumn_name_list(VerdictSQLParser.Column_name_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#column_name}.
	 * @param ctx the parse tree
	 */
	void enterColumn_name(VerdictSQLParser.Column_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#column_name}.
	 * @param ctx the parse tree
	 */
	void exitColumn_name(VerdictSQLParser.Column_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#cursor_name}.
	 * @param ctx the parse tree
	 */
	void enterCursor_name(VerdictSQLParser.Cursor_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#cursor_name}.
	 * @param ctx the parse tree
	 */
	void exitCursor_name(VerdictSQLParser.Cursor_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#on_off}.
	 * @param ctx the parse tree
	 */
	void enterOn_off(VerdictSQLParser.On_offContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#on_off}.
	 * @param ctx the parse tree
	 */
	void exitOn_off(VerdictSQLParser.On_offContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#clustered}.
	 * @param ctx the parse tree
	 */
	void enterClustered(VerdictSQLParser.ClusteredContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#clustered}.
	 * @param ctx the parse tree
	 */
	void exitClustered(VerdictSQLParser.ClusteredContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#null_notnull}.
	 * @param ctx the parse tree
	 */
	void enterNull_notnull(VerdictSQLParser.Null_notnullContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#null_notnull}.
	 * @param ctx the parse tree
	 */
	void exitNull_notnull(VerdictSQLParser.Null_notnullContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#true_orfalse}.
	 * @param ctx the parse tree
	 */
	void enterTrue_orfalse(VerdictSQLParser.True_orfalseContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#true_orfalse}.
	 * @param ctx the parse tree
	 */
	void exitTrue_orfalse(VerdictSQLParser.True_orfalseContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#scalar_function_name}.
	 * @param ctx the parse tree
	 */
	void enterScalar_function_name(VerdictSQLParser.Scalar_function_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#scalar_function_name}.
	 * @param ctx the parse tree
	 */
	void exitScalar_function_name(VerdictSQLParser.Scalar_function_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#data_type}.
	 * @param ctx the parse tree
	 */
	void enterData_type(VerdictSQLParser.Data_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#data_type}.
	 * @param ctx the parse tree
	 */
	void exitData_type(VerdictSQLParser.Data_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#default_value}.
	 * @param ctx the parse tree
	 */
	void enterDefault_value(VerdictSQLParser.Default_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#default_value}.
	 * @param ctx the parse tree
	 */
	void exitDefault_value(VerdictSQLParser.Default_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterConstant(VerdictSQLParser.ConstantContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitConstant(VerdictSQLParser.ConstantContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterNumber(VerdictSQLParser.NumberContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitNumber(VerdictSQLParser.NumberContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#sign}.
	 * @param ctx the parse tree
	 */
	void enterSign(VerdictSQLParser.SignContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#sign}.
	 * @param ctx the parse tree
	 */
	void exitSign(VerdictSQLParser.SignContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#id}.
	 * @param ctx the parse tree
	 */
	void enterId(VerdictSQLParser.IdContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#id}.
	 * @param ctx the parse tree
	 */
	void exitId(VerdictSQLParser.IdContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#simple_id}.
	 * @param ctx the parse tree
	 */
	void enterSimple_id(VerdictSQLParser.Simple_idContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#simple_id}.
	 * @param ctx the parse tree
	 */
	void exitSimple_id(VerdictSQLParser.Simple_idContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#comparison_operator}.
	 * @param ctx the parse tree
	 */
	void enterComparison_operator(VerdictSQLParser.Comparison_operatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#comparison_operator}.
	 * @param ctx the parse tree
	 */
	void exitComparison_operator(VerdictSQLParser.Comparison_operatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link VerdictSQLParser#assignment_operator}.
	 * @param ctx the parse tree
	 */
	void enterAssignment_operator(VerdictSQLParser.Assignment_operatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link VerdictSQLParser#assignment_operator}.
	 * @param ctx the parse tree
	 */
	void exitAssignment_operator(VerdictSQLParser.Assignment_operatorContext ctx);
}
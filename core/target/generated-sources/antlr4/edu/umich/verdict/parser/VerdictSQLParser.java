// Generated from edu\u005Cumich\verdict\parser\VerdictSQL.g4 by ANTLR 4.5.3
package edu.umich.verdict.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class VerdictSQLParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, SIZE=8, STORE=9, 
		POISSON=10, COLUMNS=11, STRATIFIED=12, SHOW=13, UNIFORM=14, SAMPLES=15, 
		GET=16, CONFIDENCE=17, TRIALS=18, ADD=19, ALL=20, ALTER=21, AND=22, ANY=23, 
		AS=24, ASC=25, ASCII=26, AUTHORIZATION=27, BACKUP=28, BEGIN=29, BETWEEN=30, 
		BREAK=31, BROWSE=32, BULK=33, BY=34, CASCADE=35, CASE=36, CHANGETABLE=37, 
		CHANGES=38, CHECK=39, CHECKPOINT=40, CLOSE=41, CLUSTERED=42, COALESCE=43, 
		COLLATE=44, COLUMN=45, COMMIT=46, COMPUTE=47, CONSTRAINT=48, CONTAINS=49, 
		CONTAINSTABLE=50, CONTINUE=51, CONV=52, CONVERT=53, CREATE=54, CROSS=55, 
		CURRENT=56, CURRENT_DATE=57, CURRENT_TIME=58, CURRENT_TIMESTAMP=59, CURRENT_USER=60, 
		CURSOR=61, DATABASE=62, DATABASES=63, DBCC=64, DEALLOCATE=65, DECLARE=66, 
		DELETE=67, DENY=68, DESC=69, DESCRIBE=70, DISK=71, DISTINCT=72, DISTRIBUTED=73, 
		DOUBLE=74, DROP=75, DUMP=76, ELSE=77, END=78, ERRLVL=79, ESCAPE=80, EXCEPT=81, 
		EXEC=82, EXECUTE=83, EXISTS=84, EXIT=85, EXTERNAL=86, FALSE=87, FETCH=88, 
		FILE=89, FILLFACTOR=90, FOR=91, FORCESEEK=92, FOREIGN=93, FREETEXT=94, 
		FREETEXTTABLE=95, FROM=96, FULL=97, FUNCTION=98, GOTO=99, GRANT=100, GROUP=101, 
		HAVING=102, IDENTITY=103, IDENTITYCOL=104, IDENTITY_INSERT=105, IF=106, 
		IN=107, INDEX=108, INNER=109, INSERT=110, INTERSECT=111, INTO=112, IS=113, 
		JOIN=114, KEY=115, KILL=116, LEFT=117, LIKE=118, LIMIT=119, LINENO=120, 
		LOAD=121, MERGE=122, NATIONAL=123, NOCHECK=124, NONCLUSTERED=125, NOT=126, 
		NULL=127, NULLIF=128, OF=129, OFF=130, OFFSETS=131, ON=132, OPEN=133, 
		OPENDATASOURCE=134, OPENQUERY=135, OPENROWSET=136, OPENXML=137, OPTION=138, 
		OR=139, ORDER=140, OUTER=141, OVER=142, PERCENT=143, PIVOT=144, PLAN=145, 
		PRECISION=146, PRIMARY=147, PRINT=148, PROC=149, PROCEDURE=150, RAISERROR=151, 
		READ=152, READTEXT=153, RECONFIGURE=154, REFERENCES=155, REPLICATION=156, 
		RESTORE=157, RESTRICT=158, RETURN=159, REVERT=160, REVOKE=161, RIGHT=162, 
		ROLLBACK=163, ROWCOUNT=164, ROWGUIDCOL=165, RULE=166, SAVE=167, SCHEMA=168, 
		SCHEMAS=169, SECURITYAUDIT=170, SELECT=171, SEMANTICKEYPHRASETABLE=172, 
		SEMANTICSIMILARITYDETAILSTABLE=173, SEMANTICSIMILARITYTABLE=174, SEMI=175, 
		SESSION_USER=176, SET=177, SETUSER=178, SHUTDOWN=179, SOME=180, SUBSTR=181, 
		STATISTICS=182, SYSTEM_USER=183, TABLE=184, TABLES=185, TABLESAMPLE=186, 
		TEXTSIZE=187, THEN=188, TO=189, TOP=190, TRAN=191, TRANSACTION=192, TRIGGER=193, 
		TRUE=194, TRUNCATE=195, TRY_CONVERT=196, TSEQUAL=197, UNION=198, UNIQUE=199, 
		UNPIVOT=200, UPDATE=201, UPDATETEXT=202, USE=203, USER=204, VALUES=205, 
		VARYING=206, VIEW=207, WAITFOR=208, WHEN=209, WHERE=210, WHILE=211, WITH=212, 
		WITHIN=213, WRITETEXT=214, ABSOLUTE=215, ABS=216, ACOS=217, AES_DECRYPT=218, 
		AES_ENCRYPT=219, APPLY=220, ASIN=221, ATAN=222, AUTO=223, AVG=224, BASE64=225, 
		BIN=226, BINARY_CHECKSUM=227, BROUND=228, CALLER=229, CAST=230, CATCH=231, 
		CBRT=232, CEIL=233, CHARACTER_LENGTH=234, CHECKSUM=235, CHECKSUM_AGG=236, 
		CHR=237, COMMITTED=238, CONCAT=239, CONCAT_WS=240, CONFIG=241, COOKIE=242, 
		COS=243, COUNT=244, COUNT_BIG=245, CRC32=246, DATEADD=247, DATEDIFF=248, 
		DATENAME=249, DATEPART=250, DAY=251, DAYS=252, DECODE=253, DEGREES=254, 
		DELAY=255, DELETED=256, DENSE_RANK=257, DISABLE=258, DYNAMIC=259, NATURAL_CONSTANT=260, 
		ENCODE=261, ENCRYPTION=262, ESCAPED_BY=263, EXACT=264, EXP=265, EXPLODE=266, 
		EXTRACT=267, FACTORIAL=268, FAST=269, FAST_FORWARD=270, FIELDS_SEPARATED_BY=271, 
		FIND_IN_SET=272, FIRST=273, FLOOR=274, FOLLOWING=275, FORMAT_NUMBER=276, 
		FORWARD_ONLY=277, FNV_HASH=278, FROM_UNIXTIME=279, FULLSCAN=280, GET_JSON_OBJECT=281, 
		GLOBAL=282, GO=283, GROUPING=284, GROUPING_ID=285, HASH=286, HEX=287, 
		HOUR=288, INSENSITIVE=289, INSERTED=290, INSTR=291, INTERVAL=292, IN_FILE=293, 
		ISOLATION=294, KEEPFIXED=295, KEYSET=296, LAST=297, LATERAL=298, LENGTH=299, 
		LEVEL=300, LN=301, LOCAL=302, LOCATE=303, LOCATION=304, LOCK_ESCALATION=305, 
		LOG2=306, LOG10=307, LOGIN=308, LOOP=309, LOWER=310, LTRIM=311, MARK=312, 
		MAX=313, MD5=314, MIN=315, MIN_ACTIVE_ROWVERSION=316, MINUTE=317, MOD=318, 
		MODIFY=319, MONTH=320, MONTHS=321, NEGATIVE=322, NEXT=323, NAME=324, NDV=325, 
		NOCOUNT=326, NOEXPAND=327, NORECOMPUTE=328, NTILE=329, NUMBER=330, NVL=331, 
		OFFSET=332, ONLY=333, OPTIMISTIC=334, OPTIMIZE=335, OUT=336, OUTPUT=337, 
		OWNER=338, PARTITION=339, PATH=340, PERCENTILE=341, PI=342, PMOD=343, 
		POSITIVE=344, POW=345, PRECEDING=346, PRIOR=347, QUARTER=348, QUOTED_BY=349, 
		RADIANS=350, RAND=351, RANDOM=352, RANGE=353, RANK=354, READONLY=355, 
		READ_ONLY=356, RECOMMENDED=357, RECOMPILE=358, REFRESH=359, RELATIVE=360, 
		REMOTE=361, REPEAT=362, REPEATABLE=363, REVERSE=364, ROLLUP=365, ROOT=366, 
		ROUND=367, ROW=368, ROWGUID=369, ROWS=370, ROW_NUMBER=371, SAMPLE=372, 
		SCHEMABINDING=373, SCROLL=374, SCROLL_LOCKS=375, SECOND=376, SELF=377, 
		SERIALIZABLE=378, SHA1=379, SHA2=380, SHIFTLEFT=381, SHIFTRIGHT=382, SHIFTRIGHTUNSIGNED=383, 
		SIGN=384, SIN=385, SNAPSHOT=386, SPACE_FUNCTION=387, SPATIAL_WINDOW_MAX_CELLS=388, 
		SPLIT=389, STATIC=390, STATS_STREAM=391, STDEV=392, STDDEV=393, STDEVP=394, 
		STDDEV_SAMP=395, STORED_AS_PARQUET=396, SUM=397, SQRT=398, STRTOL=399, 
		TAN=400, THROW=401, TIES=402, TIME=403, TO_DATE=404, TRIM=405, TRY=406, 
		TYPE=407, TYPE_WARNING=408, UNBOUNDED=409, UNCOMMITTED=410, UNHEX=411, 
		UNIVERSE=412, UNIX_TIMESTAMP=413, UNKNOWN=414, UPPER=415, USING=416, VAR=417, 
		VARP=418, VIEW_METADATA=419, WEEKOFYEAR=420, WORK=421, XML=422, XMLNAMESPACES=423, 
		YEAR=424, YEARS=425, DOLLAR_ACTION=426, SPACE=427, COMMENT=428, LINE_COMMENT=429, 
		DOUBLE_QUOTE_ID=430, BACKTICK_ID=431, SQUARE_BRACKET_ID=432, LOCAL_ID=433, 
		DECIMAL=434, ID=435, STRING=436, BINARY=437, FLOAT=438, REAL=439, EQUAL=440, 
		GREATER=441, LESS=442, EXCLAMATION=443, PLUS_ASSIGN=444, MINUS_ASSIGN=445, 
		MULT_ASSIGN=446, DIV_ASSIGN=447, MOD_ASSIGN=448, AND_ASSIGN=449, XOR_ASSIGN=450, 
		OR_ASSIGN=451, DOT=452, UNDERLINE=453, AT=454, SHARP=455, DOLLAR=456, 
		LR_BRACKET=457, RR_BRACKET=458, COMMA=459, SEMICOLON=460, COLON=461, STAR=462, 
		DIVIDE=463, MODULE=464, PLUS=465, MINUS=466, BIT_NOT=467, BIT_OR=468, 
		BIT_AND=469, BIT_XOR=470;
	public static final int
		RULE_verdict_statement = 0, RULE_create_sample_statement = 1, RULE_sample_type = 2, 
		RULE_on_columns = 3, RULE_delete_sample_statement = 4, RULE_show_samples_statement = 5, 
		RULE_config_statement = 6, RULE_other_statement = 7, RULE_create_database = 8, 
		RULE_drop_database = 9, RULE_config_set_statement = 10, RULE_config_get_statement = 11, 
		RULE_config_key = 12, RULE_config_value = 13, RULE_confidence_clause = 14, 
		RULE_trials_clause = 15, RULE_table_name_with_sample = 16, RULE_tsql_file = 17, 
		RULE_sql_clause = 18, RULE_ddl_clause = 19, RULE_select_statement = 20, 
		RULE_output_clause = 21, RULE_output_dml_list_elem = 22, RULE_output_column_name = 23, 
		RULE_create_table = 24, RULE_create_table_as_select = 25, RULE_create_view = 26, 
		RULE_alter_table = 27, RULE_alter_database = 28, RULE_drop_table = 29, 
		RULE_drop_view = 30, RULE_set_statment = 31, RULE_use_statement = 32, 
		RULE_show_tables_statement = 33, RULE_show_databases_statement = 34, RULE_describe_table_statement = 35, 
		RULE_refresh_statement = 36, RULE_show_config_statement = 37, RULE_table_type_definition = 38, 
		RULE_column_def_table_constraint = 39, RULE_column_definition = 40, RULE_column_constraint = 41, 
		RULE_table_constraint = 42, RULE_set_special = 43, RULE_expression = 44, 
		RULE_interval = 45, RULE_constant_expression = 46, RULE_subquery = 47, 
		RULE_dml_table_source = 48, RULE_with_expression = 49, RULE_common_table_expression = 50, 
		RULE_update_elem = 51, RULE_search_condition_list = 52, RULE_search_condition = 53, 
		RULE_search_condition_or = 54, RULE_search_condition_not = 55, RULE_predicate = 56, 
		RULE_query_expression = 57, RULE_union = 58, RULE_query_specification = 59, 
		RULE_limit_clause = 60, RULE_order_by_clause = 61, RULE_for_clause = 62, 
		RULE_xml_common_directives = 63, RULE_order_by_expression = 64, RULE_group_by_item = 65, 
		RULE_option_clause = 66, RULE_option = 67, RULE_optimize_for_arg = 68, 
		RULE_select_list = 69, RULE_select_list_elem = 70, RULE_partition_by_clause = 71, 
		RULE_table_source = 72, RULE_table_source_item_joined = 73, RULE_table_source_item = 74, 
		RULE_change_table = 75, RULE_join_part = 76, RULE_table_name_with_hint = 77, 
		RULE_rowset_function = 78, RULE_bulk_option = 79, RULE_derived_table = 80, 
		RULE_function_call = 81, RULE_datepart = 82, RULE_as_table_alias = 83, 
		RULE_table_alias = 84, RULE_with_table_hints = 85, RULE_table_hint = 86, 
		RULE_index_column_name = 87, RULE_index_value = 88, RULE_column_alias_list = 89, 
		RULE_column_alias = 90, RULE_table_value_constructor = 91, RULE_expression_list = 92, 
		RULE_case_expr = 93, RULE_ranking_windowed_function = 94, RULE_value_manipulation_function = 95, 
		RULE_nary_manipulation_function = 96, RULE_ternary_manipulation_function = 97, 
		RULE_binary_manipulation_function = 98, RULE_extract_time_function = 99, 
		RULE_extract_unit = 100, RULE_unary_manipulation_function = 101, RULE_noparam_manipulation_function = 102, 
		RULE_lateral_view_function = 103, RULE_aggregate_windowed_function = 104, 
		RULE_all_distinct_expression = 105, RULE_cast_as_expression = 106, RULE_over_clause = 107, 
		RULE_row_or_range_clause = 108, RULE_window_frame_extent = 109, RULE_window_frame_bound = 110, 
		RULE_window_frame_preceding = 111, RULE_window_frame_following = 112, 
		RULE_full_table_name = 113, RULE_table_name = 114, RULE_view_name = 115, 
		RULE_func_proc_name = 116, RULE_ddl_object = 117, RULE_full_column_name = 118, 
		RULE_column_name_list = 119, RULE_column_name = 120, RULE_cursor_name = 121, 
		RULE_on_off = 122, RULE_clustered = 123, RULE_null_notnull = 124, RULE_true_orfalse = 125, 
		RULE_scalar_function_name = 126, RULE_data_type = 127, RULE_default_value = 128, 
		RULE_constant = 129, RULE_number = 130, RULE_sign = 131, RULE_id = 132, 
		RULE_simple_id = 133, RULE_comparison_operator = 134, RULE_assignment_operator = 135;
	public static final String[] ruleNames = {
		"verdict_statement", "create_sample_statement", "sample_type", "on_columns", 
		"delete_sample_statement", "show_samples_statement", "config_statement", 
		"other_statement", "create_database", "drop_database", "config_set_statement", 
		"config_get_statement", "config_key", "config_value", "confidence_clause", 
		"trials_clause", "table_name_with_sample", "tsql_file", "sql_clause", 
		"ddl_clause", "select_statement", "output_clause", "output_dml_list_elem", 
		"output_column_name", "create_table", "create_table_as_select", "create_view", 
		"alter_table", "alter_database", "drop_table", "drop_view", "set_statment", 
		"use_statement", "show_tables_statement", "show_databases_statement", 
		"describe_table_statement", "refresh_statement", "show_config_statement", 
		"table_type_definition", "column_def_table_constraint", "column_definition", 
		"column_constraint", "table_constraint", "set_special", "expression", 
		"interval", "constant_expression", "subquery", "dml_table_source", "with_expression", 
		"common_table_expression", "update_elem", "search_condition_list", "search_condition", 
		"search_condition_or", "search_condition_not", "predicate", "query_expression", 
		"union", "query_specification", "limit_clause", "order_by_clause", "for_clause", 
		"xml_common_directives", "order_by_expression", "group_by_item", "option_clause", 
		"option", "optimize_for_arg", "select_list", "select_list_elem", "partition_by_clause", 
		"table_source", "table_source_item_joined", "table_source_item", "change_table", 
		"join_part", "table_name_with_hint", "rowset_function", "bulk_option", 
		"derived_table", "function_call", "datepart", "as_table_alias", "table_alias", 
		"with_table_hints", "table_hint", "index_column_name", "index_value", 
		"column_alias_list", "column_alias", "table_value_constructor", "expression_list", 
		"case_expr", "ranking_windowed_function", "value_manipulation_function", 
		"nary_manipulation_function", "ternary_manipulation_function", "binary_manipulation_function", 
		"extract_time_function", "extract_unit", "unary_manipulation_function", 
		"noparam_manipulation_function", "lateral_view_function", "aggregate_windowed_function", 
		"all_distinct_expression", "cast_as_expression", "over_clause", "row_or_range_clause", 
		"window_frame_extent", "window_frame_bound", "window_frame_preceding", 
		"window_frame_following", "full_table_name", "table_name", "view_name", 
		"func_proc_name", "ddl_object", "full_column_name", "column_name_list", 
		"column_name", "cursor_name", "on_off", "clustered", "null_notnull", "true_orfalse", 
		"scalar_function_name", "data_type", "default_value", "constant", "number", 
		"sign", "id", "simple_id", "comparison_operator", "assignment_operator"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'||'", "'<='", "'>='", "'<>'", "'!='", "'!>'", "'!<'", null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, "'='", "'>'", 
		"'<'", "'!'", "'+='", "'-='", "'*='", "'/='", "'%='", "'&='", "'^='", 
		"'|='", "'.'", "'_'", "'@'", "'#'", "'$'", "'('", "')'", "','", "';'", 
		"':'", "'*'", "'/'", "'%'", "'+'", "'-'", "'~'", "'|'", "'&'", "'^'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, "SIZE", "STORE", "POISSON", 
		"COLUMNS", "STRATIFIED", "SHOW", "UNIFORM", "SAMPLES", "GET", "CONFIDENCE", 
		"TRIALS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "ASCII", "AUTHORIZATION", 
		"BACKUP", "BEGIN", "BETWEEN", "BREAK", "BROWSE", "BULK", "BY", "CASCADE", 
		"CASE", "CHANGETABLE", "CHANGES", "CHECK", "CHECKPOINT", "CLOSE", "CLUSTERED", 
		"COALESCE", "COLLATE", "COLUMN", "COMMIT", "COMPUTE", "CONSTRAINT", "CONTAINS", 
		"CONTAINSTABLE", "CONTINUE", "CONV", "CONVERT", "CREATE", "CROSS", "CURRENT", 
		"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", 
		"DATABASE", "DATABASES", "DBCC", "DEALLOCATE", "DECLARE", "DELETE", "DENY", 
		"DESC", "DESCRIBE", "DISK", "DISTINCT", "DISTRIBUTED", "DOUBLE", "DROP", 
		"DUMP", "ELSE", "END", "ERRLVL", "ESCAPE", "EXCEPT", "EXEC", "EXECUTE", 
		"EXISTS", "EXIT", "EXTERNAL", "FALSE", "FETCH", "FILE", "FILLFACTOR", 
		"FOR", "FORCESEEK", "FOREIGN", "FREETEXT", "FREETEXTTABLE", "FROM", "FULL", 
		"FUNCTION", "GOTO", "GRANT", "GROUP", "HAVING", "IDENTITY", "IDENTITYCOL", 
		"IDENTITY_INSERT", "IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT", 
		"INTO", "IS", "JOIN", "KEY", "KILL", "LEFT", "LIKE", "LIMIT", "LINENO", 
		"LOAD", "MERGE", "NATIONAL", "NOCHECK", "NONCLUSTERED", "NOT", "NULL", 
		"NULLIF", "OF", "OFF", "OFFSETS", "ON", "OPEN", "OPENDATASOURCE", "OPENQUERY", 
		"OPENROWSET", "OPENXML", "OPTION", "OR", "ORDER", "OUTER", "OVER", "PERCENT", 
		"PIVOT", "PLAN", "PRECISION", "PRIMARY", "PRINT", "PROC", "PROCEDURE", 
		"RAISERROR", "READ", "READTEXT", "RECONFIGURE", "REFERENCES", "REPLICATION", 
		"RESTORE", "RESTRICT", "RETURN", "REVERT", "REVOKE", "RIGHT", "ROLLBACK", 
		"ROWCOUNT", "ROWGUIDCOL", "RULE", "SAVE", "SCHEMA", "SCHEMAS", "SECURITYAUDIT", 
		"SELECT", "SEMANTICKEYPHRASETABLE", "SEMANTICSIMILARITYDETAILSTABLE", 
		"SEMANTICSIMILARITYTABLE", "SEMI", "SESSION_USER", "SET", "SETUSER", "SHUTDOWN", 
		"SOME", "SUBSTR", "STATISTICS", "SYSTEM_USER", "TABLE", "TABLES", "TABLESAMPLE", 
		"TEXTSIZE", "THEN", "TO", "TOP", "TRAN", "TRANSACTION", "TRIGGER", "TRUE", 
		"TRUNCATE", "TRY_CONVERT", "TSEQUAL", "UNION", "UNIQUE", "UNPIVOT", "UPDATE", 
		"UPDATETEXT", "USE", "USER", "VALUES", "VARYING", "VIEW", "WAITFOR", "WHEN", 
		"WHERE", "WHILE", "WITH", "WITHIN", "WRITETEXT", "ABSOLUTE", "ABS", "ACOS", 
		"AES_DECRYPT", "AES_ENCRYPT", "APPLY", "ASIN", "ATAN", "AUTO", "AVG", 
		"BASE64", "BIN", "BINARY_CHECKSUM", "BROUND", "CALLER", "CAST", "CATCH", 
		"CBRT", "CEIL", "CHARACTER_LENGTH", "CHECKSUM", "CHECKSUM_AGG", "CHR", 
		"COMMITTED", "CONCAT", "CONCAT_WS", "CONFIG", "COOKIE", "COS", "COUNT", 
		"COUNT_BIG", "CRC32", "DATEADD", "DATEDIFF", "DATENAME", "DATEPART", "DAY", 
		"DAYS", "DECODE", "DEGREES", "DELAY", "DELETED", "DENSE_RANK", "DISABLE", 
		"DYNAMIC", "NATURAL_CONSTANT", "ENCODE", "ENCRYPTION", "ESCAPED_BY", "EXACT", 
		"EXP", "EXPLODE", "EXTRACT", "FACTORIAL", "FAST", "FAST_FORWARD", "FIELDS_SEPARATED_BY", 
		"FIND_IN_SET", "FIRST", "FLOOR", "FOLLOWING", "FORMAT_NUMBER", "FORWARD_ONLY", 
		"FNV_HASH", "FROM_UNIXTIME", "FULLSCAN", "GET_JSON_OBJECT", "GLOBAL", 
		"GO", "GROUPING", "GROUPING_ID", "HASH", "HEX", "HOUR", "INSENSITIVE", 
		"INSERTED", "INSTR", "INTERVAL", "IN_FILE", "ISOLATION", "KEEPFIXED", 
		"KEYSET", "LAST", "LATERAL", "LENGTH", "LEVEL", "LN", "LOCAL", "LOCATE", 
		"LOCATION", "LOCK_ESCALATION", "LOG2", "LOG10", "LOGIN", "LOOP", "LOWER", 
		"LTRIM", "MARK", "MAX", "MD5", "MIN", "MIN_ACTIVE_ROWVERSION", "MINUTE", 
		"MOD", "MODIFY", "MONTH", "MONTHS", "NEGATIVE", "NEXT", "NAME", "NDV", 
		"NOCOUNT", "NOEXPAND", "NORECOMPUTE", "NTILE", "NUMBER", "NVL", "OFFSET", 
		"ONLY", "OPTIMISTIC", "OPTIMIZE", "OUT", "OUTPUT", "OWNER", "PARTITION", 
		"PATH", "PERCENTILE", "PI", "PMOD", "POSITIVE", "POW", "PRECEDING", "PRIOR", 
		"QUARTER", "QUOTED_BY", "RADIANS", "RAND", "RANDOM", "RANGE", "RANK", 
		"READONLY", "READ_ONLY", "RECOMMENDED", "RECOMPILE", "REFRESH", "RELATIVE", 
		"REMOTE", "REPEAT", "REPEATABLE", "REVERSE", "ROLLUP", "ROOT", "ROUND", 
		"ROW", "ROWGUID", "ROWS", "ROW_NUMBER", "SAMPLE", "SCHEMABINDING", "SCROLL", 
		"SCROLL_LOCKS", "SECOND", "SELF", "SERIALIZABLE", "SHA1", "SHA2", "SHIFTLEFT", 
		"SHIFTRIGHT", "SHIFTRIGHTUNSIGNED", "SIGN", "SIN", "SNAPSHOT", "SPACE_FUNCTION", 
		"SPATIAL_WINDOW_MAX_CELLS", "SPLIT", "STATIC", "STATS_STREAM", "STDEV", 
		"STDDEV", "STDEVP", "STDDEV_SAMP", "STORED_AS_PARQUET", "SUM", "SQRT", 
		"STRTOL", "TAN", "THROW", "TIES", "TIME", "TO_DATE", "TRIM", "TRY", "TYPE", 
		"TYPE_WARNING", "UNBOUNDED", "UNCOMMITTED", "UNHEX", "UNIVERSE", "UNIX_TIMESTAMP", 
		"UNKNOWN", "UPPER", "USING", "VAR", "VARP", "VIEW_METADATA", "WEEKOFYEAR", 
		"WORK", "XML", "XMLNAMESPACES", "YEAR", "YEARS", "DOLLAR_ACTION", "SPACE", 
		"COMMENT", "LINE_COMMENT", "DOUBLE_QUOTE_ID", "BACKTICK_ID", "SQUARE_BRACKET_ID", 
		"LOCAL_ID", "DECIMAL", "ID", "STRING", "BINARY", "FLOAT", "REAL", "EQUAL", 
		"GREATER", "LESS", "EXCLAMATION", "PLUS_ASSIGN", "MINUS_ASSIGN", "MULT_ASSIGN", 
		"DIV_ASSIGN", "MOD_ASSIGN", "AND_ASSIGN", "XOR_ASSIGN", "OR_ASSIGN", "DOT", 
		"UNDERLINE", "AT", "SHARP", "DOLLAR", "LR_BRACKET", "RR_BRACKET", "COMMA", 
		"SEMICOLON", "COLON", "STAR", "DIVIDE", "MODULE", "PLUS", "MINUS", "BIT_NOT", 
		"BIT_OR", "BIT_AND", "BIT_XOR"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "VerdictSQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public VerdictSQLParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class Verdict_statementContext extends ParserRuleContext {
		public Select_statementContext select_statement() {
			return getRuleContext(Select_statementContext.class,0);
		}
		public Create_sample_statementContext create_sample_statement() {
			return getRuleContext(Create_sample_statementContext.class,0);
		}
		public Delete_sample_statementContext delete_sample_statement() {
			return getRuleContext(Delete_sample_statementContext.class,0);
		}
		public Show_samples_statementContext show_samples_statement() {
			return getRuleContext(Show_samples_statementContext.class,0);
		}
		public Config_statementContext config_statement() {
			return getRuleContext(Config_statementContext.class,0);
		}
		public Other_statementContext other_statement() {
			return getRuleContext(Other_statementContext.class,0);
		}
		public Create_tableContext create_table() {
			return getRuleContext(Create_tableContext.class,0);
		}
		public Create_table_as_selectContext create_table_as_select() {
			return getRuleContext(Create_table_as_selectContext.class,0);
		}
		public Create_viewContext create_view() {
			return getRuleContext(Create_viewContext.class,0);
		}
		public Drop_tableContext drop_table() {
			return getRuleContext(Drop_tableContext.class,0);
		}
		public Drop_viewContext drop_view() {
			return getRuleContext(Drop_viewContext.class,0);
		}
		public Verdict_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_verdict_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterVerdict_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitVerdict_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitVerdict_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Verdict_statementContext verdict_statement() throws RecognitionException {
		Verdict_statementContext _localctx = new Verdict_statementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_verdict_statement);
		try {
			setState(283);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(272);
				select_statement();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(273);
				create_sample_statement();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(274);
				delete_sample_statement();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(275);
				show_samples_statement();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(276);
				config_statement();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(277);
				other_statement();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(278);
				create_table();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(279);
				create_table_as_select();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(280);
				create_view();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(281);
				drop_table();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(282);
				drop_view();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_sample_statementContext extends ParserRuleContext {
		public Token size;
		public Table_nameContext original_table;
		public TerminalNode CREATE() { return getToken(VerdictSQLParser.CREATE, 0); }
		public TerminalNode SAMPLE() { return getToken(VerdictSQLParser.SAMPLE, 0); }
		public TerminalNode FROM() { return getToken(VerdictSQLParser.FROM, 0); }
		public TerminalNode OF() { return getToken(VerdictSQLParser.OF, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Sample_typeContext sample_type() {
			return getRuleContext(Sample_typeContext.class,0);
		}
		public On_columnsContext on_columns() {
			return getRuleContext(On_columnsContext.class,0);
		}
		public TerminalNode FLOAT() { return getToken(VerdictSQLParser.FLOAT, 0); }
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public Create_sample_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_sample_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterCreate_sample_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitCreate_sample_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitCreate_sample_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_sample_statementContext create_sample_statement() throws RecognitionException {
		Create_sample_statementContext _localctx = new Create_sample_statementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_create_sample_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(285);
			match(CREATE);
			setState(288);
			_la = _input.LA(1);
			if (_la==DECIMAL || _la==FLOAT) {
				{
				setState(286);
				((Create_sample_statementContext)_localctx).size = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==DECIMAL || _la==FLOAT) ) {
					((Create_sample_statementContext)_localctx).size = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(287);
				match(MODULE);
				}
			}

			setState(291);
			_la = _input.LA(1);
			if (_la==STRATIFIED || _la==UNIFORM || _la==RECOMMENDED || _la==UNIVERSE) {
				{
				setState(290);
				sample_type();
				}
			}

			setState(293);
			match(SAMPLE);
			setState(294);
			_la = _input.LA(1);
			if ( !(_la==FROM || _la==OF) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(295);
			((Create_sample_statementContext)_localctx).original_table = table_name();
			setState(297);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(296);
				on_columns();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Sample_typeContext extends ParserRuleContext {
		public TerminalNode UNIFORM() { return getToken(VerdictSQLParser.UNIFORM, 0); }
		public TerminalNode UNIVERSE() { return getToken(VerdictSQLParser.UNIVERSE, 0); }
		public TerminalNode STRATIFIED() { return getToken(VerdictSQLParser.STRATIFIED, 0); }
		public TerminalNode RECOMMENDED() { return getToken(VerdictSQLParser.RECOMMENDED, 0); }
		public Sample_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sample_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSample_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSample_type(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSample_type(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Sample_typeContext sample_type() throws RecognitionException {
		Sample_typeContext _localctx = new Sample_typeContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_sample_type);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(299);
			_la = _input.LA(1);
			if ( !(_la==STRATIFIED || _la==UNIFORM || _la==RECOMMENDED || _la==UNIVERSE) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class On_columnsContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(VerdictSQLParser.ON, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public On_columnsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_on_columns; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOn_columns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOn_columns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOn_columns(this);
			else return visitor.visitChildren(this);
		}
	}

	public final On_columnsContext on_columns() throws RecognitionException {
		On_columnsContext _localctx = new On_columnsContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_on_columns);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(301);
			match(ON);
			setState(302);
			column_name();
			setState(307);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(303);
				match(COMMA);
				setState(304);
				column_name();
				}
				}
				setState(309);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Delete_sample_statementContext extends ParserRuleContext {
		public Token size;
		public Table_nameContext original_table;
		public TerminalNode SAMPLES() { return getToken(VerdictSQLParser.SAMPLES, 0); }
		public TerminalNode OF() { return getToken(VerdictSQLParser.OF, 0); }
		public TerminalNode DELETE() { return getToken(VerdictSQLParser.DELETE, 0); }
		public TerminalNode DROP() { return getToken(VerdictSQLParser.DROP, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Sample_typeContext sample_type() {
			return getRuleContext(Sample_typeContext.class,0);
		}
		public On_columnsContext on_columns() {
			return getRuleContext(On_columnsContext.class,0);
		}
		public TerminalNode FLOAT() { return getToken(VerdictSQLParser.FLOAT, 0); }
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public Delete_sample_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete_sample_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDelete_sample_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDelete_sample_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDelete_sample_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Delete_sample_statementContext delete_sample_statement() throws RecognitionException {
		Delete_sample_statementContext _localctx = new Delete_sample_statementContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_delete_sample_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(310);
			_la = _input.LA(1);
			if ( !(_la==DELETE || _la==DROP) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(313);
			_la = _input.LA(1);
			if (_la==DECIMAL || _la==FLOAT) {
				{
				setState(311);
				((Delete_sample_statementContext)_localctx).size = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==DECIMAL || _la==FLOAT) ) {
					((Delete_sample_statementContext)_localctx).size = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(312);
				match(MODULE);
				}
			}

			setState(316);
			_la = _input.LA(1);
			if (_la==STRATIFIED || _la==UNIFORM || _la==RECOMMENDED || _la==UNIVERSE) {
				{
				setState(315);
				sample_type();
				}
			}

			setState(318);
			match(SAMPLES);
			setState(319);
			match(OF);
			setState(320);
			((Delete_sample_statementContext)_localctx).original_table = table_name();
			setState(322);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(321);
				on_columns();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Show_samples_statementContext extends ParserRuleContext {
		public Token type;
		public IdContext database;
		public TerminalNode SHOW() { return getToken(VerdictSQLParser.SHOW, 0); }
		public TerminalNode SAMPLES() { return getToken(VerdictSQLParser.SAMPLES, 0); }
		public TerminalNode FOR() { return getToken(VerdictSQLParser.FOR, 0); }
		public TerminalNode OF() { return getToken(VerdictSQLParser.OF, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode STRATIFIED() { return getToken(VerdictSQLParser.STRATIFIED, 0); }
		public TerminalNode UNIFORM() { return getToken(VerdictSQLParser.UNIFORM, 0); }
		public TerminalNode ALL() { return getToken(VerdictSQLParser.ALL, 0); }
		public Show_samples_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_show_samples_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterShow_samples_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitShow_samples_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitShow_samples_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Show_samples_statementContext show_samples_statement() throws RecognitionException {
		Show_samples_statementContext _localctx = new Show_samples_statementContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_show_samples_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(324);
			match(SHOW);
			setState(326);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << STRATIFIED) | (1L << UNIFORM) | (1L << ALL))) != 0)) {
				{
				setState(325);
				((Show_samples_statementContext)_localctx).type = _input.LT(1);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << STRATIFIED) | (1L << UNIFORM) | (1L << ALL))) != 0)) ) {
					((Show_samples_statementContext)_localctx).type = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			setState(328);
			match(SAMPLES);
			setState(331);
			_la = _input.LA(1);
			if (_la==FOR || _la==OF) {
				{
				setState(329);
				_la = _input.LA(1);
				if ( !(_la==FOR || _la==OF) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(330);
				((Show_samples_statementContext)_localctx).database = id();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Config_statementContext extends ParserRuleContext {
		public Config_set_statementContext config_set_statement() {
			return getRuleContext(Config_set_statementContext.class,0);
		}
		public Config_get_statementContext config_get_statement() {
			return getRuleContext(Config_get_statementContext.class,0);
		}
		public Config_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_config_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterConfig_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitConfig_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitConfig_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Config_statementContext config_statement() throws RecognitionException {
		Config_statementContext _localctx = new Config_statementContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_config_statement);
		try {
			setState(335);
			switch (_input.LA(1)) {
			case SET:
				enterOuterAlt(_localctx, 1);
				{
				setState(333);
				config_set_statement();
				}
				break;
			case GET:
				enterOuterAlt(_localctx, 2);
				{
				setState(334);
				config_get_statement();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Other_statementContext extends ParserRuleContext {
		public Use_statementContext use_statement() {
			return getRuleContext(Use_statementContext.class,0);
		}
		public Show_tables_statementContext show_tables_statement() {
			return getRuleContext(Show_tables_statementContext.class,0);
		}
		public Show_databases_statementContext show_databases_statement() {
			return getRuleContext(Show_databases_statementContext.class,0);
		}
		public Describe_table_statementContext describe_table_statement() {
			return getRuleContext(Describe_table_statementContext.class,0);
		}
		public Refresh_statementContext refresh_statement() {
			return getRuleContext(Refresh_statementContext.class,0);
		}
		public Show_config_statementContext show_config_statement() {
			return getRuleContext(Show_config_statementContext.class,0);
		}
		public Create_databaseContext create_database() {
			return getRuleContext(Create_databaseContext.class,0);
		}
		public Drop_databaseContext drop_database() {
			return getRuleContext(Drop_databaseContext.class,0);
		}
		public Other_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_other_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOther_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOther_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOther_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Other_statementContext other_statement() throws RecognitionException {
		Other_statementContext _localctx = new Other_statementContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_other_statement);
		try {
			setState(345);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(337);
				use_statement();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(338);
				show_tables_statement();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(339);
				show_databases_statement();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(340);
				describe_table_statement();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(341);
				refresh_statement();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(342);
				show_config_statement();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(343);
				create_database();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(344);
				drop_database();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_databaseContext extends ParserRuleContext {
		public IdContext database;
		public TerminalNode CREATE() { return getToken(VerdictSQLParser.CREATE, 0); }
		public TerminalNode DATABASE() { return getToken(VerdictSQLParser.DATABASE, 0); }
		public TerminalNode SCHEMA() { return getToken(VerdictSQLParser.SCHEMA, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Create_databaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_database; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterCreate_database(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitCreate_database(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitCreate_database(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_databaseContext create_database() throws RecognitionException {
		Create_databaseContext _localctx = new Create_databaseContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_create_database);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(347);
			match(CREATE);
			setState(348);
			_la = _input.LA(1);
			if ( !(_la==DATABASE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			{
			setState(349);
			((Create_databaseContext)_localctx).database = id();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Drop_databaseContext extends ParserRuleContext {
		public IdContext database;
		public TerminalNode DROP() { return getToken(VerdictSQLParser.DROP, 0); }
		public TerminalNode DATABASE() { return getToken(VerdictSQLParser.DATABASE, 0); }
		public TerminalNode SCHEMA() { return getToken(VerdictSQLParser.SCHEMA, 0); }
		public TerminalNode IF() { return getToken(VerdictSQLParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(VerdictSQLParser.EXISTS, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Drop_databaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_database; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDrop_database(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDrop_database(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDrop_database(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Drop_databaseContext drop_database() throws RecognitionException {
		Drop_databaseContext _localctx = new Drop_databaseContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_drop_database);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(351);
			match(DROP);
			setState(352);
			_la = _input.LA(1);
			if ( !(_la==DATABASE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(355);
			_la = _input.LA(1);
			if (_la==IF) {
				{
				setState(353);
				match(IF);
				setState(354);
				match(EXISTS);
				}
			}

			{
			setState(357);
			((Drop_databaseContext)_localctx).database = id();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Config_set_statementContext extends ParserRuleContext {
		public Config_keyContext key;
		public Config_valueContext value;
		public TerminalNode SET() { return getToken(VerdictSQLParser.SET, 0); }
		public Config_keyContext config_key() {
			return getRuleContext(Config_keyContext.class,0);
		}
		public Config_valueContext config_value() {
			return getRuleContext(Config_valueContext.class,0);
		}
		public Config_set_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_config_set_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterConfig_set_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitConfig_set_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitConfig_set_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Config_set_statementContext config_set_statement() throws RecognitionException {
		Config_set_statementContext _localctx = new Config_set_statementContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_config_set_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(359);
			match(SET);
			setState(360);
			((Config_set_statementContext)_localctx).key = config_key();
			setState(361);
			match(EQUAL);
			setState(362);
			((Config_set_statementContext)_localctx).value = config_value();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Config_get_statementContext extends ParserRuleContext {
		public Config_keyContext key;
		public TerminalNode GET() { return getToken(VerdictSQLParser.GET, 0); }
		public Config_keyContext config_key() {
			return getRuleContext(Config_keyContext.class,0);
		}
		public Config_get_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_config_get_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterConfig_get_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitConfig_get_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitConfig_get_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Config_get_statementContext config_get_statement() throws RecognitionException {
		Config_get_statementContext _localctx = new Config_get_statementContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_config_get_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(364);
			match(GET);
			setState(365);
			((Config_get_statementContext)_localctx).key = config_key();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Config_keyContext extends ParserRuleContext {
		public List<TerminalNode> ID() { return getTokens(VerdictSQLParser.ID); }
		public TerminalNode ID(int i) {
			return getToken(VerdictSQLParser.ID, i);
		}
		public List<TerminalNode> TRIALS() { return getTokens(VerdictSQLParser.TRIALS); }
		public TerminalNode TRIALS(int i) {
			return getToken(VerdictSQLParser.TRIALS, i);
		}
		public Config_keyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_config_key; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterConfig_key(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitConfig_key(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitConfig_key(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Config_keyContext config_key() throws RecognitionException {
		Config_keyContext _localctx = new Config_keyContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_config_key);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(367);
			match(ID);
			setState(372);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(368);
				match(DOT);
				setState(369);
				_la = _input.LA(1);
				if ( !(_la==TRIALS || _la==ID) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				}
				setState(374);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Config_valueContext extends ParserRuleContext {
		public TerminalNode DOUBLE_QUOTE_ID() { return getToken(VerdictSQLParser.DOUBLE_QUOTE_ID, 0); }
		public TerminalNode STRING() { return getToken(VerdictSQLParser.STRING, 0); }
		public List<TerminalNode> ID() { return getTokens(VerdictSQLParser.ID); }
		public TerminalNode ID(int i) {
			return getToken(VerdictSQLParser.ID, i);
		}
		public Config_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_config_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterConfig_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitConfig_value(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitConfig_value(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Config_valueContext config_value() throws RecognitionException {
		Config_valueContext _localctx = new Config_valueContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_config_value);
		int _la;
		try {
			setState(385);
			switch (_input.LA(1)) {
			case DOUBLE_QUOTE_ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(375);
				match(DOUBLE_QUOTE_ID);
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(376);
				match(STRING);
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 3);
				{
				setState(377);
				match(ID);
				setState(382);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(378);
					match(COMMA);
					setState(379);
					match(ID);
					}
					}
					setState(384);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Confidence_clauseContext extends ParserRuleContext {
		public Token confidence;
		public Token percent;
		public TerminalNode CONFIDENCE() { return getToken(VerdictSQLParser.CONFIDENCE, 0); }
		public TerminalNode FLOAT() { return getToken(VerdictSQLParser.FLOAT, 0); }
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public Confidence_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_confidence_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterConfidence_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitConfidence_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitConfidence_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Confidence_clauseContext confidence_clause() throws RecognitionException {
		Confidence_clauseContext _localctx = new Confidence_clauseContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_confidence_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(387);
			match(CONFIDENCE);
			setState(388);
			((Confidence_clauseContext)_localctx).confidence = _input.LT(1);
			_la = _input.LA(1);
			if ( !(_la==DECIMAL || _la==FLOAT) ) {
				((Confidence_clauseContext)_localctx).confidence = (Token)_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(390);
			_la = _input.LA(1);
			if (_la==MODULE) {
				{
				setState(389);
				((Confidence_clauseContext)_localctx).percent = match(MODULE);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Trials_clauseContext extends ParserRuleContext {
		public NumberContext trials;
		public TerminalNode TRIALS() { return getToken(VerdictSQLParser.TRIALS, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public Trials_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_trials_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTrials_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTrials_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTrials_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Trials_clauseContext trials_clause() throws RecognitionException {
		Trials_clauseContext _localctx = new Trials_clauseContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_trials_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(392);
			match(TRIALS);
			setState(393);
			((Trials_clauseContext)_localctx).trials = number();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_name_with_sampleContext extends ParserRuleContext {
		public Token size;
		public Token percent;
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode SAMPLE() { return getToken(VerdictSQLParser.SAMPLE, 0); }
		public TerminalNode FLOAT() { return getToken(VerdictSQLParser.FLOAT, 0); }
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public Table_name_with_sampleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_name_with_sample; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTable_name_with_sample(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTable_name_with_sample(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTable_name_with_sample(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_name_with_sampleContext table_name_with_sample() throws RecognitionException {
		Table_name_with_sampleContext _localctx = new Table_name_with_sampleContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_table_name_with_sample);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(395);
			table_name();
			setState(396);
			match(SAMPLE);
			setState(397);
			((Table_name_with_sampleContext)_localctx).size = _input.LT(1);
			_la = _input.LA(1);
			if ( !(_la==DECIMAL || _la==FLOAT) ) {
				((Table_name_with_sampleContext)_localctx).size = (Token)_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(399);
			_la = _input.LA(1);
			if (_la==MODULE) {
				{
				setState(398);
				((Table_name_with_sampleContext)_localctx).percent = match(MODULE);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Tsql_fileContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(VerdictSQLParser.EOF, 0); }
		public List<Sql_clauseContext> sql_clause() {
			return getRuleContexts(Sql_clauseContext.class);
		}
		public Sql_clauseContext sql_clause(int i) {
			return getRuleContext(Sql_clauseContext.class,i);
		}
		public Tsql_fileContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tsql_file; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTsql_file(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTsql_file(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTsql_file(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Tsql_fileContext tsql_file() throws RecognitionException {
		Tsql_fileContext _localctx = new Tsql_fileContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_tsql_file);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(404);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (((((_la - 13)) & ~0x3f) == 0 && ((1L << (_la - 13)) & ((1L << (SHOW - 13)) | (1L << (ALTER - 13)) | (1L << (CREATE - 13)) | (1L << (DESCRIBE - 13)) | (1L << (DROP - 13)))) != 0) || _la==USE || _la==REFRESH) {
				{
				{
				setState(401);
				sql_clause();
				}
				}
				setState(406);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(407);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Sql_clauseContext extends ParserRuleContext {
		public Ddl_clauseContext ddl_clause() {
			return getRuleContext(Ddl_clauseContext.class,0);
		}
		public Other_statementContext other_statement() {
			return getRuleContext(Other_statementContext.class,0);
		}
		public Sql_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSql_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSql_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSql_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Sql_clauseContext sql_clause() throws RecognitionException {
		Sql_clauseContext _localctx = new Sql_clauseContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_sql_clause);
		try {
			setState(411);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(409);
				ddl_clause();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(410);
				other_statement();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Ddl_clauseContext extends ParserRuleContext {
		public Create_tableContext create_table() {
			return getRuleContext(Create_tableContext.class,0);
		}
		public Create_viewContext create_view() {
			return getRuleContext(Create_viewContext.class,0);
		}
		public Alter_tableContext alter_table() {
			return getRuleContext(Alter_tableContext.class,0);
		}
		public Alter_databaseContext alter_database() {
			return getRuleContext(Alter_databaseContext.class,0);
		}
		public Drop_tableContext drop_table() {
			return getRuleContext(Drop_tableContext.class,0);
		}
		public Drop_viewContext drop_view() {
			return getRuleContext(Drop_viewContext.class,0);
		}
		public Ddl_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ddl_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDdl_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDdl_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDdl_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Ddl_clauseContext ddl_clause() throws RecognitionException {
		Ddl_clauseContext _localctx = new Ddl_clauseContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_ddl_clause);
		try {
			setState(419);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(413);
				create_table();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(414);
				create_view();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(415);
				alter_table();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(416);
				alter_database();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(417);
				drop_table();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(418);
				drop_view();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_statementContext extends ParserRuleContext {
		public Query_expressionContext query_expression() {
			return getRuleContext(Query_expressionContext.class,0);
		}
		public With_expressionContext with_expression() {
			return getRuleContext(With_expressionContext.class,0);
		}
		public TerminalNode EXACT() { return getToken(VerdictSQLParser.EXACT, 0); }
		public Order_by_clauseContext order_by_clause() {
			return getRuleContext(Order_by_clauseContext.class,0);
		}
		public Limit_clauseContext limit_clause() {
			return getRuleContext(Limit_clauseContext.class,0);
		}
		public Confidence_clauseContext confidence_clause() {
			return getRuleContext(Confidence_clauseContext.class,0);
		}
		public Select_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSelect_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSelect_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSelect_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_statementContext select_statement() throws RecognitionException {
		Select_statementContext _localctx = new Select_statementContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_select_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(422);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(421);
				with_expression();
				}
			}

			setState(425);
			_la = _input.LA(1);
			if (_la==EXACT) {
				{
				setState(424);
				match(EXACT);
				}
			}

			setState(427);
			query_expression();
			setState(429);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(428);
				order_by_clause();
				}
			}

			setState(432);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(431);
				limit_clause();
				}
			}

			setState(435);
			_la = _input.LA(1);
			if (_la==CONFIDENCE) {
				{
				setState(434);
				confidence_clause();
				}
			}

			setState(438);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
			case 1:
				{
				setState(437);
				match(SEMICOLON);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Output_clauseContext extends ParserRuleContext {
		public TerminalNode OUTPUT() { return getToken(VerdictSQLParser.OUTPUT, 0); }
		public List<Output_dml_list_elemContext> output_dml_list_elem() {
			return getRuleContexts(Output_dml_list_elemContext.class);
		}
		public Output_dml_list_elemContext output_dml_list_elem(int i) {
			return getRuleContext(Output_dml_list_elemContext.class,i);
		}
		public TerminalNode INTO() { return getToken(VerdictSQLParser.INTO, 0); }
		public TerminalNode LOCAL_ID() { return getToken(VerdictSQLParser.LOCAL_ID, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Column_name_listContext column_name_list() {
			return getRuleContext(Column_name_listContext.class,0);
		}
		public Output_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_output_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOutput_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOutput_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOutput_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Output_clauseContext output_clause() throws RecognitionException {
		Output_clauseContext _localctx = new Output_clauseContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_output_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(440);
			match(OUTPUT);
			setState(441);
			output_dml_list_elem();
			setState(446);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(442);
				match(COMMA);
				setState(443);
				output_dml_list_elem();
				}
				}
				setState(448);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(460);
			_la = _input.LA(1);
			if (_la==INTO) {
				{
				setState(449);
				match(INTO);
				setState(452);
				switch (_input.LA(1)) {
				case LOCAL_ID:
					{
					setState(450);
					match(LOCAL_ID);
					}
					break;
				case STORE:
				case FORCESEEK:
				case ABSOLUTE:
				case APPLY:
				case AUTO:
				case AVG:
				case BASE64:
				case CALLER:
				case CAST:
				case CATCH:
				case CHECKSUM_AGG:
				case COMMITTED:
				case CONCAT:
				case CONCAT_WS:
				case COOKIE:
				case COUNT:
				case COUNT_BIG:
				case DAY:
				case DAYS:
				case DELAY:
				case DELETED:
				case DENSE_RANK:
				case DISABLE:
				case DYNAMIC:
				case ENCRYPTION:
				case EXTRACT:
				case FAST:
				case FAST_FORWARD:
				case FIRST:
				case FOLLOWING:
				case FORWARD_ONLY:
				case FULLSCAN:
				case GLOBAL:
				case GO:
				case GROUPING:
				case GROUPING_ID:
				case HASH:
				case INSENSITIVE:
				case INSERTED:
				case INTERVAL:
				case ISOLATION:
				case KEEPFIXED:
				case KEYSET:
				case LAST:
				case LEVEL:
				case LOCAL:
				case LOCK_ESCALATION:
				case LOGIN:
				case LOOP:
				case MARK:
				case MAX:
				case MIN:
				case MODIFY:
				case MONTH:
				case MONTHS:
				case NEXT:
				case NAME:
				case NOCOUNT:
				case NOEXPAND:
				case NORECOMPUTE:
				case NTILE:
				case NUMBER:
				case OFFSET:
				case ONLY:
				case OPTIMISTIC:
				case OPTIMIZE:
				case OUT:
				case OUTPUT:
				case OWNER:
				case PARTITION:
				case PATH:
				case PRECEDING:
				case PRIOR:
				case RANGE:
				case RANK:
				case READONLY:
				case READ_ONLY:
				case RECOMPILE:
				case RELATIVE:
				case REMOTE:
				case REPEATABLE:
				case ROOT:
				case ROW:
				case ROWGUID:
				case ROWS:
				case ROW_NUMBER:
				case SAMPLE:
				case SCHEMABINDING:
				case SCROLL:
				case SCROLL_LOCKS:
				case SELF:
				case SERIALIZABLE:
				case SNAPSHOT:
				case SPATIAL_WINDOW_MAX_CELLS:
				case STATIC:
				case STATS_STREAM:
				case STDEV:
				case STDEVP:
				case STDDEV_SAMP:
				case SUM:
				case STRTOL:
				case THROW:
				case TIES:
				case TIME:
				case TRY:
				case TYPE:
				case TYPE_WARNING:
				case UNBOUNDED:
				case UNCOMMITTED:
				case UNKNOWN:
				case USING:
				case VAR:
				case VARP:
				case VIEW_METADATA:
				case WORK:
				case XML:
				case XMLNAMESPACES:
				case YEAR:
				case YEARS:
				case DOUBLE_QUOTE_ID:
				case BACKTICK_ID:
				case SQUARE_BRACKET_ID:
				case ID:
					{
					setState(451);
					table_name();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(458);
				_la = _input.LA(1);
				if (_la==LR_BRACKET) {
					{
					setState(454);
					match(LR_BRACKET);
					setState(455);
					column_name_list();
					setState(456);
					match(RR_BRACKET);
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Output_dml_list_elemContext extends ParserRuleContext {
		public Output_column_nameContext output_column_name() {
			return getRuleContext(Output_column_nameContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Column_aliasContext column_alias() {
			return getRuleContext(Column_aliasContext.class,0);
		}
		public TerminalNode AS() { return getToken(VerdictSQLParser.AS, 0); }
		public Output_dml_list_elemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_output_dml_list_elem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOutput_dml_list_elem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOutput_dml_list_elem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOutput_dml_list_elem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Output_dml_list_elemContext output_dml_list_elem() throws RecognitionException {
		Output_dml_list_elemContext _localctx = new Output_dml_list_elemContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_output_dml_list_elem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(464);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
			case 1:
				{
				setState(462);
				output_column_name();
				}
				break;
			case 2:
				{
				setState(463);
				expression(0);
				}
				break;
			}
			setState(470);
			_la = _input.LA(1);
			if (_la==STORE || _la==AS || _la==FORCESEEK || ((((_la - 215)) & ~0x3f) == 0 && ((1L << (_la - 215)) & ((1L << (ABSOLUTE - 215)) | (1L << (APPLY - 215)) | (1L << (AUTO - 215)) | (1L << (AVG - 215)) | (1L << (BASE64 - 215)) | (1L << (CALLER - 215)) | (1L << (CAST - 215)) | (1L << (CATCH - 215)) | (1L << (CHECKSUM_AGG - 215)) | (1L << (COMMITTED - 215)) | (1L << (CONCAT - 215)) | (1L << (CONCAT_WS - 215)) | (1L << (COOKIE - 215)) | (1L << (COUNT - 215)) | (1L << (COUNT_BIG - 215)) | (1L << (DAY - 215)) | (1L << (DAYS - 215)) | (1L << (DELAY - 215)) | (1L << (DELETED - 215)) | (1L << (DENSE_RANK - 215)) | (1L << (DISABLE - 215)) | (1L << (DYNAMIC - 215)) | (1L << (ENCRYPTION - 215)) | (1L << (EXTRACT - 215)) | (1L << (FAST - 215)) | (1L << (FAST_FORWARD - 215)) | (1L << (FIRST - 215)) | (1L << (FOLLOWING - 215)) | (1L << (FORWARD_ONLY - 215)))) != 0) || ((((_la - 280)) & ~0x3f) == 0 && ((1L << (_la - 280)) & ((1L << (FULLSCAN - 280)) | (1L << (GLOBAL - 280)) | (1L << (GO - 280)) | (1L << (GROUPING - 280)) | (1L << (GROUPING_ID - 280)) | (1L << (HASH - 280)) | (1L << (INSENSITIVE - 280)) | (1L << (INSERTED - 280)) | (1L << (INTERVAL - 280)) | (1L << (ISOLATION - 280)) | (1L << (KEEPFIXED - 280)) | (1L << (KEYSET - 280)) | (1L << (LAST - 280)) | (1L << (LEVEL - 280)) | (1L << (LOCAL - 280)) | (1L << (LOCK_ESCALATION - 280)) | (1L << (LOGIN - 280)) | (1L << (LOOP - 280)) | (1L << (MARK - 280)) | (1L << (MAX - 280)) | (1L << (MIN - 280)) | (1L << (MODIFY - 280)) | (1L << (MONTH - 280)) | (1L << (MONTHS - 280)) | (1L << (NEXT - 280)) | (1L << (NAME - 280)) | (1L << (NOCOUNT - 280)) | (1L << (NOEXPAND - 280)) | (1L << (NORECOMPUTE - 280)) | (1L << (NTILE - 280)) | (1L << (NUMBER - 280)) | (1L << (OFFSET - 280)) | (1L << (ONLY - 280)) | (1L << (OPTIMISTIC - 280)) | (1L << (OPTIMIZE - 280)) | (1L << (OUT - 280)) | (1L << (OUTPUT - 280)) | (1L << (OWNER - 280)) | (1L << (PARTITION - 280)) | (1L << (PATH - 280)))) != 0) || ((((_la - 346)) & ~0x3f) == 0 && ((1L << (_la - 346)) & ((1L << (PRECEDING - 346)) | (1L << (PRIOR - 346)) | (1L << (RANGE - 346)) | (1L << (RANK - 346)) | (1L << (READONLY - 346)) | (1L << (READ_ONLY - 346)) | (1L << (RECOMPILE - 346)) | (1L << (RELATIVE - 346)) | (1L << (REMOTE - 346)) | (1L << (REPEATABLE - 346)) | (1L << (ROOT - 346)) | (1L << (ROW - 346)) | (1L << (ROWGUID - 346)) | (1L << (ROWS - 346)) | (1L << (ROW_NUMBER - 346)) | (1L << (SAMPLE - 346)) | (1L << (SCHEMABINDING - 346)) | (1L << (SCROLL - 346)) | (1L << (SCROLL_LOCKS - 346)) | (1L << (SELF - 346)) | (1L << (SERIALIZABLE - 346)) | (1L << (SNAPSHOT - 346)) | (1L << (SPATIAL_WINDOW_MAX_CELLS - 346)) | (1L << (STATIC - 346)) | (1L << (STATS_STREAM - 346)) | (1L << (STDEV - 346)) | (1L << (STDEVP - 346)) | (1L << (STDDEV_SAMP - 346)) | (1L << (SUM - 346)) | (1L << (STRTOL - 346)) | (1L << (THROW - 346)) | (1L << (TIES - 346)) | (1L << (TIME - 346)) | (1L << (TRY - 346)) | (1L << (TYPE - 346)) | (1L << (TYPE_WARNING - 346)) | (1L << (UNBOUNDED - 346)))) != 0) || ((((_la - 410)) & ~0x3f) == 0 && ((1L << (_la - 410)) & ((1L << (UNCOMMITTED - 410)) | (1L << (UNKNOWN - 410)) | (1L << (USING - 410)) | (1L << (VAR - 410)) | (1L << (VARP - 410)) | (1L << (VIEW_METADATA - 410)) | (1L << (WORK - 410)) | (1L << (XML - 410)) | (1L << (XMLNAMESPACES - 410)) | (1L << (YEAR - 410)) | (1L << (YEARS - 410)) | (1L << (DOUBLE_QUOTE_ID - 410)) | (1L << (BACKTICK_ID - 410)) | (1L << (SQUARE_BRACKET_ID - 410)) | (1L << (ID - 410)) | (1L << (STRING - 410)))) != 0)) {
				{
				setState(467);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(466);
					match(AS);
					}
				}

				setState(469);
				column_alias();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Output_column_nameContext extends ParserRuleContext {
		public TerminalNode DELETED() { return getToken(VerdictSQLParser.DELETED, 0); }
		public TerminalNode INSERTED() { return getToken(VerdictSQLParser.INSERTED, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Column_nameContext column_name() {
			return getRuleContext(Column_nameContext.class,0);
		}
		public TerminalNode DOLLAR_ACTION() { return getToken(VerdictSQLParser.DOLLAR_ACTION, 0); }
		public Output_column_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_output_column_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOutput_column_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOutput_column_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOutput_column_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Output_column_nameContext output_column_name() throws RecognitionException {
		Output_column_nameContext _localctx = new Output_column_nameContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_output_column_name);
		try {
			setState(483);
			switch (_input.LA(1)) {
			case STORE:
			case FORCESEEK:
			case ABSOLUTE:
			case APPLY:
			case AUTO:
			case AVG:
			case BASE64:
			case CALLER:
			case CAST:
			case CATCH:
			case CHECKSUM_AGG:
			case COMMITTED:
			case CONCAT:
			case CONCAT_WS:
			case COOKIE:
			case COUNT:
			case COUNT_BIG:
			case DAY:
			case DAYS:
			case DELAY:
			case DELETED:
			case DENSE_RANK:
			case DISABLE:
			case DYNAMIC:
			case ENCRYPTION:
			case EXTRACT:
			case FAST:
			case FAST_FORWARD:
			case FIRST:
			case FOLLOWING:
			case FORWARD_ONLY:
			case FULLSCAN:
			case GLOBAL:
			case GO:
			case GROUPING:
			case GROUPING_ID:
			case HASH:
			case INSENSITIVE:
			case INSERTED:
			case INTERVAL:
			case ISOLATION:
			case KEEPFIXED:
			case KEYSET:
			case LAST:
			case LEVEL:
			case LOCAL:
			case LOCK_ESCALATION:
			case LOGIN:
			case LOOP:
			case MARK:
			case MAX:
			case MIN:
			case MODIFY:
			case MONTH:
			case MONTHS:
			case NEXT:
			case NAME:
			case NOCOUNT:
			case NOEXPAND:
			case NORECOMPUTE:
			case NTILE:
			case NUMBER:
			case OFFSET:
			case ONLY:
			case OPTIMISTIC:
			case OPTIMIZE:
			case OUT:
			case OUTPUT:
			case OWNER:
			case PARTITION:
			case PATH:
			case PRECEDING:
			case PRIOR:
			case RANGE:
			case RANK:
			case READONLY:
			case READ_ONLY:
			case RECOMPILE:
			case RELATIVE:
			case REMOTE:
			case REPEATABLE:
			case ROOT:
			case ROW:
			case ROWGUID:
			case ROWS:
			case ROW_NUMBER:
			case SAMPLE:
			case SCHEMABINDING:
			case SCROLL:
			case SCROLL_LOCKS:
			case SELF:
			case SERIALIZABLE:
			case SNAPSHOT:
			case SPATIAL_WINDOW_MAX_CELLS:
			case STATIC:
			case STATS_STREAM:
			case STDEV:
			case STDEVP:
			case STDDEV_SAMP:
			case SUM:
			case STRTOL:
			case THROW:
			case TIES:
			case TIME:
			case TRY:
			case TYPE:
			case TYPE_WARNING:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNKNOWN:
			case USING:
			case VAR:
			case VARP:
			case VIEW_METADATA:
			case WORK:
			case XML:
			case XMLNAMESPACES:
			case YEAR:
			case YEARS:
			case DOUBLE_QUOTE_ID:
			case BACKTICK_ID:
			case SQUARE_BRACKET_ID:
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(475);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
				case 1:
					{
					setState(472);
					match(DELETED);
					}
					break;
				case 2:
					{
					setState(473);
					match(INSERTED);
					}
					break;
				case 3:
					{
					setState(474);
					table_name();
					}
					break;
				}
				setState(477);
				match(DOT);
				setState(480);
				switch (_input.LA(1)) {
				case STAR:
					{
					setState(478);
					match(STAR);
					}
					break;
				case STORE:
				case FORCESEEK:
				case ABSOLUTE:
				case APPLY:
				case AUTO:
				case AVG:
				case BASE64:
				case CALLER:
				case CAST:
				case CATCH:
				case CHECKSUM_AGG:
				case COMMITTED:
				case CONCAT:
				case CONCAT_WS:
				case COOKIE:
				case COUNT:
				case COUNT_BIG:
				case DAY:
				case DAYS:
				case DELAY:
				case DELETED:
				case DENSE_RANK:
				case DISABLE:
				case DYNAMIC:
				case ENCRYPTION:
				case EXTRACT:
				case FAST:
				case FAST_FORWARD:
				case FIRST:
				case FOLLOWING:
				case FORWARD_ONLY:
				case FULLSCAN:
				case GLOBAL:
				case GO:
				case GROUPING:
				case GROUPING_ID:
				case HASH:
				case INSENSITIVE:
				case INSERTED:
				case INTERVAL:
				case ISOLATION:
				case KEEPFIXED:
				case KEYSET:
				case LAST:
				case LEVEL:
				case LOCAL:
				case LOCK_ESCALATION:
				case LOGIN:
				case LOOP:
				case MARK:
				case MAX:
				case MIN:
				case MODIFY:
				case MONTH:
				case MONTHS:
				case NEXT:
				case NAME:
				case NOCOUNT:
				case NOEXPAND:
				case NORECOMPUTE:
				case NTILE:
				case NUMBER:
				case OFFSET:
				case ONLY:
				case OPTIMISTIC:
				case OPTIMIZE:
				case OUT:
				case OUTPUT:
				case OWNER:
				case PARTITION:
				case PATH:
				case PRECEDING:
				case PRIOR:
				case RANGE:
				case RANK:
				case READONLY:
				case READ_ONLY:
				case RECOMPILE:
				case RELATIVE:
				case REMOTE:
				case REPEATABLE:
				case ROOT:
				case ROW:
				case ROWGUID:
				case ROWS:
				case ROW_NUMBER:
				case SAMPLE:
				case SCHEMABINDING:
				case SCROLL:
				case SCROLL_LOCKS:
				case SELF:
				case SERIALIZABLE:
				case SNAPSHOT:
				case SPATIAL_WINDOW_MAX_CELLS:
				case STATIC:
				case STATS_STREAM:
				case STDEV:
				case STDEVP:
				case STDDEV_SAMP:
				case SUM:
				case STRTOL:
				case THROW:
				case TIES:
				case TIME:
				case TRY:
				case TYPE:
				case TYPE_WARNING:
				case UNBOUNDED:
				case UNCOMMITTED:
				case UNKNOWN:
				case USING:
				case VAR:
				case VARP:
				case VIEW_METADATA:
				case WORK:
				case XML:
				case XMLNAMESPACES:
				case YEAR:
				case YEARS:
				case DOUBLE_QUOTE_ID:
				case BACKTICK_ID:
				case SQUARE_BRACKET_ID:
				case ID:
					{
					setState(479);
					column_name();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case DOLLAR_ACTION:
				enterOuterAlt(_localctx, 2);
				{
				setState(482);
				match(DOLLAR_ACTION);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_tableContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(VerdictSQLParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(VerdictSQLParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public List<Column_def_table_constraintContext> column_def_table_constraint() {
			return getRuleContexts(Column_def_table_constraintContext.class);
		}
		public Column_def_table_constraintContext column_def_table_constraint(int i) {
			return getRuleContext(Column_def_table_constraintContext.class,i);
		}
		public Create_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterCreate_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitCreate_table(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitCreate_table(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_tableContext create_table() throws RecognitionException {
		Create_tableContext _localctx = new Create_tableContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_create_table);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(485);
			match(CREATE);
			setState(486);
			match(TABLE);
			setState(487);
			table_name();
			setState(488);
			match(LR_BRACKET);
			setState(489);
			column_def_table_constraint();
			setState(496);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,38,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(491);
					_la = _input.LA(1);
					if (_la==COMMA) {
						{
						setState(490);
						match(COMMA);
						}
					}

					setState(493);
					column_def_table_constraint();
					}
					} 
				}
				setState(498);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,38,_ctx);
			}
			setState(500);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(499);
				match(COMMA);
				}
			}

			setState(502);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_table_as_selectContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(VerdictSQLParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(VerdictSQLParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode AS() { return getToken(VerdictSQLParser.AS, 0); }
		public Select_statementContext select_statement() {
			return getRuleContext(Select_statementContext.class,0);
		}
		public TerminalNode IF() { return getToken(VerdictSQLParser.IF, 0); }
		public TerminalNode NOT() { return getToken(VerdictSQLParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(VerdictSQLParser.EXISTS, 0); }
		public TerminalNode STORED_AS_PARQUET() { return getToken(VerdictSQLParser.STORED_AS_PARQUET, 0); }
		public Create_table_as_selectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_table_as_select; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterCreate_table_as_select(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitCreate_table_as_select(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitCreate_table_as_select(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_table_as_selectContext create_table_as_select() throws RecognitionException {
		Create_table_as_selectContext _localctx = new Create_table_as_selectContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_create_table_as_select);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(504);
			match(CREATE);
			setState(505);
			match(TABLE);
			setState(509);
			_la = _input.LA(1);
			if (_la==IF) {
				{
				setState(506);
				match(IF);
				setState(507);
				match(NOT);
				setState(508);
				match(EXISTS);
				}
			}

			setState(511);
			table_name();
			setState(513);
			_la = _input.LA(1);
			if (_la==STORED_AS_PARQUET) {
				{
				setState(512);
				match(STORED_AS_PARQUET);
				}
			}

			setState(515);
			match(AS);
			setState(516);
			select_statement();
			setState(518);
			_la = _input.LA(1);
			if (_la==SEMICOLON) {
				{
				setState(517);
				match(SEMICOLON);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_viewContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(VerdictSQLParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(VerdictSQLParser.VIEW, 0); }
		public View_nameContext view_name() {
			return getRuleContext(View_nameContext.class,0);
		}
		public TerminalNode AS() { return getToken(VerdictSQLParser.AS, 0); }
		public Select_statementContext select_statement() {
			return getRuleContext(Select_statementContext.class,0);
		}
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public TerminalNode WITH() { return getToken(VerdictSQLParser.WITH, 0); }
		public TerminalNode CHECK() { return getToken(VerdictSQLParser.CHECK, 0); }
		public TerminalNode OPTION() { return getToken(VerdictSQLParser.OPTION, 0); }
		public Create_viewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_view; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterCreate_view(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitCreate_view(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitCreate_view(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_viewContext create_view() throws RecognitionException {
		Create_viewContext _localctx = new Create_viewContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_create_view);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(520);
			match(CREATE);
			setState(521);
			match(VIEW);
			setState(522);
			view_name();
			setState(534);
			_la = _input.LA(1);
			if (_la==LR_BRACKET) {
				{
				setState(523);
				match(LR_BRACKET);
				setState(524);
				column_name();
				setState(529);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(525);
					match(COMMA);
					setState(526);
					column_name();
					}
					}
					setState(531);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(532);
				match(RR_BRACKET);
				}
			}

			setState(536);
			match(AS);
			setState(537);
			select_statement();
			setState(541);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(538);
				match(WITH);
				setState(539);
				match(CHECK);
				setState(540);
				match(OPTION);
				}
			}

			setState(544);
			_la = _input.LA(1);
			if (_la==SEMICOLON) {
				{
				setState(543);
				match(SEMICOLON);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Alter_tableContext extends ParserRuleContext {
		public TerminalNode ALTER() { return getToken(VerdictSQLParser.ALTER, 0); }
		public List<TerminalNode> TABLE() { return getTokens(VerdictSQLParser.TABLE); }
		public TerminalNode TABLE(int i) {
			return getToken(VerdictSQLParser.TABLE, i);
		}
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode SET() { return getToken(VerdictSQLParser.SET, 0); }
		public TerminalNode LOCK_ESCALATION() { return getToken(VerdictSQLParser.LOCK_ESCALATION, 0); }
		public TerminalNode AUTO() { return getToken(VerdictSQLParser.AUTO, 0); }
		public TerminalNode DISABLE() { return getToken(VerdictSQLParser.DISABLE, 0); }
		public TerminalNode ADD() { return getToken(VerdictSQLParser.ADD, 0); }
		public Column_def_table_constraintContext column_def_table_constraint() {
			return getRuleContext(Column_def_table_constraintContext.class,0);
		}
		public Alter_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterAlter_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitAlter_table(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitAlter_table(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Alter_tableContext alter_table() throws RecognitionException {
		Alter_tableContext _localctx = new Alter_tableContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_alter_table);
		int _la;
		try {
			setState(566);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(546);
				match(ALTER);
				setState(547);
				match(TABLE);
				setState(548);
				table_name();
				setState(549);
				match(SET);
				setState(550);
				match(LR_BRACKET);
				setState(551);
				match(LOCK_ESCALATION);
				setState(552);
				match(EQUAL);
				setState(553);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==AUTO || _la==DISABLE) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(554);
				match(RR_BRACKET);
				setState(556);
				_la = _input.LA(1);
				if (_la==SEMICOLON) {
					{
					setState(555);
					match(SEMICOLON);
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(558);
				match(ALTER);
				setState(559);
				match(TABLE);
				setState(560);
				table_name();
				setState(561);
				match(ADD);
				setState(562);
				column_def_table_constraint();
				setState(564);
				_la = _input.LA(1);
				if (_la==SEMICOLON) {
					{
					setState(563);
					match(SEMICOLON);
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Alter_databaseContext extends ParserRuleContext {
		public IdContext database;
		public IdContext new_name;
		public TerminalNode ALTER() { return getToken(VerdictSQLParser.ALTER, 0); }
		public TerminalNode DATABASE() { return getToken(VerdictSQLParser.DATABASE, 0); }
		public TerminalNode MODIFY() { return getToken(VerdictSQLParser.MODIFY, 0); }
		public TerminalNode NAME() { return getToken(VerdictSQLParser.NAME, 0); }
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public TerminalNode CURRENT() { return getToken(VerdictSQLParser.CURRENT, 0); }
		public Alter_databaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_database; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterAlter_database(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitAlter_database(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitAlter_database(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Alter_databaseContext alter_database() throws RecognitionException {
		Alter_databaseContext _localctx = new Alter_databaseContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_alter_database);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(568);
			match(ALTER);
			setState(569);
			match(DATABASE);
			setState(572);
			switch (_input.LA(1)) {
			case STORE:
			case FORCESEEK:
			case ABSOLUTE:
			case APPLY:
			case AUTO:
			case AVG:
			case BASE64:
			case CALLER:
			case CAST:
			case CATCH:
			case CHECKSUM_AGG:
			case COMMITTED:
			case CONCAT:
			case CONCAT_WS:
			case COOKIE:
			case COUNT:
			case COUNT_BIG:
			case DAY:
			case DAYS:
			case DELAY:
			case DELETED:
			case DENSE_RANK:
			case DISABLE:
			case DYNAMIC:
			case ENCRYPTION:
			case EXTRACT:
			case FAST:
			case FAST_FORWARD:
			case FIRST:
			case FOLLOWING:
			case FORWARD_ONLY:
			case FULLSCAN:
			case GLOBAL:
			case GO:
			case GROUPING:
			case GROUPING_ID:
			case HASH:
			case INSENSITIVE:
			case INSERTED:
			case INTERVAL:
			case ISOLATION:
			case KEEPFIXED:
			case KEYSET:
			case LAST:
			case LEVEL:
			case LOCAL:
			case LOCK_ESCALATION:
			case LOGIN:
			case LOOP:
			case MARK:
			case MAX:
			case MIN:
			case MODIFY:
			case MONTH:
			case MONTHS:
			case NEXT:
			case NAME:
			case NOCOUNT:
			case NOEXPAND:
			case NORECOMPUTE:
			case NTILE:
			case NUMBER:
			case OFFSET:
			case ONLY:
			case OPTIMISTIC:
			case OPTIMIZE:
			case OUT:
			case OUTPUT:
			case OWNER:
			case PARTITION:
			case PATH:
			case PRECEDING:
			case PRIOR:
			case RANGE:
			case RANK:
			case READONLY:
			case READ_ONLY:
			case RECOMPILE:
			case RELATIVE:
			case REMOTE:
			case REPEATABLE:
			case ROOT:
			case ROW:
			case ROWGUID:
			case ROWS:
			case ROW_NUMBER:
			case SAMPLE:
			case SCHEMABINDING:
			case SCROLL:
			case SCROLL_LOCKS:
			case SELF:
			case SERIALIZABLE:
			case SNAPSHOT:
			case SPATIAL_WINDOW_MAX_CELLS:
			case STATIC:
			case STATS_STREAM:
			case STDEV:
			case STDEVP:
			case STDDEV_SAMP:
			case SUM:
			case STRTOL:
			case THROW:
			case TIES:
			case TIME:
			case TRY:
			case TYPE:
			case TYPE_WARNING:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNKNOWN:
			case USING:
			case VAR:
			case VARP:
			case VIEW_METADATA:
			case WORK:
			case XML:
			case XMLNAMESPACES:
			case YEAR:
			case YEARS:
			case DOUBLE_QUOTE_ID:
			case BACKTICK_ID:
			case SQUARE_BRACKET_ID:
			case ID:
				{
				setState(570);
				((Alter_databaseContext)_localctx).database = id();
				}
				break;
			case CURRENT:
				{
				setState(571);
				match(CURRENT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(574);
			match(MODIFY);
			setState(575);
			match(NAME);
			setState(576);
			match(EQUAL);
			setState(577);
			((Alter_databaseContext)_localctx).new_name = id();
			setState(579);
			_la = _input.LA(1);
			if (_la==SEMICOLON) {
				{
				setState(578);
				match(SEMICOLON);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Drop_tableContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(VerdictSQLParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(VerdictSQLParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode IF() { return getToken(VerdictSQLParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(VerdictSQLParser.EXISTS, 0); }
		public Drop_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDrop_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDrop_table(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDrop_table(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Drop_tableContext drop_table() throws RecognitionException {
		Drop_tableContext _localctx = new Drop_tableContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_drop_table);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(581);
			match(DROP);
			setState(582);
			match(TABLE);
			setState(585);
			_la = _input.LA(1);
			if (_la==IF) {
				{
				setState(583);
				match(IF);
				setState(584);
				match(EXISTS);
				}
			}

			setState(587);
			table_name();
			setState(589);
			_la = _input.LA(1);
			if (_la==SEMICOLON) {
				{
				setState(588);
				match(SEMICOLON);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Drop_viewContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(VerdictSQLParser.DROP, 0); }
		public TerminalNode VIEW() { return getToken(VerdictSQLParser.VIEW, 0); }
		public List<View_nameContext> view_name() {
			return getRuleContexts(View_nameContext.class);
		}
		public View_nameContext view_name(int i) {
			return getRuleContext(View_nameContext.class,i);
		}
		public TerminalNode IF() { return getToken(VerdictSQLParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(VerdictSQLParser.EXISTS, 0); }
		public Drop_viewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_view; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDrop_view(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDrop_view(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDrop_view(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Drop_viewContext drop_view() throws RecognitionException {
		Drop_viewContext _localctx = new Drop_viewContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_drop_view);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(591);
			match(DROP);
			setState(592);
			match(VIEW);
			setState(595);
			_la = _input.LA(1);
			if (_la==IF) {
				{
				setState(593);
				match(IF);
				setState(594);
				match(EXISTS);
				}
			}

			setState(597);
			view_name();
			setState(602);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(598);
				match(COMMA);
				setState(599);
				view_name();
				}
				}
				setState(604);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(606);
			_la = _input.LA(1);
			if (_la==SEMICOLON) {
				{
				setState(605);
				match(SEMICOLON);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Set_statmentContext extends ParserRuleContext {
		public IdContext member_name;
		public TerminalNode SET() { return getToken(VerdictSQLParser.SET, 0); }
		public TerminalNode LOCAL_ID() { return getToken(VerdictSQLParser.LOCAL_ID, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Assignment_operatorContext assignment_operator() {
			return getRuleContext(Assignment_operatorContext.class,0);
		}
		public Set_specialContext set_special() {
			return getRuleContext(Set_specialContext.class,0);
		}
		public Set_statmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_set_statment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSet_statment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSet_statment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSet_statment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Set_statmentContext set_statment() throws RecognitionException {
		Set_statmentContext _localctx = new Set_statmentContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_set_statment);
		int _la;
		try {
			setState(627);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,60,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(608);
				match(SET);
				setState(609);
				match(LOCAL_ID);
				setState(612);
				_la = _input.LA(1);
				if (_la==DOT) {
					{
					setState(610);
					match(DOT);
					setState(611);
					((Set_statmentContext)_localctx).member_name = id();
					}
				}

				setState(614);
				match(EQUAL);
				setState(615);
				expression(0);
				setState(617);
				_la = _input.LA(1);
				if (_la==SEMICOLON) {
					{
					setState(616);
					match(SEMICOLON);
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(619);
				match(SET);
				setState(620);
				match(LOCAL_ID);
				setState(621);
				assignment_operator();
				setState(622);
				expression(0);
				setState(624);
				_la = _input.LA(1);
				if (_la==SEMICOLON) {
					{
					setState(623);
					match(SEMICOLON);
					}
				}

				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(626);
				set_special();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Use_statementContext extends ParserRuleContext {
		public IdContext database;
		public TerminalNode USE() { return getToken(VerdictSQLParser.USE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Use_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_use_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterUse_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitUse_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitUse_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Use_statementContext use_statement() throws RecognitionException {
		Use_statementContext _localctx = new Use_statementContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_use_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(629);
			match(USE);
			setState(630);
			((Use_statementContext)_localctx).database = id();
			setState(632);
			_la = _input.LA(1);
			if (_la==SEMICOLON) {
				{
				setState(631);
				match(SEMICOLON);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Show_tables_statementContext extends ParserRuleContext {
		public IdContext schema;
		public TerminalNode SHOW() { return getToken(VerdictSQLParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(VerdictSQLParser.TABLES, 0); }
		public TerminalNode IN() { return getToken(VerdictSQLParser.IN, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Show_tables_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_show_tables_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterShow_tables_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitShow_tables_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitShow_tables_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Show_tables_statementContext show_tables_statement() throws RecognitionException {
		Show_tables_statementContext _localctx = new Show_tables_statementContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_show_tables_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(634);
			match(SHOW);
			setState(635);
			match(TABLES);
			setState(638);
			_la = _input.LA(1);
			if (_la==IN) {
				{
				setState(636);
				match(IN);
				setState(637);
				((Show_tables_statementContext)_localctx).schema = id();
				}
			}

			setState(641);
			_la = _input.LA(1);
			if (_la==SEMICOLON) {
				{
				setState(640);
				match(SEMICOLON);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Show_databases_statementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(VerdictSQLParser.SHOW, 0); }
		public TerminalNode DATABASES() { return getToken(VerdictSQLParser.DATABASES, 0); }
		public TerminalNode SCHEMAS() { return getToken(VerdictSQLParser.SCHEMAS, 0); }
		public Show_databases_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_show_databases_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterShow_databases_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitShow_databases_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitShow_databases_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Show_databases_statementContext show_databases_statement() throws RecognitionException {
		Show_databases_statementContext _localctx = new Show_databases_statementContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_show_databases_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(643);
			match(SHOW);
			setState(644);
			_la = _input.LA(1);
			if ( !(_la==DATABASES || _la==SCHEMAS) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Describe_table_statementContext extends ParserRuleContext {
		public Table_nameContext table;
		public TerminalNode DESCRIBE() { return getToken(VerdictSQLParser.DESCRIBE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Describe_table_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describe_table_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDescribe_table_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDescribe_table_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDescribe_table_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Describe_table_statementContext describe_table_statement() throws RecognitionException {
		Describe_table_statementContext _localctx = new Describe_table_statementContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_describe_table_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(646);
			match(DESCRIBE);
			setState(647);
			((Describe_table_statementContext)_localctx).table = table_name();
			setState(649);
			_la = _input.LA(1);
			if (_la==SEMICOLON) {
				{
				setState(648);
				match(SEMICOLON);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Refresh_statementContext extends ParserRuleContext {
		public IdContext schema;
		public TerminalNode REFRESH() { return getToken(VerdictSQLParser.REFRESH, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Refresh_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_refresh_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterRefresh_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitRefresh_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitRefresh_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Refresh_statementContext refresh_statement() throws RecognitionException {
		Refresh_statementContext _localctx = new Refresh_statementContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_refresh_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(651);
			match(REFRESH);
			setState(653);
			_la = _input.LA(1);
			if (_la==STORE || _la==FORCESEEK || ((((_la - 215)) & ~0x3f) == 0 && ((1L << (_la - 215)) & ((1L << (ABSOLUTE - 215)) | (1L << (APPLY - 215)) | (1L << (AUTO - 215)) | (1L << (AVG - 215)) | (1L << (BASE64 - 215)) | (1L << (CALLER - 215)) | (1L << (CAST - 215)) | (1L << (CATCH - 215)) | (1L << (CHECKSUM_AGG - 215)) | (1L << (COMMITTED - 215)) | (1L << (CONCAT - 215)) | (1L << (CONCAT_WS - 215)) | (1L << (COOKIE - 215)) | (1L << (COUNT - 215)) | (1L << (COUNT_BIG - 215)) | (1L << (DAY - 215)) | (1L << (DAYS - 215)) | (1L << (DELAY - 215)) | (1L << (DELETED - 215)) | (1L << (DENSE_RANK - 215)) | (1L << (DISABLE - 215)) | (1L << (DYNAMIC - 215)) | (1L << (ENCRYPTION - 215)) | (1L << (EXTRACT - 215)) | (1L << (FAST - 215)) | (1L << (FAST_FORWARD - 215)) | (1L << (FIRST - 215)) | (1L << (FOLLOWING - 215)) | (1L << (FORWARD_ONLY - 215)))) != 0) || ((((_la - 280)) & ~0x3f) == 0 && ((1L << (_la - 280)) & ((1L << (FULLSCAN - 280)) | (1L << (GLOBAL - 280)) | (1L << (GO - 280)) | (1L << (GROUPING - 280)) | (1L << (GROUPING_ID - 280)) | (1L << (HASH - 280)) | (1L << (INSENSITIVE - 280)) | (1L << (INSERTED - 280)) | (1L << (INTERVAL - 280)) | (1L << (ISOLATION - 280)) | (1L << (KEEPFIXED - 280)) | (1L << (KEYSET - 280)) | (1L << (LAST - 280)) | (1L << (LEVEL - 280)) | (1L << (LOCAL - 280)) | (1L << (LOCK_ESCALATION - 280)) | (1L << (LOGIN - 280)) | (1L << (LOOP - 280)) | (1L << (MARK - 280)) | (1L << (MAX - 280)) | (1L << (MIN - 280)) | (1L << (MODIFY - 280)) | (1L << (MONTH - 280)) | (1L << (MONTHS - 280)) | (1L << (NEXT - 280)) | (1L << (NAME - 280)) | (1L << (NOCOUNT - 280)) | (1L << (NOEXPAND - 280)) | (1L << (NORECOMPUTE - 280)) | (1L << (NTILE - 280)) | (1L << (NUMBER - 280)) | (1L << (OFFSET - 280)) | (1L << (ONLY - 280)) | (1L << (OPTIMISTIC - 280)) | (1L << (OPTIMIZE - 280)) | (1L << (OUT - 280)) | (1L << (OUTPUT - 280)) | (1L << (OWNER - 280)) | (1L << (PARTITION - 280)) | (1L << (PATH - 280)))) != 0) || ((((_la - 346)) & ~0x3f) == 0 && ((1L << (_la - 346)) & ((1L << (PRECEDING - 346)) | (1L << (PRIOR - 346)) | (1L << (RANGE - 346)) | (1L << (RANK - 346)) | (1L << (READONLY - 346)) | (1L << (READ_ONLY - 346)) | (1L << (RECOMPILE - 346)) | (1L << (RELATIVE - 346)) | (1L << (REMOTE - 346)) | (1L << (REPEATABLE - 346)) | (1L << (ROOT - 346)) | (1L << (ROW - 346)) | (1L << (ROWGUID - 346)) | (1L << (ROWS - 346)) | (1L << (ROW_NUMBER - 346)) | (1L << (SAMPLE - 346)) | (1L << (SCHEMABINDING - 346)) | (1L << (SCROLL - 346)) | (1L << (SCROLL_LOCKS - 346)) | (1L << (SELF - 346)) | (1L << (SERIALIZABLE - 346)) | (1L << (SNAPSHOT - 346)) | (1L << (SPATIAL_WINDOW_MAX_CELLS - 346)) | (1L << (STATIC - 346)) | (1L << (STATS_STREAM - 346)) | (1L << (STDEV - 346)) | (1L << (STDEVP - 346)) | (1L << (STDDEV_SAMP - 346)) | (1L << (SUM - 346)) | (1L << (STRTOL - 346)) | (1L << (THROW - 346)) | (1L << (TIES - 346)) | (1L << (TIME - 346)) | (1L << (TRY - 346)) | (1L << (TYPE - 346)) | (1L << (TYPE_WARNING - 346)) | (1L << (UNBOUNDED - 346)))) != 0) || ((((_la - 410)) & ~0x3f) == 0 && ((1L << (_la - 410)) & ((1L << (UNCOMMITTED - 410)) | (1L << (UNKNOWN - 410)) | (1L << (USING - 410)) | (1L << (VAR - 410)) | (1L << (VARP - 410)) | (1L << (VIEW_METADATA - 410)) | (1L << (WORK - 410)) | (1L << (XML - 410)) | (1L << (XMLNAMESPACES - 410)) | (1L << (YEAR - 410)) | (1L << (YEARS - 410)) | (1L << (DOUBLE_QUOTE_ID - 410)) | (1L << (BACKTICK_ID - 410)) | (1L << (SQUARE_BRACKET_ID - 410)) | (1L << (ID - 410)))) != 0)) {
				{
				setState(652);
				((Refresh_statementContext)_localctx).schema = id();
				}
			}

			setState(656);
			_la = _input.LA(1);
			if (_la==SEMICOLON) {
				{
				setState(655);
				match(SEMICOLON);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Show_config_statementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(VerdictSQLParser.SHOW, 0); }
		public TerminalNode CONFIG() { return getToken(VerdictSQLParser.CONFIG, 0); }
		public Show_config_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_show_config_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterShow_config_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitShow_config_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitShow_config_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Show_config_statementContext show_config_statement() throws RecognitionException {
		Show_config_statementContext _localctx = new Show_config_statementContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_show_config_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(658);
			match(SHOW);
			setState(659);
			match(CONFIG);
			setState(661);
			_la = _input.LA(1);
			if (_la==SEMICOLON) {
				{
				setState(660);
				match(SEMICOLON);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_type_definitionContext extends ParserRuleContext {
		public TerminalNode TABLE() { return getToken(VerdictSQLParser.TABLE, 0); }
		public List<Column_def_table_constraintContext> column_def_table_constraint() {
			return getRuleContexts(Column_def_table_constraintContext.class);
		}
		public Column_def_table_constraintContext column_def_table_constraint(int i) {
			return getRuleContext(Column_def_table_constraintContext.class,i);
		}
		public Table_type_definitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_type_definition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTable_type_definition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTable_type_definition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTable_type_definition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_type_definitionContext table_type_definition() throws RecognitionException {
		Table_type_definitionContext _localctx = new Table_type_definitionContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_table_type_definition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(663);
			match(TABLE);
			setState(664);
			match(LR_BRACKET);
			setState(665);
			column_def_table_constraint();
			setState(672);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==STORE || _la==CONSTRAINT || _la==FORCESEEK || ((((_la - 215)) & ~0x3f) == 0 && ((1L << (_la - 215)) & ((1L << (ABSOLUTE - 215)) | (1L << (APPLY - 215)) | (1L << (AUTO - 215)) | (1L << (AVG - 215)) | (1L << (BASE64 - 215)) | (1L << (CALLER - 215)) | (1L << (CAST - 215)) | (1L << (CATCH - 215)) | (1L << (CHECKSUM_AGG - 215)) | (1L << (COMMITTED - 215)) | (1L << (CONCAT - 215)) | (1L << (CONCAT_WS - 215)) | (1L << (COOKIE - 215)) | (1L << (COUNT - 215)) | (1L << (COUNT_BIG - 215)) | (1L << (DAY - 215)) | (1L << (DAYS - 215)) | (1L << (DELAY - 215)) | (1L << (DELETED - 215)) | (1L << (DENSE_RANK - 215)) | (1L << (DISABLE - 215)) | (1L << (DYNAMIC - 215)) | (1L << (ENCRYPTION - 215)) | (1L << (EXTRACT - 215)) | (1L << (FAST - 215)) | (1L << (FAST_FORWARD - 215)) | (1L << (FIRST - 215)) | (1L << (FOLLOWING - 215)) | (1L << (FORWARD_ONLY - 215)))) != 0) || ((((_la - 280)) & ~0x3f) == 0 && ((1L << (_la - 280)) & ((1L << (FULLSCAN - 280)) | (1L << (GLOBAL - 280)) | (1L << (GO - 280)) | (1L << (GROUPING - 280)) | (1L << (GROUPING_ID - 280)) | (1L << (HASH - 280)) | (1L << (INSENSITIVE - 280)) | (1L << (INSERTED - 280)) | (1L << (INTERVAL - 280)) | (1L << (ISOLATION - 280)) | (1L << (KEEPFIXED - 280)) | (1L << (KEYSET - 280)) | (1L << (LAST - 280)) | (1L << (LEVEL - 280)) | (1L << (LOCAL - 280)) | (1L << (LOCK_ESCALATION - 280)) | (1L << (LOGIN - 280)) | (1L << (LOOP - 280)) | (1L << (MARK - 280)) | (1L << (MAX - 280)) | (1L << (MIN - 280)) | (1L << (MODIFY - 280)) | (1L << (MONTH - 280)) | (1L << (MONTHS - 280)) | (1L << (NEXT - 280)) | (1L << (NAME - 280)) | (1L << (NOCOUNT - 280)) | (1L << (NOEXPAND - 280)) | (1L << (NORECOMPUTE - 280)) | (1L << (NTILE - 280)) | (1L << (NUMBER - 280)) | (1L << (OFFSET - 280)) | (1L << (ONLY - 280)) | (1L << (OPTIMISTIC - 280)) | (1L << (OPTIMIZE - 280)) | (1L << (OUT - 280)) | (1L << (OUTPUT - 280)) | (1L << (OWNER - 280)) | (1L << (PARTITION - 280)) | (1L << (PATH - 280)))) != 0) || ((((_la - 346)) & ~0x3f) == 0 && ((1L << (_la - 346)) & ((1L << (PRECEDING - 346)) | (1L << (PRIOR - 346)) | (1L << (RANGE - 346)) | (1L << (RANK - 346)) | (1L << (READONLY - 346)) | (1L << (READ_ONLY - 346)) | (1L << (RECOMPILE - 346)) | (1L << (RELATIVE - 346)) | (1L << (REMOTE - 346)) | (1L << (REPEATABLE - 346)) | (1L << (ROOT - 346)) | (1L << (ROW - 346)) | (1L << (ROWGUID - 346)) | (1L << (ROWS - 346)) | (1L << (ROW_NUMBER - 346)) | (1L << (SAMPLE - 346)) | (1L << (SCHEMABINDING - 346)) | (1L << (SCROLL - 346)) | (1L << (SCROLL_LOCKS - 346)) | (1L << (SELF - 346)) | (1L << (SERIALIZABLE - 346)) | (1L << (SNAPSHOT - 346)) | (1L << (SPATIAL_WINDOW_MAX_CELLS - 346)) | (1L << (STATIC - 346)) | (1L << (STATS_STREAM - 346)) | (1L << (STDEV - 346)) | (1L << (STDEVP - 346)) | (1L << (STDDEV_SAMP - 346)) | (1L << (SUM - 346)) | (1L << (STRTOL - 346)) | (1L << (THROW - 346)) | (1L << (TIES - 346)) | (1L << (TIME - 346)) | (1L << (TRY - 346)) | (1L << (TYPE - 346)) | (1L << (TYPE_WARNING - 346)) | (1L << (UNBOUNDED - 346)))) != 0) || ((((_la - 410)) & ~0x3f) == 0 && ((1L << (_la - 410)) & ((1L << (UNCOMMITTED - 410)) | (1L << (UNKNOWN - 410)) | (1L << (USING - 410)) | (1L << (VAR - 410)) | (1L << (VARP - 410)) | (1L << (VIEW_METADATA - 410)) | (1L << (WORK - 410)) | (1L << (XML - 410)) | (1L << (XMLNAMESPACES - 410)) | (1L << (YEAR - 410)) | (1L << (YEARS - 410)) | (1L << (DOUBLE_QUOTE_ID - 410)) | (1L << (BACKTICK_ID - 410)) | (1L << (SQUARE_BRACKET_ID - 410)) | (1L << (ID - 410)) | (1L << (LR_BRACKET - 410)) | (1L << (COMMA - 410)))) != 0)) {
				{
				{
				setState(667);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(666);
					match(COMMA);
					}
				}

				setState(669);
				column_def_table_constraint();
				}
				}
				setState(674);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(675);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_def_table_constraintContext extends ParserRuleContext {
		public Column_definitionContext column_definition() {
			return getRuleContext(Column_definitionContext.class,0);
		}
		public Table_constraintContext table_constraint() {
			return getRuleContext(Table_constraintContext.class,0);
		}
		public Column_def_table_constraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_def_table_constraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterColumn_def_table_constraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitColumn_def_table_constraint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitColumn_def_table_constraint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_def_table_constraintContext column_def_table_constraint() throws RecognitionException {
		Column_def_table_constraintContext _localctx = new Column_def_table_constraintContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_column_def_table_constraint);
		try {
			setState(679);
			switch (_input.LA(1)) {
			case STORE:
			case FORCESEEK:
			case ABSOLUTE:
			case APPLY:
			case AUTO:
			case AVG:
			case BASE64:
			case CALLER:
			case CAST:
			case CATCH:
			case CHECKSUM_AGG:
			case COMMITTED:
			case CONCAT:
			case CONCAT_WS:
			case COOKIE:
			case COUNT:
			case COUNT_BIG:
			case DAY:
			case DAYS:
			case DELAY:
			case DELETED:
			case DENSE_RANK:
			case DISABLE:
			case DYNAMIC:
			case ENCRYPTION:
			case EXTRACT:
			case FAST:
			case FAST_FORWARD:
			case FIRST:
			case FOLLOWING:
			case FORWARD_ONLY:
			case FULLSCAN:
			case GLOBAL:
			case GO:
			case GROUPING:
			case GROUPING_ID:
			case HASH:
			case INSENSITIVE:
			case INSERTED:
			case INTERVAL:
			case ISOLATION:
			case KEEPFIXED:
			case KEYSET:
			case LAST:
			case LEVEL:
			case LOCAL:
			case LOCK_ESCALATION:
			case LOGIN:
			case LOOP:
			case MARK:
			case MAX:
			case MIN:
			case MODIFY:
			case MONTH:
			case MONTHS:
			case NEXT:
			case NAME:
			case NOCOUNT:
			case NOEXPAND:
			case NORECOMPUTE:
			case NTILE:
			case NUMBER:
			case OFFSET:
			case ONLY:
			case OPTIMISTIC:
			case OPTIMIZE:
			case OUT:
			case OUTPUT:
			case OWNER:
			case PARTITION:
			case PATH:
			case PRECEDING:
			case PRIOR:
			case RANGE:
			case RANK:
			case READONLY:
			case READ_ONLY:
			case RECOMPILE:
			case RELATIVE:
			case REMOTE:
			case REPEATABLE:
			case ROOT:
			case ROW:
			case ROWGUID:
			case ROWS:
			case ROW_NUMBER:
			case SAMPLE:
			case SCHEMABINDING:
			case SCROLL:
			case SCROLL_LOCKS:
			case SELF:
			case SERIALIZABLE:
			case SNAPSHOT:
			case SPATIAL_WINDOW_MAX_CELLS:
			case STATIC:
			case STATS_STREAM:
			case STDEV:
			case STDEVP:
			case STDDEV_SAMP:
			case SUM:
			case STRTOL:
			case THROW:
			case TIES:
			case TIME:
			case TRY:
			case TYPE:
			case TYPE_WARNING:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNKNOWN:
			case USING:
			case VAR:
			case VARP:
			case VIEW_METADATA:
			case WORK:
			case XML:
			case XMLNAMESPACES:
			case YEAR:
			case YEARS:
			case DOUBLE_QUOTE_ID:
			case BACKTICK_ID:
			case SQUARE_BRACKET_ID:
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(677);
				column_definition();
				}
				break;
			case CONSTRAINT:
			case LR_BRACKET:
				enterOuterAlt(_localctx, 2);
				{
				setState(678);
				table_constraint();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_definitionContext extends ParserRuleContext {
		public Column_nameContext column_name() {
			return getRuleContext(Column_nameContext.class,0);
		}
		public Data_typeContext data_type() {
			return getRuleContext(Data_typeContext.class,0);
		}
		public TerminalNode AS() { return getToken(VerdictSQLParser.AS, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Null_notnullContext null_notnull() {
			return getRuleContext(Null_notnullContext.class,0);
		}
		public Column_definitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_definition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterColumn_definition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitColumn_definition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitColumn_definition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_definitionContext column_definition() throws RecognitionException {
		Column_definitionContext _localctx = new Column_definitionContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_column_definition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(681);
			column_name();
			setState(685);
			switch (_input.LA(1)) {
			case STORE:
			case FORCESEEK:
			case ABSOLUTE:
			case APPLY:
			case AUTO:
			case AVG:
			case BASE64:
			case CALLER:
			case CAST:
			case CATCH:
			case CHECKSUM_AGG:
			case COMMITTED:
			case CONCAT:
			case CONCAT_WS:
			case COOKIE:
			case COUNT:
			case COUNT_BIG:
			case DAY:
			case DAYS:
			case DELAY:
			case DELETED:
			case DENSE_RANK:
			case DISABLE:
			case DYNAMIC:
			case ENCRYPTION:
			case EXTRACT:
			case FAST:
			case FAST_FORWARD:
			case FIRST:
			case FOLLOWING:
			case FORWARD_ONLY:
			case FULLSCAN:
			case GLOBAL:
			case GO:
			case GROUPING:
			case GROUPING_ID:
			case HASH:
			case INSENSITIVE:
			case INSERTED:
			case INTERVAL:
			case ISOLATION:
			case KEEPFIXED:
			case KEYSET:
			case LAST:
			case LEVEL:
			case LOCAL:
			case LOCK_ESCALATION:
			case LOGIN:
			case LOOP:
			case MARK:
			case MAX:
			case MIN:
			case MODIFY:
			case MONTH:
			case MONTHS:
			case NEXT:
			case NAME:
			case NOCOUNT:
			case NOEXPAND:
			case NORECOMPUTE:
			case NTILE:
			case NUMBER:
			case OFFSET:
			case ONLY:
			case OPTIMISTIC:
			case OPTIMIZE:
			case OUT:
			case OUTPUT:
			case OWNER:
			case PARTITION:
			case PATH:
			case PRECEDING:
			case PRIOR:
			case RANGE:
			case RANK:
			case READONLY:
			case READ_ONLY:
			case RECOMPILE:
			case RELATIVE:
			case REMOTE:
			case REPEATABLE:
			case ROOT:
			case ROW:
			case ROWGUID:
			case ROWS:
			case ROW_NUMBER:
			case SAMPLE:
			case SCHEMABINDING:
			case SCROLL:
			case SCROLL_LOCKS:
			case SELF:
			case SERIALIZABLE:
			case SNAPSHOT:
			case SPATIAL_WINDOW_MAX_CELLS:
			case STATIC:
			case STATS_STREAM:
			case STDEV:
			case STDEVP:
			case STDDEV_SAMP:
			case SUM:
			case STRTOL:
			case THROW:
			case TIES:
			case TIME:
			case TRY:
			case TYPE:
			case TYPE_WARNING:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNKNOWN:
			case USING:
			case VAR:
			case VARP:
			case VIEW_METADATA:
			case WORK:
			case XML:
			case XMLNAMESPACES:
			case YEAR:
			case YEARS:
			case DOUBLE_QUOTE_ID:
			case BACKTICK_ID:
			case SQUARE_BRACKET_ID:
			case ID:
				{
				setState(682);
				data_type();
				}
				break;
			case AS:
				{
				setState(683);
				match(AS);
				setState(684);
				expression(0);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(688);
			_la = _input.LA(1);
			if (_la==NOT || _la==NULL) {
				{
				setState(687);
				null_notnull();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_constraintContext extends ParserRuleContext {
		public TerminalNode CONSTRAINT() { return getToken(VerdictSQLParser.CONSTRAINT, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Null_notnullContext null_notnull() {
			return getRuleContext(Null_notnullContext.class,0);
		}
		public Column_constraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_constraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterColumn_constraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitColumn_constraint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitColumn_constraint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_constraintContext column_constraint() throws RecognitionException {
		Column_constraintContext _localctx = new Column_constraintContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_column_constraint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(692);
			_la = _input.LA(1);
			if (_la==CONSTRAINT) {
				{
				setState(690);
				match(CONSTRAINT);
				setState(691);
				id();
				}
			}

			setState(695);
			_la = _input.LA(1);
			if (_la==NOT || _la==NULL) {
				{
				setState(694);
				null_notnull();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_constraintContext extends ParserRuleContext {
		public Column_name_listContext column_name_list() {
			return getRuleContext(Column_name_listContext.class,0);
		}
		public TerminalNode CONSTRAINT() { return getToken(VerdictSQLParser.CONSTRAINT, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Table_constraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_constraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTable_constraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTable_constraint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTable_constraint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_constraintContext table_constraint() throws RecognitionException {
		Table_constraintContext _localctx = new Table_constraintContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_table_constraint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(699);
			_la = _input.LA(1);
			if (_la==CONSTRAINT) {
				{
				setState(697);
				match(CONSTRAINT);
				setState(698);
				id();
				}
			}

			setState(701);
			match(LR_BRACKET);
			setState(702);
			column_name_list();
			setState(703);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Set_specialContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(VerdictSQLParser.SET, 0); }
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public TerminalNode LOCAL_ID() { return getToken(VerdictSQLParser.LOCAL_ID, 0); }
		public On_offContext on_off() {
			return getRuleContext(On_offContext.class,0);
		}
		public TerminalNode TRANSACTION() { return getToken(VerdictSQLParser.TRANSACTION, 0); }
		public TerminalNode ISOLATION() { return getToken(VerdictSQLParser.ISOLATION, 0); }
		public TerminalNode LEVEL() { return getToken(VerdictSQLParser.LEVEL, 0); }
		public TerminalNode READ() { return getToken(VerdictSQLParser.READ, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(VerdictSQLParser.UNCOMMITTED, 0); }
		public TerminalNode COMMITTED() { return getToken(VerdictSQLParser.COMMITTED, 0); }
		public TerminalNode REPEATABLE() { return getToken(VerdictSQLParser.REPEATABLE, 0); }
		public TerminalNode SNAPSHOT() { return getToken(VerdictSQLParser.SNAPSHOT, 0); }
		public TerminalNode SERIALIZABLE() { return getToken(VerdictSQLParser.SERIALIZABLE, 0); }
		public TerminalNode IDENTITY_INSERT() { return getToken(VerdictSQLParser.IDENTITY_INSERT, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Set_specialContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_set_special; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSet_special(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSet_special(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSet_special(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Set_specialContext set_special() throws RecognitionException {
		Set_specialContext _localctx = new Set_specialContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_set_special);
		int _la;
		try {
			setState(740);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,81,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(705);
				match(SET);
				setState(706);
				id();
				setState(711);
				switch (_input.LA(1)) {
				case STORE:
				case FORCESEEK:
				case ABSOLUTE:
				case APPLY:
				case AUTO:
				case AVG:
				case BASE64:
				case CALLER:
				case CAST:
				case CATCH:
				case CHECKSUM_AGG:
				case COMMITTED:
				case CONCAT:
				case CONCAT_WS:
				case COOKIE:
				case COUNT:
				case COUNT_BIG:
				case DAY:
				case DAYS:
				case DELAY:
				case DELETED:
				case DENSE_RANK:
				case DISABLE:
				case DYNAMIC:
				case ENCRYPTION:
				case EXTRACT:
				case FAST:
				case FAST_FORWARD:
				case FIRST:
				case FOLLOWING:
				case FORWARD_ONLY:
				case FULLSCAN:
				case GLOBAL:
				case GO:
				case GROUPING:
				case GROUPING_ID:
				case HASH:
				case INSENSITIVE:
				case INSERTED:
				case INTERVAL:
				case ISOLATION:
				case KEEPFIXED:
				case KEYSET:
				case LAST:
				case LEVEL:
				case LOCAL:
				case LOCK_ESCALATION:
				case LOGIN:
				case LOOP:
				case MARK:
				case MAX:
				case MIN:
				case MODIFY:
				case MONTH:
				case MONTHS:
				case NEXT:
				case NAME:
				case NOCOUNT:
				case NOEXPAND:
				case NORECOMPUTE:
				case NTILE:
				case NUMBER:
				case OFFSET:
				case ONLY:
				case OPTIMISTIC:
				case OPTIMIZE:
				case OUT:
				case OUTPUT:
				case OWNER:
				case PARTITION:
				case PATH:
				case PRECEDING:
				case PRIOR:
				case RANGE:
				case RANK:
				case READONLY:
				case READ_ONLY:
				case RECOMPILE:
				case RELATIVE:
				case REMOTE:
				case REPEATABLE:
				case ROOT:
				case ROW:
				case ROWGUID:
				case ROWS:
				case ROW_NUMBER:
				case SAMPLE:
				case SCHEMABINDING:
				case SCROLL:
				case SCROLL_LOCKS:
				case SELF:
				case SERIALIZABLE:
				case SNAPSHOT:
				case SPATIAL_WINDOW_MAX_CELLS:
				case STATIC:
				case STATS_STREAM:
				case STDEV:
				case STDEVP:
				case STDDEV_SAMP:
				case SUM:
				case STRTOL:
				case THROW:
				case TIES:
				case TIME:
				case TRY:
				case TYPE:
				case TYPE_WARNING:
				case UNBOUNDED:
				case UNCOMMITTED:
				case UNKNOWN:
				case USING:
				case VAR:
				case VARP:
				case VIEW_METADATA:
				case WORK:
				case XML:
				case XMLNAMESPACES:
				case YEAR:
				case YEARS:
				case DOUBLE_QUOTE_ID:
				case BACKTICK_ID:
				case SQUARE_BRACKET_ID:
				case ID:
					{
					setState(707);
					id();
					}
					break;
				case DECIMAL:
				case STRING:
				case BINARY:
				case FLOAT:
				case REAL:
				case DOLLAR:
				case PLUS:
				case MINUS:
					{
					setState(708);
					constant();
					}
					break;
				case LOCAL_ID:
					{
					setState(709);
					match(LOCAL_ID);
					}
					break;
				case OFF:
				case ON:
					{
					setState(710);
					on_off();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(714);
				_la = _input.LA(1);
				if (_la==SEMICOLON) {
					{
					setState(713);
					match(SEMICOLON);
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(716);
				match(SET);
				setState(717);
				match(TRANSACTION);
				setState(718);
				match(ISOLATION);
				setState(719);
				match(LEVEL);
				setState(728);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
				case 1:
					{
					setState(720);
					match(READ);
					setState(721);
					match(UNCOMMITTED);
					}
					break;
				case 2:
					{
					setState(722);
					match(READ);
					setState(723);
					match(COMMITTED);
					}
					break;
				case 3:
					{
					setState(724);
					match(REPEATABLE);
					setState(725);
					match(READ);
					}
					break;
				case 4:
					{
					setState(726);
					match(SNAPSHOT);
					}
					break;
				case 5:
					{
					setState(727);
					match(SERIALIZABLE);
					}
					break;
				}
				setState(731);
				_la = _input.LA(1);
				if (_la==SEMICOLON) {
					{
					setState(730);
					match(SEMICOLON);
					}
				}

				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(733);
				match(SET);
				setState(734);
				match(IDENTITY_INSERT);
				setState(735);
				table_name();
				setState(736);
				on_off();
				setState(738);
				_la = _input.LA(1);
				if (_la==SEMICOLON) {
					{
					setState(737);
					match(SEMICOLON);
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
	 
		public ExpressionContext() { }
		public void copyFrom(ExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class Binary_operator_expressionContext extends ExpressionContext {
		public Token op;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public Comparison_operatorContext comparison_operator() {
			return getRuleContext(Comparison_operatorContext.class,0);
		}
		public Binary_operator_expressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterBinary_operator_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitBinary_operator_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitBinary_operator_expression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Primitive_expressionContext extends ExpressionContext {
		public TerminalNode NULL() { return getToken(VerdictSQLParser.NULL, 0); }
		public TerminalNode LOCAL_ID() { return getToken(VerdictSQLParser.LOCAL_ID, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public True_orfalseContext true_orfalse() {
			return getRuleContext(True_orfalseContext.class,0);
		}
		public Primitive_expressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterPrimitive_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitPrimitive_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitPrimitive_expression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Bracket_expressionContext extends ExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Bracket_expressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterBracket_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitBracket_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitBracket_expression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Unary_operator_expressionContext extends ExpressionContext {
		public Token op;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Unary_operator_expressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterUnary_operator_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitUnary_operator_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitUnary_operator_expression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Interval_expressionContext extends ExpressionContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public Interval_expressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterInterval_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitInterval_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitInterval_expression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Function_call_expressionContext extends ExpressionContext {
		public Function_callContext function_call() {
			return getRuleContext(Function_callContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode COLLATE() { return getToken(VerdictSQLParser.COLLATE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Function_call_expressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterFunction_call_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitFunction_call_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitFunction_call_expression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Case_expressionContext extends ExpressionContext {
		public Case_exprContext case_expr() {
			return getRuleContext(Case_exprContext.class,0);
		}
		public Case_expressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterCase_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitCase_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitCase_expression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Column_ref_expressionContext extends ExpressionContext {
		public Full_column_nameContext full_column_name() {
			return getRuleContext(Full_column_nameContext.class,0);
		}
		public Column_ref_expressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterColumn_ref_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitColumn_ref_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitColumn_ref_expression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Subquery_expressionContext extends ExpressionContext {
		public SubqueryContext subquery() {
			return getRuleContext(SubqueryContext.class,0);
		}
		public Subquery_expressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSubquery_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSubquery_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSubquery_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		return expression(0);
	}

	private ExpressionContext expression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
		ExpressionContext _prevctx = _localctx;
		int _startState = 88;
		enterRecursionRule(_localctx, 88, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(763);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
			case 1:
				{
				_localctx = new Primitive_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(743);
				match(NULL);
				}
				break;
			case 2:
				{
				_localctx = new Primitive_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(744);
				match(LOCAL_ID);
				}
				break;
			case 3:
				{
				_localctx = new Primitive_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(745);
				constant();
				}
				break;
			case 4:
				{
				_localctx = new Primitive_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(746);
				true_orfalse();
				}
				break;
			case 5:
				{
				_localctx = new Function_call_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(747);
				function_call();
				}
				break;
			case 6:
				{
				_localctx = new Case_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(748);
				case_expr();
				}
				break;
			case 7:
				{
				_localctx = new Column_ref_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(749);
				full_column_name();
				}
				break;
			case 8:
				{
				_localctx = new Bracket_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(750);
				match(LR_BRACKET);
				setState(751);
				expression(0);
				setState(752);
				match(RR_BRACKET);
				}
				break;
			case 9:
				{
				_localctx = new Subquery_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(754);
				match(LR_BRACKET);
				setState(755);
				subquery();
				setState(756);
				match(RR_BRACKET);
				}
				break;
			case 10:
				{
				_localctx = new Unary_operator_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(758);
				match(BIT_NOT);
				setState(759);
				expression(6);
				}
				break;
			case 11:
				{
				_localctx = new Unary_operator_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(760);
				((Unary_operator_expressionContext)_localctx).op = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((Unary_operator_expressionContext)_localctx).op = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(761);
				expression(4);
				}
				break;
			case 12:
				{
				_localctx = new Interval_expressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(762);
				interval();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(780);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,84,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(778);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,83,_ctx) ) {
					case 1:
						{
						_localctx = new Binary_operator_expressionContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(765);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(766);
						((Binary_operator_expressionContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 462)) & ~0x3f) == 0 && ((1L << (_la - 462)) & ((1L << (STAR - 462)) | (1L << (DIVIDE - 462)) | (1L << (MODULE - 462)))) != 0)) ) {
							((Binary_operator_expressionContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(767);
						expression(6);
						}
						break;
					case 2:
						{
						_localctx = new Binary_operator_expressionContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(768);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(769);
						((Binary_operator_expressionContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==T__0 || ((((_la - 465)) & ~0x3f) == 0 && ((1L << (_la - 465)) & ((1L << (PLUS - 465)) | (1L << (MINUS - 465)) | (1L << (BIT_OR - 465)) | (1L << (BIT_AND - 465)) | (1L << (BIT_XOR - 465)))) != 0)) ) {
							((Binary_operator_expressionContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(770);
						expression(4);
						}
						break;
					case 3:
						{
						_localctx = new Binary_operator_expressionContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(771);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(772);
						comparison_operator();
						setState(773);
						expression(3);
						}
						break;
					case 4:
						{
						_localctx = new Function_call_expressionContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(775);
						if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
						setState(776);
						match(COLLATE);
						setState(777);
						id();
						}
						break;
					}
					} 
				}
				setState(782);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,84,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class IntervalContext extends ParserRuleContext {
		public TerminalNode INTERVAL() { return getToken(VerdictSQLParser.INTERVAL, 0); }
		public Constant_expressionContext constant_expression() {
			return getRuleContext(Constant_expressionContext.class,0);
		}
		public TerminalNode DAY() { return getToken(VerdictSQLParser.DAY, 0); }
		public TerminalNode DAYS() { return getToken(VerdictSQLParser.DAYS, 0); }
		public TerminalNode MONTH() { return getToken(VerdictSQLParser.MONTH, 0); }
		public TerminalNode MONTHS() { return getToken(VerdictSQLParser.MONTHS, 0); }
		public TerminalNode YEAR() { return getToken(VerdictSQLParser.YEAR, 0); }
		public TerminalNode YEARS() { return getToken(VerdictSQLParser.YEARS, 0); }
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_interval);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(783);
			match(INTERVAL);
			setState(784);
			constant_expression();
			setState(785);
			_la = _input.LA(1);
			if ( !(_la==DAY || _la==DAYS || _la==MONTH || _la==MONTHS || _la==YEAR || _la==YEARS) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Constant_expressionContext extends ParserRuleContext {
		public TerminalNode NULL() { return getToken(VerdictSQLParser.NULL, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public Function_callContext function_call() {
			return getRuleContext(Function_callContext.class,0);
		}
		public TerminalNode LOCAL_ID() { return getToken(VerdictSQLParser.LOCAL_ID, 0); }
		public Constant_expressionContext constant_expression() {
			return getRuleContext(Constant_expressionContext.class,0);
		}
		public Constant_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterConstant_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitConstant_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitConstant_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Constant_expressionContext constant_expression() throws RecognitionException {
		Constant_expressionContext _localctx = new Constant_expressionContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_constant_expression);
		try {
			setState(795);
			switch (_input.LA(1)) {
			case NULL:
				enterOuterAlt(_localctx, 1);
				{
				setState(787);
				match(NULL);
				}
				break;
			case DECIMAL:
			case STRING:
			case BINARY:
			case FLOAT:
			case REAL:
			case DOLLAR:
			case PLUS:
			case MINUS:
				enterOuterAlt(_localctx, 2);
				{
				setState(788);
				constant();
				}
				break;
			case ASCII:
			case COALESCE:
			case CONV:
			case CURRENT_TIMESTAMP:
			case SUBSTR:
			case ABS:
			case ACOS:
			case AES_DECRYPT:
			case AES_ENCRYPT:
			case ASIN:
			case ATAN:
			case AVG:
			case BIN:
			case BROUND:
			case CAST:
			case CBRT:
			case CEIL:
			case CHARACTER_LENGTH:
			case CHECKSUM_AGG:
			case CHR:
			case CONCAT:
			case CONCAT_WS:
			case COS:
			case COUNT:
			case COUNT_BIG:
			case CRC32:
			case DAY:
			case DECODE:
			case DEGREES:
			case DENSE_RANK:
			case NATURAL_CONSTANT:
			case ENCODE:
			case EXP:
			case EXTRACT:
			case FACTORIAL:
			case FIND_IN_SET:
			case FLOOR:
			case FORMAT_NUMBER:
			case FNV_HASH:
			case FROM_UNIXTIME:
			case GET_JSON_OBJECT:
			case GROUPING:
			case GROUPING_ID:
			case HEX:
			case HOUR:
			case INSTR:
			case IN_FILE:
			case LENGTH:
			case LN:
			case LOCATE:
			case LOG2:
			case LOG10:
			case LOWER:
			case LTRIM:
			case MAX:
			case MD5:
			case MIN:
			case MINUTE:
			case MOD:
			case MONTH:
			case NEGATIVE:
			case NDV:
			case NTILE:
			case NVL:
			case PERCENTILE:
			case PI:
			case PMOD:
			case POSITIVE:
			case POW:
			case QUARTER:
			case RADIANS:
			case RAND:
			case RANDOM:
			case RANK:
			case REPEAT:
			case REVERSE:
			case ROUND:
			case ROW_NUMBER:
			case SECOND:
			case SHA1:
			case SHA2:
			case SHIFTLEFT:
			case SHIFTRIGHT:
			case SHIFTRIGHTUNSIGNED:
			case SIGN:
			case SIN:
			case SPACE_FUNCTION:
			case SPLIT:
			case STDEV:
			case STDDEV:
			case STDEVP:
			case STDDEV_SAMP:
			case SUM:
			case SQRT:
			case STRTOL:
			case TAN:
			case TO_DATE:
			case TRIM:
			case UNHEX:
			case UNIX_TIMESTAMP:
			case UPPER:
			case VAR:
			case VARP:
			case WEEKOFYEAR:
			case YEAR:
				enterOuterAlt(_localctx, 3);
				{
				setState(789);
				function_call();
				}
				break;
			case LOCAL_ID:
				enterOuterAlt(_localctx, 4);
				{
				setState(790);
				match(LOCAL_ID);
				}
				break;
			case LR_BRACKET:
				enterOuterAlt(_localctx, 5);
				{
				setState(791);
				match(LR_BRACKET);
				setState(792);
				constant_expression();
				setState(793);
				match(RR_BRACKET);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubqueryContext extends ParserRuleContext {
		public Select_statementContext select_statement() {
			return getRuleContext(Select_statementContext.class,0);
		}
		public SubqueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subquery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubqueryContext subquery() throws RecognitionException {
		SubqueryContext _localctx = new SubqueryContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_subquery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(797);
			select_statement();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Dml_table_sourceContext extends ParserRuleContext {
		public Query_specificationContext query_specification() {
			return getRuleContext(Query_specificationContext.class,0);
		}
		public Dml_table_sourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dml_table_source; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDml_table_source(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDml_table_source(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDml_table_source(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Dml_table_sourceContext dml_table_source() throws RecognitionException {
		Dml_table_sourceContext _localctx = new Dml_table_sourceContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_dml_table_source);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(799);
			query_specification();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class With_expressionContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(VerdictSQLParser.WITH, 0); }
		public List<Common_table_expressionContext> common_table_expression() {
			return getRuleContexts(Common_table_expressionContext.class);
		}
		public Common_table_expressionContext common_table_expression(int i) {
			return getRuleContext(Common_table_expressionContext.class,i);
		}
		public TerminalNode XMLNAMESPACES() { return getToken(VerdictSQLParser.XMLNAMESPACES, 0); }
		public With_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterWith_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitWith_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitWith_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final With_expressionContext with_expression() throws RecognitionException {
		With_expressionContext _localctx = new With_expressionContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_with_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(801);
			match(WITH);
			setState(804);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
			case 1:
				{
				setState(802);
				match(XMLNAMESPACES);
				setState(803);
				match(COMMA);
				}
				break;
			}
			setState(806);
			common_table_expression();
			setState(811);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(807);
				match(COMMA);
				setState(808);
				common_table_expression();
				}
				}
				setState(813);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Common_table_expressionContext extends ParserRuleContext {
		public IdContext expression_name;
		public TerminalNode AS() { return getToken(VerdictSQLParser.AS, 0); }
		public Select_statementContext select_statement() {
			return getRuleContext(Select_statementContext.class,0);
		}
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Column_name_listContext column_name_list() {
			return getRuleContext(Column_name_listContext.class,0);
		}
		public Common_table_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_common_table_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterCommon_table_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitCommon_table_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitCommon_table_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Common_table_expressionContext common_table_expression() throws RecognitionException {
		Common_table_expressionContext _localctx = new Common_table_expressionContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_common_table_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(814);
			((Common_table_expressionContext)_localctx).expression_name = id();
			setState(819);
			_la = _input.LA(1);
			if (_la==LR_BRACKET) {
				{
				setState(815);
				match(LR_BRACKET);
				setState(816);
				column_name_list();
				setState(817);
				match(RR_BRACKET);
				}
			}

			setState(821);
			match(AS);
			setState(822);
			match(LR_BRACKET);
			setState(823);
			select_statement();
			setState(824);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Update_elemContext extends ParserRuleContext {
		public IdContext udt_column_name;
		public IdContext method_name;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Full_column_nameContext full_column_name() {
			return getRuleContext(Full_column_nameContext.class,0);
		}
		public TerminalNode LOCAL_ID() { return getToken(VerdictSQLParser.LOCAL_ID, 0); }
		public Assignment_operatorContext assignment_operator() {
			return getRuleContext(Assignment_operatorContext.class,0);
		}
		public Expression_listContext expression_list() {
			return getRuleContext(Expression_listContext.class,0);
		}
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public Update_elemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_elem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterUpdate_elem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitUpdate_elem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitUpdate_elem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Update_elemContext update_elem() throws RecognitionException {
		Update_elemContext _localctx = new Update_elemContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_update_elem);
		try {
			setState(842);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(828);
				switch (_input.LA(1)) {
				case STORE:
				case FORCESEEK:
				case ABSOLUTE:
				case APPLY:
				case AUTO:
				case AVG:
				case BASE64:
				case CALLER:
				case CAST:
				case CATCH:
				case CHECKSUM_AGG:
				case COMMITTED:
				case CONCAT:
				case CONCAT_WS:
				case COOKIE:
				case COUNT:
				case COUNT_BIG:
				case DAY:
				case DAYS:
				case DELAY:
				case DELETED:
				case DENSE_RANK:
				case DISABLE:
				case DYNAMIC:
				case ENCRYPTION:
				case EXTRACT:
				case FAST:
				case FAST_FORWARD:
				case FIRST:
				case FOLLOWING:
				case FORWARD_ONLY:
				case FULLSCAN:
				case GLOBAL:
				case GO:
				case GROUPING:
				case GROUPING_ID:
				case HASH:
				case INSENSITIVE:
				case INSERTED:
				case INTERVAL:
				case ISOLATION:
				case KEEPFIXED:
				case KEYSET:
				case LAST:
				case LEVEL:
				case LOCAL:
				case LOCK_ESCALATION:
				case LOGIN:
				case LOOP:
				case MARK:
				case MAX:
				case MIN:
				case MODIFY:
				case MONTH:
				case MONTHS:
				case NEXT:
				case NAME:
				case NOCOUNT:
				case NOEXPAND:
				case NORECOMPUTE:
				case NTILE:
				case NUMBER:
				case OFFSET:
				case ONLY:
				case OPTIMISTIC:
				case OPTIMIZE:
				case OUT:
				case OUTPUT:
				case OWNER:
				case PARTITION:
				case PATH:
				case PRECEDING:
				case PRIOR:
				case RANGE:
				case RANK:
				case READONLY:
				case READ_ONLY:
				case RECOMPILE:
				case RELATIVE:
				case REMOTE:
				case REPEATABLE:
				case ROOT:
				case ROW:
				case ROWGUID:
				case ROWS:
				case ROW_NUMBER:
				case SAMPLE:
				case SCHEMABINDING:
				case SCROLL:
				case SCROLL_LOCKS:
				case SELF:
				case SERIALIZABLE:
				case SNAPSHOT:
				case SPATIAL_WINDOW_MAX_CELLS:
				case STATIC:
				case STATS_STREAM:
				case STDEV:
				case STDEVP:
				case STDDEV_SAMP:
				case SUM:
				case STRTOL:
				case THROW:
				case TIES:
				case TIME:
				case TRY:
				case TYPE:
				case TYPE_WARNING:
				case UNBOUNDED:
				case UNCOMMITTED:
				case UNKNOWN:
				case USING:
				case VAR:
				case VARP:
				case VIEW_METADATA:
				case WORK:
				case XML:
				case XMLNAMESPACES:
				case YEAR:
				case YEARS:
				case DOUBLE_QUOTE_ID:
				case BACKTICK_ID:
				case SQUARE_BRACKET_ID:
				case ID:
					{
					setState(826);
					full_column_name();
					}
					break;
				case LOCAL_ID:
					{
					setState(827);
					match(LOCAL_ID);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(832);
				switch (_input.LA(1)) {
				case EQUAL:
					{
					setState(830);
					match(EQUAL);
					}
					break;
				case PLUS_ASSIGN:
				case MINUS_ASSIGN:
				case MULT_ASSIGN:
				case DIV_ASSIGN:
				case MOD_ASSIGN:
				case AND_ASSIGN:
				case XOR_ASSIGN:
				case OR_ASSIGN:
					{
					setState(831);
					assignment_operator();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(834);
				expression(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(835);
				((Update_elemContext)_localctx).udt_column_name = id();
				setState(836);
				match(DOT);
				setState(837);
				((Update_elemContext)_localctx).method_name = id();
				setState(838);
				match(LR_BRACKET);
				setState(839);
				expression_list();
				setState(840);
				match(RR_BRACKET);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Search_condition_listContext extends ParserRuleContext {
		public List<Search_conditionContext> search_condition() {
			return getRuleContexts(Search_conditionContext.class);
		}
		public Search_conditionContext search_condition(int i) {
			return getRuleContext(Search_conditionContext.class,i);
		}
		public Search_condition_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_search_condition_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSearch_condition_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSearch_condition_list(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSearch_condition_list(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Search_condition_listContext search_condition_list() throws RecognitionException {
		Search_condition_listContext _localctx = new Search_condition_listContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_search_condition_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(844);
			search_condition();
			setState(849);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(845);
				match(COMMA);
				setState(846);
				search_condition();
				}
				}
				setState(851);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Search_conditionContext extends ParserRuleContext {
		public List<Search_condition_orContext> search_condition_or() {
			return getRuleContexts(Search_condition_orContext.class);
		}
		public Search_condition_orContext search_condition_or(int i) {
			return getRuleContext(Search_condition_orContext.class,i);
		}
		public List<TerminalNode> AND() { return getTokens(VerdictSQLParser.AND); }
		public TerminalNode AND(int i) {
			return getToken(VerdictSQLParser.AND, i);
		}
		public Search_conditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_search_condition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSearch_condition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSearch_condition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSearch_condition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Search_conditionContext search_condition() throws RecognitionException {
		Search_conditionContext _localctx = new Search_conditionContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_search_condition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(852);
			search_condition_or();
			setState(857);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==AND) {
				{
				{
				setState(853);
				match(AND);
				setState(854);
				search_condition_or();
				}
				}
				setState(859);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Search_condition_orContext extends ParserRuleContext {
		public List<Search_condition_notContext> search_condition_not() {
			return getRuleContexts(Search_condition_notContext.class);
		}
		public Search_condition_notContext search_condition_not(int i) {
			return getRuleContext(Search_condition_notContext.class,i);
		}
		public List<TerminalNode> OR() { return getTokens(VerdictSQLParser.OR); }
		public TerminalNode OR(int i) {
			return getToken(VerdictSQLParser.OR, i);
		}
		public Search_condition_orContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_search_condition_or; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSearch_condition_or(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSearch_condition_or(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSearch_condition_or(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Search_condition_orContext search_condition_or() throws RecognitionException {
		Search_condition_orContext _localctx = new Search_condition_orContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_search_condition_or);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(860);
			search_condition_not();
			setState(865);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==OR) {
				{
				{
				setState(861);
				match(OR);
				setState(862);
				search_condition_not();
				}
				}
				setState(867);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Search_condition_notContext extends ParserRuleContext {
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public TerminalNode NOT() { return getToken(VerdictSQLParser.NOT, 0); }
		public Search_condition_notContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_search_condition_not; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSearch_condition_not(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSearch_condition_not(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSearch_condition_not(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Search_condition_notContext search_condition_not() throws RecognitionException {
		Search_condition_notContext _localctx = new Search_condition_notContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_search_condition_not);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(869);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(868);
				match(NOT);
				}
			}

			setState(871);
			predicate();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateContext extends ParserRuleContext {
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
	 
		public PredicateContext() { }
		public void copyFrom(PredicateContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class Setcomp_expr_predicateContext extends PredicateContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Comparison_operatorContext comparison_operator() {
			return getRuleContext(Comparison_operatorContext.class,0);
		}
		public SubqueryContext subquery() {
			return getRuleContext(SubqueryContext.class,0);
		}
		public TerminalNode ALL() { return getToken(VerdictSQLParser.ALL, 0); }
		public TerminalNode SOME() { return getToken(VerdictSQLParser.SOME, 0); }
		public TerminalNode ANY() { return getToken(VerdictSQLParser.ANY, 0); }
		public Setcomp_expr_predicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSetcomp_expr_predicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSetcomp_expr_predicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSetcomp_expr_predicate(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Is_predicateContext extends PredicateContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IS() { return getToken(VerdictSQLParser.IS, 0); }
		public Null_notnullContext null_notnull() {
			return getRuleContext(Null_notnullContext.class,0);
		}
		public Is_predicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterIs_predicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitIs_predicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitIs_predicate(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Comp_expr_predicateContext extends PredicateContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public Comparison_operatorContext comparison_operator() {
			return getRuleContext(Comparison_operatorContext.class,0);
		}
		public Comp_expr_predicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterComp_expr_predicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitComp_expr_predicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitComp_expr_predicate(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class In_predicateContext extends PredicateContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IN() { return getToken(VerdictSQLParser.IN, 0); }
		public SubqueryContext subquery() {
			return getRuleContext(SubqueryContext.class,0);
		}
		public Expression_listContext expression_list() {
			return getRuleContext(Expression_listContext.class,0);
		}
		public TerminalNode NOT() { return getToken(VerdictSQLParser.NOT, 0); }
		public In_predicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterIn_predicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitIn_predicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitIn_predicate(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Bracket_predicateContext extends PredicateContext {
		public Search_conditionContext search_condition() {
			return getRuleContext(Search_conditionContext.class,0);
		}
		public Bracket_predicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterBracket_predicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitBracket_predicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitBracket_predicate(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Exists_predicateContext extends PredicateContext {
		public TerminalNode EXISTS() { return getToken(VerdictSQLParser.EXISTS, 0); }
		public SubqueryContext subquery() {
			return getRuleContext(SubqueryContext.class,0);
		}
		public Exists_predicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterExists_predicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitExists_predicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitExists_predicate(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Comp_between_exprContext extends PredicateContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode BETWEEN() { return getToken(VerdictSQLParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(VerdictSQLParser.AND, 0); }
		public TerminalNode NOT() { return getToken(VerdictSQLParser.NOT, 0); }
		public Comp_between_exprContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterComp_between_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitComp_between_expr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitComp_between_expr(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Like_predicateContext extends PredicateContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode LIKE() { return getToken(VerdictSQLParser.LIKE, 0); }
		public TerminalNode NOT() { return getToken(VerdictSQLParser.NOT, 0); }
		public TerminalNode ESCAPE() { return getToken(VerdictSQLParser.ESCAPE, 0); }
		public Like_predicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterLike_predicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitLike_predicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitLike_predicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_predicate);
		int _la;
		try {
			setState(928);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,101,_ctx) ) {
			case 1:
				_localctx = new Exists_predicateContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(873);
				match(EXISTS);
				setState(874);
				match(LR_BRACKET);
				setState(875);
				subquery();
				setState(876);
				match(RR_BRACKET);
				}
				break;
			case 2:
				_localctx = new Comp_expr_predicateContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(878);
				expression(0);
				setState(879);
				comparison_operator();
				setState(880);
				expression(0);
				}
				break;
			case 3:
				_localctx = new Setcomp_expr_predicateContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(882);
				expression(0);
				setState(883);
				comparison_operator();
				setState(884);
				_la = _input.LA(1);
				if ( !(_la==ALL || _la==ANY || _la==SOME) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(885);
				match(LR_BRACKET);
				setState(886);
				subquery();
				setState(887);
				match(RR_BRACKET);
				}
				break;
			case 4:
				_localctx = new Comp_between_exprContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(889);
				expression(0);
				setState(891);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(890);
					match(NOT);
					}
				}

				setState(893);
				match(BETWEEN);
				setState(894);
				expression(0);
				setState(895);
				match(AND);
				setState(896);
				expression(0);
				}
				break;
			case 5:
				_localctx = new In_predicateContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(898);
				expression(0);
				setState(900);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(899);
					match(NOT);
					}
				}

				setState(902);
				match(IN);
				setState(903);
				match(LR_BRACKET);
				setState(906);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
				case 1:
					{
					setState(904);
					subquery();
					}
					break;
				case 2:
					{
					setState(905);
					expression_list();
					}
					break;
				}
				setState(908);
				match(RR_BRACKET);
				}
				break;
			case 6:
				_localctx = new Like_predicateContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(910);
				expression(0);
				setState(912);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(911);
					match(NOT);
					}
				}

				setState(914);
				match(LIKE);
				setState(915);
				expression(0);
				setState(918);
				_la = _input.LA(1);
				if (_la==ESCAPE) {
					{
					setState(916);
					match(ESCAPE);
					setState(917);
					expression(0);
					}
				}

				}
				break;
			case 7:
				_localctx = new Is_predicateContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(920);
				expression(0);
				setState(921);
				match(IS);
				setState(922);
				null_notnull();
				}
				break;
			case 8:
				_localctx = new Bracket_predicateContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(924);
				match(LR_BRACKET);
				setState(925);
				search_condition();
				setState(926);
				match(RR_BRACKET);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Query_expressionContext extends ParserRuleContext {
		public Query_specificationContext query_specification() {
			return getRuleContext(Query_specificationContext.class,0);
		}
		public Query_expressionContext query_expression() {
			return getRuleContext(Query_expressionContext.class,0);
		}
		public List<UnionContext> union() {
			return getRuleContexts(UnionContext.class);
		}
		public UnionContext union(int i) {
			return getRuleContext(UnionContext.class,i);
		}
		public Query_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterQuery_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitQuery_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitQuery_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Query_expressionContext query_expression() throws RecognitionException {
		Query_expressionContext _localctx = new Query_expressionContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_query_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(935);
			switch (_input.LA(1)) {
			case SELECT:
				{
				setState(930);
				query_specification();
				}
				break;
			case LR_BRACKET:
				{
				setState(931);
				match(LR_BRACKET);
				setState(932);
				query_expression();
				setState(933);
				match(RR_BRACKET);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(940);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==EXCEPT || _la==INTERSECT || _la==UNION) {
				{
				{
				setState(937);
				union();
				}
				}
				setState(942);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnionContext extends ParserRuleContext {
		public TerminalNode UNION() { return getToken(VerdictSQLParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(VerdictSQLParser.EXCEPT, 0); }
		public TerminalNode INTERSECT() { return getToken(VerdictSQLParser.INTERSECT, 0); }
		public Query_specificationContext query_specification() {
			return getRuleContext(Query_specificationContext.class,0);
		}
		public TerminalNode ALL() { return getToken(VerdictSQLParser.ALL, 0); }
		public List<Query_expressionContext> query_expression() {
			return getRuleContexts(Query_expressionContext.class);
		}
		public Query_expressionContext query_expression(int i) {
			return getRuleContext(Query_expressionContext.class,i);
		}
		public UnionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_union; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterUnion(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitUnion(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitUnion(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnionContext union() throws RecognitionException {
		UnionContext _localctx = new UnionContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_union);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(949);
			switch (_input.LA(1)) {
			case UNION:
				{
				setState(943);
				match(UNION);
				setState(945);
				_la = _input.LA(1);
				if (_la==ALL) {
					{
					setState(944);
					match(ALL);
					}
				}

				}
				break;
			case EXCEPT:
				{
				setState(947);
				match(EXCEPT);
				}
				break;
			case INTERSECT:
				{
				setState(948);
				match(INTERSECT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(960);
			switch (_input.LA(1)) {
			case SELECT:
				{
				setState(951);
				query_specification();
				}
				break;
			case LR_BRACKET:
				{
				setState(956); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(952);
					match(LR_BRACKET);
					setState(953);
					query_expression();
					setState(954);
					match(RR_BRACKET);
					}
					}
					setState(958); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==LR_BRACKET );
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Query_specificationContext extends ParserRuleContext {
		public Table_nameContext into_table;
		public Search_conditionContext where;
		public Search_conditionContext having;
		public TerminalNode SELECT() { return getToken(VerdictSQLParser.SELECT, 0); }
		public Select_listContext select_list() {
			return getRuleContext(Select_listContext.class,0);
		}
		public TerminalNode TOP() { return getToken(VerdictSQLParser.TOP, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode INTO() { return getToken(VerdictSQLParser.INTO, 0); }
		public TerminalNode FROM() { return getToken(VerdictSQLParser.FROM, 0); }
		public TerminalNode WHERE() { return getToken(VerdictSQLParser.WHERE, 0); }
		public TerminalNode HAVING() { return getToken(VerdictSQLParser.HAVING, 0); }
		public TerminalNode ALL() { return getToken(VerdictSQLParser.ALL, 0); }
		public TerminalNode DISTINCT() { return getToken(VerdictSQLParser.DISTINCT, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public List<Search_conditionContext> search_condition() {
			return getRuleContexts(Search_conditionContext.class);
		}
		public Search_conditionContext search_condition(int i) {
			return getRuleContext(Search_conditionContext.class,i);
		}
		public List<Table_sourceContext> table_source() {
			return getRuleContexts(Table_sourceContext.class);
		}
		public Table_sourceContext table_source(int i) {
			return getRuleContext(Table_sourceContext.class,i);
		}
		public TerminalNode GROUP() { return getToken(VerdictSQLParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(VerdictSQLParser.BY, 0); }
		public List<Group_by_itemContext> group_by_item() {
			return getRuleContexts(Group_by_itemContext.class);
		}
		public Group_by_itemContext group_by_item(int i) {
			return getRuleContext(Group_by_itemContext.class,i);
		}
		public TerminalNode ROLLUP() { return getToken(VerdictSQLParser.ROLLUP, 0); }
		public TerminalNode PERCENT() { return getToken(VerdictSQLParser.PERCENT, 0); }
		public List<TerminalNode> WITH() { return getTokens(VerdictSQLParser.WITH); }
		public TerminalNode WITH(int i) {
			return getToken(VerdictSQLParser.WITH, i);
		}
		public TerminalNode TIES() { return getToken(VerdictSQLParser.TIES, 0); }
		public Query_specificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query_specification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterQuery_specification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitQuery_specification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitQuery_specification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Query_specificationContext query_specification() throws RecognitionException {
		Query_specificationContext _localctx = new Query_specificationContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_query_specification);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(962);
			match(SELECT);
			setState(964);
			_la = _input.LA(1);
			if (_la==ALL || _la==DISTINCT) {
				{
				setState(963);
				_la = _input.LA(1);
				if ( !(_la==ALL || _la==DISTINCT) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			setState(975);
			_la = _input.LA(1);
			if (_la==TOP) {
				{
				setState(966);
				match(TOP);
				setState(967);
				expression(0);
				setState(969);
				_la = _input.LA(1);
				if (_la==PERCENT) {
					{
					setState(968);
					match(PERCENT);
					}
				}

				setState(973);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(971);
					match(WITH);
					setState(972);
					match(TIES);
					}
				}

				}
			}

			setState(977);
			select_list();
			setState(980);
			_la = _input.LA(1);
			if (_la==INTO) {
				{
				setState(978);
				match(INTO);
				setState(979);
				((Query_specificationContext)_localctx).into_table = table_name();
				}
			}

			setState(991);
			_la = _input.LA(1);
			if (_la==FROM) {
				{
				setState(982);
				match(FROM);
				{
				setState(983);
				table_source();
				setState(988);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(984);
					match(COMMA);
					setState(985);
					table_source();
					}
					}
					setState(990);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				}
			}

			setState(995);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(993);
				match(WHERE);
				setState(994);
				((Query_specificationContext)_localctx).where = search_condition();
				}
			}

			setState(1025);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,119,_ctx) ) {
			case 1:
				{
				{
				setState(997);
				match(GROUP);
				setState(998);
				match(BY);
				setState(999);
				group_by_item();
				setState(1004);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1000);
					match(COMMA);
					setState(1001);
					group_by_item();
					}
					}
					setState(1006);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1009);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,117,_ctx) ) {
				case 1:
					{
					setState(1007);
					match(WITH);
					setState(1008);
					match(ROLLUP);
					}
					break;
				}
				}
				}
				break;
			case 2:
				{
				{
				setState(1011);
				match(GROUP);
				setState(1012);
				match(BY);
				setState(1013);
				match(ROLLUP);
				setState(1014);
				match(LR_BRACKET);
				setState(1015);
				group_by_item();
				setState(1020);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1016);
					match(COMMA);
					setState(1017);
					group_by_item();
					}
					}
					setState(1022);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1023);
				match(RR_BRACKET);
				}
				}
				break;
			}
			setState(1029);
			_la = _input.LA(1);
			if (_la==HAVING) {
				{
				setState(1027);
				match(HAVING);
				setState(1028);
				((Query_specificationContext)_localctx).having = search_condition();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Limit_clauseContext extends ParserRuleContext {
		public TerminalNode LIMIT() { return getToken(VerdictSQLParser.LIMIT, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public Limit_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limit_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterLimit_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitLimit_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitLimit_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Limit_clauseContext limit_clause() throws RecognitionException {
		Limit_clauseContext _localctx = new Limit_clauseContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_limit_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1031);
			match(LIMIT);
			setState(1032);
			number();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Order_by_clauseContext extends ParserRuleContext {
		public TerminalNode ORDER() { return getToken(VerdictSQLParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(VerdictSQLParser.BY, 0); }
		public List<Order_by_expressionContext> order_by_expression() {
			return getRuleContexts(Order_by_expressionContext.class);
		}
		public Order_by_expressionContext order_by_expression(int i) {
			return getRuleContext(Order_by_expressionContext.class,i);
		}
		public TerminalNode OFFSET() { return getToken(VerdictSQLParser.OFFSET, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<TerminalNode> ROW() { return getTokens(VerdictSQLParser.ROW); }
		public TerminalNode ROW(int i) {
			return getToken(VerdictSQLParser.ROW, i);
		}
		public List<TerminalNode> ROWS() { return getTokens(VerdictSQLParser.ROWS); }
		public TerminalNode ROWS(int i) {
			return getToken(VerdictSQLParser.ROWS, i);
		}
		public TerminalNode FETCH() { return getToken(VerdictSQLParser.FETCH, 0); }
		public TerminalNode ONLY() { return getToken(VerdictSQLParser.ONLY, 0); }
		public TerminalNode FIRST() { return getToken(VerdictSQLParser.FIRST, 0); }
		public TerminalNode NEXT() { return getToken(VerdictSQLParser.NEXT, 0); }
		public Order_by_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_order_by_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOrder_by_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOrder_by_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOrder_by_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Order_by_clauseContext order_by_clause() throws RecognitionException {
		Order_by_clauseContext _localctx = new Order_by_clauseContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_order_by_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1034);
			match(ORDER);
			setState(1035);
			match(BY);
			setState(1036);
			order_by_expression();
			setState(1041);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1037);
				match(COMMA);
				setState(1038);
				order_by_expression();
				}
				}
				setState(1043);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1055);
			_la = _input.LA(1);
			if (_la==OFFSET) {
				{
				setState(1044);
				match(OFFSET);
				setState(1045);
				expression(0);
				setState(1046);
				_la = _input.LA(1);
				if ( !(_la==ROW || _la==ROWS) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(1053);
				_la = _input.LA(1);
				if (_la==FETCH) {
					{
					setState(1047);
					match(FETCH);
					setState(1048);
					_la = _input.LA(1);
					if ( !(_la==FIRST || _la==NEXT) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					setState(1049);
					expression(0);
					setState(1050);
					_la = _input.LA(1);
					if ( !(_la==ROW || _la==ROWS) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					setState(1051);
					match(ONLY);
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class For_clauseContext extends ParserRuleContext {
		public TerminalNode FOR() { return getToken(VerdictSQLParser.FOR, 0); }
		public TerminalNode BROWSE() { return getToken(VerdictSQLParser.BROWSE, 0); }
		public TerminalNode XML() { return getToken(VerdictSQLParser.XML, 0); }
		public TerminalNode AUTO() { return getToken(VerdictSQLParser.AUTO, 0); }
		public Xml_common_directivesContext xml_common_directives() {
			return getRuleContext(Xml_common_directivesContext.class,0);
		}
		public TerminalNode PATH() { return getToken(VerdictSQLParser.PATH, 0); }
		public TerminalNode STRING() { return getToken(VerdictSQLParser.STRING, 0); }
		public For_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_for_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterFor_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitFor_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitFor_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final For_clauseContext for_clause() throws RecognitionException {
		For_clauseContext _localctx = new For_clauseContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_for_clause);
		int _la;
		try {
			setState(1076);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,127,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1057);
				match(FOR);
				setState(1058);
				match(BROWSE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1059);
				match(FOR);
				setState(1060);
				match(XML);
				setState(1061);
				match(AUTO);
				setState(1063);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1062);
					xml_common_directives();
					}
				}

				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1065);
				match(FOR);
				setState(1066);
				match(XML);
				setState(1067);
				match(PATH);
				setState(1071);
				_la = _input.LA(1);
				if (_la==LR_BRACKET) {
					{
					setState(1068);
					match(LR_BRACKET);
					setState(1069);
					match(STRING);
					setState(1070);
					match(RR_BRACKET);
					}
				}

				setState(1074);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1073);
					xml_common_directives();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Xml_common_directivesContext extends ParserRuleContext {
		public TerminalNode BINARY() { return getToken(VerdictSQLParser.BINARY, 0); }
		public TerminalNode BASE64() { return getToken(VerdictSQLParser.BASE64, 0); }
		public TerminalNode TYPE() { return getToken(VerdictSQLParser.TYPE, 0); }
		public TerminalNode ROOT() { return getToken(VerdictSQLParser.ROOT, 0); }
		public Xml_common_directivesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_xml_common_directives; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterXml_common_directives(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitXml_common_directives(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitXml_common_directives(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Xml_common_directivesContext xml_common_directives() throws RecognitionException {
		Xml_common_directivesContext _localctx = new Xml_common_directivesContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_xml_common_directives);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1078);
			match(COMMA);
			setState(1083);
			switch (_input.LA(1)) {
			case BINARY:
				{
				setState(1079);
				match(BINARY);
				setState(1080);
				match(BASE64);
				}
				break;
			case TYPE:
				{
				setState(1081);
				match(TYPE);
				}
				break;
			case ROOT:
				{
				setState(1082);
				match(ROOT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Order_by_expressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode ASC() { return getToken(VerdictSQLParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(VerdictSQLParser.DESC, 0); }
		public Order_by_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_order_by_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOrder_by_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOrder_by_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOrder_by_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Order_by_expressionContext order_by_expression() throws RecognitionException {
		Order_by_expressionContext _localctx = new Order_by_expressionContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_order_by_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1085);
			expression(0);
			setState(1087);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1086);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Group_by_itemContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Group_by_itemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_group_by_item; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterGroup_by_item(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitGroup_by_item(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitGroup_by_item(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Group_by_itemContext group_by_item() throws RecognitionException {
		Group_by_itemContext _localctx = new Group_by_itemContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_group_by_item);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1089);
			expression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Option_clauseContext extends ParserRuleContext {
		public TerminalNode OPTION() { return getToken(VerdictSQLParser.OPTION, 0); }
		public List<OptionContext> option() {
			return getRuleContexts(OptionContext.class);
		}
		public OptionContext option(int i) {
			return getRuleContext(OptionContext.class,i);
		}
		public Option_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_option_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOption_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOption_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOption_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Option_clauseContext option_clause() throws RecognitionException {
		Option_clauseContext _localctx = new Option_clauseContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_option_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1091);
			match(OPTION);
			setState(1092);
			match(LR_BRACKET);
			setState(1093);
			option();
			setState(1098);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1094);
				match(COMMA);
				setState(1095);
				option();
				}
				}
				setState(1100);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1101);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OptionContext extends ParserRuleContext {
		public Token number_rows;
		public TerminalNode FAST() { return getToken(VerdictSQLParser.FAST, 0); }
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public TerminalNode GROUP() { return getToken(VerdictSQLParser.GROUP, 0); }
		public TerminalNode HASH() { return getToken(VerdictSQLParser.HASH, 0); }
		public TerminalNode ORDER() { return getToken(VerdictSQLParser.ORDER, 0); }
		public TerminalNode UNION() { return getToken(VerdictSQLParser.UNION, 0); }
		public TerminalNode MERGE() { return getToken(VerdictSQLParser.MERGE, 0); }
		public TerminalNode CONCAT() { return getToken(VerdictSQLParser.CONCAT, 0); }
		public TerminalNode KEEPFIXED() { return getToken(VerdictSQLParser.KEEPFIXED, 0); }
		public TerminalNode PLAN() { return getToken(VerdictSQLParser.PLAN, 0); }
		public TerminalNode OPTIMIZE() { return getToken(VerdictSQLParser.OPTIMIZE, 0); }
		public TerminalNode FOR() { return getToken(VerdictSQLParser.FOR, 0); }
		public List<Optimize_for_argContext> optimize_for_arg() {
			return getRuleContexts(Optimize_for_argContext.class);
		}
		public Optimize_for_argContext optimize_for_arg(int i) {
			return getRuleContext(Optimize_for_argContext.class,i);
		}
		public TerminalNode UNKNOWN() { return getToken(VerdictSQLParser.UNKNOWN, 0); }
		public OptionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_option; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOption(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOption(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOption(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OptionContext option() throws RecognitionException {
		OptionContext _localctx = new OptionContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_option);
		int _la;
		try {
			setState(1127);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,132,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1103);
				match(FAST);
				setState(1104);
				((OptionContext)_localctx).number_rows = match(DECIMAL);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1105);
				_la = _input.LA(1);
				if ( !(_la==ORDER || _la==HASH) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(1106);
				match(GROUP);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1107);
				_la = _input.LA(1);
				if ( !(_la==MERGE || _la==CONCAT || _la==HASH) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(1108);
				match(UNION);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1109);
				match(KEEPFIXED);
				setState(1110);
				match(PLAN);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1111);
				match(OPTIMIZE);
				setState(1112);
				match(FOR);
				setState(1113);
				match(LR_BRACKET);
				setState(1114);
				optimize_for_arg();
				setState(1119);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1115);
					match(COMMA);
					setState(1116);
					optimize_for_arg();
					}
					}
					setState(1121);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1122);
				match(RR_BRACKET);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1124);
				match(OPTIMIZE);
				setState(1125);
				match(FOR);
				setState(1126);
				match(UNKNOWN);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Optimize_for_argContext extends ParserRuleContext {
		public TerminalNode LOCAL_ID() { return getToken(VerdictSQLParser.LOCAL_ID, 0); }
		public TerminalNode UNKNOWN() { return getToken(VerdictSQLParser.UNKNOWN, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public Optimize_for_argContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_optimize_for_arg; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOptimize_for_arg(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOptimize_for_arg(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOptimize_for_arg(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Optimize_for_argContext optimize_for_arg() throws RecognitionException {
		Optimize_for_argContext _localctx = new Optimize_for_argContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_optimize_for_arg);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1129);
			match(LOCAL_ID);
			setState(1133);
			switch (_input.LA(1)) {
			case UNKNOWN:
				{
				setState(1130);
				match(UNKNOWN);
				}
				break;
			case EQUAL:
				{
				setState(1131);
				match(EQUAL);
				setState(1132);
				constant();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_listContext extends ParserRuleContext {
		public List<Select_list_elemContext> select_list_elem() {
			return getRuleContexts(Select_list_elemContext.class);
		}
		public Select_list_elemContext select_list_elem(int i) {
			return getRuleContext(Select_list_elemContext.class,i);
		}
		public Select_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSelect_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSelect_list(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSelect_list(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_listContext select_list() throws RecognitionException {
		Select_listContext _localctx = new Select_listContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_select_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1135);
			select_list_elem();
			setState(1140);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1136);
				match(COMMA);
				setState(1137);
				select_list_elem();
				}
				}
				setState(1142);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_list_elemContext extends ParserRuleContext {
		public TerminalNode STAR() { return getToken(VerdictSQLParser.STAR, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode IDENTITY() { return getToken(VerdictSQLParser.IDENTITY, 0); }
		public TerminalNode ROWGUID() { return getToken(VerdictSQLParser.ROWGUID, 0); }
		public Column_aliasContext column_alias() {
			return getRuleContext(Column_aliasContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(VerdictSQLParser.AS, 0); }
		public Select_list_elemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_list_elem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSelect_list_elem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSelect_list_elem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSelect_list_elem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_list_elemContext select_list_elem() throws RecognitionException {
		Select_list_elemContext _localctx = new Select_list_elemContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_select_list_elem);
		int _la;
		try {
			setState(1164);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,139,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1146);
				_la = _input.LA(1);
				if (_la==STORE || _la==FORCESEEK || ((((_la - 215)) & ~0x3f) == 0 && ((1L << (_la - 215)) & ((1L << (ABSOLUTE - 215)) | (1L << (APPLY - 215)) | (1L << (AUTO - 215)) | (1L << (AVG - 215)) | (1L << (BASE64 - 215)) | (1L << (CALLER - 215)) | (1L << (CAST - 215)) | (1L << (CATCH - 215)) | (1L << (CHECKSUM_AGG - 215)) | (1L << (COMMITTED - 215)) | (1L << (CONCAT - 215)) | (1L << (CONCAT_WS - 215)) | (1L << (COOKIE - 215)) | (1L << (COUNT - 215)) | (1L << (COUNT_BIG - 215)) | (1L << (DAY - 215)) | (1L << (DAYS - 215)) | (1L << (DELAY - 215)) | (1L << (DELETED - 215)) | (1L << (DENSE_RANK - 215)) | (1L << (DISABLE - 215)) | (1L << (DYNAMIC - 215)) | (1L << (ENCRYPTION - 215)) | (1L << (EXTRACT - 215)) | (1L << (FAST - 215)) | (1L << (FAST_FORWARD - 215)) | (1L << (FIRST - 215)) | (1L << (FOLLOWING - 215)) | (1L << (FORWARD_ONLY - 215)))) != 0) || ((((_la - 280)) & ~0x3f) == 0 && ((1L << (_la - 280)) & ((1L << (FULLSCAN - 280)) | (1L << (GLOBAL - 280)) | (1L << (GO - 280)) | (1L << (GROUPING - 280)) | (1L << (GROUPING_ID - 280)) | (1L << (HASH - 280)) | (1L << (INSENSITIVE - 280)) | (1L << (INSERTED - 280)) | (1L << (INTERVAL - 280)) | (1L << (ISOLATION - 280)) | (1L << (KEEPFIXED - 280)) | (1L << (KEYSET - 280)) | (1L << (LAST - 280)) | (1L << (LEVEL - 280)) | (1L << (LOCAL - 280)) | (1L << (LOCK_ESCALATION - 280)) | (1L << (LOGIN - 280)) | (1L << (LOOP - 280)) | (1L << (MARK - 280)) | (1L << (MAX - 280)) | (1L << (MIN - 280)) | (1L << (MODIFY - 280)) | (1L << (MONTH - 280)) | (1L << (MONTHS - 280)) | (1L << (NEXT - 280)) | (1L << (NAME - 280)) | (1L << (NOCOUNT - 280)) | (1L << (NOEXPAND - 280)) | (1L << (NORECOMPUTE - 280)) | (1L << (NTILE - 280)) | (1L << (NUMBER - 280)) | (1L << (OFFSET - 280)) | (1L << (ONLY - 280)) | (1L << (OPTIMISTIC - 280)) | (1L << (OPTIMIZE - 280)) | (1L << (OUT - 280)) | (1L << (OUTPUT - 280)) | (1L << (OWNER - 280)) | (1L << (PARTITION - 280)) | (1L << (PATH - 280)))) != 0) || ((((_la - 346)) & ~0x3f) == 0 && ((1L << (_la - 346)) & ((1L << (PRECEDING - 346)) | (1L << (PRIOR - 346)) | (1L << (RANGE - 346)) | (1L << (RANK - 346)) | (1L << (READONLY - 346)) | (1L << (READ_ONLY - 346)) | (1L << (RECOMPILE - 346)) | (1L << (RELATIVE - 346)) | (1L << (REMOTE - 346)) | (1L << (REPEATABLE - 346)) | (1L << (ROOT - 346)) | (1L << (ROW - 346)) | (1L << (ROWGUID - 346)) | (1L << (ROWS - 346)) | (1L << (ROW_NUMBER - 346)) | (1L << (SAMPLE - 346)) | (1L << (SCHEMABINDING - 346)) | (1L << (SCROLL - 346)) | (1L << (SCROLL_LOCKS - 346)) | (1L << (SELF - 346)) | (1L << (SERIALIZABLE - 346)) | (1L << (SNAPSHOT - 346)) | (1L << (SPATIAL_WINDOW_MAX_CELLS - 346)) | (1L << (STATIC - 346)) | (1L << (STATS_STREAM - 346)) | (1L << (STDEV - 346)) | (1L << (STDEVP - 346)) | (1L << (STDDEV_SAMP - 346)) | (1L << (SUM - 346)) | (1L << (STRTOL - 346)) | (1L << (THROW - 346)) | (1L << (TIES - 346)) | (1L << (TIME - 346)) | (1L << (TRY - 346)) | (1L << (TYPE - 346)) | (1L << (TYPE_WARNING - 346)) | (1L << (UNBOUNDED - 346)))) != 0) || ((((_la - 410)) & ~0x3f) == 0 && ((1L << (_la - 410)) & ((1L << (UNCOMMITTED - 410)) | (1L << (UNKNOWN - 410)) | (1L << (USING - 410)) | (1L << (VAR - 410)) | (1L << (VARP - 410)) | (1L << (VIEW_METADATA - 410)) | (1L << (WORK - 410)) | (1L << (XML - 410)) | (1L << (XMLNAMESPACES - 410)) | (1L << (YEAR - 410)) | (1L << (YEARS - 410)) | (1L << (DOUBLE_QUOTE_ID - 410)) | (1L << (BACKTICK_ID - 410)) | (1L << (SQUARE_BRACKET_ID - 410)) | (1L << (ID - 410)))) != 0)) {
					{
					setState(1143);
					table_name();
					setState(1144);
					match(DOT);
					}
				}

				setState(1151);
				switch (_input.LA(1)) {
				case STAR:
					{
					setState(1148);
					match(STAR);
					}
					break;
				case DOLLAR:
					{
					setState(1149);
					match(DOLLAR);
					setState(1150);
					_la = _input.LA(1);
					if ( !(_la==IDENTITY || _la==ROWGUID) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1153);
				column_alias();
				setState(1154);
				match(EQUAL);
				setState(1155);
				expression(0);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1157);
				expression(0);
				setState(1162);
				_la = _input.LA(1);
				if (_la==STORE || _la==AS || _la==FORCESEEK || ((((_la - 215)) & ~0x3f) == 0 && ((1L << (_la - 215)) & ((1L << (ABSOLUTE - 215)) | (1L << (APPLY - 215)) | (1L << (AUTO - 215)) | (1L << (AVG - 215)) | (1L << (BASE64 - 215)) | (1L << (CALLER - 215)) | (1L << (CAST - 215)) | (1L << (CATCH - 215)) | (1L << (CHECKSUM_AGG - 215)) | (1L << (COMMITTED - 215)) | (1L << (CONCAT - 215)) | (1L << (CONCAT_WS - 215)) | (1L << (COOKIE - 215)) | (1L << (COUNT - 215)) | (1L << (COUNT_BIG - 215)) | (1L << (DAY - 215)) | (1L << (DAYS - 215)) | (1L << (DELAY - 215)) | (1L << (DELETED - 215)) | (1L << (DENSE_RANK - 215)) | (1L << (DISABLE - 215)) | (1L << (DYNAMIC - 215)) | (1L << (ENCRYPTION - 215)) | (1L << (EXTRACT - 215)) | (1L << (FAST - 215)) | (1L << (FAST_FORWARD - 215)) | (1L << (FIRST - 215)) | (1L << (FOLLOWING - 215)) | (1L << (FORWARD_ONLY - 215)))) != 0) || ((((_la - 280)) & ~0x3f) == 0 && ((1L << (_la - 280)) & ((1L << (FULLSCAN - 280)) | (1L << (GLOBAL - 280)) | (1L << (GO - 280)) | (1L << (GROUPING - 280)) | (1L << (GROUPING_ID - 280)) | (1L << (HASH - 280)) | (1L << (INSENSITIVE - 280)) | (1L << (INSERTED - 280)) | (1L << (INTERVAL - 280)) | (1L << (ISOLATION - 280)) | (1L << (KEEPFIXED - 280)) | (1L << (KEYSET - 280)) | (1L << (LAST - 280)) | (1L << (LEVEL - 280)) | (1L << (LOCAL - 280)) | (1L << (LOCK_ESCALATION - 280)) | (1L << (LOGIN - 280)) | (1L << (LOOP - 280)) | (1L << (MARK - 280)) | (1L << (MAX - 280)) | (1L << (MIN - 280)) | (1L << (MODIFY - 280)) | (1L << (MONTH - 280)) | (1L << (MONTHS - 280)) | (1L << (NEXT - 280)) | (1L << (NAME - 280)) | (1L << (NOCOUNT - 280)) | (1L << (NOEXPAND - 280)) | (1L << (NORECOMPUTE - 280)) | (1L << (NTILE - 280)) | (1L << (NUMBER - 280)) | (1L << (OFFSET - 280)) | (1L << (ONLY - 280)) | (1L << (OPTIMISTIC - 280)) | (1L << (OPTIMIZE - 280)) | (1L << (OUT - 280)) | (1L << (OUTPUT - 280)) | (1L << (OWNER - 280)) | (1L << (PARTITION - 280)) | (1L << (PATH - 280)))) != 0) || ((((_la - 346)) & ~0x3f) == 0 && ((1L << (_la - 346)) & ((1L << (PRECEDING - 346)) | (1L << (PRIOR - 346)) | (1L << (RANGE - 346)) | (1L << (RANK - 346)) | (1L << (READONLY - 346)) | (1L << (READ_ONLY - 346)) | (1L << (RECOMPILE - 346)) | (1L << (RELATIVE - 346)) | (1L << (REMOTE - 346)) | (1L << (REPEATABLE - 346)) | (1L << (ROOT - 346)) | (1L << (ROW - 346)) | (1L << (ROWGUID - 346)) | (1L << (ROWS - 346)) | (1L << (ROW_NUMBER - 346)) | (1L << (SAMPLE - 346)) | (1L << (SCHEMABINDING - 346)) | (1L << (SCROLL - 346)) | (1L << (SCROLL_LOCKS - 346)) | (1L << (SELF - 346)) | (1L << (SERIALIZABLE - 346)) | (1L << (SNAPSHOT - 346)) | (1L << (SPATIAL_WINDOW_MAX_CELLS - 346)) | (1L << (STATIC - 346)) | (1L << (STATS_STREAM - 346)) | (1L << (STDEV - 346)) | (1L << (STDEVP - 346)) | (1L << (STDDEV_SAMP - 346)) | (1L << (SUM - 346)) | (1L << (STRTOL - 346)) | (1L << (THROW - 346)) | (1L << (TIES - 346)) | (1L << (TIME - 346)) | (1L << (TRY - 346)) | (1L << (TYPE - 346)) | (1L << (TYPE_WARNING - 346)) | (1L << (UNBOUNDED - 346)))) != 0) || ((((_la - 410)) & ~0x3f) == 0 && ((1L << (_la - 410)) & ((1L << (UNCOMMITTED - 410)) | (1L << (UNKNOWN - 410)) | (1L << (USING - 410)) | (1L << (VAR - 410)) | (1L << (VARP - 410)) | (1L << (VIEW_METADATA - 410)) | (1L << (WORK - 410)) | (1L << (XML - 410)) | (1L << (XMLNAMESPACES - 410)) | (1L << (YEAR - 410)) | (1L << (YEARS - 410)) | (1L << (DOUBLE_QUOTE_ID - 410)) | (1L << (BACKTICK_ID - 410)) | (1L << (SQUARE_BRACKET_ID - 410)) | (1L << (ID - 410)) | (1L << (STRING - 410)))) != 0)) {
					{
					setState(1159);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1158);
						match(AS);
						}
					}

					setState(1161);
					column_alias();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Partition_by_clauseContext extends ParserRuleContext {
		public TerminalNode PARTITION() { return getToken(VerdictSQLParser.PARTITION, 0); }
		public TerminalNode BY() { return getToken(VerdictSQLParser.BY, 0); }
		public Expression_listContext expression_list() {
			return getRuleContext(Expression_listContext.class,0);
		}
		public Partition_by_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partition_by_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterPartition_by_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitPartition_by_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitPartition_by_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Partition_by_clauseContext partition_by_clause() throws RecognitionException {
		Partition_by_clauseContext _localctx = new Partition_by_clauseContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_partition_by_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1166);
			match(PARTITION);
			setState(1167);
			match(BY);
			setState(1168);
			expression_list();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_sourceContext extends ParserRuleContext {
		public Table_source_item_joinedContext table_source_item_joined() {
			return getRuleContext(Table_source_item_joinedContext.class,0);
		}
		public Table_sourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_source; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTable_source(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTable_source(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTable_source(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_sourceContext table_source() throws RecognitionException {
		Table_sourceContext _localctx = new Table_sourceContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_table_source);
		try {
			setState(1175);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,140,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1170);
				table_source_item_joined();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1171);
				match(LR_BRACKET);
				setState(1172);
				table_source_item_joined();
				setState(1173);
				match(RR_BRACKET);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_source_item_joinedContext extends ParserRuleContext {
		public Table_source_itemContext table_source_item() {
			return getRuleContext(Table_source_itemContext.class,0);
		}
		public List<Join_partContext> join_part() {
			return getRuleContexts(Join_partContext.class);
		}
		public Join_partContext join_part(int i) {
			return getRuleContext(Join_partContext.class,i);
		}
		public Table_source_item_joinedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_source_item_joined; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTable_source_item_joined(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTable_source_item_joined(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTable_source_item_joined(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_source_item_joinedContext table_source_item_joined() throws RecognitionException {
		Table_source_item_joinedContext _localctx = new Table_source_item_joinedContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_table_source_item_joined);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1177);
			table_source_item();
			setState(1181);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,141,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1178);
					join_part();
					}
					} 
				}
				setState(1183);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,141,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_source_itemContext extends ParserRuleContext {
		public Table_source_itemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_source_item; }
	 
		public Table_source_itemContext() { }
		public void copyFrom(Table_source_itemContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class Hinted_table_name_itemContext extends Table_source_itemContext {
		public Table_name_with_hintContext table_name_with_hint() {
			return getRuleContext(Table_name_with_hintContext.class,0);
		}
		public As_table_aliasContext as_table_alias() {
			return getRuleContext(As_table_aliasContext.class,0);
		}
		public Hinted_table_name_itemContext(Table_source_itemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterHinted_table_name_item(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitHinted_table_name_item(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitHinted_table_name_item(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Derived_table_source_itemContext extends Table_source_itemContext {
		public Derived_tableContext derived_table() {
			return getRuleContext(Derived_tableContext.class,0);
		}
		public As_table_aliasContext as_table_alias() {
			return getRuleContext(As_table_aliasContext.class,0);
		}
		public Column_alias_listContext column_alias_list() {
			return getRuleContext(Column_alias_listContext.class,0);
		}
		public Derived_table_source_itemContext(Table_source_itemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDerived_table_source_item(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDerived_table_source_item(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDerived_table_source_item(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Sample_table_name_itemContext extends Table_source_itemContext {
		public Table_name_with_sampleContext table_name_with_sample() {
			return getRuleContext(Table_name_with_sampleContext.class,0);
		}
		public As_table_aliasContext as_table_alias() {
			return getRuleContext(As_table_aliasContext.class,0);
		}
		public Sample_table_name_itemContext(Table_source_itemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSample_table_name_item(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSample_table_name_item(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSample_table_name_item(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_source_itemContext table_source_item() throws RecognitionException {
		Table_source_itemContext _localctx = new Table_source_itemContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_table_source_item);
		int _la;
		try {
			setState(1199);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,146,_ctx) ) {
			case 1:
				_localctx = new Sample_table_name_itemContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1184);
				table_name_with_sample();
				setState(1186);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,142,_ctx) ) {
				case 1:
					{
					setState(1185);
					as_table_alias();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new Hinted_table_name_itemContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1188);
				table_name_with_hint();
				setState(1190);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,143,_ctx) ) {
				case 1:
					{
					setState(1189);
					as_table_alias();
					}
					break;
				}
				}
				break;
			case 3:
				_localctx = new Derived_table_source_itemContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1192);
				derived_table();
				setState(1197);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,145,_ctx) ) {
				case 1:
					{
					setState(1193);
					as_table_alias();
					setState(1195);
					_la = _input.LA(1);
					if (_la==LR_BRACKET) {
						{
						setState(1194);
						column_alias_list();
						}
					}

					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Change_tableContext extends ParserRuleContext {
		public TerminalNode CHANGETABLE() { return getToken(VerdictSQLParser.CHANGETABLE, 0); }
		public TerminalNode CHANGES() { return getToken(VerdictSQLParser.CHANGES, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode NULL() { return getToken(VerdictSQLParser.NULL, 0); }
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public TerminalNode LOCAL_ID() { return getToken(VerdictSQLParser.LOCAL_ID, 0); }
		public Change_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_change_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterChange_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitChange_table(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitChange_table(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Change_tableContext change_table() throws RecognitionException {
		Change_tableContext _localctx = new Change_tableContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_change_table);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1201);
			match(CHANGETABLE);
			setState(1202);
			match(LR_BRACKET);
			setState(1203);
			match(CHANGES);
			setState(1204);
			table_name();
			setState(1205);
			match(COMMA);
			setState(1206);
			_la = _input.LA(1);
			if ( !(_la==NULL || _la==LOCAL_ID || _la==DECIMAL) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(1207);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Join_partContext extends ParserRuleContext {
		public Token join_type;
		public Token join_hint;
		public TerminalNode JOIN() { return getToken(VerdictSQLParser.JOIN, 0); }
		public Table_sourceContext table_source() {
			return getRuleContext(Table_sourceContext.class,0);
		}
		public TerminalNode ON() { return getToken(VerdictSQLParser.ON, 0); }
		public Search_conditionContext search_condition() {
			return getRuleContext(Search_conditionContext.class,0);
		}
		public TerminalNode LEFT() { return getToken(VerdictSQLParser.LEFT, 0); }
		public TerminalNode RIGHT() { return getToken(VerdictSQLParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(VerdictSQLParser.FULL, 0); }
		public TerminalNode INNER() { return getToken(VerdictSQLParser.INNER, 0); }
		public TerminalNode OUTER() { return getToken(VerdictSQLParser.OUTER, 0); }
		public TerminalNode LOOP() { return getToken(VerdictSQLParser.LOOP, 0); }
		public TerminalNode HASH() { return getToken(VerdictSQLParser.HASH, 0); }
		public TerminalNode MERGE() { return getToken(VerdictSQLParser.MERGE, 0); }
		public TerminalNode REMOTE() { return getToken(VerdictSQLParser.REMOTE, 0); }
		public TerminalNode SEMI() { return getToken(VerdictSQLParser.SEMI, 0); }
		public TerminalNode CROSS() { return getToken(VerdictSQLParser.CROSS, 0); }
		public TerminalNode APPLY() { return getToken(VerdictSQLParser.APPLY, 0); }
		public TerminalNode LATERAL() { return getToken(VerdictSQLParser.LATERAL, 0); }
		public TerminalNode VIEW() { return getToken(VerdictSQLParser.VIEW, 0); }
		public Lateral_view_functionContext lateral_view_function() {
			return getRuleContext(Lateral_view_functionContext.class,0);
		}
		public Table_aliasContext table_alias() {
			return getRuleContext(Table_aliasContext.class,0);
		}
		public Column_aliasContext column_alias() {
			return getRuleContext(Column_aliasContext.class,0);
		}
		public TerminalNode AS() { return getToken(VerdictSQLParser.AS, 0); }
		public Join_partContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_join_part; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterJoin_part(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitJoin_part(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitJoin_part(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Join_partContext join_part() throws RecognitionException {
		Join_partContext _localctx = new Join_partContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_join_part);
		int _la;
		try {
			setState(1247);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,154,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1216);
				switch (_input.LA(1)) {
				case INNER:
				case JOIN:
				case MERGE:
				case SEMI:
				case HASH:
				case LOOP:
				case REMOTE:
					{
					setState(1210);
					_la = _input.LA(1);
					if (_la==INNER) {
						{
						setState(1209);
						match(INNER);
						}
					}

					}
					break;
				case FULL:
				case LEFT:
				case RIGHT:
					{
					setState(1212);
					((Join_partContext)_localctx).join_type = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==FULL || _la==LEFT || _la==RIGHT) ) {
						((Join_partContext)_localctx).join_type = (Token)_errHandler.recoverInline(this);
					} else {
						consume();
					}
					setState(1214);
					_la = _input.LA(1);
					if (_la==OUTER) {
						{
						setState(1213);
						match(OUTER);
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1219);
				_la = _input.LA(1);
				if (_la==MERGE || _la==SEMI || _la==HASH || _la==LOOP || _la==REMOTE) {
					{
					setState(1218);
					((Join_partContext)_localctx).join_hint = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==MERGE || _la==SEMI || _la==HASH || _la==LOOP || _la==REMOTE) ) {
						((Join_partContext)_localctx).join_hint = (Token)_errHandler.recoverInline(this);
					} else {
						consume();
					}
					}
				}

				setState(1221);
				match(JOIN);
				setState(1222);
				table_source();
				setState(1223);
				match(ON);
				setState(1224);
				search_condition();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1226);
				match(CROSS);
				setState(1227);
				match(JOIN);
				setState(1228);
				table_source();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1229);
				match(CROSS);
				setState(1230);
				match(APPLY);
				setState(1231);
				table_source();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1232);
				match(OUTER);
				setState(1233);
				match(APPLY);
				setState(1234);
				table_source();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1235);
				match(LATERAL);
				setState(1236);
				match(VIEW);
				setState(1237);
				lateral_view_function();
				setState(1239);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,151,_ctx) ) {
				case 1:
					{
					setState(1238);
					table_alias();
					}
					break;
				}
				setState(1245);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,153,_ctx) ) {
				case 1:
					{
					setState(1242);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1241);
						match(AS);
						}
					}

					setState(1244);
					column_alias();
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_name_with_hintContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public With_table_hintsContext with_table_hints() {
			return getRuleContext(With_table_hintsContext.class,0);
		}
		public Table_name_with_hintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_name_with_hint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTable_name_with_hint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTable_name_with_hint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTable_name_with_hint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_name_with_hintContext table_name_with_hint() throws RecognitionException {
		Table_name_with_hintContext _localctx = new Table_name_with_hintContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_table_name_with_hint);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1249);
			table_name();
			setState(1251);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,155,_ctx) ) {
			case 1:
				{
				setState(1250);
				with_table_hints();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Rowset_functionContext extends ParserRuleContext {
		public Token data_file;
		public TerminalNode OPENROWSET() { return getToken(VerdictSQLParser.OPENROWSET, 0); }
		public TerminalNode BULK() { return getToken(VerdictSQLParser.BULK, 0); }
		public TerminalNode STRING() { return getToken(VerdictSQLParser.STRING, 0); }
		public List<Bulk_optionContext> bulk_option() {
			return getRuleContexts(Bulk_optionContext.class);
		}
		public Bulk_optionContext bulk_option(int i) {
			return getRuleContext(Bulk_optionContext.class,i);
		}
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Rowset_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowset_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterRowset_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitRowset_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitRowset_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Rowset_functionContext rowset_function() throws RecognitionException {
		Rowset_functionContext _localctx = new Rowset_functionContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_rowset_function);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1253);
			match(OPENROWSET);
			setState(1254);
			match(LR_BRACKET);
			setState(1255);
			match(BULK);
			setState(1256);
			((Rowset_functionContext)_localctx).data_file = match(STRING);
			setState(1257);
			match(COMMA);
			setState(1267);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,157,_ctx) ) {
			case 1:
				{
				setState(1258);
				bulk_option();
				setState(1263);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1259);
					match(COMMA);
					setState(1260);
					bulk_option();
					}
					}
					setState(1265);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				{
				setState(1266);
				id();
				}
				break;
			}
			setState(1269);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Bulk_optionContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public TerminalNode STRING() { return getToken(VerdictSQLParser.STRING, 0); }
		public Bulk_optionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bulk_option; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterBulk_option(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitBulk_option(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitBulk_option(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Bulk_optionContext bulk_option() throws RecognitionException {
		Bulk_optionContext _localctx = new Bulk_optionContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_bulk_option);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1271);
			id();
			setState(1272);
			match(EQUAL);
			setState(1273);
			_la = _input.LA(1);
			if ( !(_la==DECIMAL || _la==STRING) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Derived_tableContext extends ParserRuleContext {
		public SubqueryContext subquery() {
			return getRuleContext(SubqueryContext.class,0);
		}
		public Table_value_constructorContext table_value_constructor() {
			return getRuleContext(Table_value_constructorContext.class,0);
		}
		public Derived_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_derived_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDerived_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDerived_table(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDerived_table(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Derived_tableContext derived_table() throws RecognitionException {
		Derived_tableContext _localctx = new Derived_tableContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_derived_table);
		try {
			setState(1280);
			switch (_input.LA(1)) {
			case LR_BRACKET:
				enterOuterAlt(_localctx, 1);
				{
				setState(1275);
				match(LR_BRACKET);
				setState(1276);
				subquery();
				setState(1277);
				match(RR_BRACKET);
				}
				break;
			case VALUES:
				enterOuterAlt(_localctx, 2);
				{
				setState(1279);
				table_value_constructor();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Function_callContext extends ParserRuleContext {
		public Ranking_windowed_functionContext ranking_windowed_function() {
			return getRuleContext(Ranking_windowed_functionContext.class,0);
		}
		public Value_manipulation_functionContext value_manipulation_function() {
			return getRuleContext(Value_manipulation_functionContext.class,0);
		}
		public Aggregate_windowed_functionContext aggregate_windowed_function() {
			return getRuleContext(Aggregate_windowed_functionContext.class,0);
		}
		public Function_callContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_function_call; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterFunction_call(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitFunction_call(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitFunction_call(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Function_callContext function_call() throws RecognitionException {
		Function_callContext _localctx = new Function_callContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_function_call);
		try {
			setState(1285);
			switch (_input.LA(1)) {
			case DENSE_RANK:
			case NTILE:
			case RANK:
			case ROW_NUMBER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1282);
				ranking_windowed_function();
				}
				break;
			case ASCII:
			case COALESCE:
			case CONV:
			case CURRENT_TIMESTAMP:
			case SUBSTR:
			case ABS:
			case ACOS:
			case AES_DECRYPT:
			case AES_ENCRYPT:
			case ASIN:
			case ATAN:
			case BIN:
			case BROUND:
			case CAST:
			case CBRT:
			case CEIL:
			case CHARACTER_LENGTH:
			case CHR:
			case CONCAT:
			case CONCAT_WS:
			case COS:
			case CRC32:
			case DAY:
			case DECODE:
			case DEGREES:
			case NATURAL_CONSTANT:
			case ENCODE:
			case EXP:
			case EXTRACT:
			case FACTORIAL:
			case FIND_IN_SET:
			case FLOOR:
			case FORMAT_NUMBER:
			case FNV_HASH:
			case FROM_UNIXTIME:
			case GET_JSON_OBJECT:
			case HEX:
			case HOUR:
			case INSTR:
			case IN_FILE:
			case LENGTH:
			case LN:
			case LOCATE:
			case LOG2:
			case LOG10:
			case LOWER:
			case LTRIM:
			case MD5:
			case MINUTE:
			case MOD:
			case MONTH:
			case NEGATIVE:
			case NVL:
			case PERCENTILE:
			case PI:
			case PMOD:
			case POSITIVE:
			case POW:
			case QUARTER:
			case RADIANS:
			case RAND:
			case RANDOM:
			case REPEAT:
			case REVERSE:
			case ROUND:
			case SECOND:
			case SHA1:
			case SHA2:
			case SHIFTLEFT:
			case SHIFTRIGHT:
			case SHIFTRIGHTUNSIGNED:
			case SIGN:
			case SIN:
			case SPACE_FUNCTION:
			case SPLIT:
			case STDDEV:
			case SQRT:
			case STRTOL:
			case TAN:
			case TO_DATE:
			case TRIM:
			case UNHEX:
			case UNIX_TIMESTAMP:
			case UPPER:
			case WEEKOFYEAR:
			case YEAR:
				enterOuterAlt(_localctx, 2);
				{
				setState(1283);
				value_manipulation_function();
				}
				break;
			case AVG:
			case CHECKSUM_AGG:
			case COUNT:
			case COUNT_BIG:
			case GROUPING:
			case GROUPING_ID:
			case MAX:
			case MIN:
			case NDV:
			case STDEV:
			case STDEVP:
			case STDDEV_SAMP:
			case SUM:
			case VAR:
			case VARP:
				enterOuterAlt(_localctx, 3);
				{
				setState(1284);
				aggregate_windowed_function();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatepartContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(VerdictSQLParser.ID, 0); }
		public DatepartContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datepart; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDatepart(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDatepart(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDatepart(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatepartContext datepart() throws RecognitionException {
		DatepartContext _localctx = new DatepartContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_datepart);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1287);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class As_table_aliasContext extends ParserRuleContext {
		public Table_aliasContext table_alias() {
			return getRuleContext(Table_aliasContext.class,0);
		}
		public TerminalNode AS() { return getToken(VerdictSQLParser.AS, 0); }
		public As_table_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_as_table_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterAs_table_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitAs_table_alias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitAs_table_alias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final As_table_aliasContext as_table_alias() throws RecognitionException {
		As_table_aliasContext _localctx = new As_table_aliasContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_as_table_alias);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1290);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1289);
				match(AS);
				}
			}

			setState(1292);
			table_alias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_aliasContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public With_table_hintsContext with_table_hints() {
			return getRuleContext(With_table_hintsContext.class,0);
		}
		public Table_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTable_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTable_alias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTable_alias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_aliasContext table_alias() throws RecognitionException {
		Table_aliasContext _localctx = new Table_aliasContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_table_alias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1294);
			id();
			setState(1296);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,161,_ctx) ) {
			case 1:
				{
				setState(1295);
				with_table_hints();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class With_table_hintsContext extends ParserRuleContext {
		public List<Table_hintContext> table_hint() {
			return getRuleContexts(Table_hintContext.class);
		}
		public Table_hintContext table_hint(int i) {
			return getRuleContext(Table_hintContext.class,i);
		}
		public TerminalNode WITH() { return getToken(VerdictSQLParser.WITH, 0); }
		public With_table_hintsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_table_hints; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterWith_table_hints(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitWith_table_hints(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitWith_table_hints(this);
			else return visitor.visitChildren(this);
		}
	}

	public final With_table_hintsContext with_table_hints() throws RecognitionException {
		With_table_hintsContext _localctx = new With_table_hintsContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_with_table_hints);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1299);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1298);
				match(WITH);
				}
			}

			setState(1301);
			match(LR_BRACKET);
			setState(1302);
			table_hint();
			setState(1307);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1303);
				match(COMMA);
				setState(1304);
				table_hint();
				}
				}
				setState(1309);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1310);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_hintContext extends ParserRuleContext {
		public TerminalNode NOEXPAND() { return getToken(VerdictSQLParser.NOEXPAND, 0); }
		public TerminalNode INDEX() { return getToken(VerdictSQLParser.INDEX, 0); }
		public List<Index_valueContext> index_value() {
			return getRuleContexts(Index_valueContext.class);
		}
		public Index_valueContext index_value(int i) {
			return getRuleContext(Index_valueContext.class,i);
		}
		public TerminalNode FORCESEEK() { return getToken(VerdictSQLParser.FORCESEEK, 0); }
		public TerminalNode SERIALIZABLE() { return getToken(VerdictSQLParser.SERIALIZABLE, 0); }
		public TerminalNode SNAPSHOT() { return getToken(VerdictSQLParser.SNAPSHOT, 0); }
		public TerminalNode SPATIAL_WINDOW_MAX_CELLS() { return getToken(VerdictSQLParser.SPATIAL_WINDOW_MAX_CELLS, 0); }
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public TerminalNode ID() { return getToken(VerdictSQLParser.ID, 0); }
		public List<Index_column_nameContext> index_column_name() {
			return getRuleContexts(Index_column_nameContext.class);
		}
		public Index_column_nameContext index_column_name(int i) {
			return getRuleContext(Index_column_nameContext.class,i);
		}
		public Table_hintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_hint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTable_hint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTable_hint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTable_hint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_hintContext table_hint() throws RecognitionException {
		Table_hintContext _localctx = new Table_hintContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_table_hint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1313);
			_la = _input.LA(1);
			if (_la==NOEXPAND) {
				{
				setState(1312);
				match(NOEXPAND);
				}
			}

			setState(1353);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,168,_ctx) ) {
			case 1:
				{
				setState(1315);
				match(INDEX);
				setState(1316);
				match(LR_BRACKET);
				setState(1317);
				index_value();
				setState(1322);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1318);
					match(COMMA);
					setState(1319);
					index_value();
					}
					}
					setState(1324);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1325);
				match(RR_BRACKET);
				}
				break;
			case 2:
				{
				setState(1327);
				match(INDEX);
				setState(1328);
				match(EQUAL);
				setState(1329);
				index_value();
				}
				break;
			case 3:
				{
				setState(1330);
				match(FORCESEEK);
				setState(1345);
				_la = _input.LA(1);
				if (_la==LR_BRACKET) {
					{
					setState(1331);
					match(LR_BRACKET);
					setState(1332);
					index_value();
					setState(1333);
					match(LR_BRACKET);
					setState(1334);
					index_column_name();
					setState(1339);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(1335);
						match(COMMA);
						setState(1336);
						index_column_name();
						}
						}
						setState(1341);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1342);
					match(RR_BRACKET);
					setState(1343);
					match(RR_BRACKET);
					}
				}

				}
				break;
			case 4:
				{
				setState(1347);
				match(SERIALIZABLE);
				}
				break;
			case 5:
				{
				setState(1348);
				match(SNAPSHOT);
				}
				break;
			case 6:
				{
				setState(1349);
				match(SPATIAL_WINDOW_MAX_CELLS);
				setState(1350);
				match(EQUAL);
				setState(1351);
				match(DECIMAL);
				}
				break;
			case 7:
				{
				setState(1352);
				match(ID);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Index_column_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(VerdictSQLParser.ID, 0); }
		public Index_column_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_column_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterIndex_column_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitIndex_column_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitIndex_column_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Index_column_nameContext index_column_name() throws RecognitionException {
		Index_column_nameContext _localctx = new Index_column_nameContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_index_column_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1355);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Index_valueContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(VerdictSQLParser.ID, 0); }
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public Index_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterIndex_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitIndex_value(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitIndex_value(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Index_valueContext index_value() throws RecognitionException {
		Index_valueContext _localctx = new Index_valueContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_index_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1357);
			_la = _input.LA(1);
			if ( !(_la==DECIMAL || _la==ID) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_alias_listContext extends ParserRuleContext {
		public List<Column_aliasContext> column_alias() {
			return getRuleContexts(Column_aliasContext.class);
		}
		public Column_aliasContext column_alias(int i) {
			return getRuleContext(Column_aliasContext.class,i);
		}
		public Column_alias_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_alias_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterColumn_alias_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitColumn_alias_list(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitColumn_alias_list(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_alias_listContext column_alias_list() throws RecognitionException {
		Column_alias_listContext _localctx = new Column_alias_listContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_column_alias_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1359);
			match(LR_BRACKET);
			setState(1360);
			column_alias();
			setState(1365);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1361);
				match(COMMA);
				setState(1362);
				column_alias();
				}
				}
				setState(1367);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1368);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_aliasContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode STRING() { return getToken(VerdictSQLParser.STRING, 0); }
		public Column_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterColumn_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitColumn_alias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitColumn_alias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_aliasContext column_alias() throws RecognitionException {
		Column_aliasContext _localctx = new Column_aliasContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_column_alias);
		try {
			setState(1372);
			switch (_input.LA(1)) {
			case STORE:
			case FORCESEEK:
			case ABSOLUTE:
			case APPLY:
			case AUTO:
			case AVG:
			case BASE64:
			case CALLER:
			case CAST:
			case CATCH:
			case CHECKSUM_AGG:
			case COMMITTED:
			case CONCAT:
			case CONCAT_WS:
			case COOKIE:
			case COUNT:
			case COUNT_BIG:
			case DAY:
			case DAYS:
			case DELAY:
			case DELETED:
			case DENSE_RANK:
			case DISABLE:
			case DYNAMIC:
			case ENCRYPTION:
			case EXTRACT:
			case FAST:
			case FAST_FORWARD:
			case FIRST:
			case FOLLOWING:
			case FORWARD_ONLY:
			case FULLSCAN:
			case GLOBAL:
			case GO:
			case GROUPING:
			case GROUPING_ID:
			case HASH:
			case INSENSITIVE:
			case INSERTED:
			case INTERVAL:
			case ISOLATION:
			case KEEPFIXED:
			case KEYSET:
			case LAST:
			case LEVEL:
			case LOCAL:
			case LOCK_ESCALATION:
			case LOGIN:
			case LOOP:
			case MARK:
			case MAX:
			case MIN:
			case MODIFY:
			case MONTH:
			case MONTHS:
			case NEXT:
			case NAME:
			case NOCOUNT:
			case NOEXPAND:
			case NORECOMPUTE:
			case NTILE:
			case NUMBER:
			case OFFSET:
			case ONLY:
			case OPTIMISTIC:
			case OPTIMIZE:
			case OUT:
			case OUTPUT:
			case OWNER:
			case PARTITION:
			case PATH:
			case PRECEDING:
			case PRIOR:
			case RANGE:
			case RANK:
			case READONLY:
			case READ_ONLY:
			case RECOMPILE:
			case RELATIVE:
			case REMOTE:
			case REPEATABLE:
			case ROOT:
			case ROW:
			case ROWGUID:
			case ROWS:
			case ROW_NUMBER:
			case SAMPLE:
			case SCHEMABINDING:
			case SCROLL:
			case SCROLL_LOCKS:
			case SELF:
			case SERIALIZABLE:
			case SNAPSHOT:
			case SPATIAL_WINDOW_MAX_CELLS:
			case STATIC:
			case STATS_STREAM:
			case STDEV:
			case STDEVP:
			case STDDEV_SAMP:
			case SUM:
			case STRTOL:
			case THROW:
			case TIES:
			case TIME:
			case TRY:
			case TYPE:
			case TYPE_WARNING:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNKNOWN:
			case USING:
			case VAR:
			case VARP:
			case VIEW_METADATA:
			case WORK:
			case XML:
			case XMLNAMESPACES:
			case YEAR:
			case YEARS:
			case DOUBLE_QUOTE_ID:
			case BACKTICK_ID:
			case SQUARE_BRACKET_ID:
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(1370);
				id();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1371);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_value_constructorContext extends ParserRuleContext {
		public TerminalNode VALUES() { return getToken(VerdictSQLParser.VALUES, 0); }
		public List<Expression_listContext> expression_list() {
			return getRuleContexts(Expression_listContext.class);
		}
		public Expression_listContext expression_list(int i) {
			return getRuleContext(Expression_listContext.class,i);
		}
		public Table_value_constructorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_value_constructor; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTable_value_constructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTable_value_constructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTable_value_constructor(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_value_constructorContext table_value_constructor() throws RecognitionException {
		Table_value_constructorContext _localctx = new Table_value_constructorContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_table_value_constructor);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1374);
			match(VALUES);
			setState(1375);
			match(LR_BRACKET);
			setState(1376);
			expression_list();
			setState(1377);
			match(RR_BRACKET);
			setState(1385);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,171,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1378);
					match(COMMA);
					setState(1379);
					match(LR_BRACKET);
					setState(1380);
					expression_list();
					setState(1381);
					match(RR_BRACKET);
					}
					} 
				}
				setState(1387);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,171,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Expression_listContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public Expression_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterExpression_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitExpression_list(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitExpression_list(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Expression_listContext expression_list() throws RecognitionException {
		Expression_listContext _localctx = new Expression_listContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_expression_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1388);
			expression(0);
			setState(1393);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1389);
				match(COMMA);
				setState(1390);
				expression(0);
				}
				}
				setState(1395);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Case_exprContext extends ParserRuleContext {
		public TerminalNode CASE() { return getToken(VerdictSQLParser.CASE, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode END() { return getToken(VerdictSQLParser.END, 0); }
		public List<TerminalNode> WHEN() { return getTokens(VerdictSQLParser.WHEN); }
		public TerminalNode WHEN(int i) {
			return getToken(VerdictSQLParser.WHEN, i);
		}
		public List<TerminalNode> THEN() { return getTokens(VerdictSQLParser.THEN); }
		public TerminalNode THEN(int i) {
			return getToken(VerdictSQLParser.THEN, i);
		}
		public TerminalNode ELSE() { return getToken(VerdictSQLParser.ELSE, 0); }
		public List<Search_conditionContext> search_condition() {
			return getRuleContexts(Search_conditionContext.class);
		}
		public Search_conditionContext search_condition(int i) {
			return getRuleContext(Search_conditionContext.class,i);
		}
		public Case_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_case_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterCase_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitCase_expr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitCase_expr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Case_exprContext case_expr() throws RecognitionException {
		Case_exprContext _localctx = new Case_exprContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_case_expr);
		int _la;
		try {
			setState(1429);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,177,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1396);
				match(CASE);
				setState(1397);
				expression(0);
				setState(1403); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1398);
					match(WHEN);
					setState(1399);
					expression(0);
					setState(1400);
					match(THEN);
					setState(1401);
					expression(0);
					}
					}
					setState(1405); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1409);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1407);
					match(ELSE);
					setState(1408);
					expression(0);
					}
				}

				setState(1411);
				match(END);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1413);
				match(CASE);
				setState(1419); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1414);
					match(WHEN);
					setState(1415);
					search_condition();
					setState(1416);
					match(THEN);
					setState(1417);
					expression(0);
					}
					}
					setState(1421); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1425);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1423);
					match(ELSE);
					setState(1424);
					expression(0);
					}
				}

				setState(1427);
				match(END);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Ranking_windowed_functionContext extends ParserRuleContext {
		public TerminalNode RANK() { return getToken(VerdictSQLParser.RANK, 0); }
		public Over_clauseContext over_clause() {
			return getRuleContext(Over_clauseContext.class,0);
		}
		public TerminalNode DENSE_RANK() { return getToken(VerdictSQLParser.DENSE_RANK, 0); }
		public TerminalNode NTILE() { return getToken(VerdictSQLParser.NTILE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode ROW_NUMBER() { return getToken(VerdictSQLParser.ROW_NUMBER, 0); }
		public Ranking_windowed_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ranking_windowed_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterRanking_windowed_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitRanking_windowed_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitRanking_windowed_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Ranking_windowed_functionContext ranking_windowed_function() throws RecognitionException {
		Ranking_windowed_functionContext _localctx = new Ranking_windowed_functionContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_ranking_windowed_function);
		try {
			setState(1449);
			switch (_input.LA(1)) {
			case RANK:
				enterOuterAlt(_localctx, 1);
				{
				setState(1431);
				match(RANK);
				setState(1432);
				match(LR_BRACKET);
				setState(1433);
				match(RR_BRACKET);
				setState(1434);
				over_clause();
				}
				break;
			case DENSE_RANK:
				enterOuterAlt(_localctx, 2);
				{
				setState(1435);
				match(DENSE_RANK);
				setState(1436);
				match(LR_BRACKET);
				setState(1437);
				match(RR_BRACKET);
				setState(1438);
				over_clause();
				}
				break;
			case NTILE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1439);
				match(NTILE);
				setState(1440);
				match(LR_BRACKET);
				setState(1441);
				expression(0);
				setState(1442);
				match(RR_BRACKET);
				setState(1443);
				over_clause();
				}
				break;
			case ROW_NUMBER:
				enterOuterAlt(_localctx, 4);
				{
				setState(1445);
				match(ROW_NUMBER);
				setState(1446);
				match(LR_BRACKET);
				setState(1447);
				match(RR_BRACKET);
				setState(1448);
				over_clause();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Value_manipulation_functionContext extends ParserRuleContext {
		public Unary_manipulation_functionContext unary_manipulation_function() {
			return getRuleContext(Unary_manipulation_functionContext.class,0);
		}
		public Noparam_manipulation_functionContext noparam_manipulation_function() {
			return getRuleContext(Noparam_manipulation_functionContext.class,0);
		}
		public Binary_manipulation_functionContext binary_manipulation_function() {
			return getRuleContext(Binary_manipulation_functionContext.class,0);
		}
		public Ternary_manipulation_functionContext ternary_manipulation_function() {
			return getRuleContext(Ternary_manipulation_functionContext.class,0);
		}
		public Nary_manipulation_functionContext nary_manipulation_function() {
			return getRuleContext(Nary_manipulation_functionContext.class,0);
		}
		public Extract_time_functionContext extract_time_function() {
			return getRuleContext(Extract_time_functionContext.class,0);
		}
		public Value_manipulation_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_value_manipulation_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterValue_manipulation_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitValue_manipulation_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitValue_manipulation_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Value_manipulation_functionContext value_manipulation_function() throws RecognitionException {
		Value_manipulation_functionContext _localctx = new Value_manipulation_functionContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_value_manipulation_function);
		try {
			setState(1457);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,179,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1451);
				unary_manipulation_function();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1452);
				noparam_manipulation_function();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1453);
				binary_manipulation_function();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1454);
				ternary_manipulation_function();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1455);
				nary_manipulation_function();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1456);
				extract_time_function();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Nary_manipulation_functionContext extends ParserRuleContext {
		public Token function_name;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode CONCAT() { return getToken(VerdictSQLParser.CONCAT, 0); }
		public TerminalNode CONCAT_WS() { return getToken(VerdictSQLParser.CONCAT_WS, 0); }
		public TerminalNode COALESCE() { return getToken(VerdictSQLParser.COALESCE, 0); }
		public Nary_manipulation_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nary_manipulation_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterNary_manipulation_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitNary_manipulation_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitNary_manipulation_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Nary_manipulation_functionContext nary_manipulation_function() throws RecognitionException {
		Nary_manipulation_functionContext _localctx = new Nary_manipulation_functionContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_nary_manipulation_function);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1459);
			((Nary_manipulation_functionContext)_localctx).function_name = _input.LT(1);
			_la = _input.LA(1);
			if ( !(_la==COALESCE || _la==CONCAT || _la==CONCAT_WS) ) {
				((Nary_manipulation_functionContext)_localctx).function_name = (Token)_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(1460);
			match(LR_BRACKET);
			setState(1461);
			expression(0);
			setState(1466);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1462);
				match(COMMA);
				setState(1463);
				expression(0);
				}
				}
				setState(1468);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1469);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Ternary_manipulation_functionContext extends ParserRuleContext {
		public Token function_name;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode CONV() { return getToken(VerdictSQLParser.CONV, 0); }
		public TerminalNode SUBSTR() { return getToken(VerdictSQLParser.SUBSTR, 0); }
		public Ternary_manipulation_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ternary_manipulation_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTernary_manipulation_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTernary_manipulation_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTernary_manipulation_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Ternary_manipulation_functionContext ternary_manipulation_function() throws RecognitionException {
		Ternary_manipulation_functionContext _localctx = new Ternary_manipulation_functionContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_ternary_manipulation_function);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1471);
			((Ternary_manipulation_functionContext)_localctx).function_name = _input.LT(1);
			_la = _input.LA(1);
			if ( !(_la==CONV || _la==SUBSTR) ) {
				((Ternary_manipulation_functionContext)_localctx).function_name = (Token)_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(1472);
			match(LR_BRACKET);
			setState(1473);
			expression(0);
			setState(1474);
			match(COMMA);
			setState(1475);
			expression(0);
			setState(1476);
			match(COMMA);
			setState(1477);
			expression(0);
			setState(1478);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Binary_manipulation_functionContext extends ParserRuleContext {
		public Token function_name;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ROUND() { return getToken(VerdictSQLParser.ROUND, 0); }
		public TerminalNode MOD() { return getToken(VerdictSQLParser.MOD, 0); }
		public TerminalNode PMOD() { return getToken(VerdictSQLParser.PMOD, 0); }
		public TerminalNode STRTOL() { return getToken(VerdictSQLParser.STRTOL, 0); }
		public TerminalNode POW() { return getToken(VerdictSQLParser.POW, 0); }
		public TerminalNode PERCENTILE() { return getToken(VerdictSQLParser.PERCENTILE, 0); }
		public TerminalNode SPLIT() { return getToken(VerdictSQLParser.SPLIT, 0); }
		public TerminalNode INSTR() { return getToken(VerdictSQLParser.INSTR, 0); }
		public TerminalNode ENCODE() { return getToken(VerdictSQLParser.ENCODE, 0); }
		public TerminalNode DECODE() { return getToken(VerdictSQLParser.DECODE, 0); }
		public TerminalNode SHIFTLEFT() { return getToken(VerdictSQLParser.SHIFTLEFT, 0); }
		public TerminalNode SHIFTRIGHT() { return getToken(VerdictSQLParser.SHIFTRIGHT, 0); }
		public TerminalNode SHIFTRIGHTUNSIGNED() { return getToken(VerdictSQLParser.SHIFTRIGHTUNSIGNED, 0); }
		public TerminalNode NVL() { return getToken(VerdictSQLParser.NVL, 0); }
		public TerminalNode FIND_IN_SET() { return getToken(VerdictSQLParser.FIND_IN_SET, 0); }
		public TerminalNode FORMAT_NUMBER() { return getToken(VerdictSQLParser.FORMAT_NUMBER, 0); }
		public TerminalNode GET_JSON_OBJECT() { return getToken(VerdictSQLParser.GET_JSON_OBJECT, 0); }
		public TerminalNode IN_FILE() { return getToken(VerdictSQLParser.IN_FILE, 0); }
		public TerminalNode LOCATE() { return getToken(VerdictSQLParser.LOCATE, 0); }
		public TerminalNode REPEAT() { return getToken(VerdictSQLParser.REPEAT, 0); }
		public TerminalNode AES_ENCRYPT() { return getToken(VerdictSQLParser.AES_ENCRYPT, 0); }
		public TerminalNode AES_DECRYPT() { return getToken(VerdictSQLParser.AES_DECRYPT, 0); }
		public Binary_manipulation_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_binary_manipulation_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterBinary_manipulation_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitBinary_manipulation_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitBinary_manipulation_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Binary_manipulation_functionContext binary_manipulation_function() throws RecognitionException {
		Binary_manipulation_functionContext _localctx = new Binary_manipulation_functionContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_binary_manipulation_function);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1480);
			((Binary_manipulation_functionContext)_localctx).function_name = _input.LT(1);
			_la = _input.LA(1);
			if ( !(((((_la - 218)) & ~0x3f) == 0 && ((1L << (_la - 218)) & ((1L << (AES_DECRYPT - 218)) | (1L << (AES_ENCRYPT - 218)) | (1L << (DECODE - 218)) | (1L << (ENCODE - 218)) | (1L << (FIND_IN_SET - 218)) | (1L << (FORMAT_NUMBER - 218)) | (1L << (GET_JSON_OBJECT - 218)))) != 0) || ((((_la - 291)) & ~0x3f) == 0 && ((1L << (_la - 291)) & ((1L << (INSTR - 291)) | (1L << (IN_FILE - 291)) | (1L << (LOCATE - 291)) | (1L << (MOD - 291)) | (1L << (NVL - 291)) | (1L << (PERCENTILE - 291)) | (1L << (PMOD - 291)) | (1L << (POW - 291)))) != 0) || ((((_la - 362)) & ~0x3f) == 0 && ((1L << (_la - 362)) & ((1L << (REPEAT - 362)) | (1L << (ROUND - 362)) | (1L << (SHIFTLEFT - 362)) | (1L << (SHIFTRIGHT - 362)) | (1L << (SHIFTRIGHTUNSIGNED - 362)) | (1L << (SPLIT - 362)) | (1L << (STRTOL - 362)))) != 0)) ) {
				((Binary_manipulation_functionContext)_localctx).function_name = (Token)_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(1481);
			match(LR_BRACKET);
			setState(1482);
			expression(0);
			setState(1483);
			match(COMMA);
			setState(1484);
			expression(0);
			setState(1485);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Extract_time_functionContext extends ParserRuleContext {
		public Token function_name;
		public Extract_unitContext extract_unit() {
			return getRuleContext(Extract_unitContext.class,0);
		}
		public TerminalNode FROM() { return getToken(VerdictSQLParser.FROM, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode EXTRACT() { return getToken(VerdictSQLParser.EXTRACT, 0); }
		public Extract_time_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_extract_time_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterExtract_time_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitExtract_time_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitExtract_time_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Extract_time_functionContext extract_time_function() throws RecognitionException {
		Extract_time_functionContext _localctx = new Extract_time_functionContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_extract_time_function);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1487);
			((Extract_time_functionContext)_localctx).function_name = match(EXTRACT);
			setState(1488);
			match(LR_BRACKET);
			setState(1489);
			extract_unit();
			setState(1490);
			match(FROM);
			setState(1491);
			expression(0);
			setState(1492);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Extract_unitContext extends ParserRuleContext {
		public TerminalNode YEAR() { return getToken(VerdictSQLParser.YEAR, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Extract_unitContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_extract_unit; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterExtract_unit(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitExtract_unit(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitExtract_unit(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Extract_unitContext extract_unit() throws RecognitionException {
		Extract_unitContext _localctx = new Extract_unitContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_extract_unit);
		try {
			setState(1496);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,181,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1494);
				match(YEAR);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1495);
				expression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Unary_manipulation_functionContext extends ParserRuleContext {
		public Token function_name;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode ROUND() { return getToken(VerdictSQLParser.ROUND, 0); }
		public TerminalNode FLOOR() { return getToken(VerdictSQLParser.FLOOR, 0); }
		public TerminalNode CEIL() { return getToken(VerdictSQLParser.CEIL, 0); }
		public TerminalNode EXP() { return getToken(VerdictSQLParser.EXP, 0); }
		public TerminalNode LN() { return getToken(VerdictSQLParser.LN, 0); }
		public TerminalNode LOG10() { return getToken(VerdictSQLParser.LOG10, 0); }
		public TerminalNode LOG2() { return getToken(VerdictSQLParser.LOG2, 0); }
		public TerminalNode SIN() { return getToken(VerdictSQLParser.SIN, 0); }
		public TerminalNode COS() { return getToken(VerdictSQLParser.COS, 0); }
		public TerminalNode TAN() { return getToken(VerdictSQLParser.TAN, 0); }
		public TerminalNode SIGN() { return getToken(VerdictSQLParser.SIGN, 0); }
		public TerminalNode RAND() { return getToken(VerdictSQLParser.RAND, 0); }
		public TerminalNode FNV_HASH() { return getToken(VerdictSQLParser.FNV_HASH, 0); }
		public TerminalNode ABS() { return getToken(VerdictSQLParser.ABS, 0); }
		public TerminalNode STDDEV() { return getToken(VerdictSQLParser.STDDEV, 0); }
		public TerminalNode SQRT() { return getToken(VerdictSQLParser.SQRT, 0); }
		public TerminalNode MD5() { return getToken(VerdictSQLParser.MD5, 0); }
		public TerminalNode CRC32() { return getToken(VerdictSQLParser.CRC32, 0); }
		public TerminalNode YEAR() { return getToken(VerdictSQLParser.YEAR, 0); }
		public TerminalNode QUARTER() { return getToken(VerdictSQLParser.QUARTER, 0); }
		public TerminalNode MONTH() { return getToken(VerdictSQLParser.MONTH, 0); }
		public TerminalNode DAY() { return getToken(VerdictSQLParser.DAY, 0); }
		public TerminalNode HOUR() { return getToken(VerdictSQLParser.HOUR, 0); }
		public TerminalNode MINUTE() { return getToken(VerdictSQLParser.MINUTE, 0); }
		public TerminalNode SECOND() { return getToken(VerdictSQLParser.SECOND, 0); }
		public TerminalNode WEEKOFYEAR() { return getToken(VerdictSQLParser.WEEKOFYEAR, 0); }
		public TerminalNode LOWER() { return getToken(VerdictSQLParser.LOWER, 0); }
		public TerminalNode UPPER() { return getToken(VerdictSQLParser.UPPER, 0); }
		public TerminalNode ASCII() { return getToken(VerdictSQLParser.ASCII, 0); }
		public TerminalNode CHARACTER_LENGTH() { return getToken(VerdictSQLParser.CHARACTER_LENGTH, 0); }
		public TerminalNode FACTORIAL() { return getToken(VerdictSQLParser.FACTORIAL, 0); }
		public TerminalNode CBRT() { return getToken(VerdictSQLParser.CBRT, 0); }
		public TerminalNode LENGTH() { return getToken(VerdictSQLParser.LENGTH, 0); }
		public TerminalNode TRIM() { return getToken(VerdictSQLParser.TRIM, 0); }
		public TerminalNode ASIN() { return getToken(VerdictSQLParser.ASIN, 0); }
		public TerminalNode ACOS() { return getToken(VerdictSQLParser.ACOS, 0); }
		public TerminalNode ATAN() { return getToken(VerdictSQLParser.ATAN, 0); }
		public TerminalNode DEGREES() { return getToken(VerdictSQLParser.DEGREES, 0); }
		public TerminalNode RADIANS() { return getToken(VerdictSQLParser.RADIANS, 0); }
		public TerminalNode POSITIVE() { return getToken(VerdictSQLParser.POSITIVE, 0); }
		public TerminalNode NEGATIVE() { return getToken(VerdictSQLParser.NEGATIVE, 0); }
		public TerminalNode BROUND() { return getToken(VerdictSQLParser.BROUND, 0); }
		public TerminalNode BIN() { return getToken(VerdictSQLParser.BIN, 0); }
		public TerminalNode HEX() { return getToken(VerdictSQLParser.HEX, 0); }
		public TerminalNode UNHEX() { return getToken(VerdictSQLParser.UNHEX, 0); }
		public TerminalNode FROM_UNIXTIME() { return getToken(VerdictSQLParser.FROM_UNIXTIME, 0); }
		public TerminalNode TO_DATE() { return getToken(VerdictSQLParser.TO_DATE, 0); }
		public TerminalNode CHR() { return getToken(VerdictSQLParser.CHR, 0); }
		public TerminalNode LTRIM() { return getToken(VerdictSQLParser.LTRIM, 0); }
		public TerminalNode REVERSE() { return getToken(VerdictSQLParser.REVERSE, 0); }
		public TerminalNode SPACE_FUNCTION() { return getToken(VerdictSQLParser.SPACE_FUNCTION, 0); }
		public TerminalNode SHA1() { return getToken(VerdictSQLParser.SHA1, 0); }
		public TerminalNode SHA2() { return getToken(VerdictSQLParser.SHA2, 0); }
		public Cast_as_expressionContext cast_as_expression() {
			return getRuleContext(Cast_as_expressionContext.class,0);
		}
		public TerminalNode CAST() { return getToken(VerdictSQLParser.CAST, 0); }
		public Unary_manipulation_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unary_manipulation_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterUnary_manipulation_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitUnary_manipulation_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitUnary_manipulation_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Unary_manipulation_functionContext unary_manipulation_function() throws RecognitionException {
		Unary_manipulation_functionContext _localctx = new Unary_manipulation_functionContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_unary_manipulation_function);
		int _la;
		try {
			setState(1508);
			switch (_input.LA(1)) {
			case ASCII:
			case ABS:
			case ACOS:
			case ASIN:
			case ATAN:
			case BIN:
			case BROUND:
			case CBRT:
			case CEIL:
			case CHARACTER_LENGTH:
			case CHR:
			case COS:
			case CRC32:
			case DAY:
			case DEGREES:
			case EXP:
			case FACTORIAL:
			case FLOOR:
			case FNV_HASH:
			case FROM_UNIXTIME:
			case HEX:
			case HOUR:
			case LENGTH:
			case LN:
			case LOG2:
			case LOG10:
			case LOWER:
			case LTRIM:
			case MD5:
			case MINUTE:
			case MONTH:
			case NEGATIVE:
			case POSITIVE:
			case QUARTER:
			case RADIANS:
			case RAND:
			case REVERSE:
			case ROUND:
			case SECOND:
			case SHA1:
			case SHA2:
			case SIGN:
			case SIN:
			case SPACE_FUNCTION:
			case STDDEV:
			case SQRT:
			case TAN:
			case TO_DATE:
			case TRIM:
			case UNHEX:
			case UPPER:
			case WEEKOFYEAR:
			case YEAR:
				enterOuterAlt(_localctx, 1);
				{
				setState(1498);
				((Unary_manipulation_functionContext)_localctx).function_name = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASCII || ((((_la - 216)) & ~0x3f) == 0 && ((1L << (_la - 216)) & ((1L << (ABS - 216)) | (1L << (ACOS - 216)) | (1L << (ASIN - 216)) | (1L << (ATAN - 216)) | (1L << (BIN - 216)) | (1L << (BROUND - 216)) | (1L << (CBRT - 216)) | (1L << (CEIL - 216)) | (1L << (CHARACTER_LENGTH - 216)) | (1L << (CHR - 216)) | (1L << (COS - 216)) | (1L << (CRC32 - 216)) | (1L << (DAY - 216)) | (1L << (DEGREES - 216)) | (1L << (EXP - 216)) | (1L << (FACTORIAL - 216)) | (1L << (FLOOR - 216)) | (1L << (FNV_HASH - 216)) | (1L << (FROM_UNIXTIME - 216)))) != 0) || ((((_la - 287)) & ~0x3f) == 0 && ((1L << (_la - 287)) & ((1L << (HEX - 287)) | (1L << (HOUR - 287)) | (1L << (LENGTH - 287)) | (1L << (LN - 287)) | (1L << (LOG2 - 287)) | (1L << (LOG10 - 287)) | (1L << (LOWER - 287)) | (1L << (LTRIM - 287)) | (1L << (MD5 - 287)) | (1L << (MINUTE - 287)) | (1L << (MONTH - 287)) | (1L << (NEGATIVE - 287)) | (1L << (POSITIVE - 287)) | (1L << (QUARTER - 287)) | (1L << (RADIANS - 287)))) != 0) || ((((_la - 351)) & ~0x3f) == 0 && ((1L << (_la - 351)) & ((1L << (RAND - 351)) | (1L << (REVERSE - 351)) | (1L << (ROUND - 351)) | (1L << (SECOND - 351)) | (1L << (SHA1 - 351)) | (1L << (SHA2 - 351)) | (1L << (SIGN - 351)) | (1L << (SIN - 351)) | (1L << (SPACE_FUNCTION - 351)) | (1L << (STDDEV - 351)) | (1L << (SQRT - 351)) | (1L << (TAN - 351)) | (1L << (TO_DATE - 351)) | (1L << (TRIM - 351)) | (1L << (UNHEX - 351)))) != 0) || ((((_la - 415)) & ~0x3f) == 0 && ((1L << (_la - 415)) & ((1L << (UPPER - 415)) | (1L << (WEEKOFYEAR - 415)) | (1L << (YEAR - 415)))) != 0)) ) {
					((Unary_manipulation_functionContext)_localctx).function_name = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(1499);
				match(LR_BRACKET);
				setState(1500);
				expression(0);
				setState(1501);
				match(RR_BRACKET);
				}
				break;
			case CAST:
				enterOuterAlt(_localctx, 2);
				{
				setState(1503);
				((Unary_manipulation_functionContext)_localctx).function_name = match(CAST);
				setState(1504);
				match(LR_BRACKET);
				setState(1505);
				cast_as_expression();
				setState(1506);
				match(RR_BRACKET);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Noparam_manipulation_functionContext extends ParserRuleContext {
		public Token function_name;
		public TerminalNode UNIX_TIMESTAMP() { return getToken(VerdictSQLParser.UNIX_TIMESTAMP, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(VerdictSQLParser.CURRENT_TIMESTAMP, 0); }
		public TerminalNode RANDOM() { return getToken(VerdictSQLParser.RANDOM, 0); }
		public TerminalNode NATURAL_CONSTANT() { return getToken(VerdictSQLParser.NATURAL_CONSTANT, 0); }
		public TerminalNode PI() { return getToken(VerdictSQLParser.PI, 0); }
		public Noparam_manipulation_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_noparam_manipulation_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterNoparam_manipulation_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitNoparam_manipulation_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitNoparam_manipulation_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Noparam_manipulation_functionContext noparam_manipulation_function() throws RecognitionException {
		Noparam_manipulation_functionContext _localctx = new Noparam_manipulation_functionContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_noparam_manipulation_function);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1510);
			((Noparam_manipulation_functionContext)_localctx).function_name = _input.LT(1);
			_la = _input.LA(1);
			if ( !(_la==CURRENT_TIMESTAMP || _la==NATURAL_CONSTANT || _la==PI || _la==RANDOM || _la==UNIX_TIMESTAMP) ) {
				((Noparam_manipulation_functionContext)_localctx).function_name = (Token)_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(1511);
			match(LR_BRACKET);
			setState(1512);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Lateral_view_functionContext extends ParserRuleContext {
		public Token function_name;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode EXPLODE() { return getToken(VerdictSQLParser.EXPLODE, 0); }
		public Lateral_view_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lateral_view_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterLateral_view_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitLateral_view_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitLateral_view_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Lateral_view_functionContext lateral_view_function() throws RecognitionException {
		Lateral_view_functionContext _localctx = new Lateral_view_functionContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_lateral_view_function);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1514);
			((Lateral_view_functionContext)_localctx).function_name = match(EXPLODE);
			setState(1515);
			match(LR_BRACKET);
			setState(1516);
			expression(0);
			setState(1517);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Aggregate_windowed_functionContext extends ParserRuleContext {
		public TerminalNode AVG() { return getToken(VerdictSQLParser.AVG, 0); }
		public All_distinct_expressionContext all_distinct_expression() {
			return getRuleContext(All_distinct_expressionContext.class,0);
		}
		public Over_clauseContext over_clause() {
			return getRuleContext(Over_clauseContext.class,0);
		}
		public TerminalNode CHECKSUM_AGG() { return getToken(VerdictSQLParser.CHECKSUM_AGG, 0); }
		public TerminalNode GROUPING() { return getToken(VerdictSQLParser.GROUPING, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode GROUPING_ID() { return getToken(VerdictSQLParser.GROUPING_ID, 0); }
		public Expression_listContext expression_list() {
			return getRuleContext(Expression_listContext.class,0);
		}
		public TerminalNode MAX() { return getToken(VerdictSQLParser.MAX, 0); }
		public TerminalNode MIN() { return getToken(VerdictSQLParser.MIN, 0); }
		public TerminalNode SUM() { return getToken(VerdictSQLParser.SUM, 0); }
		public TerminalNode STDEV() { return getToken(VerdictSQLParser.STDEV, 0); }
		public TerminalNode STDEVP() { return getToken(VerdictSQLParser.STDEVP, 0); }
		public TerminalNode STDDEV_SAMP() { return getToken(VerdictSQLParser.STDDEV_SAMP, 0); }
		public TerminalNode VAR() { return getToken(VerdictSQLParser.VAR, 0); }
		public TerminalNode VARP() { return getToken(VerdictSQLParser.VARP, 0); }
		public TerminalNode COUNT() { return getToken(VerdictSQLParser.COUNT, 0); }
		public TerminalNode NDV() { return getToken(VerdictSQLParser.NDV, 0); }
		public TerminalNode COUNT_BIG() { return getToken(VerdictSQLParser.COUNT_BIG, 0); }
		public Aggregate_windowed_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregate_windowed_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterAggregate_windowed_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitAggregate_windowed_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitAggregate_windowed_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Aggregate_windowed_functionContext aggregate_windowed_function() throws RecognitionException {
		Aggregate_windowed_functionContext _localctx = new Aggregate_windowed_functionContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_aggregate_windowed_function);
		try {
			setState(1624);
			switch (_input.LA(1)) {
			case AVG:
				enterOuterAlt(_localctx, 1);
				{
				setState(1519);
				match(AVG);
				setState(1520);
				match(LR_BRACKET);
				setState(1521);
				all_distinct_expression();
				setState(1522);
				match(RR_BRACKET);
				setState(1524);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,183,_ctx) ) {
				case 1:
					{
					setState(1523);
					over_clause();
					}
					break;
				}
				}
				break;
			case CHECKSUM_AGG:
				enterOuterAlt(_localctx, 2);
				{
				setState(1526);
				match(CHECKSUM_AGG);
				setState(1527);
				match(LR_BRACKET);
				setState(1528);
				all_distinct_expression();
				setState(1529);
				match(RR_BRACKET);
				}
				break;
			case GROUPING:
				enterOuterAlt(_localctx, 3);
				{
				setState(1531);
				match(GROUPING);
				setState(1532);
				match(LR_BRACKET);
				setState(1533);
				expression(0);
				setState(1534);
				match(RR_BRACKET);
				}
				break;
			case GROUPING_ID:
				enterOuterAlt(_localctx, 4);
				{
				setState(1536);
				match(GROUPING_ID);
				setState(1537);
				match(LR_BRACKET);
				setState(1538);
				expression_list();
				setState(1539);
				match(RR_BRACKET);
				}
				break;
			case MAX:
				enterOuterAlt(_localctx, 5);
				{
				setState(1541);
				match(MAX);
				setState(1542);
				match(LR_BRACKET);
				setState(1543);
				all_distinct_expression();
				setState(1544);
				match(RR_BRACKET);
				setState(1546);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,184,_ctx) ) {
				case 1:
					{
					setState(1545);
					over_clause();
					}
					break;
				}
				}
				break;
			case MIN:
				enterOuterAlt(_localctx, 6);
				{
				setState(1548);
				match(MIN);
				setState(1549);
				match(LR_BRACKET);
				setState(1550);
				all_distinct_expression();
				setState(1551);
				match(RR_BRACKET);
				setState(1553);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,185,_ctx) ) {
				case 1:
					{
					setState(1552);
					over_clause();
					}
					break;
				}
				}
				break;
			case SUM:
				enterOuterAlt(_localctx, 7);
				{
				setState(1555);
				match(SUM);
				setState(1556);
				match(LR_BRACKET);
				setState(1557);
				all_distinct_expression();
				setState(1558);
				match(RR_BRACKET);
				setState(1560);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,186,_ctx) ) {
				case 1:
					{
					setState(1559);
					over_clause();
					}
					break;
				}
				}
				break;
			case STDEV:
				enterOuterAlt(_localctx, 8);
				{
				setState(1562);
				match(STDEV);
				setState(1563);
				match(LR_BRACKET);
				setState(1564);
				all_distinct_expression();
				setState(1565);
				match(RR_BRACKET);
				setState(1567);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,187,_ctx) ) {
				case 1:
					{
					setState(1566);
					over_clause();
					}
					break;
				}
				}
				break;
			case STDEVP:
				enterOuterAlt(_localctx, 9);
				{
				setState(1569);
				match(STDEVP);
				setState(1570);
				match(LR_BRACKET);
				setState(1571);
				all_distinct_expression();
				setState(1572);
				match(RR_BRACKET);
				setState(1574);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,188,_ctx) ) {
				case 1:
					{
					setState(1573);
					over_clause();
					}
					break;
				}
				}
				break;
			case STDDEV_SAMP:
				enterOuterAlt(_localctx, 10);
				{
				setState(1576);
				match(STDDEV_SAMP);
				setState(1577);
				match(LR_BRACKET);
				setState(1578);
				all_distinct_expression();
				setState(1579);
				match(RR_BRACKET);
				setState(1581);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,189,_ctx) ) {
				case 1:
					{
					setState(1580);
					over_clause();
					}
					break;
				}
				}
				break;
			case VAR:
				enterOuterAlt(_localctx, 11);
				{
				setState(1583);
				match(VAR);
				setState(1584);
				match(LR_BRACKET);
				setState(1585);
				all_distinct_expression();
				setState(1586);
				match(RR_BRACKET);
				setState(1588);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,190,_ctx) ) {
				case 1:
					{
					setState(1587);
					over_clause();
					}
					break;
				}
				}
				break;
			case VARP:
				enterOuterAlt(_localctx, 12);
				{
				setState(1590);
				match(VARP);
				setState(1591);
				match(LR_BRACKET);
				setState(1592);
				all_distinct_expression();
				setState(1593);
				match(RR_BRACKET);
				setState(1595);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,191,_ctx) ) {
				case 1:
					{
					setState(1594);
					over_clause();
					}
					break;
				}
				}
				break;
			case COUNT:
				enterOuterAlt(_localctx, 13);
				{
				setState(1597);
				match(COUNT);
				setState(1598);
				match(LR_BRACKET);
				setState(1601);
				switch (_input.LA(1)) {
				case STAR:
					{
					setState(1599);
					match(STAR);
					}
					break;
				case STORE:
				case ALL:
				case ASCII:
				case CASE:
				case COALESCE:
				case CONV:
				case CURRENT_TIMESTAMP:
				case DISTINCT:
				case FALSE:
				case FORCESEEK:
				case NULL:
				case SUBSTR:
				case TRUE:
				case ABSOLUTE:
				case ABS:
				case ACOS:
				case AES_DECRYPT:
				case AES_ENCRYPT:
				case APPLY:
				case ASIN:
				case ATAN:
				case AUTO:
				case AVG:
				case BASE64:
				case BIN:
				case BROUND:
				case CALLER:
				case CAST:
				case CATCH:
				case CBRT:
				case CEIL:
				case CHARACTER_LENGTH:
				case CHECKSUM_AGG:
				case CHR:
				case COMMITTED:
				case CONCAT:
				case CONCAT_WS:
				case COOKIE:
				case COS:
				case COUNT:
				case COUNT_BIG:
				case CRC32:
				case DAY:
				case DAYS:
				case DECODE:
				case DEGREES:
				case DELAY:
				case DELETED:
				case DENSE_RANK:
				case DISABLE:
				case DYNAMIC:
				case NATURAL_CONSTANT:
				case ENCODE:
				case ENCRYPTION:
				case EXP:
				case EXTRACT:
				case FACTORIAL:
				case FAST:
				case FAST_FORWARD:
				case FIND_IN_SET:
				case FIRST:
				case FLOOR:
				case FOLLOWING:
				case FORMAT_NUMBER:
				case FORWARD_ONLY:
				case FNV_HASH:
				case FROM_UNIXTIME:
				case FULLSCAN:
				case GET_JSON_OBJECT:
				case GLOBAL:
				case GO:
				case GROUPING:
				case GROUPING_ID:
				case HASH:
				case HEX:
				case HOUR:
				case INSENSITIVE:
				case INSERTED:
				case INSTR:
				case INTERVAL:
				case IN_FILE:
				case ISOLATION:
				case KEEPFIXED:
				case KEYSET:
				case LAST:
				case LENGTH:
				case LEVEL:
				case LN:
				case LOCAL:
				case LOCATE:
				case LOCK_ESCALATION:
				case LOG2:
				case LOG10:
				case LOGIN:
				case LOOP:
				case LOWER:
				case LTRIM:
				case MARK:
				case MAX:
				case MD5:
				case MIN:
				case MINUTE:
				case MOD:
				case MODIFY:
				case MONTH:
				case MONTHS:
				case NEGATIVE:
				case NEXT:
				case NAME:
				case NDV:
				case NOCOUNT:
				case NOEXPAND:
				case NORECOMPUTE:
				case NTILE:
				case NUMBER:
				case NVL:
				case OFFSET:
				case ONLY:
				case OPTIMISTIC:
				case OPTIMIZE:
				case OUT:
				case OUTPUT:
				case OWNER:
				case PARTITION:
				case PATH:
				case PERCENTILE:
				case PI:
				case PMOD:
				case POSITIVE:
				case POW:
				case PRECEDING:
				case PRIOR:
				case QUARTER:
				case RADIANS:
				case RAND:
				case RANDOM:
				case RANGE:
				case RANK:
				case READONLY:
				case READ_ONLY:
				case RECOMPILE:
				case RELATIVE:
				case REMOTE:
				case REPEAT:
				case REPEATABLE:
				case REVERSE:
				case ROOT:
				case ROUND:
				case ROW:
				case ROWGUID:
				case ROWS:
				case ROW_NUMBER:
				case SAMPLE:
				case SCHEMABINDING:
				case SCROLL:
				case SCROLL_LOCKS:
				case SECOND:
				case SELF:
				case SERIALIZABLE:
				case SHA1:
				case SHA2:
				case SHIFTLEFT:
				case SHIFTRIGHT:
				case SHIFTRIGHTUNSIGNED:
				case SIGN:
				case SIN:
				case SNAPSHOT:
				case SPACE_FUNCTION:
				case SPATIAL_WINDOW_MAX_CELLS:
				case SPLIT:
				case STATIC:
				case STATS_STREAM:
				case STDEV:
				case STDDEV:
				case STDEVP:
				case STDDEV_SAMP:
				case SUM:
				case SQRT:
				case STRTOL:
				case TAN:
				case THROW:
				case TIES:
				case TIME:
				case TO_DATE:
				case TRIM:
				case TRY:
				case TYPE:
				case TYPE_WARNING:
				case UNBOUNDED:
				case UNCOMMITTED:
				case UNHEX:
				case UNIX_TIMESTAMP:
				case UNKNOWN:
				case UPPER:
				case USING:
				case VAR:
				case VARP:
				case VIEW_METADATA:
				case WEEKOFYEAR:
				case WORK:
				case XML:
				case XMLNAMESPACES:
				case YEAR:
				case YEARS:
				case DOUBLE_QUOTE_ID:
				case BACKTICK_ID:
				case SQUARE_BRACKET_ID:
				case LOCAL_ID:
				case DECIMAL:
				case ID:
				case STRING:
				case BINARY:
				case FLOAT:
				case REAL:
				case DOLLAR:
				case LR_BRACKET:
				case PLUS:
				case MINUS:
				case BIT_NOT:
					{
					setState(1600);
					all_distinct_expression();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1603);
				match(RR_BRACKET);
				setState(1605);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,193,_ctx) ) {
				case 1:
					{
					setState(1604);
					over_clause();
					}
					break;
				}
				}
				break;
			case NDV:
				enterOuterAlt(_localctx, 14);
				{
				setState(1607);
				match(NDV);
				setState(1608);
				match(LR_BRACKET);
				setState(1609);
				all_distinct_expression();
				setState(1610);
				match(RR_BRACKET);
				setState(1612);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,194,_ctx) ) {
				case 1:
					{
					setState(1611);
					over_clause();
					}
					break;
				}
				}
				break;
			case COUNT_BIG:
				enterOuterAlt(_localctx, 15);
				{
				setState(1614);
				match(COUNT_BIG);
				setState(1615);
				match(LR_BRACKET);
				setState(1618);
				switch (_input.LA(1)) {
				case STAR:
					{
					setState(1616);
					match(STAR);
					}
					break;
				case STORE:
				case ALL:
				case ASCII:
				case CASE:
				case COALESCE:
				case CONV:
				case CURRENT_TIMESTAMP:
				case DISTINCT:
				case FALSE:
				case FORCESEEK:
				case NULL:
				case SUBSTR:
				case TRUE:
				case ABSOLUTE:
				case ABS:
				case ACOS:
				case AES_DECRYPT:
				case AES_ENCRYPT:
				case APPLY:
				case ASIN:
				case ATAN:
				case AUTO:
				case AVG:
				case BASE64:
				case BIN:
				case BROUND:
				case CALLER:
				case CAST:
				case CATCH:
				case CBRT:
				case CEIL:
				case CHARACTER_LENGTH:
				case CHECKSUM_AGG:
				case CHR:
				case COMMITTED:
				case CONCAT:
				case CONCAT_WS:
				case COOKIE:
				case COS:
				case COUNT:
				case COUNT_BIG:
				case CRC32:
				case DAY:
				case DAYS:
				case DECODE:
				case DEGREES:
				case DELAY:
				case DELETED:
				case DENSE_RANK:
				case DISABLE:
				case DYNAMIC:
				case NATURAL_CONSTANT:
				case ENCODE:
				case ENCRYPTION:
				case EXP:
				case EXTRACT:
				case FACTORIAL:
				case FAST:
				case FAST_FORWARD:
				case FIND_IN_SET:
				case FIRST:
				case FLOOR:
				case FOLLOWING:
				case FORMAT_NUMBER:
				case FORWARD_ONLY:
				case FNV_HASH:
				case FROM_UNIXTIME:
				case FULLSCAN:
				case GET_JSON_OBJECT:
				case GLOBAL:
				case GO:
				case GROUPING:
				case GROUPING_ID:
				case HASH:
				case HEX:
				case HOUR:
				case INSENSITIVE:
				case INSERTED:
				case INSTR:
				case INTERVAL:
				case IN_FILE:
				case ISOLATION:
				case KEEPFIXED:
				case KEYSET:
				case LAST:
				case LENGTH:
				case LEVEL:
				case LN:
				case LOCAL:
				case LOCATE:
				case LOCK_ESCALATION:
				case LOG2:
				case LOG10:
				case LOGIN:
				case LOOP:
				case LOWER:
				case LTRIM:
				case MARK:
				case MAX:
				case MD5:
				case MIN:
				case MINUTE:
				case MOD:
				case MODIFY:
				case MONTH:
				case MONTHS:
				case NEGATIVE:
				case NEXT:
				case NAME:
				case NDV:
				case NOCOUNT:
				case NOEXPAND:
				case NORECOMPUTE:
				case NTILE:
				case NUMBER:
				case NVL:
				case OFFSET:
				case ONLY:
				case OPTIMISTIC:
				case OPTIMIZE:
				case OUT:
				case OUTPUT:
				case OWNER:
				case PARTITION:
				case PATH:
				case PERCENTILE:
				case PI:
				case PMOD:
				case POSITIVE:
				case POW:
				case PRECEDING:
				case PRIOR:
				case QUARTER:
				case RADIANS:
				case RAND:
				case RANDOM:
				case RANGE:
				case RANK:
				case READONLY:
				case READ_ONLY:
				case RECOMPILE:
				case RELATIVE:
				case REMOTE:
				case REPEAT:
				case REPEATABLE:
				case REVERSE:
				case ROOT:
				case ROUND:
				case ROW:
				case ROWGUID:
				case ROWS:
				case ROW_NUMBER:
				case SAMPLE:
				case SCHEMABINDING:
				case SCROLL:
				case SCROLL_LOCKS:
				case SECOND:
				case SELF:
				case SERIALIZABLE:
				case SHA1:
				case SHA2:
				case SHIFTLEFT:
				case SHIFTRIGHT:
				case SHIFTRIGHTUNSIGNED:
				case SIGN:
				case SIN:
				case SNAPSHOT:
				case SPACE_FUNCTION:
				case SPATIAL_WINDOW_MAX_CELLS:
				case SPLIT:
				case STATIC:
				case STATS_STREAM:
				case STDEV:
				case STDDEV:
				case STDEVP:
				case STDDEV_SAMP:
				case SUM:
				case SQRT:
				case STRTOL:
				case TAN:
				case THROW:
				case TIES:
				case TIME:
				case TO_DATE:
				case TRIM:
				case TRY:
				case TYPE:
				case TYPE_WARNING:
				case UNBOUNDED:
				case UNCOMMITTED:
				case UNHEX:
				case UNIX_TIMESTAMP:
				case UNKNOWN:
				case UPPER:
				case USING:
				case VAR:
				case VARP:
				case VIEW_METADATA:
				case WEEKOFYEAR:
				case WORK:
				case XML:
				case XMLNAMESPACES:
				case YEAR:
				case YEARS:
				case DOUBLE_QUOTE_ID:
				case BACKTICK_ID:
				case SQUARE_BRACKET_ID:
				case LOCAL_ID:
				case DECIMAL:
				case ID:
				case STRING:
				case BINARY:
				case FLOAT:
				case REAL:
				case DOLLAR:
				case LR_BRACKET:
				case PLUS:
				case MINUS:
				case BIT_NOT:
					{
					setState(1617);
					all_distinct_expression();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1620);
				match(RR_BRACKET);
				setState(1622);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,196,_ctx) ) {
				case 1:
					{
					setState(1621);
					over_clause();
					}
					break;
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class All_distinct_expressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode ALL() { return getToken(VerdictSQLParser.ALL, 0); }
		public TerminalNode DISTINCT() { return getToken(VerdictSQLParser.DISTINCT, 0); }
		public All_distinct_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_all_distinct_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterAll_distinct_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitAll_distinct_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitAll_distinct_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final All_distinct_expressionContext all_distinct_expression() throws RecognitionException {
		All_distinct_expressionContext _localctx = new All_distinct_expressionContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_all_distinct_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1627);
			_la = _input.LA(1);
			if (_la==ALL || _la==DISTINCT) {
				{
				setState(1626);
				_la = _input.LA(1);
				if ( !(_la==ALL || _la==DISTINCT) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			setState(1629);
			expression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Cast_as_expressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(VerdictSQLParser.AS, 0); }
		public Data_typeContext data_type() {
			return getRuleContext(Data_typeContext.class,0);
		}
		public Cast_as_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_cast_as_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterCast_as_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitCast_as_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitCast_as_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Cast_as_expressionContext cast_as_expression() throws RecognitionException {
		Cast_as_expressionContext _localctx = new Cast_as_expressionContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_cast_as_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1631);
			expression(0);
			setState(1632);
			match(AS);
			setState(1633);
			data_type();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Over_clauseContext extends ParserRuleContext {
		public TerminalNode OVER() { return getToken(VerdictSQLParser.OVER, 0); }
		public Partition_by_clauseContext partition_by_clause() {
			return getRuleContext(Partition_by_clauseContext.class,0);
		}
		public Order_by_clauseContext order_by_clause() {
			return getRuleContext(Order_by_clauseContext.class,0);
		}
		public Row_or_range_clauseContext row_or_range_clause() {
			return getRuleContext(Row_or_range_clauseContext.class,0);
		}
		public Over_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_over_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOver_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOver_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOver_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Over_clauseContext over_clause() throws RecognitionException {
		Over_clauseContext _localctx = new Over_clauseContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_over_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1635);
			match(OVER);
			setState(1636);
			match(LR_BRACKET);
			setState(1638);
			_la = _input.LA(1);
			if (_la==PARTITION) {
				{
				setState(1637);
				partition_by_clause();
				}
			}

			setState(1641);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1640);
				order_by_clause();
				}
			}

			setState(1644);
			_la = _input.LA(1);
			if (_la==RANGE || _la==ROWS) {
				{
				setState(1643);
				row_or_range_clause();
				}
			}

			setState(1646);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Row_or_range_clauseContext extends ParserRuleContext {
		public Window_frame_extentContext window_frame_extent() {
			return getRuleContext(Window_frame_extentContext.class,0);
		}
		public TerminalNode ROWS() { return getToken(VerdictSQLParser.ROWS, 0); }
		public TerminalNode RANGE() { return getToken(VerdictSQLParser.RANGE, 0); }
		public Row_or_range_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_row_or_range_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterRow_or_range_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitRow_or_range_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitRow_or_range_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Row_or_range_clauseContext row_or_range_clause() throws RecognitionException {
		Row_or_range_clauseContext _localctx = new Row_or_range_clauseContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_row_or_range_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1648);
			_la = _input.LA(1);
			if ( !(_la==RANGE || _la==ROWS) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(1649);
			window_frame_extent();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Window_frame_extentContext extends ParserRuleContext {
		public Window_frame_precedingContext window_frame_preceding() {
			return getRuleContext(Window_frame_precedingContext.class,0);
		}
		public TerminalNode BETWEEN() { return getToken(VerdictSQLParser.BETWEEN, 0); }
		public List<Window_frame_boundContext> window_frame_bound() {
			return getRuleContexts(Window_frame_boundContext.class);
		}
		public Window_frame_boundContext window_frame_bound(int i) {
			return getRuleContext(Window_frame_boundContext.class,i);
		}
		public TerminalNode AND() { return getToken(VerdictSQLParser.AND, 0); }
		public Window_frame_extentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_window_frame_extent; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterWindow_frame_extent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitWindow_frame_extent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitWindow_frame_extent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Window_frame_extentContext window_frame_extent() throws RecognitionException {
		Window_frame_extentContext _localctx = new Window_frame_extentContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_window_frame_extent);
		try {
			setState(1657);
			switch (_input.LA(1)) {
			case CURRENT:
			case UNBOUNDED:
			case DECIMAL:
				enterOuterAlt(_localctx, 1);
				{
				setState(1651);
				window_frame_preceding();
				}
				break;
			case BETWEEN:
				enterOuterAlt(_localctx, 2);
				{
				setState(1652);
				match(BETWEEN);
				setState(1653);
				window_frame_bound();
				setState(1654);
				match(AND);
				setState(1655);
				window_frame_bound();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Window_frame_boundContext extends ParserRuleContext {
		public Window_frame_precedingContext window_frame_preceding() {
			return getRuleContext(Window_frame_precedingContext.class,0);
		}
		public Window_frame_followingContext window_frame_following() {
			return getRuleContext(Window_frame_followingContext.class,0);
		}
		public Window_frame_boundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_window_frame_bound; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterWindow_frame_bound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitWindow_frame_bound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitWindow_frame_bound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Window_frame_boundContext window_frame_bound() throws RecognitionException {
		Window_frame_boundContext _localctx = new Window_frame_boundContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_window_frame_bound);
		try {
			setState(1661);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,203,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1659);
				window_frame_preceding();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1660);
				window_frame_following();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Window_frame_precedingContext extends ParserRuleContext {
		public TerminalNode UNBOUNDED() { return getToken(VerdictSQLParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(VerdictSQLParser.PRECEDING, 0); }
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public TerminalNode CURRENT() { return getToken(VerdictSQLParser.CURRENT, 0); }
		public TerminalNode ROW() { return getToken(VerdictSQLParser.ROW, 0); }
		public Window_frame_precedingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_window_frame_preceding; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterWindow_frame_preceding(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitWindow_frame_preceding(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitWindow_frame_preceding(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Window_frame_precedingContext window_frame_preceding() throws RecognitionException {
		Window_frame_precedingContext _localctx = new Window_frame_precedingContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_window_frame_preceding);
		try {
			setState(1669);
			switch (_input.LA(1)) {
			case UNBOUNDED:
				enterOuterAlt(_localctx, 1);
				{
				setState(1663);
				match(UNBOUNDED);
				setState(1664);
				match(PRECEDING);
				}
				break;
			case DECIMAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(1665);
				match(DECIMAL);
				setState(1666);
				match(PRECEDING);
				}
				break;
			case CURRENT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1667);
				match(CURRENT);
				setState(1668);
				match(ROW);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Window_frame_followingContext extends ParserRuleContext {
		public TerminalNode UNBOUNDED() { return getToken(VerdictSQLParser.UNBOUNDED, 0); }
		public TerminalNode FOLLOWING() { return getToken(VerdictSQLParser.FOLLOWING, 0); }
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public Window_frame_followingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_window_frame_following; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterWindow_frame_following(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitWindow_frame_following(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitWindow_frame_following(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Window_frame_followingContext window_frame_following() throws RecognitionException {
		Window_frame_followingContext _localctx = new Window_frame_followingContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_window_frame_following);
		try {
			setState(1675);
			switch (_input.LA(1)) {
			case UNBOUNDED:
				enterOuterAlt(_localctx, 1);
				{
				setState(1671);
				match(UNBOUNDED);
				setState(1672);
				match(FOLLOWING);
				}
				break;
			case DECIMAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(1673);
				match(DECIMAL);
				setState(1674);
				match(FOLLOWING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Full_table_nameContext extends ParserRuleContext {
		public IdContext server;
		public IdContext database;
		public IdContext schema;
		public IdContext table;
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public Full_table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_full_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterFull_table_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitFull_table_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitFull_table_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Full_table_nameContext full_table_name() throws RecognitionException {
		Full_table_nameContext _localctx = new Full_table_nameContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_full_table_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1694);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,207,_ctx) ) {
			case 1:
				{
				setState(1677);
				((Full_table_nameContext)_localctx).server = id();
				setState(1678);
				match(DOT);
				setState(1679);
				((Full_table_nameContext)_localctx).database = id();
				setState(1680);
				match(DOT);
				setState(1681);
				((Full_table_nameContext)_localctx).schema = id();
				setState(1682);
				match(DOT);
				}
				break;
			case 2:
				{
				setState(1684);
				((Full_table_nameContext)_localctx).database = id();
				setState(1685);
				match(DOT);
				setState(1687);
				_la = _input.LA(1);
				if (_la==STORE || _la==FORCESEEK || ((((_la - 215)) & ~0x3f) == 0 && ((1L << (_la - 215)) & ((1L << (ABSOLUTE - 215)) | (1L << (APPLY - 215)) | (1L << (AUTO - 215)) | (1L << (AVG - 215)) | (1L << (BASE64 - 215)) | (1L << (CALLER - 215)) | (1L << (CAST - 215)) | (1L << (CATCH - 215)) | (1L << (CHECKSUM_AGG - 215)) | (1L << (COMMITTED - 215)) | (1L << (CONCAT - 215)) | (1L << (CONCAT_WS - 215)) | (1L << (COOKIE - 215)) | (1L << (COUNT - 215)) | (1L << (COUNT_BIG - 215)) | (1L << (DAY - 215)) | (1L << (DAYS - 215)) | (1L << (DELAY - 215)) | (1L << (DELETED - 215)) | (1L << (DENSE_RANK - 215)) | (1L << (DISABLE - 215)) | (1L << (DYNAMIC - 215)) | (1L << (ENCRYPTION - 215)) | (1L << (EXTRACT - 215)) | (1L << (FAST - 215)) | (1L << (FAST_FORWARD - 215)) | (1L << (FIRST - 215)) | (1L << (FOLLOWING - 215)) | (1L << (FORWARD_ONLY - 215)))) != 0) || ((((_la - 280)) & ~0x3f) == 0 && ((1L << (_la - 280)) & ((1L << (FULLSCAN - 280)) | (1L << (GLOBAL - 280)) | (1L << (GO - 280)) | (1L << (GROUPING - 280)) | (1L << (GROUPING_ID - 280)) | (1L << (HASH - 280)) | (1L << (INSENSITIVE - 280)) | (1L << (INSERTED - 280)) | (1L << (INTERVAL - 280)) | (1L << (ISOLATION - 280)) | (1L << (KEEPFIXED - 280)) | (1L << (KEYSET - 280)) | (1L << (LAST - 280)) | (1L << (LEVEL - 280)) | (1L << (LOCAL - 280)) | (1L << (LOCK_ESCALATION - 280)) | (1L << (LOGIN - 280)) | (1L << (LOOP - 280)) | (1L << (MARK - 280)) | (1L << (MAX - 280)) | (1L << (MIN - 280)) | (1L << (MODIFY - 280)) | (1L << (MONTH - 280)) | (1L << (MONTHS - 280)) | (1L << (NEXT - 280)) | (1L << (NAME - 280)) | (1L << (NOCOUNT - 280)) | (1L << (NOEXPAND - 280)) | (1L << (NORECOMPUTE - 280)) | (1L << (NTILE - 280)) | (1L << (NUMBER - 280)) | (1L << (OFFSET - 280)) | (1L << (ONLY - 280)) | (1L << (OPTIMISTIC - 280)) | (1L << (OPTIMIZE - 280)) | (1L << (OUT - 280)) | (1L << (OUTPUT - 280)) | (1L << (OWNER - 280)) | (1L << (PARTITION - 280)) | (1L << (PATH - 280)))) != 0) || ((((_la - 346)) & ~0x3f) == 0 && ((1L << (_la - 346)) & ((1L << (PRECEDING - 346)) | (1L << (PRIOR - 346)) | (1L << (RANGE - 346)) | (1L << (RANK - 346)) | (1L << (READONLY - 346)) | (1L << (READ_ONLY - 346)) | (1L << (RECOMPILE - 346)) | (1L << (RELATIVE - 346)) | (1L << (REMOTE - 346)) | (1L << (REPEATABLE - 346)) | (1L << (ROOT - 346)) | (1L << (ROW - 346)) | (1L << (ROWGUID - 346)) | (1L << (ROWS - 346)) | (1L << (ROW_NUMBER - 346)) | (1L << (SAMPLE - 346)) | (1L << (SCHEMABINDING - 346)) | (1L << (SCROLL - 346)) | (1L << (SCROLL_LOCKS - 346)) | (1L << (SELF - 346)) | (1L << (SERIALIZABLE - 346)) | (1L << (SNAPSHOT - 346)) | (1L << (SPATIAL_WINDOW_MAX_CELLS - 346)) | (1L << (STATIC - 346)) | (1L << (STATS_STREAM - 346)) | (1L << (STDEV - 346)) | (1L << (STDEVP - 346)) | (1L << (STDDEV_SAMP - 346)) | (1L << (SUM - 346)) | (1L << (STRTOL - 346)) | (1L << (THROW - 346)) | (1L << (TIES - 346)) | (1L << (TIME - 346)) | (1L << (TRY - 346)) | (1L << (TYPE - 346)) | (1L << (TYPE_WARNING - 346)) | (1L << (UNBOUNDED - 346)))) != 0) || ((((_la - 410)) & ~0x3f) == 0 && ((1L << (_la - 410)) & ((1L << (UNCOMMITTED - 410)) | (1L << (UNKNOWN - 410)) | (1L << (USING - 410)) | (1L << (VAR - 410)) | (1L << (VARP - 410)) | (1L << (VIEW_METADATA - 410)) | (1L << (WORK - 410)) | (1L << (XML - 410)) | (1L << (XMLNAMESPACES - 410)) | (1L << (YEAR - 410)) | (1L << (YEARS - 410)) | (1L << (DOUBLE_QUOTE_ID - 410)) | (1L << (BACKTICK_ID - 410)) | (1L << (SQUARE_BRACKET_ID - 410)) | (1L << (ID - 410)))) != 0)) {
					{
					setState(1686);
					((Full_table_nameContext)_localctx).schema = id();
					}
				}

				setState(1689);
				match(DOT);
				}
				break;
			case 3:
				{
				setState(1691);
				((Full_table_nameContext)_localctx).schema = id();
				setState(1692);
				match(DOT);
				}
				break;
			}
			setState(1696);
			((Full_table_nameContext)_localctx).table = id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_nameContext extends ParserRuleContext {
		public IdContext database;
		public IdContext schema;
		public IdContext table;
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public Table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTable_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTable_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTable_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_nameContext table_name() throws RecognitionException {
		Table_nameContext _localctx = new Table_nameContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_table_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1708);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,209,_ctx) ) {
			case 1:
				{
				setState(1698);
				((Table_nameContext)_localctx).database = id();
				setState(1699);
				match(DOT);
				setState(1701);
				_la = _input.LA(1);
				if (_la==STORE || _la==FORCESEEK || ((((_la - 215)) & ~0x3f) == 0 && ((1L << (_la - 215)) & ((1L << (ABSOLUTE - 215)) | (1L << (APPLY - 215)) | (1L << (AUTO - 215)) | (1L << (AVG - 215)) | (1L << (BASE64 - 215)) | (1L << (CALLER - 215)) | (1L << (CAST - 215)) | (1L << (CATCH - 215)) | (1L << (CHECKSUM_AGG - 215)) | (1L << (COMMITTED - 215)) | (1L << (CONCAT - 215)) | (1L << (CONCAT_WS - 215)) | (1L << (COOKIE - 215)) | (1L << (COUNT - 215)) | (1L << (COUNT_BIG - 215)) | (1L << (DAY - 215)) | (1L << (DAYS - 215)) | (1L << (DELAY - 215)) | (1L << (DELETED - 215)) | (1L << (DENSE_RANK - 215)) | (1L << (DISABLE - 215)) | (1L << (DYNAMIC - 215)) | (1L << (ENCRYPTION - 215)) | (1L << (EXTRACT - 215)) | (1L << (FAST - 215)) | (1L << (FAST_FORWARD - 215)) | (1L << (FIRST - 215)) | (1L << (FOLLOWING - 215)) | (1L << (FORWARD_ONLY - 215)))) != 0) || ((((_la - 280)) & ~0x3f) == 0 && ((1L << (_la - 280)) & ((1L << (FULLSCAN - 280)) | (1L << (GLOBAL - 280)) | (1L << (GO - 280)) | (1L << (GROUPING - 280)) | (1L << (GROUPING_ID - 280)) | (1L << (HASH - 280)) | (1L << (INSENSITIVE - 280)) | (1L << (INSERTED - 280)) | (1L << (INTERVAL - 280)) | (1L << (ISOLATION - 280)) | (1L << (KEEPFIXED - 280)) | (1L << (KEYSET - 280)) | (1L << (LAST - 280)) | (1L << (LEVEL - 280)) | (1L << (LOCAL - 280)) | (1L << (LOCK_ESCALATION - 280)) | (1L << (LOGIN - 280)) | (1L << (LOOP - 280)) | (1L << (MARK - 280)) | (1L << (MAX - 280)) | (1L << (MIN - 280)) | (1L << (MODIFY - 280)) | (1L << (MONTH - 280)) | (1L << (MONTHS - 280)) | (1L << (NEXT - 280)) | (1L << (NAME - 280)) | (1L << (NOCOUNT - 280)) | (1L << (NOEXPAND - 280)) | (1L << (NORECOMPUTE - 280)) | (1L << (NTILE - 280)) | (1L << (NUMBER - 280)) | (1L << (OFFSET - 280)) | (1L << (ONLY - 280)) | (1L << (OPTIMISTIC - 280)) | (1L << (OPTIMIZE - 280)) | (1L << (OUT - 280)) | (1L << (OUTPUT - 280)) | (1L << (OWNER - 280)) | (1L << (PARTITION - 280)) | (1L << (PATH - 280)))) != 0) || ((((_la - 346)) & ~0x3f) == 0 && ((1L << (_la - 346)) & ((1L << (PRECEDING - 346)) | (1L << (PRIOR - 346)) | (1L << (RANGE - 346)) | (1L << (RANK - 346)) | (1L << (READONLY - 346)) | (1L << (READ_ONLY - 346)) | (1L << (RECOMPILE - 346)) | (1L << (RELATIVE - 346)) | (1L << (REMOTE - 346)) | (1L << (REPEATABLE - 346)) | (1L << (ROOT - 346)) | (1L << (ROW - 346)) | (1L << (ROWGUID - 346)) | (1L << (ROWS - 346)) | (1L << (ROW_NUMBER - 346)) | (1L << (SAMPLE - 346)) | (1L << (SCHEMABINDING - 346)) | (1L << (SCROLL - 346)) | (1L << (SCROLL_LOCKS - 346)) | (1L << (SELF - 346)) | (1L << (SERIALIZABLE - 346)) | (1L << (SNAPSHOT - 346)) | (1L << (SPATIAL_WINDOW_MAX_CELLS - 346)) | (1L << (STATIC - 346)) | (1L << (STATS_STREAM - 346)) | (1L << (STDEV - 346)) | (1L << (STDEVP - 346)) | (1L << (STDDEV_SAMP - 346)) | (1L << (SUM - 346)) | (1L << (STRTOL - 346)) | (1L << (THROW - 346)) | (1L << (TIES - 346)) | (1L << (TIME - 346)) | (1L << (TRY - 346)) | (1L << (TYPE - 346)) | (1L << (TYPE_WARNING - 346)) | (1L << (UNBOUNDED - 346)))) != 0) || ((((_la - 410)) & ~0x3f) == 0 && ((1L << (_la - 410)) & ((1L << (UNCOMMITTED - 410)) | (1L << (UNKNOWN - 410)) | (1L << (USING - 410)) | (1L << (VAR - 410)) | (1L << (VARP - 410)) | (1L << (VIEW_METADATA - 410)) | (1L << (WORK - 410)) | (1L << (XML - 410)) | (1L << (XMLNAMESPACES - 410)) | (1L << (YEAR - 410)) | (1L << (YEARS - 410)) | (1L << (DOUBLE_QUOTE_ID - 410)) | (1L << (BACKTICK_ID - 410)) | (1L << (SQUARE_BRACKET_ID - 410)) | (1L << (ID - 410)))) != 0)) {
					{
					setState(1700);
					((Table_nameContext)_localctx).schema = id();
					}
				}

				setState(1703);
				match(DOT);
				}
				break;
			case 2:
				{
				setState(1705);
				((Table_nameContext)_localctx).schema = id();
				setState(1706);
				match(DOT);
				}
				break;
			}
			setState(1710);
			((Table_nameContext)_localctx).table = id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class View_nameContext extends ParserRuleContext {
		public IdContext schema;
		public IdContext view;
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public View_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_view_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterView_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitView_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitView_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final View_nameContext view_name() throws RecognitionException {
		View_nameContext _localctx = new View_nameContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_view_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1715);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,210,_ctx) ) {
			case 1:
				{
				setState(1712);
				((View_nameContext)_localctx).schema = id();
				setState(1713);
				match(DOT);
				}
				break;
			}
			setState(1717);
			((View_nameContext)_localctx).view = id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Func_proc_nameContext extends ParserRuleContext {
		public IdContext database;
		public IdContext schema;
		public IdContext procedure;
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public Func_proc_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_func_proc_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterFunc_proc_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitFunc_proc_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitFunc_proc_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Func_proc_nameContext func_proc_name() throws RecognitionException {
		Func_proc_nameContext _localctx = new Func_proc_nameContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_func_proc_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1729);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,212,_ctx) ) {
			case 1:
				{
				setState(1719);
				((Func_proc_nameContext)_localctx).database = id();
				setState(1720);
				match(DOT);
				setState(1722);
				_la = _input.LA(1);
				if (_la==STORE || _la==FORCESEEK || ((((_la - 215)) & ~0x3f) == 0 && ((1L << (_la - 215)) & ((1L << (ABSOLUTE - 215)) | (1L << (APPLY - 215)) | (1L << (AUTO - 215)) | (1L << (AVG - 215)) | (1L << (BASE64 - 215)) | (1L << (CALLER - 215)) | (1L << (CAST - 215)) | (1L << (CATCH - 215)) | (1L << (CHECKSUM_AGG - 215)) | (1L << (COMMITTED - 215)) | (1L << (CONCAT - 215)) | (1L << (CONCAT_WS - 215)) | (1L << (COOKIE - 215)) | (1L << (COUNT - 215)) | (1L << (COUNT_BIG - 215)) | (1L << (DAY - 215)) | (1L << (DAYS - 215)) | (1L << (DELAY - 215)) | (1L << (DELETED - 215)) | (1L << (DENSE_RANK - 215)) | (1L << (DISABLE - 215)) | (1L << (DYNAMIC - 215)) | (1L << (ENCRYPTION - 215)) | (1L << (EXTRACT - 215)) | (1L << (FAST - 215)) | (1L << (FAST_FORWARD - 215)) | (1L << (FIRST - 215)) | (1L << (FOLLOWING - 215)) | (1L << (FORWARD_ONLY - 215)))) != 0) || ((((_la - 280)) & ~0x3f) == 0 && ((1L << (_la - 280)) & ((1L << (FULLSCAN - 280)) | (1L << (GLOBAL - 280)) | (1L << (GO - 280)) | (1L << (GROUPING - 280)) | (1L << (GROUPING_ID - 280)) | (1L << (HASH - 280)) | (1L << (INSENSITIVE - 280)) | (1L << (INSERTED - 280)) | (1L << (INTERVAL - 280)) | (1L << (ISOLATION - 280)) | (1L << (KEEPFIXED - 280)) | (1L << (KEYSET - 280)) | (1L << (LAST - 280)) | (1L << (LEVEL - 280)) | (1L << (LOCAL - 280)) | (1L << (LOCK_ESCALATION - 280)) | (1L << (LOGIN - 280)) | (1L << (LOOP - 280)) | (1L << (MARK - 280)) | (1L << (MAX - 280)) | (1L << (MIN - 280)) | (1L << (MODIFY - 280)) | (1L << (MONTH - 280)) | (1L << (MONTHS - 280)) | (1L << (NEXT - 280)) | (1L << (NAME - 280)) | (1L << (NOCOUNT - 280)) | (1L << (NOEXPAND - 280)) | (1L << (NORECOMPUTE - 280)) | (1L << (NTILE - 280)) | (1L << (NUMBER - 280)) | (1L << (OFFSET - 280)) | (1L << (ONLY - 280)) | (1L << (OPTIMISTIC - 280)) | (1L << (OPTIMIZE - 280)) | (1L << (OUT - 280)) | (1L << (OUTPUT - 280)) | (1L << (OWNER - 280)) | (1L << (PARTITION - 280)) | (1L << (PATH - 280)))) != 0) || ((((_la - 346)) & ~0x3f) == 0 && ((1L << (_la - 346)) & ((1L << (PRECEDING - 346)) | (1L << (PRIOR - 346)) | (1L << (RANGE - 346)) | (1L << (RANK - 346)) | (1L << (READONLY - 346)) | (1L << (READ_ONLY - 346)) | (1L << (RECOMPILE - 346)) | (1L << (RELATIVE - 346)) | (1L << (REMOTE - 346)) | (1L << (REPEATABLE - 346)) | (1L << (ROOT - 346)) | (1L << (ROW - 346)) | (1L << (ROWGUID - 346)) | (1L << (ROWS - 346)) | (1L << (ROW_NUMBER - 346)) | (1L << (SAMPLE - 346)) | (1L << (SCHEMABINDING - 346)) | (1L << (SCROLL - 346)) | (1L << (SCROLL_LOCKS - 346)) | (1L << (SELF - 346)) | (1L << (SERIALIZABLE - 346)) | (1L << (SNAPSHOT - 346)) | (1L << (SPATIAL_WINDOW_MAX_CELLS - 346)) | (1L << (STATIC - 346)) | (1L << (STATS_STREAM - 346)) | (1L << (STDEV - 346)) | (1L << (STDEVP - 346)) | (1L << (STDDEV_SAMP - 346)) | (1L << (SUM - 346)) | (1L << (STRTOL - 346)) | (1L << (THROW - 346)) | (1L << (TIES - 346)) | (1L << (TIME - 346)) | (1L << (TRY - 346)) | (1L << (TYPE - 346)) | (1L << (TYPE_WARNING - 346)) | (1L << (UNBOUNDED - 346)))) != 0) || ((((_la - 410)) & ~0x3f) == 0 && ((1L << (_la - 410)) & ((1L << (UNCOMMITTED - 410)) | (1L << (UNKNOWN - 410)) | (1L << (USING - 410)) | (1L << (VAR - 410)) | (1L << (VARP - 410)) | (1L << (VIEW_METADATA - 410)) | (1L << (WORK - 410)) | (1L << (XML - 410)) | (1L << (XMLNAMESPACES - 410)) | (1L << (YEAR - 410)) | (1L << (YEARS - 410)) | (1L << (DOUBLE_QUOTE_ID - 410)) | (1L << (BACKTICK_ID - 410)) | (1L << (SQUARE_BRACKET_ID - 410)) | (1L << (ID - 410)))) != 0)) {
					{
					setState(1721);
					((Func_proc_nameContext)_localctx).schema = id();
					}
				}

				setState(1724);
				match(DOT);
				}
				break;
			case 2:
				{
				{
				setState(1726);
				((Func_proc_nameContext)_localctx).schema = id();
				}
				setState(1727);
				match(DOT);
				}
				break;
			}
			setState(1731);
			((Func_proc_nameContext)_localctx).procedure = id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Ddl_objectContext extends ParserRuleContext {
		public Full_table_nameContext full_table_name() {
			return getRuleContext(Full_table_nameContext.class,0);
		}
		public TerminalNode LOCAL_ID() { return getToken(VerdictSQLParser.LOCAL_ID, 0); }
		public Ddl_objectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ddl_object; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDdl_object(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDdl_object(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDdl_object(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Ddl_objectContext ddl_object() throws RecognitionException {
		Ddl_objectContext _localctx = new Ddl_objectContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_ddl_object);
		try {
			setState(1735);
			switch (_input.LA(1)) {
			case STORE:
			case FORCESEEK:
			case ABSOLUTE:
			case APPLY:
			case AUTO:
			case AVG:
			case BASE64:
			case CALLER:
			case CAST:
			case CATCH:
			case CHECKSUM_AGG:
			case COMMITTED:
			case CONCAT:
			case CONCAT_WS:
			case COOKIE:
			case COUNT:
			case COUNT_BIG:
			case DAY:
			case DAYS:
			case DELAY:
			case DELETED:
			case DENSE_RANK:
			case DISABLE:
			case DYNAMIC:
			case ENCRYPTION:
			case EXTRACT:
			case FAST:
			case FAST_FORWARD:
			case FIRST:
			case FOLLOWING:
			case FORWARD_ONLY:
			case FULLSCAN:
			case GLOBAL:
			case GO:
			case GROUPING:
			case GROUPING_ID:
			case HASH:
			case INSENSITIVE:
			case INSERTED:
			case INTERVAL:
			case ISOLATION:
			case KEEPFIXED:
			case KEYSET:
			case LAST:
			case LEVEL:
			case LOCAL:
			case LOCK_ESCALATION:
			case LOGIN:
			case LOOP:
			case MARK:
			case MAX:
			case MIN:
			case MODIFY:
			case MONTH:
			case MONTHS:
			case NEXT:
			case NAME:
			case NOCOUNT:
			case NOEXPAND:
			case NORECOMPUTE:
			case NTILE:
			case NUMBER:
			case OFFSET:
			case ONLY:
			case OPTIMISTIC:
			case OPTIMIZE:
			case OUT:
			case OUTPUT:
			case OWNER:
			case PARTITION:
			case PATH:
			case PRECEDING:
			case PRIOR:
			case RANGE:
			case RANK:
			case READONLY:
			case READ_ONLY:
			case RECOMPILE:
			case RELATIVE:
			case REMOTE:
			case REPEATABLE:
			case ROOT:
			case ROW:
			case ROWGUID:
			case ROWS:
			case ROW_NUMBER:
			case SAMPLE:
			case SCHEMABINDING:
			case SCROLL:
			case SCROLL_LOCKS:
			case SELF:
			case SERIALIZABLE:
			case SNAPSHOT:
			case SPATIAL_WINDOW_MAX_CELLS:
			case STATIC:
			case STATS_STREAM:
			case STDEV:
			case STDEVP:
			case STDDEV_SAMP:
			case SUM:
			case STRTOL:
			case THROW:
			case TIES:
			case TIME:
			case TRY:
			case TYPE:
			case TYPE_WARNING:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNKNOWN:
			case USING:
			case VAR:
			case VARP:
			case VIEW_METADATA:
			case WORK:
			case XML:
			case XMLNAMESPACES:
			case YEAR:
			case YEARS:
			case DOUBLE_QUOTE_ID:
			case BACKTICK_ID:
			case SQUARE_BRACKET_ID:
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(1733);
				full_table_name();
				}
				break;
			case LOCAL_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(1734);
				match(LOCAL_ID);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Full_column_nameContext extends ParserRuleContext {
		public Column_nameContext column_name() {
			return getRuleContext(Column_nameContext.class,0);
		}
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Full_column_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_full_column_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterFull_column_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitFull_column_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitFull_column_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Full_column_nameContext full_column_name() throws RecognitionException {
		Full_column_nameContext _localctx = new Full_column_nameContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_full_column_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1740);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,214,_ctx) ) {
			case 1:
				{
				setState(1737);
				table_name();
				setState(1738);
				match(DOT);
				}
				break;
			}
			setState(1742);
			column_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_name_listContext extends ParserRuleContext {
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public Column_name_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_name_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterColumn_name_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitColumn_name_list(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitColumn_name_list(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_name_listContext column_name_list() throws RecognitionException {
		Column_name_listContext _localctx = new Column_name_listContext(_ctx, getState());
		enterRule(_localctx, 238, RULE_column_name_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1744);
			column_name();
			setState(1749);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1745);
				match(COMMA);
				setState(1746);
				column_name();
				}
				}
				setState(1751);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_nameContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Column_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterColumn_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitColumn_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitColumn_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_nameContext column_name() throws RecognitionException {
		Column_nameContext _localctx = new Column_nameContext(_ctx, getState());
		enterRule(_localctx, 240, RULE_column_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1752);
			id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Cursor_nameContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode LOCAL_ID() { return getToken(VerdictSQLParser.LOCAL_ID, 0); }
		public Cursor_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_cursor_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterCursor_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitCursor_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitCursor_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Cursor_nameContext cursor_name() throws RecognitionException {
		Cursor_nameContext _localctx = new Cursor_nameContext(_ctx, getState());
		enterRule(_localctx, 242, RULE_cursor_name);
		try {
			setState(1756);
			switch (_input.LA(1)) {
			case STORE:
			case FORCESEEK:
			case ABSOLUTE:
			case APPLY:
			case AUTO:
			case AVG:
			case BASE64:
			case CALLER:
			case CAST:
			case CATCH:
			case CHECKSUM_AGG:
			case COMMITTED:
			case CONCAT:
			case CONCAT_WS:
			case COOKIE:
			case COUNT:
			case COUNT_BIG:
			case DAY:
			case DAYS:
			case DELAY:
			case DELETED:
			case DENSE_RANK:
			case DISABLE:
			case DYNAMIC:
			case ENCRYPTION:
			case EXTRACT:
			case FAST:
			case FAST_FORWARD:
			case FIRST:
			case FOLLOWING:
			case FORWARD_ONLY:
			case FULLSCAN:
			case GLOBAL:
			case GO:
			case GROUPING:
			case GROUPING_ID:
			case HASH:
			case INSENSITIVE:
			case INSERTED:
			case INTERVAL:
			case ISOLATION:
			case KEEPFIXED:
			case KEYSET:
			case LAST:
			case LEVEL:
			case LOCAL:
			case LOCK_ESCALATION:
			case LOGIN:
			case LOOP:
			case MARK:
			case MAX:
			case MIN:
			case MODIFY:
			case MONTH:
			case MONTHS:
			case NEXT:
			case NAME:
			case NOCOUNT:
			case NOEXPAND:
			case NORECOMPUTE:
			case NTILE:
			case NUMBER:
			case OFFSET:
			case ONLY:
			case OPTIMISTIC:
			case OPTIMIZE:
			case OUT:
			case OUTPUT:
			case OWNER:
			case PARTITION:
			case PATH:
			case PRECEDING:
			case PRIOR:
			case RANGE:
			case RANK:
			case READONLY:
			case READ_ONLY:
			case RECOMPILE:
			case RELATIVE:
			case REMOTE:
			case REPEATABLE:
			case ROOT:
			case ROW:
			case ROWGUID:
			case ROWS:
			case ROW_NUMBER:
			case SAMPLE:
			case SCHEMABINDING:
			case SCROLL:
			case SCROLL_LOCKS:
			case SELF:
			case SERIALIZABLE:
			case SNAPSHOT:
			case SPATIAL_WINDOW_MAX_CELLS:
			case STATIC:
			case STATS_STREAM:
			case STDEV:
			case STDEVP:
			case STDDEV_SAMP:
			case SUM:
			case STRTOL:
			case THROW:
			case TIES:
			case TIME:
			case TRY:
			case TYPE:
			case TYPE_WARNING:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNKNOWN:
			case USING:
			case VAR:
			case VARP:
			case VIEW_METADATA:
			case WORK:
			case XML:
			case XMLNAMESPACES:
			case YEAR:
			case YEARS:
			case DOUBLE_QUOTE_ID:
			case BACKTICK_ID:
			case SQUARE_BRACKET_ID:
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(1754);
				id();
				}
				break;
			case LOCAL_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(1755);
				match(LOCAL_ID);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class On_offContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(VerdictSQLParser.ON, 0); }
		public TerminalNode OFF() { return getToken(VerdictSQLParser.OFF, 0); }
		public On_offContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_on_off; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterOn_off(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitOn_off(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitOn_off(this);
			else return visitor.visitChildren(this);
		}
	}

	public final On_offContext on_off() throws RecognitionException {
		On_offContext _localctx = new On_offContext(_ctx, getState());
		enterRule(_localctx, 244, RULE_on_off);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1758);
			_la = _input.LA(1);
			if ( !(_la==OFF || _la==ON) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ClusteredContext extends ParserRuleContext {
		public TerminalNode CLUSTERED() { return getToken(VerdictSQLParser.CLUSTERED, 0); }
		public TerminalNode NONCLUSTERED() { return getToken(VerdictSQLParser.NONCLUSTERED, 0); }
		public ClusteredContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_clustered; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterClustered(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitClustered(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitClustered(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ClusteredContext clustered() throws RecognitionException {
		ClusteredContext _localctx = new ClusteredContext(_ctx, getState());
		enterRule(_localctx, 246, RULE_clustered);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1760);
			_la = _input.LA(1);
			if ( !(_la==CLUSTERED || _la==NONCLUSTERED) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Null_notnullContext extends ParserRuleContext {
		public TerminalNode NULL() { return getToken(VerdictSQLParser.NULL, 0); }
		public TerminalNode NOT() { return getToken(VerdictSQLParser.NOT, 0); }
		public Null_notnullContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_null_notnull; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterNull_notnull(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitNull_notnull(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitNull_notnull(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Null_notnullContext null_notnull() throws RecognitionException {
		Null_notnullContext _localctx = new Null_notnullContext(_ctx, getState());
		enterRule(_localctx, 248, RULE_null_notnull);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1763);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(1762);
				match(NOT);
				}
			}

			setState(1765);
			match(NULL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class True_orfalseContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(VerdictSQLParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(VerdictSQLParser.FALSE, 0); }
		public True_orfalseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_true_orfalse; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterTrue_orfalse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitTrue_orfalse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitTrue_orfalse(this);
			else return visitor.visitChildren(this);
		}
	}

	public final True_orfalseContext true_orfalse() throws RecognitionException {
		True_orfalseContext _localctx = new True_orfalseContext(_ctx, getState());
		enterRule(_localctx, 250, RULE_true_orfalse);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1767);
			_la = _input.LA(1);
			if ( !(_la==FALSE || _la==TRUE) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Scalar_function_nameContext extends ParserRuleContext {
		public Func_proc_nameContext func_proc_name() {
			return getRuleContext(Func_proc_nameContext.class,0);
		}
		public TerminalNode RIGHT() { return getToken(VerdictSQLParser.RIGHT, 0); }
		public TerminalNode LEFT() { return getToken(VerdictSQLParser.LEFT, 0); }
		public TerminalNode BINARY_CHECKSUM() { return getToken(VerdictSQLParser.BINARY_CHECKSUM, 0); }
		public TerminalNode CHECKSUM() { return getToken(VerdictSQLParser.CHECKSUM, 0); }
		public Scalar_function_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_scalar_function_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterScalar_function_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitScalar_function_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitScalar_function_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Scalar_function_nameContext scalar_function_name() throws RecognitionException {
		Scalar_function_nameContext _localctx = new Scalar_function_nameContext(_ctx, getState());
		enterRule(_localctx, 252, RULE_scalar_function_name);
		try {
			setState(1774);
			switch (_input.LA(1)) {
			case STORE:
			case FORCESEEK:
			case ABSOLUTE:
			case APPLY:
			case AUTO:
			case AVG:
			case BASE64:
			case CALLER:
			case CAST:
			case CATCH:
			case CHECKSUM_AGG:
			case COMMITTED:
			case CONCAT:
			case CONCAT_WS:
			case COOKIE:
			case COUNT:
			case COUNT_BIG:
			case DAY:
			case DAYS:
			case DELAY:
			case DELETED:
			case DENSE_RANK:
			case DISABLE:
			case DYNAMIC:
			case ENCRYPTION:
			case EXTRACT:
			case FAST:
			case FAST_FORWARD:
			case FIRST:
			case FOLLOWING:
			case FORWARD_ONLY:
			case FULLSCAN:
			case GLOBAL:
			case GO:
			case GROUPING:
			case GROUPING_ID:
			case HASH:
			case INSENSITIVE:
			case INSERTED:
			case INTERVAL:
			case ISOLATION:
			case KEEPFIXED:
			case KEYSET:
			case LAST:
			case LEVEL:
			case LOCAL:
			case LOCK_ESCALATION:
			case LOGIN:
			case LOOP:
			case MARK:
			case MAX:
			case MIN:
			case MODIFY:
			case MONTH:
			case MONTHS:
			case NEXT:
			case NAME:
			case NOCOUNT:
			case NOEXPAND:
			case NORECOMPUTE:
			case NTILE:
			case NUMBER:
			case OFFSET:
			case ONLY:
			case OPTIMISTIC:
			case OPTIMIZE:
			case OUT:
			case OUTPUT:
			case OWNER:
			case PARTITION:
			case PATH:
			case PRECEDING:
			case PRIOR:
			case RANGE:
			case RANK:
			case READONLY:
			case READ_ONLY:
			case RECOMPILE:
			case RELATIVE:
			case REMOTE:
			case REPEATABLE:
			case ROOT:
			case ROW:
			case ROWGUID:
			case ROWS:
			case ROW_NUMBER:
			case SAMPLE:
			case SCHEMABINDING:
			case SCROLL:
			case SCROLL_LOCKS:
			case SELF:
			case SERIALIZABLE:
			case SNAPSHOT:
			case SPATIAL_WINDOW_MAX_CELLS:
			case STATIC:
			case STATS_STREAM:
			case STDEV:
			case STDEVP:
			case STDDEV_SAMP:
			case SUM:
			case STRTOL:
			case THROW:
			case TIES:
			case TIME:
			case TRY:
			case TYPE:
			case TYPE_WARNING:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNKNOWN:
			case USING:
			case VAR:
			case VARP:
			case VIEW_METADATA:
			case WORK:
			case XML:
			case XMLNAMESPACES:
			case YEAR:
			case YEARS:
			case DOUBLE_QUOTE_ID:
			case BACKTICK_ID:
			case SQUARE_BRACKET_ID:
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(1769);
				func_proc_name();
				}
				break;
			case RIGHT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1770);
				match(RIGHT);
				}
				break;
			case LEFT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1771);
				match(LEFT);
				}
				break;
			case BINARY_CHECKSUM:
				enterOuterAlt(_localctx, 4);
				{
				setState(1772);
				match(BINARY_CHECKSUM);
				}
				break;
			case CHECKSUM:
				enterOuterAlt(_localctx, 5);
				{
				setState(1773);
				match(CHECKSUM);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Data_typeContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode IDENTITY() { return getToken(VerdictSQLParser.IDENTITY, 0); }
		public List<TerminalNode> DECIMAL() { return getTokens(VerdictSQLParser.DECIMAL); }
		public TerminalNode DECIMAL(int i) {
			return getToken(VerdictSQLParser.DECIMAL, i);
		}
		public TerminalNode MAX() { return getToken(VerdictSQLParser.MAX, 0); }
		public Data_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_data_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterData_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitData_type(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitData_type(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Data_typeContext data_type() throws RecognitionException {
		Data_typeContext _localctx = new Data_typeContext(_ctx, getState());
		enterRule(_localctx, 254, RULE_data_type);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1776);
			id();
			setState(1778);
			_la = _input.LA(1);
			if (_la==IDENTITY) {
				{
				setState(1777);
				match(IDENTITY);
				}
			}

			setState(1787);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,221,_ctx) ) {
			case 1:
				{
				setState(1780);
				match(LR_BRACKET);
				setState(1781);
				_la = _input.LA(1);
				if ( !(_la==MAX || _la==DECIMAL) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(1784);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1782);
					match(COMMA);
					setState(1783);
					match(DECIMAL);
					}
				}

				setState(1786);
				match(RR_BRACKET);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Default_valueContext extends ParserRuleContext {
		public TerminalNode NULL() { return getToken(VerdictSQLParser.NULL, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public Default_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_default_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterDefault_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitDefault_value(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitDefault_value(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Default_valueContext default_value() throws RecognitionException {
		Default_valueContext _localctx = new Default_valueContext(_ctx, getState());
		enterRule(_localctx, 256, RULE_default_value);
		try {
			setState(1791);
			switch (_input.LA(1)) {
			case NULL:
				enterOuterAlt(_localctx, 1);
				{
				setState(1789);
				match(NULL);
				}
				break;
			case DECIMAL:
			case STRING:
			case BINARY:
			case FLOAT:
			case REAL:
			case DOLLAR:
			case PLUS:
			case MINUS:
				enterOuterAlt(_localctx, 2);
				{
				setState(1790);
				constant();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstantContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(VerdictSQLParser.STRING, 0); }
		public TerminalNode BINARY() { return getToken(VerdictSQLParser.BINARY, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public TerminalNode REAL() { return getToken(VerdictSQLParser.REAL, 0); }
		public TerminalNode FLOAT() { return getToken(VerdictSQLParser.FLOAT, 0); }
		public SignContext sign() {
			return getRuleContext(SignContext.class,0);
		}
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterConstant(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitConstant(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitConstant(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 258, RULE_constant);
		int _la;
		try {
			setState(1805);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,225,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1793);
				match(STRING);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1794);
				match(BINARY);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1795);
				number();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1797);
				_la = _input.LA(1);
				if (_la==PLUS || _la==MINUS) {
					{
					setState(1796);
					sign();
					}
				}

				setState(1799);
				_la = _input.LA(1);
				if ( !(_la==FLOAT || _la==REAL) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1801);
				_la = _input.LA(1);
				if (_la==PLUS || _la==MINUS) {
					{
					setState(1800);
					sign();
					}
				}

				setState(1803);
				match(DOLLAR);
				setState(1804);
				_la = _input.LA(1);
				if ( !(_la==DECIMAL || _la==FLOAT) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NumberContext extends ParserRuleContext {
		public TerminalNode DECIMAL() { return getToken(VerdictSQLParser.DECIMAL, 0); }
		public SignContext sign() {
			return getRuleContext(SignContext.class,0);
		}
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterNumber(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitNumber(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitNumber(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 260, RULE_number);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1808);
			_la = _input.LA(1);
			if (_la==PLUS || _la==MINUS) {
				{
				setState(1807);
				sign();
				}
			}

			setState(1810);
			match(DECIMAL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SignContext extends ParserRuleContext {
		public TerminalNode PLUS() { return getToken(VerdictSQLParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(VerdictSQLParser.MINUS, 0); }
		public SignContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sign; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSign(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSign(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSign(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SignContext sign() throws RecognitionException {
		SignContext _localctx = new SignContext(_ctx, getState());
		enterRule(_localctx, 262, RULE_sign);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1812);
			_la = _input.LA(1);
			if ( !(_la==PLUS || _la==MINUS) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdContext extends ParserRuleContext {
		public Simple_idContext simple_id() {
			return getRuleContext(Simple_idContext.class,0);
		}
		public TerminalNode DOUBLE_QUOTE_ID() { return getToken(VerdictSQLParser.DOUBLE_QUOTE_ID, 0); }
		public TerminalNode SQUARE_BRACKET_ID() { return getToken(VerdictSQLParser.SQUARE_BRACKET_ID, 0); }
		public TerminalNode BACKTICK_ID() { return getToken(VerdictSQLParser.BACKTICK_ID, 0); }
		public IdContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterId(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitId(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitId(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdContext id() throws RecognitionException {
		IdContext _localctx = new IdContext(_ctx, getState());
		enterRule(_localctx, 264, RULE_id);
		try {
			setState(1818);
			switch (_input.LA(1)) {
			case STORE:
			case FORCESEEK:
			case ABSOLUTE:
			case APPLY:
			case AUTO:
			case AVG:
			case BASE64:
			case CALLER:
			case CAST:
			case CATCH:
			case CHECKSUM_AGG:
			case COMMITTED:
			case CONCAT:
			case CONCAT_WS:
			case COOKIE:
			case COUNT:
			case COUNT_BIG:
			case DAY:
			case DAYS:
			case DELAY:
			case DELETED:
			case DENSE_RANK:
			case DISABLE:
			case DYNAMIC:
			case ENCRYPTION:
			case EXTRACT:
			case FAST:
			case FAST_FORWARD:
			case FIRST:
			case FOLLOWING:
			case FORWARD_ONLY:
			case FULLSCAN:
			case GLOBAL:
			case GO:
			case GROUPING:
			case GROUPING_ID:
			case HASH:
			case INSENSITIVE:
			case INSERTED:
			case INTERVAL:
			case ISOLATION:
			case KEEPFIXED:
			case KEYSET:
			case LAST:
			case LEVEL:
			case LOCAL:
			case LOCK_ESCALATION:
			case LOGIN:
			case LOOP:
			case MARK:
			case MAX:
			case MIN:
			case MODIFY:
			case MONTH:
			case MONTHS:
			case NEXT:
			case NAME:
			case NOCOUNT:
			case NOEXPAND:
			case NORECOMPUTE:
			case NTILE:
			case NUMBER:
			case OFFSET:
			case ONLY:
			case OPTIMISTIC:
			case OPTIMIZE:
			case OUT:
			case OUTPUT:
			case OWNER:
			case PARTITION:
			case PATH:
			case PRECEDING:
			case PRIOR:
			case RANGE:
			case RANK:
			case READONLY:
			case READ_ONLY:
			case RECOMPILE:
			case RELATIVE:
			case REMOTE:
			case REPEATABLE:
			case ROOT:
			case ROW:
			case ROWGUID:
			case ROWS:
			case ROW_NUMBER:
			case SAMPLE:
			case SCHEMABINDING:
			case SCROLL:
			case SCROLL_LOCKS:
			case SELF:
			case SERIALIZABLE:
			case SNAPSHOT:
			case SPATIAL_WINDOW_MAX_CELLS:
			case STATIC:
			case STATS_STREAM:
			case STDEV:
			case STDEVP:
			case STDDEV_SAMP:
			case SUM:
			case STRTOL:
			case THROW:
			case TIES:
			case TIME:
			case TRY:
			case TYPE:
			case TYPE_WARNING:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNKNOWN:
			case USING:
			case VAR:
			case VARP:
			case VIEW_METADATA:
			case WORK:
			case XML:
			case XMLNAMESPACES:
			case YEAR:
			case YEARS:
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(1814);
				simple_id();
				}
				break;
			case DOUBLE_QUOTE_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(1815);
				match(DOUBLE_QUOTE_ID);
				}
				break;
			case SQUARE_BRACKET_ID:
				enterOuterAlt(_localctx, 3);
				{
				setState(1816);
				match(SQUARE_BRACKET_ID);
				}
				break;
			case BACKTICK_ID:
				enterOuterAlt(_localctx, 4);
				{
				setState(1817);
				match(BACKTICK_ID);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Simple_idContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(VerdictSQLParser.ID, 0); }
		public TerminalNode ABSOLUTE() { return getToken(VerdictSQLParser.ABSOLUTE, 0); }
		public TerminalNode APPLY() { return getToken(VerdictSQLParser.APPLY, 0); }
		public TerminalNode AUTO() { return getToken(VerdictSQLParser.AUTO, 0); }
		public TerminalNode AVG() { return getToken(VerdictSQLParser.AVG, 0); }
		public TerminalNode BASE64() { return getToken(VerdictSQLParser.BASE64, 0); }
		public TerminalNode CALLER() { return getToken(VerdictSQLParser.CALLER, 0); }
		public TerminalNode CAST() { return getToken(VerdictSQLParser.CAST, 0); }
		public TerminalNode CATCH() { return getToken(VerdictSQLParser.CATCH, 0); }
		public TerminalNode CHECKSUM_AGG() { return getToken(VerdictSQLParser.CHECKSUM_AGG, 0); }
		public TerminalNode COMMITTED() { return getToken(VerdictSQLParser.COMMITTED, 0); }
		public TerminalNode CONCAT() { return getToken(VerdictSQLParser.CONCAT, 0); }
		public TerminalNode CONCAT_WS() { return getToken(VerdictSQLParser.CONCAT_WS, 0); }
		public TerminalNode COOKIE() { return getToken(VerdictSQLParser.COOKIE, 0); }
		public TerminalNode COUNT() { return getToken(VerdictSQLParser.COUNT, 0); }
		public TerminalNode COUNT_BIG() { return getToken(VerdictSQLParser.COUNT_BIG, 0); }
		public TerminalNode DELAY() { return getToken(VerdictSQLParser.DELAY, 0); }
		public TerminalNode DELETED() { return getToken(VerdictSQLParser.DELETED, 0); }
		public TerminalNode DENSE_RANK() { return getToken(VerdictSQLParser.DENSE_RANK, 0); }
		public TerminalNode DISABLE() { return getToken(VerdictSQLParser.DISABLE, 0); }
		public TerminalNode DYNAMIC() { return getToken(VerdictSQLParser.DYNAMIC, 0); }
		public TerminalNode ENCRYPTION() { return getToken(VerdictSQLParser.ENCRYPTION, 0); }
		public TerminalNode EXTRACT() { return getToken(VerdictSQLParser.EXTRACT, 0); }
		public TerminalNode FAST() { return getToken(VerdictSQLParser.FAST, 0); }
		public TerminalNode FAST_FORWARD() { return getToken(VerdictSQLParser.FAST_FORWARD, 0); }
		public TerminalNode FIRST() { return getToken(VerdictSQLParser.FIRST, 0); }
		public TerminalNode FOLLOWING() { return getToken(VerdictSQLParser.FOLLOWING, 0); }
		public TerminalNode FORCESEEK() { return getToken(VerdictSQLParser.FORCESEEK, 0); }
		public TerminalNode FORWARD_ONLY() { return getToken(VerdictSQLParser.FORWARD_ONLY, 0); }
		public TerminalNode FULLSCAN() { return getToken(VerdictSQLParser.FULLSCAN, 0); }
		public TerminalNode GLOBAL() { return getToken(VerdictSQLParser.GLOBAL, 0); }
		public TerminalNode GO() { return getToken(VerdictSQLParser.GO, 0); }
		public TerminalNode GROUPING() { return getToken(VerdictSQLParser.GROUPING, 0); }
		public TerminalNode GROUPING_ID() { return getToken(VerdictSQLParser.GROUPING_ID, 0); }
		public TerminalNode HASH() { return getToken(VerdictSQLParser.HASH, 0); }
		public TerminalNode INSENSITIVE() { return getToken(VerdictSQLParser.INSENSITIVE, 0); }
		public TerminalNode INSERTED() { return getToken(VerdictSQLParser.INSERTED, 0); }
		public TerminalNode ISOLATION() { return getToken(VerdictSQLParser.ISOLATION, 0); }
		public TerminalNode KEYSET() { return getToken(VerdictSQLParser.KEYSET, 0); }
		public TerminalNode KEEPFIXED() { return getToken(VerdictSQLParser.KEEPFIXED, 0); }
		public TerminalNode LAST() { return getToken(VerdictSQLParser.LAST, 0); }
		public TerminalNode LEVEL() { return getToken(VerdictSQLParser.LEVEL, 0); }
		public TerminalNode LOCAL() { return getToken(VerdictSQLParser.LOCAL, 0); }
		public TerminalNode LOCK_ESCALATION() { return getToken(VerdictSQLParser.LOCK_ESCALATION, 0); }
		public TerminalNode LOGIN() { return getToken(VerdictSQLParser.LOGIN, 0); }
		public TerminalNode LOOP() { return getToken(VerdictSQLParser.LOOP, 0); }
		public TerminalNode MARK() { return getToken(VerdictSQLParser.MARK, 0); }
		public TerminalNode MAX() { return getToken(VerdictSQLParser.MAX, 0); }
		public TerminalNode MIN() { return getToken(VerdictSQLParser.MIN, 0); }
		public TerminalNode MODIFY() { return getToken(VerdictSQLParser.MODIFY, 0); }
		public TerminalNode NAME() { return getToken(VerdictSQLParser.NAME, 0); }
		public TerminalNode NEXT() { return getToken(VerdictSQLParser.NEXT, 0); }
		public TerminalNode NOCOUNT() { return getToken(VerdictSQLParser.NOCOUNT, 0); }
		public TerminalNode NOEXPAND() { return getToken(VerdictSQLParser.NOEXPAND, 0); }
		public TerminalNode NORECOMPUTE() { return getToken(VerdictSQLParser.NORECOMPUTE, 0); }
		public TerminalNode NTILE() { return getToken(VerdictSQLParser.NTILE, 0); }
		public TerminalNode NUMBER() { return getToken(VerdictSQLParser.NUMBER, 0); }
		public TerminalNode OFFSET() { return getToken(VerdictSQLParser.OFFSET, 0); }
		public TerminalNode ONLY() { return getToken(VerdictSQLParser.ONLY, 0); }
		public TerminalNode OPTIMISTIC() { return getToken(VerdictSQLParser.OPTIMISTIC, 0); }
		public TerminalNode OPTIMIZE() { return getToken(VerdictSQLParser.OPTIMIZE, 0); }
		public TerminalNode OUT() { return getToken(VerdictSQLParser.OUT, 0); }
		public TerminalNode OUTPUT() { return getToken(VerdictSQLParser.OUTPUT, 0); }
		public TerminalNode OWNER() { return getToken(VerdictSQLParser.OWNER, 0); }
		public TerminalNode PARTITION() { return getToken(VerdictSQLParser.PARTITION, 0); }
		public TerminalNode PATH() { return getToken(VerdictSQLParser.PATH, 0); }
		public TerminalNode PRECEDING() { return getToken(VerdictSQLParser.PRECEDING, 0); }
		public TerminalNode PRIOR() { return getToken(VerdictSQLParser.PRIOR, 0); }
		public TerminalNode RANGE() { return getToken(VerdictSQLParser.RANGE, 0); }
		public TerminalNode RANK() { return getToken(VerdictSQLParser.RANK, 0); }
		public TerminalNode READONLY() { return getToken(VerdictSQLParser.READONLY, 0); }
		public TerminalNode READ_ONLY() { return getToken(VerdictSQLParser.READ_ONLY, 0); }
		public TerminalNode RECOMPILE() { return getToken(VerdictSQLParser.RECOMPILE, 0); }
		public TerminalNode RELATIVE() { return getToken(VerdictSQLParser.RELATIVE, 0); }
		public TerminalNode REMOTE() { return getToken(VerdictSQLParser.REMOTE, 0); }
		public TerminalNode REPEATABLE() { return getToken(VerdictSQLParser.REPEATABLE, 0); }
		public TerminalNode ROOT() { return getToken(VerdictSQLParser.ROOT, 0); }
		public TerminalNode ROW() { return getToken(VerdictSQLParser.ROW, 0); }
		public TerminalNode ROWGUID() { return getToken(VerdictSQLParser.ROWGUID, 0); }
		public TerminalNode ROWS() { return getToken(VerdictSQLParser.ROWS, 0); }
		public TerminalNode ROW_NUMBER() { return getToken(VerdictSQLParser.ROW_NUMBER, 0); }
		public TerminalNode SAMPLE() { return getToken(VerdictSQLParser.SAMPLE, 0); }
		public TerminalNode SCHEMABINDING() { return getToken(VerdictSQLParser.SCHEMABINDING, 0); }
		public TerminalNode SCROLL() { return getToken(VerdictSQLParser.SCROLL, 0); }
		public TerminalNode SCROLL_LOCKS() { return getToken(VerdictSQLParser.SCROLL_LOCKS, 0); }
		public TerminalNode SELF() { return getToken(VerdictSQLParser.SELF, 0); }
		public TerminalNode SERIALIZABLE() { return getToken(VerdictSQLParser.SERIALIZABLE, 0); }
		public TerminalNode SNAPSHOT() { return getToken(VerdictSQLParser.SNAPSHOT, 0); }
		public TerminalNode SPATIAL_WINDOW_MAX_CELLS() { return getToken(VerdictSQLParser.SPATIAL_WINDOW_MAX_CELLS, 0); }
		public TerminalNode STATIC() { return getToken(VerdictSQLParser.STATIC, 0); }
		public TerminalNode STATS_STREAM() { return getToken(VerdictSQLParser.STATS_STREAM, 0); }
		public TerminalNode STDEV() { return getToken(VerdictSQLParser.STDEV, 0); }
		public TerminalNode STDEVP() { return getToken(VerdictSQLParser.STDEVP, 0); }
		public TerminalNode STDDEV_SAMP() { return getToken(VerdictSQLParser.STDDEV_SAMP, 0); }
		public TerminalNode STRTOL() { return getToken(VerdictSQLParser.STRTOL, 0); }
		public TerminalNode SUM() { return getToken(VerdictSQLParser.SUM, 0); }
		public TerminalNode THROW() { return getToken(VerdictSQLParser.THROW, 0); }
		public TerminalNode TIES() { return getToken(VerdictSQLParser.TIES, 0); }
		public TerminalNode TIME() { return getToken(VerdictSQLParser.TIME, 0); }
		public TerminalNode TRY() { return getToken(VerdictSQLParser.TRY, 0); }
		public TerminalNode TYPE() { return getToken(VerdictSQLParser.TYPE, 0); }
		public TerminalNode TYPE_WARNING() { return getToken(VerdictSQLParser.TYPE_WARNING, 0); }
		public TerminalNode UNBOUNDED() { return getToken(VerdictSQLParser.UNBOUNDED, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(VerdictSQLParser.UNCOMMITTED, 0); }
		public TerminalNode UNKNOWN() { return getToken(VerdictSQLParser.UNKNOWN, 0); }
		public TerminalNode USING() { return getToken(VerdictSQLParser.USING, 0); }
		public TerminalNode VAR() { return getToken(VerdictSQLParser.VAR, 0); }
		public TerminalNode VARP() { return getToken(VerdictSQLParser.VARP, 0); }
		public TerminalNode VIEW_METADATA() { return getToken(VerdictSQLParser.VIEW_METADATA, 0); }
		public TerminalNode WORK() { return getToken(VerdictSQLParser.WORK, 0); }
		public TerminalNode XML() { return getToken(VerdictSQLParser.XML, 0); }
		public TerminalNode XMLNAMESPACES() { return getToken(VerdictSQLParser.XMLNAMESPACES, 0); }
		public TerminalNode DAY() { return getToken(VerdictSQLParser.DAY, 0); }
		public TerminalNode MONTH() { return getToken(VerdictSQLParser.MONTH, 0); }
		public TerminalNode YEAR() { return getToken(VerdictSQLParser.YEAR, 0); }
		public TerminalNode DAYS() { return getToken(VerdictSQLParser.DAYS, 0); }
		public TerminalNode MONTHS() { return getToken(VerdictSQLParser.MONTHS, 0); }
		public TerminalNode YEARS() { return getToken(VerdictSQLParser.YEARS, 0); }
		public TerminalNode STORE() { return getToken(VerdictSQLParser.STORE, 0); }
		public TerminalNode INTERVAL() { return getToken(VerdictSQLParser.INTERVAL, 0); }
		public Simple_idContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_simple_id; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterSimple_id(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitSimple_id(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitSimple_id(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Simple_idContext simple_id() throws RecognitionException {
		Simple_idContext _localctx = new Simple_idContext(_ctx, getState());
		enterRule(_localctx, 266, RULE_simple_id);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1820);
			_la = _input.LA(1);
			if ( !(_la==STORE || _la==FORCESEEK || ((((_la - 215)) & ~0x3f) == 0 && ((1L << (_la - 215)) & ((1L << (ABSOLUTE - 215)) | (1L << (APPLY - 215)) | (1L << (AUTO - 215)) | (1L << (AVG - 215)) | (1L << (BASE64 - 215)) | (1L << (CALLER - 215)) | (1L << (CAST - 215)) | (1L << (CATCH - 215)) | (1L << (CHECKSUM_AGG - 215)) | (1L << (COMMITTED - 215)) | (1L << (CONCAT - 215)) | (1L << (CONCAT_WS - 215)) | (1L << (COOKIE - 215)) | (1L << (COUNT - 215)) | (1L << (COUNT_BIG - 215)) | (1L << (DAY - 215)) | (1L << (DAYS - 215)) | (1L << (DELAY - 215)) | (1L << (DELETED - 215)) | (1L << (DENSE_RANK - 215)) | (1L << (DISABLE - 215)) | (1L << (DYNAMIC - 215)) | (1L << (ENCRYPTION - 215)) | (1L << (EXTRACT - 215)) | (1L << (FAST - 215)) | (1L << (FAST_FORWARD - 215)) | (1L << (FIRST - 215)) | (1L << (FOLLOWING - 215)) | (1L << (FORWARD_ONLY - 215)))) != 0) || ((((_la - 280)) & ~0x3f) == 0 && ((1L << (_la - 280)) & ((1L << (FULLSCAN - 280)) | (1L << (GLOBAL - 280)) | (1L << (GO - 280)) | (1L << (GROUPING - 280)) | (1L << (GROUPING_ID - 280)) | (1L << (HASH - 280)) | (1L << (INSENSITIVE - 280)) | (1L << (INSERTED - 280)) | (1L << (INTERVAL - 280)) | (1L << (ISOLATION - 280)) | (1L << (KEEPFIXED - 280)) | (1L << (KEYSET - 280)) | (1L << (LAST - 280)) | (1L << (LEVEL - 280)) | (1L << (LOCAL - 280)) | (1L << (LOCK_ESCALATION - 280)) | (1L << (LOGIN - 280)) | (1L << (LOOP - 280)) | (1L << (MARK - 280)) | (1L << (MAX - 280)) | (1L << (MIN - 280)) | (1L << (MODIFY - 280)) | (1L << (MONTH - 280)) | (1L << (MONTHS - 280)) | (1L << (NEXT - 280)) | (1L << (NAME - 280)) | (1L << (NOCOUNT - 280)) | (1L << (NOEXPAND - 280)) | (1L << (NORECOMPUTE - 280)) | (1L << (NTILE - 280)) | (1L << (NUMBER - 280)) | (1L << (OFFSET - 280)) | (1L << (ONLY - 280)) | (1L << (OPTIMISTIC - 280)) | (1L << (OPTIMIZE - 280)) | (1L << (OUT - 280)) | (1L << (OUTPUT - 280)) | (1L << (OWNER - 280)) | (1L << (PARTITION - 280)) | (1L << (PATH - 280)))) != 0) || ((((_la - 346)) & ~0x3f) == 0 && ((1L << (_la - 346)) & ((1L << (PRECEDING - 346)) | (1L << (PRIOR - 346)) | (1L << (RANGE - 346)) | (1L << (RANK - 346)) | (1L << (READONLY - 346)) | (1L << (READ_ONLY - 346)) | (1L << (RECOMPILE - 346)) | (1L << (RELATIVE - 346)) | (1L << (REMOTE - 346)) | (1L << (REPEATABLE - 346)) | (1L << (ROOT - 346)) | (1L << (ROW - 346)) | (1L << (ROWGUID - 346)) | (1L << (ROWS - 346)) | (1L << (ROW_NUMBER - 346)) | (1L << (SAMPLE - 346)) | (1L << (SCHEMABINDING - 346)) | (1L << (SCROLL - 346)) | (1L << (SCROLL_LOCKS - 346)) | (1L << (SELF - 346)) | (1L << (SERIALIZABLE - 346)) | (1L << (SNAPSHOT - 346)) | (1L << (SPATIAL_WINDOW_MAX_CELLS - 346)) | (1L << (STATIC - 346)) | (1L << (STATS_STREAM - 346)) | (1L << (STDEV - 346)) | (1L << (STDEVP - 346)) | (1L << (STDDEV_SAMP - 346)) | (1L << (SUM - 346)) | (1L << (STRTOL - 346)) | (1L << (THROW - 346)) | (1L << (TIES - 346)) | (1L << (TIME - 346)) | (1L << (TRY - 346)) | (1L << (TYPE - 346)) | (1L << (TYPE_WARNING - 346)) | (1L << (UNBOUNDED - 346)))) != 0) || ((((_la - 410)) & ~0x3f) == 0 && ((1L << (_la - 410)) & ((1L << (UNCOMMITTED - 410)) | (1L << (UNKNOWN - 410)) | (1L << (USING - 410)) | (1L << (VAR - 410)) | (1L << (VARP - 410)) | (1L << (VIEW_METADATA - 410)) | (1L << (WORK - 410)) | (1L << (XML - 410)) | (1L << (XMLNAMESPACES - 410)) | (1L << (YEAR - 410)) | (1L << (YEARS - 410)) | (1L << (ID - 410)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Comparison_operatorContext extends ParserRuleContext {
		public Comparison_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparison_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterComparison_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitComparison_operator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitComparison_operator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Comparison_operatorContext comparison_operator() throws RecognitionException {
		Comparison_operatorContext _localctx = new Comparison_operatorContext(_ctx, getState());
		enterRule(_localctx, 268, RULE_comparison_operator);
		try {
			setState(1843);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,228,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1822);
				match(EQUAL);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1823);
				match(GREATER);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1824);
				match(LESS);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1825);
				match(T__1);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1826);
				match(T__2);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1827);
				match(T__3);
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(1828);
				match(T__4);
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(1829);
				match(T__5);
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(1830);
				match(T__6);
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(1831);
				match(LESS);
				setState(1832);
				match(EQUAL);
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(1833);
				match(GREATER);
				setState(1834);
				match(EQUAL);
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(1835);
				match(LESS);
				setState(1836);
				match(GREATER);
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(1837);
				match(EXCLAMATION);
				setState(1838);
				match(EQUAL);
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(1839);
				match(EXCLAMATION);
				setState(1840);
				match(GREATER);
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(1841);
				match(EXCLAMATION);
				setState(1842);
				match(LESS);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Assignment_operatorContext extends ParserRuleContext {
		public Assignment_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_assignment_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).enterAssignment_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof VerdictSQLListener ) ((VerdictSQLListener)listener).exitAssignment_operator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof VerdictSQLVisitor ) return ((VerdictSQLVisitor<? extends T>)visitor).visitAssignment_operator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Assignment_operatorContext assignment_operator() throws RecognitionException {
		Assignment_operatorContext _localctx = new Assignment_operatorContext(_ctx, getState());
		enterRule(_localctx, 270, RULE_assignment_operator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1845);
			_la = _input.LA(1);
			if ( !(((((_la - 444)) & ~0x3f) == 0 && ((1L << (_la - 444)) & ((1L << (PLUS_ASSIGN - 444)) | (1L << (MINUS_ASSIGN - 444)) | (1L << (MULT_ASSIGN - 444)) | (1L << (DIV_ASSIGN - 444)) | (1L << (MOD_ASSIGN - 444)) | (1L << (AND_ASSIGN - 444)) | (1L << (XOR_ASSIGN - 444)) | (1L << (OR_ASSIGN - 444)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 44:
			return expression_sempred((ExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 5);
		case 1:
			return precpred(_ctx, 3);
		case 2:
			return precpred(_ctx, 2);
		case 3:
			return precpred(_ctx, 11);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\u01d8\u073a\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_\4"+
		"`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\4f\tf\4g\tg\4h\th\4i\ti\4j\tj\4k\t"+
		"k\4l\tl\4m\tm\4n\tn\4o\to\4p\tp\4q\tq\4r\tr\4s\ts\4t\tt\4u\tu\4v\tv\4"+
		"w\tw\4x\tx\4y\ty\4z\tz\4{\t{\4|\t|\4}\t}\4~\t~\4\177\t\177\4\u0080\t\u0080"+
		"\4\u0081\t\u0081\4\u0082\t\u0082\4\u0083\t\u0083\4\u0084\t\u0084\4\u0085"+
		"\t\u0085\4\u0086\t\u0086\4\u0087\t\u0087\4\u0088\t\u0088\4\u0089\t\u0089"+
		"\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\5\2\u011e\n\2\3\3\3\3\3\3"+
		"\5\3\u0123\n\3\3\3\5\3\u0126\n\3\3\3\3\3\3\3\3\3\5\3\u012c\n\3\3\4\3\4"+
		"\3\5\3\5\3\5\3\5\7\5\u0134\n\5\f\5\16\5\u0137\13\5\3\6\3\6\3\6\5\6\u013c"+
		"\n\6\3\6\5\6\u013f\n\6\3\6\3\6\3\6\3\6\5\6\u0145\n\6\3\7\3\7\5\7\u0149"+
		"\n\7\3\7\3\7\3\7\5\7\u014e\n\7\3\b\3\b\5\b\u0152\n\b\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\5\t\u015c\n\t\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\5\13"+
		"\u0166\n\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\16\3\16\3\16\7"+
		"\16\u0175\n\16\f\16\16\16\u0178\13\16\3\17\3\17\3\17\3\17\3\17\7\17\u017f"+
		"\n\17\f\17\16\17\u0182\13\17\5\17\u0184\n\17\3\20\3\20\3\20\5\20\u0189"+
		"\n\20\3\21\3\21\3\21\3\22\3\22\3\22\3\22\5\22\u0192\n\22\3\23\7\23\u0195"+
		"\n\23\f\23\16\23\u0198\13\23\3\23\3\23\3\24\3\24\5\24\u019e\n\24\3\25"+
		"\3\25\3\25\3\25\3\25\3\25\5\25\u01a6\n\25\3\26\5\26\u01a9\n\26\3\26\5"+
		"\26\u01ac\n\26\3\26\3\26\5\26\u01b0\n\26\3\26\5\26\u01b3\n\26\3\26\5\26"+
		"\u01b6\n\26\3\26\5\26\u01b9\n\26\3\27\3\27\3\27\3\27\7\27\u01bf\n\27\f"+
		"\27\16\27\u01c2\13\27\3\27\3\27\3\27\5\27\u01c7\n\27\3\27\3\27\3\27\3"+
		"\27\5\27\u01cd\n\27\5\27\u01cf\n\27\3\30\3\30\5\30\u01d3\n\30\3\30\5\30"+
		"\u01d6\n\30\3\30\5\30\u01d9\n\30\3\31\3\31\3\31\5\31\u01de\n\31\3\31\3"+
		"\31\3\31\5\31\u01e3\n\31\3\31\5\31\u01e6\n\31\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\5\32\u01ee\n\32\3\32\7\32\u01f1\n\32\f\32\16\32\u01f4\13\32\3\32"+
		"\5\32\u01f7\n\32\3\32\3\32\3\33\3\33\3\33\3\33\3\33\5\33\u0200\n\33\3"+
		"\33\3\33\5\33\u0204\n\33\3\33\3\33\3\33\5\33\u0209\n\33\3\34\3\34\3\34"+
		"\3\34\3\34\3\34\3\34\7\34\u0212\n\34\f\34\16\34\u0215\13\34\3\34\3\34"+
		"\5\34\u0219\n\34\3\34\3\34\3\34\3\34\3\34\5\34\u0220\n\34\3\34\5\34\u0223"+
		"\n\34\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\5\35\u022f\n\35"+
		"\3\35\3\35\3\35\3\35\3\35\3\35\5\35\u0237\n\35\5\35\u0239\n\35\3\36\3"+
		"\36\3\36\3\36\5\36\u023f\n\36\3\36\3\36\3\36\3\36\3\36\5\36\u0246\n\36"+
		"\3\37\3\37\3\37\3\37\5\37\u024c\n\37\3\37\3\37\5\37\u0250\n\37\3 \3 \3"+
		" \3 \5 \u0256\n \3 \3 \3 \7 \u025b\n \f \16 \u025e\13 \3 \5 \u0261\n "+
		"\3!\3!\3!\3!\5!\u0267\n!\3!\3!\3!\5!\u026c\n!\3!\3!\3!\3!\3!\5!\u0273"+
		"\n!\3!\5!\u0276\n!\3\"\3\"\3\"\5\"\u027b\n\"\3#\3#\3#\3#\5#\u0281\n#\3"+
		"#\5#\u0284\n#\3$\3$\3$\3%\3%\3%\5%\u028c\n%\3&\3&\5&\u0290\n&\3&\5&\u0293"+
		"\n&\3\'\3\'\3\'\5\'\u0298\n\'\3(\3(\3(\3(\5(\u029e\n(\3(\7(\u02a1\n(\f"+
		"(\16(\u02a4\13(\3(\3(\3)\3)\5)\u02aa\n)\3*\3*\3*\3*\5*\u02b0\n*\3*\5*"+
		"\u02b3\n*\3+\3+\5+\u02b7\n+\3+\5+\u02ba\n+\3,\3,\5,\u02be\n,\3,\3,\3,"+
		"\3,\3-\3-\3-\3-\3-\3-\5-\u02ca\n-\3-\5-\u02cd\n-\3-\3-\3-\3-\3-\3-\3-"+
		"\3-\3-\3-\3-\3-\5-\u02db\n-\3-\5-\u02de\n-\3-\3-\3-\3-\3-\5-\u02e5\n-"+
		"\5-\u02e7\n-\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3."+
		"\3.\3.\5.\u02fe\n.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\7.\u030d\n."+
		"\f.\16.\u0310\13.\3/\3/\3/\3/\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60"+
		"\5\60\u031e\n\60\3\61\3\61\3\62\3\62\3\63\3\63\3\63\5\63\u0327\n\63\3"+
		"\63\3\63\3\63\7\63\u032c\n\63\f\63\16\63\u032f\13\63\3\64\3\64\3\64\3"+
		"\64\3\64\5\64\u0336\n\64\3\64\3\64\3\64\3\64\3\64\3\65\3\65\5\65\u033f"+
		"\n\65\3\65\3\65\5\65\u0343\n\65\3\65\3\65\3\65\3\65\3\65\3\65\3\65\3\65"+
		"\5\65\u034d\n\65\3\66\3\66\3\66\7\66\u0352\n\66\f\66\16\66\u0355\13\66"+
		"\3\67\3\67\3\67\7\67\u035a\n\67\f\67\16\67\u035d\13\67\38\38\38\78\u0362"+
		"\n8\f8\168\u0365\138\39\59\u0368\n9\39\39\3:\3:\3:\3:\3:\3:\3:\3:\3:\3"+
		":\3:\3:\3:\3:\3:\3:\3:\3:\5:\u037e\n:\3:\3:\3:\3:\3:\3:\3:\5:\u0387\n"+
		":\3:\3:\3:\3:\5:\u038d\n:\3:\3:\3:\3:\5:\u0393\n:\3:\3:\3:\3:\5:\u0399"+
		"\n:\3:\3:\3:\3:\3:\3:\3:\3:\5:\u03a3\n:\3;\3;\3;\3;\3;\5;\u03aa\n;\3;"+
		"\7;\u03ad\n;\f;\16;\u03b0\13;\3<\3<\5<\u03b4\n<\3<\3<\5<\u03b8\n<\3<\3"+
		"<\3<\3<\3<\6<\u03bf\n<\r<\16<\u03c0\5<\u03c3\n<\3=\3=\5=\u03c7\n=\3=\3"+
		"=\3=\5=\u03cc\n=\3=\3=\5=\u03d0\n=\5=\u03d2\n=\3=\3=\3=\5=\u03d7\n=\3"+
		"=\3=\3=\3=\7=\u03dd\n=\f=\16=\u03e0\13=\5=\u03e2\n=\3=\3=\5=\u03e6\n="+
		"\3=\3=\3=\3=\3=\7=\u03ed\n=\f=\16=\u03f0\13=\3=\3=\5=\u03f4\n=\3=\3=\3"+
		"=\3=\3=\3=\3=\7=\u03fd\n=\f=\16=\u0400\13=\3=\3=\5=\u0404\n=\3=\3=\5="+
		"\u0408\n=\3>\3>\3>\3?\3?\3?\3?\3?\7?\u0412\n?\f?\16?\u0415\13?\3?\3?\3"+
		"?\3?\3?\3?\3?\3?\3?\5?\u0420\n?\5?\u0422\n?\3@\3@\3@\3@\3@\3@\5@\u042a"+
		"\n@\3@\3@\3@\3@\3@\3@\5@\u0432\n@\3@\5@\u0435\n@\5@\u0437\n@\3A\3A\3A"+
		"\3A\3A\5A\u043e\nA\3B\3B\5B\u0442\nB\3C\3C\3D\3D\3D\3D\3D\7D\u044b\nD"+
		"\fD\16D\u044e\13D\3D\3D\3E\3E\3E\3E\3E\3E\3E\3E\3E\3E\3E\3E\3E\3E\7E\u0460"+
		"\nE\fE\16E\u0463\13E\3E\3E\3E\3E\3E\5E\u046a\nE\3F\3F\3F\3F\5F\u0470\n"+
		"F\3G\3G\3G\7G\u0475\nG\fG\16G\u0478\13G\3H\3H\3H\5H\u047d\nH\3H\3H\3H"+
		"\5H\u0482\nH\3H\3H\3H\3H\3H\3H\5H\u048a\nH\3H\5H\u048d\nH\5H\u048f\nH"+
		"\3I\3I\3I\3I\3J\3J\3J\3J\3J\5J\u049a\nJ\3K\3K\7K\u049e\nK\fK\16K\u04a1"+
		"\13K\3L\3L\5L\u04a5\nL\3L\3L\5L\u04a9\nL\3L\3L\3L\5L\u04ae\nL\5L\u04b0"+
		"\nL\5L\u04b2\nL\3M\3M\3M\3M\3M\3M\3M\3M\3N\5N\u04bd\nN\3N\3N\5N\u04c1"+
		"\nN\5N\u04c3\nN\3N\5N\u04c6\nN\3N\3N\3N\3N\3N\3N\3N\3N\3N\3N\3N\3N\3N"+
		"\3N\3N\3N\3N\3N\5N\u04da\nN\3N\5N\u04dd\nN\3N\5N\u04e0\nN\5N\u04e2\nN"+
		"\3O\3O\5O\u04e6\nO\3P\3P\3P\3P\3P\3P\3P\3P\7P\u04f0\nP\fP\16P\u04f3\13"+
		"P\3P\5P\u04f6\nP\3P\3P\3Q\3Q\3Q\3Q\3R\3R\3R\3R\3R\5R\u0503\nR\3S\3S\3"+
		"S\5S\u0508\nS\3T\3T\3U\5U\u050d\nU\3U\3U\3V\3V\5V\u0513\nV\3W\5W\u0516"+
		"\nW\3W\3W\3W\3W\7W\u051c\nW\fW\16W\u051f\13W\3W\3W\3X\5X\u0524\nX\3X\3"+
		"X\3X\3X\3X\7X\u052b\nX\fX\16X\u052e\13X\3X\3X\3X\3X\3X\3X\3X\3X\3X\3X"+
		"\3X\3X\7X\u053c\nX\fX\16X\u053f\13X\3X\3X\3X\5X\u0544\nX\3X\3X\3X\3X\3"+
		"X\3X\5X\u054c\nX\3Y\3Y\3Z\3Z\3[\3[\3[\3[\7[\u0556\n[\f[\16[\u0559\13["+
		"\3[\3[\3\\\3\\\5\\\u055f\n\\\3]\3]\3]\3]\3]\3]\3]\3]\3]\7]\u056a\n]\f"+
		"]\16]\u056d\13]\3^\3^\3^\7^\u0572\n^\f^\16^\u0575\13^\3_\3_\3_\3_\3_\3"+
		"_\3_\6_\u057e\n_\r_\16_\u057f\3_\3_\5_\u0584\n_\3_\3_\3_\3_\3_\3_\3_\3"+
		"_\6_\u058e\n_\r_\16_\u058f\3_\3_\5_\u0594\n_\3_\3_\5_\u0598\n_\3`\3`\3"+
		"`\3`\3`\3`\3`\3`\3`\3`\3`\3`\3`\3`\3`\3`\3`\3`\5`\u05ac\n`\3a\3a\3a\3"+
		"a\3a\3a\5a\u05b4\na\3b\3b\3b\3b\3b\7b\u05bb\nb\fb\16b\u05be\13b\3b\3b"+
		"\3c\3c\3c\3c\3c\3c\3c\3c\3c\3d\3d\3d\3d\3d\3d\3d\3e\3e\3e\3e\3e\3e\3e"+
		"\3f\3f\5f\u05db\nf\3g\3g\3g\3g\3g\3g\3g\3g\3g\3g\5g\u05e7\ng\3h\3h\3h"+
		"\3h\3i\3i\3i\3i\3i\3j\3j\3j\3j\3j\5j\u05f7\nj\3j\3j\3j\3j\3j\3j\3j\3j"+
		"\3j\3j\3j\3j\3j\3j\3j\3j\3j\3j\3j\3j\5j\u060d\nj\3j\3j\3j\3j\3j\5j\u0614"+
		"\nj\3j\3j\3j\3j\3j\5j\u061b\nj\3j\3j\3j\3j\3j\5j\u0622\nj\3j\3j\3j\3j"+
		"\3j\5j\u0629\nj\3j\3j\3j\3j\3j\5j\u0630\nj\3j\3j\3j\3j\3j\5j\u0637\nj"+
		"\3j\3j\3j\3j\3j\5j\u063e\nj\3j\3j\3j\3j\5j\u0644\nj\3j\3j\5j\u0648\nj"+
		"\3j\3j\3j\3j\3j\5j\u064f\nj\3j\3j\3j\3j\5j\u0655\nj\3j\3j\5j\u0659\nj"+
		"\5j\u065b\nj\3k\5k\u065e\nk\3k\3k\3l\3l\3l\3l\3m\3m\3m\5m\u0669\nm\3m"+
		"\5m\u066c\nm\3m\5m\u066f\nm\3m\3m\3n\3n\3n\3o\3o\3o\3o\3o\3o\5o\u067c"+
		"\no\3p\3p\5p\u0680\np\3q\3q\3q\3q\3q\3q\5q\u0688\nq\3r\3r\3r\3r\5r\u068e"+
		"\nr\3s\3s\3s\3s\3s\3s\3s\3s\3s\3s\5s\u069a\ns\3s\3s\3s\3s\3s\5s\u06a1"+
		"\ns\3s\3s\3t\3t\3t\5t\u06a8\nt\3t\3t\3t\3t\3t\5t\u06af\nt\3t\3t\3u\3u"+
		"\3u\5u\u06b6\nu\3u\3u\3v\3v\3v\5v\u06bd\nv\3v\3v\3v\3v\3v\5v\u06c4\nv"+
		"\3v\3v\3w\3w\5w\u06ca\nw\3x\3x\3x\5x\u06cf\nx\3x\3x\3y\3y\3y\7y\u06d6"+
		"\ny\fy\16y\u06d9\13y\3z\3z\3{\3{\5{\u06df\n{\3|\3|\3}\3}\3~\5~\u06e6\n"+
		"~\3~\3~\3\177\3\177\3\u0080\3\u0080\3\u0080\3\u0080\3\u0080\5\u0080\u06f1"+
		"\n\u0080\3\u0081\3\u0081\5\u0081\u06f5\n\u0081\3\u0081\3\u0081\3\u0081"+
		"\3\u0081\5\u0081\u06fb\n\u0081\3\u0081\5\u0081\u06fe\n\u0081\3\u0082\3"+
		"\u0082\5\u0082\u0702\n\u0082\3\u0083\3\u0083\3\u0083\3\u0083\5\u0083\u0708"+
		"\n\u0083\3\u0083\3\u0083\5\u0083\u070c\n\u0083\3\u0083\3\u0083\5\u0083"+
		"\u0710\n\u0083\3\u0084\5\u0084\u0713\n\u0084\3\u0084\3\u0084\3\u0085\3"+
		"\u0085\3\u0086\3\u0086\3\u0086\3\u0086\5\u0086\u071d\n\u0086\3\u0087\3"+
		"\u0087\3\u0088\3\u0088\3\u0088\3\u0088\3\u0088\3\u0088\3\u0088\3\u0088"+
		"\3\u0088\3\u0088\3\u0088\3\u0088\3\u0088\3\u0088\3\u0088\3\u0088\3\u0088"+
		"\3\u0088\3\u0088\3\u0088\3\u0088\5\u0088\u0736\n\u0088\3\u0089\3\u0089"+
		"\3\u0089\2\3Z\u008a\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60"+
		"\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086"+
		"\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c\u009e"+
		"\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6"+
		"\u00b8\u00ba\u00bc\u00be\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc\u00ce"+
		"\u00d0\u00d2\u00d4\u00d6\u00d8\u00da\u00dc\u00de\u00e0\u00e2\u00e4\u00e6"+
		"\u00e8\u00ea\u00ec\u00ee\u00f0\u00f2\u00f4\u00f6\u00f8\u00fa\u00fc\u00fe"+
		"\u0100\u0102\u0104\u0106\u0108\u010a\u010c\u010e\u0110\2*\4\2\u01b4\u01b4"+
		"\u01b8\u01b8\4\2bb\u0083\u0083\6\2\16\16\20\20\u0167\u0167\u019e\u019e"+
		"\4\2EEMM\5\2\16\16\20\20\26\26\4\2]]\u0083\u0083\4\2@@\u00aa\u00aa\4\2"+
		"\24\24\u01b5\u01b5\5\2\u00ba\u00ba\u00e1\u00e1\u0104\u0104\4\2AA\u00ab"+
		"\u00ab\3\2\u01d3\u01d4\3\2\u01d0\u01d2\5\2\3\3\u01d3\u01d4\u01d6\u01d8"+
		"\5\2\u00fd\u00fe\u0142\u0143\u01aa\u01ab\5\2\26\26\31\31\u00b6\u00b6\4"+
		"\2\26\26JJ\4\2\u0172\u0172\u0174\u0174\4\2\u0113\u0113\u0145\u0145\4\2"+
		"\33\33GG\4\2\u008e\u008e\u0120\u0120\5\2||\u00f1\u00f1\u0120\u0120\4\2"+
		"ii\u0173\u0173\4\2\u0081\u0081\u01b3\u01b4\5\2ccww\u00a4\u00a4\7\2||\u00b1"+
		"\u00b1\u0120\u0120\u0137\u0137\u016b\u016b\4\2\u01b4\u01b4\u01b6\u01b6"+
		"\3\2\u01b4\u01b5\4\2--\u00f1\u00f2\4\2\66\66\u00b7\u00b7\25\2\u00dc\u00dd"+
		"\u00ff\u00ff\u0107\u0107\u0112\u0112\u0116\u0116\u011b\u011b\u0125\u0125"+
		"\u0127\u0127\u0131\u0131\u0140\u0140\u014d\u014d\u0157\u0157\u0159\u0159"+
		"\u015b\u015b\u016c\u016c\u0171\u0171\u017f\u0181\u0187\u0187\u0191\u0191"+
		"+\2\34\34\u00da\u00db\u00df\u00e0\u00e4\u00e4\u00e6\u00e6\u00ea\u00ec"+
		"\u00ef\u00ef\u00f5\u00f5\u00f8\u00f8\u00fd\u00fd\u0100\u0100\u010b\u010b"+
		"\u010e\u010e\u0114\u0114\u0118\u0119\u0121\u0122\u012d\u012d\u012f\u012f"+
		"\u0134\u0135\u0138\u0139\u013c\u013c\u013f\u013f\u0142\u0142\u0144\u0144"+
		"\u015a\u015a\u015e\u015e\u0160\u0161\u016e\u016e\u0171\u0171\u017a\u017a"+
		"\u017d\u017e\u0182\u0183\u0185\u0185\u018b\u018b\u0190\u0190\u0192\u0192"+
		"\u0196\u0197\u019d\u019d\u01a1\u01a1\u01a6\u01a6\u01aa\u01aa\7\2==\u0106"+
		"\u0106\u0158\u0158\u0162\u0162\u019f\u019f\4\2\u0163\u0163\u0174\u0174"+
		"\4\2\u0084\u0084\u0086\u0086\4\2,,\177\177\4\2YY\u00c4\u00c4\4\2\u013b"+
		"\u013b\u01b4\u01b4\3\2\u01b8\u01b9\67\2\13\13^^\u00d9\u00d9\u00de\u00de"+
		"\u00e1\u00e3\u00e7\u00e9\u00ee\u00ee\u00f0\u00f2\u00f4\u00f4\u00f6\u00f7"+
		"\u00fd\u00fe\u0101\u0105\u0108\u0108\u010d\u010d\u010f\u0110\u0113\u0113"+
		"\u0115\u0115\u0117\u0117\u011a\u011a\u011c\u0120\u0123\u0124\u0126\u0126"+
		"\u0128\u012b\u012e\u012e\u0130\u0130\u0133\u0133\u0136\u0137\u013a\u013b"+
		"\u013d\u013d\u0141\u0143\u0145\u0146\u0148\u014c\u014e\u0156\u015c\u015d"+
		"\u0163\u0166\u0168\u0168\u016a\u016b\u016d\u016d\u0170\u0170\u0172\u0179"+
		"\u017b\u017c\u0184\u0184\u0186\u0186\u0188\u018a\u018c\u018d\u018f\u018f"+
		"\u0191\u0191\u0193\u0195\u0198\u019c\u01a0\u01a0\u01a2\u01a5\u01a7\u01ab"+
		"\u01b5\u01b5\3\2\u01be\u01c5\u0808\2\u011d\3\2\2\2\4\u011f\3\2\2\2\6\u012d"+
		"\3\2\2\2\b\u012f\3\2\2\2\n\u0138\3\2\2\2\f\u0146\3\2\2\2\16\u0151\3\2"+
		"\2\2\20\u015b\3\2\2\2\22\u015d\3\2\2\2\24\u0161\3\2\2\2\26\u0169\3\2\2"+
		"\2\30\u016e\3\2\2\2\32\u0171\3\2\2\2\34\u0183\3\2\2\2\36\u0185\3\2\2\2"+
		" \u018a\3\2\2\2\"\u018d\3\2\2\2$\u0196\3\2\2\2&\u019d\3\2\2\2(\u01a5\3"+
		"\2\2\2*\u01a8\3\2\2\2,\u01ba\3\2\2\2.\u01d2\3\2\2\2\60\u01e5\3\2\2\2\62"+
		"\u01e7\3\2\2\2\64\u01fa\3\2\2\2\66\u020a\3\2\2\28\u0238\3\2\2\2:\u023a"+
		"\3\2\2\2<\u0247\3\2\2\2>\u0251\3\2\2\2@\u0275\3\2\2\2B\u0277\3\2\2\2D"+
		"\u027c\3\2\2\2F\u0285\3\2\2\2H\u0288\3\2\2\2J\u028d\3\2\2\2L\u0294\3\2"+
		"\2\2N\u0299\3\2\2\2P\u02a9\3\2\2\2R\u02ab\3\2\2\2T\u02b6\3\2\2\2V\u02bd"+
		"\3\2\2\2X\u02e6\3\2\2\2Z\u02fd\3\2\2\2\\\u0311\3\2\2\2^\u031d\3\2\2\2"+
		"`\u031f\3\2\2\2b\u0321\3\2\2\2d\u0323\3\2\2\2f\u0330\3\2\2\2h\u034c\3"+
		"\2\2\2j\u034e\3\2\2\2l\u0356\3\2\2\2n\u035e\3\2\2\2p\u0367\3\2\2\2r\u03a2"+
		"\3\2\2\2t\u03a9\3\2\2\2v\u03b7\3\2\2\2x\u03c4\3\2\2\2z\u0409\3\2\2\2|"+
		"\u040c\3\2\2\2~\u0436\3\2\2\2\u0080\u0438\3\2\2\2\u0082\u043f\3\2\2\2"+
		"\u0084\u0443\3\2\2\2\u0086\u0445\3\2\2\2\u0088\u0469\3\2\2\2\u008a\u046b"+
		"\3\2\2\2\u008c\u0471\3\2\2\2\u008e\u048e\3\2\2\2\u0090\u0490\3\2\2\2\u0092"+
		"\u0499\3\2\2\2\u0094\u049b\3\2\2\2\u0096\u04b1\3\2\2\2\u0098\u04b3\3\2"+
		"\2\2\u009a\u04e1\3\2\2\2\u009c\u04e3\3\2\2\2\u009e\u04e7\3\2\2\2\u00a0"+
		"\u04f9\3\2\2\2\u00a2\u0502\3\2\2\2\u00a4\u0507\3\2\2\2\u00a6\u0509\3\2"+
		"\2\2\u00a8\u050c\3\2\2\2\u00aa\u0510\3\2\2\2\u00ac\u0515\3\2\2\2\u00ae"+
		"\u0523\3\2\2\2\u00b0\u054d\3\2\2\2\u00b2\u054f\3\2\2\2\u00b4\u0551\3\2"+
		"\2\2\u00b6\u055e\3\2\2\2\u00b8\u0560\3\2\2\2\u00ba\u056e\3\2\2\2\u00bc"+
		"\u0597\3\2\2\2\u00be\u05ab\3\2\2\2\u00c0\u05b3\3\2\2\2\u00c2\u05b5\3\2"+
		"\2\2\u00c4\u05c1\3\2\2\2\u00c6\u05ca\3\2\2\2\u00c8\u05d1\3\2\2\2\u00ca"+
		"\u05da\3\2\2\2\u00cc\u05e6\3\2\2\2\u00ce\u05e8\3\2\2\2\u00d0\u05ec\3\2"+
		"\2\2\u00d2\u065a\3\2\2\2\u00d4\u065d\3\2\2\2\u00d6\u0661\3\2\2\2\u00d8"+
		"\u0665\3\2\2\2\u00da\u0672\3\2\2\2\u00dc\u067b\3\2\2\2\u00de\u067f\3\2"+
		"\2\2\u00e0\u0687\3\2\2\2\u00e2\u068d\3\2\2\2\u00e4\u06a0\3\2\2\2\u00e6"+
		"\u06ae\3\2\2\2\u00e8\u06b5\3\2\2\2\u00ea\u06c3\3\2\2\2\u00ec\u06c9\3\2"+
		"\2\2\u00ee\u06ce\3\2\2\2\u00f0\u06d2\3\2\2\2\u00f2\u06da\3\2\2\2\u00f4"+
		"\u06de\3\2\2\2\u00f6\u06e0\3\2\2\2\u00f8\u06e2\3\2\2\2\u00fa\u06e5\3\2"+
		"\2\2\u00fc\u06e9\3\2\2\2\u00fe\u06f0\3\2\2\2\u0100\u06f2\3\2\2\2\u0102"+
		"\u0701\3\2\2\2\u0104\u070f\3\2\2\2\u0106\u0712\3\2\2\2\u0108\u0716\3\2"+
		"\2\2\u010a\u071c\3\2\2\2\u010c\u071e\3\2\2\2\u010e\u0735\3\2\2\2\u0110"+
		"\u0737\3\2\2\2\u0112\u011e\5*\26\2\u0113\u011e\5\4\3\2\u0114\u011e\5\n"+
		"\6\2\u0115\u011e\5\f\7\2\u0116\u011e\5\16\b\2\u0117\u011e\5\20\t\2\u0118"+
		"\u011e\5\62\32\2\u0119\u011e\5\64\33\2\u011a\u011e\5\66\34\2\u011b\u011e"+
		"\5<\37\2\u011c\u011e\5> \2\u011d\u0112\3\2\2\2\u011d\u0113\3\2\2\2\u011d"+
		"\u0114\3\2\2\2\u011d\u0115\3\2\2\2\u011d\u0116\3\2\2\2\u011d\u0117\3\2"+
		"\2\2\u011d\u0118\3\2\2\2\u011d\u0119\3\2\2\2\u011d\u011a\3\2\2\2\u011d"+
		"\u011b\3\2\2\2\u011d\u011c\3\2\2\2\u011e\3\3\2\2\2\u011f\u0122\78\2\2"+
		"\u0120\u0121\t\2\2\2\u0121\u0123\7\u01d2\2\2\u0122\u0120\3\2\2\2\u0122"+
		"\u0123\3\2\2\2\u0123\u0125\3\2\2\2\u0124\u0126\5\6\4\2\u0125\u0124\3\2"+
		"\2\2\u0125\u0126\3\2\2\2\u0126\u0127\3\2\2\2\u0127\u0128\7\u0176\2\2\u0128"+
		"\u0129\t\3\2\2\u0129\u012b\5\u00e6t\2\u012a\u012c\5\b\5\2\u012b\u012a"+
		"\3\2\2\2\u012b\u012c\3\2\2\2\u012c\5\3\2\2\2\u012d\u012e\t\4\2\2\u012e"+
		"\7\3\2\2\2\u012f\u0130\7\u0086\2\2\u0130\u0135\5\u00f2z\2\u0131\u0132"+
		"\7\u01cd\2\2\u0132\u0134\5\u00f2z\2\u0133\u0131\3\2\2\2\u0134\u0137\3"+
		"\2\2\2\u0135\u0133\3\2\2\2\u0135\u0136\3\2\2\2\u0136\t\3\2\2\2\u0137\u0135"+
		"\3\2\2\2\u0138\u013b\t\5\2\2\u0139\u013a\t\2\2\2\u013a\u013c\7\u01d2\2"+
		"\2\u013b\u0139\3\2\2\2\u013b\u013c\3\2\2\2\u013c\u013e\3\2\2\2\u013d\u013f"+
		"\5\6\4\2\u013e\u013d\3\2\2\2\u013e\u013f\3\2\2\2\u013f\u0140\3\2\2\2\u0140"+
		"\u0141\7\21\2\2\u0141\u0142\7\u0083\2\2\u0142\u0144\5\u00e6t\2\u0143\u0145"+
		"\5\b\5\2\u0144\u0143\3\2\2\2\u0144\u0145\3\2\2\2\u0145\13\3\2\2\2\u0146"+
		"\u0148\7\17\2\2\u0147\u0149\t\6\2\2\u0148\u0147\3\2\2\2\u0148\u0149\3"+
		"\2\2\2\u0149\u014a\3\2\2\2\u014a\u014d\7\21\2\2\u014b\u014c\t\7\2\2\u014c"+
		"\u014e\5\u010a\u0086\2\u014d\u014b\3\2\2\2\u014d\u014e\3\2\2\2\u014e\r"+
		"\3\2\2\2\u014f\u0152\5\26\f\2\u0150\u0152\5\30\r\2\u0151\u014f\3\2\2\2"+
		"\u0151\u0150\3\2\2\2\u0152\17\3\2\2\2\u0153\u015c\5B\"\2\u0154\u015c\5"+
		"D#\2\u0155\u015c\5F$\2\u0156\u015c\5H%\2\u0157\u015c\5J&\2\u0158\u015c"+
		"\5L\'\2\u0159\u015c\5\22\n\2\u015a\u015c\5\24\13\2\u015b\u0153\3\2\2\2"+
		"\u015b\u0154\3\2\2\2\u015b\u0155\3\2\2\2\u015b\u0156\3\2\2\2\u015b\u0157"+
		"\3\2\2\2\u015b\u0158\3\2\2\2\u015b\u0159\3\2\2\2\u015b\u015a\3\2\2\2\u015c"+
		"\21\3\2\2\2\u015d\u015e\78\2\2\u015e\u015f\t\b\2\2\u015f\u0160\5\u010a"+
		"\u0086\2\u0160\23\3\2\2\2\u0161\u0162\7M\2\2\u0162\u0165\t\b\2\2\u0163"+
		"\u0164\7l\2\2\u0164\u0166\7V\2\2\u0165\u0163\3\2\2\2\u0165\u0166\3\2\2"+
		"\2\u0166\u0167\3\2\2\2\u0167\u0168\5\u010a\u0086\2\u0168\25\3\2\2\2\u0169"+
		"\u016a\7\u00b3\2\2\u016a\u016b\5\32\16\2\u016b\u016c\7\u01ba\2\2\u016c"+
		"\u016d\5\34\17\2\u016d\27\3\2\2\2\u016e\u016f\7\22\2\2\u016f\u0170\5\32"+
		"\16\2\u0170\31\3\2\2\2\u0171\u0176\7\u01b5\2\2\u0172\u0173\7\u01c6\2\2"+
		"\u0173\u0175\t\t\2\2\u0174\u0172\3\2\2\2\u0175\u0178\3\2\2\2\u0176\u0174"+
		"\3\2\2\2\u0176\u0177\3\2\2\2\u0177\33\3\2\2\2\u0178\u0176\3\2\2\2\u0179"+
		"\u0184\7\u01b0\2\2\u017a\u0184\7\u01b6\2\2\u017b\u0180\7\u01b5\2\2\u017c"+
		"\u017d\7\u01cd\2\2\u017d\u017f\7\u01b5\2\2\u017e\u017c\3\2\2\2\u017f\u0182"+
		"\3\2\2\2\u0180\u017e\3\2\2\2\u0180\u0181\3\2\2\2\u0181\u0184\3\2\2\2\u0182"+
		"\u0180\3\2\2\2\u0183\u0179\3\2\2\2\u0183\u017a\3\2\2\2\u0183\u017b\3\2"+
		"\2\2\u0184\35\3\2\2\2\u0185\u0186\7\23\2\2\u0186\u0188\t\2\2\2\u0187\u0189"+
		"\7\u01d2\2\2\u0188\u0187\3\2\2\2\u0188\u0189\3\2\2\2\u0189\37\3\2\2\2"+
		"\u018a\u018b\7\24\2\2\u018b\u018c\5\u0106\u0084\2\u018c!\3\2\2\2\u018d"+
		"\u018e\5\u00e6t\2\u018e\u018f\7\u0176\2\2\u018f\u0191\t\2\2\2\u0190\u0192"+
		"\7\u01d2\2\2\u0191\u0190\3\2\2\2\u0191\u0192\3\2\2\2\u0192#\3\2\2\2\u0193"+
		"\u0195\5&\24\2\u0194\u0193\3\2\2\2\u0195\u0198\3\2\2\2\u0196\u0194\3\2"+
		"\2\2\u0196\u0197\3\2\2\2\u0197\u0199\3\2\2\2\u0198\u0196\3\2\2\2\u0199"+
		"\u019a\7\2\2\3\u019a%\3\2\2\2\u019b\u019e\5(\25\2\u019c\u019e\5\20\t\2"+
		"\u019d\u019b\3\2\2\2\u019d\u019c\3\2\2\2\u019e\'\3\2\2\2\u019f\u01a6\5"+
		"\62\32\2\u01a0\u01a6\5\66\34\2\u01a1\u01a6\58\35\2\u01a2\u01a6\5:\36\2"+
		"\u01a3\u01a6\5<\37\2\u01a4\u01a6\5> \2\u01a5\u019f\3\2\2\2\u01a5\u01a0"+
		"\3\2\2\2\u01a5\u01a1\3\2\2\2\u01a5\u01a2\3\2\2\2\u01a5\u01a3\3\2\2\2\u01a5"+
		"\u01a4\3\2\2\2\u01a6)\3\2\2\2\u01a7\u01a9\5d\63\2\u01a8\u01a7\3\2\2\2"+
		"\u01a8\u01a9\3\2\2\2\u01a9\u01ab\3\2\2\2\u01aa\u01ac\7\u010a\2\2\u01ab"+
		"\u01aa\3\2\2\2\u01ab\u01ac\3\2\2\2\u01ac\u01ad\3\2\2\2\u01ad\u01af\5t"+
		";\2\u01ae\u01b0\5|?\2\u01af\u01ae\3\2\2\2\u01af\u01b0\3\2\2\2\u01b0\u01b2"+
		"\3\2\2\2\u01b1\u01b3\5z>\2\u01b2\u01b1\3\2\2\2\u01b2\u01b3\3\2\2\2\u01b3"+
		"\u01b5\3\2\2\2\u01b4\u01b6\5\36\20\2\u01b5\u01b4\3\2\2\2\u01b5\u01b6\3"+
		"\2\2\2\u01b6\u01b8\3\2\2\2\u01b7\u01b9\7\u01ce\2\2\u01b8\u01b7\3\2\2\2"+
		"\u01b8\u01b9\3\2\2\2\u01b9+\3\2\2\2\u01ba\u01bb\7\u0153\2\2\u01bb\u01c0"+
		"\5.\30\2\u01bc\u01bd\7\u01cd\2\2\u01bd\u01bf\5.\30\2\u01be\u01bc\3\2\2"+
		"\2\u01bf\u01c2\3\2\2\2\u01c0\u01be\3\2\2\2\u01c0\u01c1\3\2\2\2\u01c1\u01ce"+
		"\3\2\2\2\u01c2\u01c0\3\2\2\2\u01c3\u01c6\7r\2\2\u01c4\u01c7\7\u01b3\2"+
		"\2\u01c5\u01c7\5\u00e6t\2\u01c6\u01c4\3\2\2\2\u01c6\u01c5\3\2\2\2\u01c7"+
		"\u01cc\3\2\2\2\u01c8\u01c9\7\u01cb\2\2\u01c9\u01ca\5\u00f0y\2\u01ca\u01cb"+
		"\7\u01cc\2\2\u01cb\u01cd\3\2\2\2\u01cc\u01c8\3\2\2\2\u01cc\u01cd\3\2\2"+
		"\2\u01cd\u01cf\3\2\2\2\u01ce\u01c3\3\2\2\2\u01ce\u01cf\3\2\2\2\u01cf-"+
		"\3\2\2\2\u01d0\u01d3\5\60\31\2\u01d1\u01d3\5Z.\2\u01d2\u01d0\3\2\2\2\u01d2"+
		"\u01d1\3\2\2\2\u01d3\u01d8\3\2\2\2\u01d4\u01d6\7\32\2\2\u01d5\u01d4\3"+
		"\2\2\2\u01d5\u01d6\3\2\2\2\u01d6\u01d7\3\2\2\2\u01d7\u01d9\5\u00b6\\\2"+
		"\u01d8\u01d5\3\2\2\2\u01d8\u01d9\3\2\2\2\u01d9/\3\2\2\2\u01da\u01de\7"+
		"\u0102\2\2\u01db\u01de\7\u0124\2\2\u01dc\u01de\5\u00e6t\2\u01dd\u01da"+
		"\3\2\2\2\u01dd\u01db\3\2\2\2\u01dd\u01dc\3\2\2\2\u01de\u01df\3\2\2\2\u01df"+
		"\u01e2\7\u01c6\2\2\u01e0\u01e3\7\u01d0\2\2\u01e1\u01e3\5\u00f2z\2\u01e2"+
		"\u01e0\3\2\2\2\u01e2\u01e1\3\2\2\2\u01e3\u01e6\3\2\2\2\u01e4\u01e6\7\u01ac"+
		"\2\2\u01e5\u01dd\3\2\2\2\u01e5\u01e4\3\2\2\2\u01e6\61\3\2\2\2\u01e7\u01e8"+
		"\78\2\2\u01e8\u01e9\7\u00ba\2\2\u01e9\u01ea\5\u00e6t\2\u01ea\u01eb\7\u01cb"+
		"\2\2\u01eb\u01f2\5P)\2\u01ec\u01ee\7\u01cd\2\2\u01ed\u01ec\3\2\2\2\u01ed"+
		"\u01ee\3\2\2\2\u01ee\u01ef\3\2\2\2\u01ef\u01f1\5P)\2\u01f0\u01ed\3\2\2"+
		"\2\u01f1\u01f4\3\2\2\2\u01f2\u01f0\3\2\2\2\u01f2\u01f3\3\2\2\2\u01f3\u01f6"+
		"\3\2\2\2\u01f4\u01f2\3\2\2\2\u01f5\u01f7\7\u01cd\2\2\u01f6\u01f5\3\2\2"+
		"\2\u01f6\u01f7\3\2\2\2\u01f7\u01f8\3\2\2\2\u01f8\u01f9\7\u01cc\2\2\u01f9"+
		"\63\3\2\2\2\u01fa\u01fb\78\2\2\u01fb\u01ff\7\u00ba\2\2\u01fc\u01fd\7l"+
		"\2\2\u01fd\u01fe\7\u0080\2\2\u01fe\u0200\7V\2\2\u01ff\u01fc\3\2\2\2\u01ff"+
		"\u0200\3\2\2\2\u0200\u0201\3\2\2\2\u0201\u0203\5\u00e6t\2\u0202\u0204"+
		"\7\u018e\2\2\u0203\u0202\3\2\2\2\u0203\u0204\3\2\2\2\u0204\u0205\3\2\2"+
		"\2\u0205\u0206\7\32\2\2\u0206\u0208\5*\26\2\u0207\u0209\7\u01ce\2\2\u0208"+
		"\u0207\3\2\2\2\u0208\u0209\3\2\2\2\u0209\65\3\2\2\2\u020a\u020b\78\2\2"+
		"\u020b\u020c\7\u00d1\2\2\u020c\u0218\5\u00e8u\2\u020d\u020e\7\u01cb\2"+
		"\2\u020e\u0213\5\u00f2z\2\u020f\u0210\7\u01cd\2\2\u0210\u0212\5\u00f2"+
		"z\2\u0211\u020f\3\2\2\2\u0212\u0215\3\2\2\2\u0213\u0211\3\2\2\2\u0213"+
		"\u0214\3\2\2\2\u0214\u0216\3\2\2\2\u0215\u0213\3\2\2\2\u0216\u0217\7\u01cc"+
		"\2\2\u0217\u0219\3\2\2\2\u0218\u020d\3\2\2\2\u0218\u0219\3\2\2\2\u0219"+
		"\u021a\3\2\2\2\u021a\u021b\7\32\2\2\u021b\u021f\5*\26\2\u021c\u021d\7"+
		"\u00d6\2\2\u021d\u021e\7)\2\2\u021e\u0220\7\u008c\2\2\u021f\u021c\3\2"+
		"\2\2\u021f\u0220\3\2\2\2\u0220\u0222\3\2\2\2\u0221\u0223\7\u01ce\2\2\u0222"+
		"\u0221\3\2\2\2\u0222\u0223\3\2\2\2\u0223\67\3\2\2\2\u0224\u0225\7\27\2"+
		"\2\u0225\u0226\7\u00ba\2\2\u0226\u0227\5\u00e6t\2\u0227\u0228\7\u00b3"+
		"\2\2\u0228\u0229\7\u01cb\2\2\u0229\u022a\7\u0133\2\2\u022a\u022b\7\u01ba"+
		"\2\2\u022b\u022c\t\n\2\2\u022c\u022e\7\u01cc\2\2\u022d\u022f\7\u01ce\2"+
		"\2\u022e\u022d\3\2\2\2\u022e\u022f\3\2\2\2\u022f\u0239\3\2\2\2\u0230\u0231"+
		"\7\27\2\2\u0231\u0232\7\u00ba\2\2\u0232\u0233\5\u00e6t\2\u0233\u0234\7"+
		"\25\2\2\u0234\u0236\5P)\2\u0235\u0237\7\u01ce\2\2\u0236\u0235\3\2\2\2"+
		"\u0236\u0237\3\2\2\2\u0237\u0239\3\2\2\2\u0238\u0224\3\2\2\2\u0238\u0230"+
		"\3\2\2\2\u02399\3\2\2\2\u023a\u023b\7\27\2\2\u023b\u023e\7@\2\2\u023c"+
		"\u023f\5\u010a\u0086\2\u023d\u023f\7:\2\2\u023e\u023c\3\2\2\2\u023e\u023d"+
		"\3\2\2\2\u023f\u0240\3\2\2\2\u0240\u0241\7\u0141\2\2\u0241\u0242\7\u0146"+
		"\2\2\u0242\u0243\7\u01ba\2\2\u0243\u0245\5\u010a\u0086\2\u0244\u0246\7"+
		"\u01ce\2\2\u0245\u0244\3\2\2\2\u0245\u0246\3\2\2\2\u0246;\3\2\2\2\u0247"+
		"\u0248\7M\2\2\u0248\u024b\7\u00ba\2\2\u0249\u024a\7l\2\2\u024a\u024c\7"+
		"V\2\2\u024b\u0249\3\2\2\2\u024b\u024c\3\2\2\2\u024c\u024d\3\2\2\2\u024d"+
		"\u024f\5\u00e6t\2\u024e\u0250\7\u01ce\2\2\u024f\u024e\3\2\2\2\u024f\u0250"+
		"\3\2\2\2\u0250=\3\2\2\2\u0251\u0252\7M\2\2\u0252\u0255\7\u00d1\2\2\u0253"+
		"\u0254\7l\2\2\u0254\u0256\7V\2\2\u0255\u0253\3\2\2\2\u0255\u0256\3\2\2"+
		"\2\u0256\u0257\3\2\2\2\u0257\u025c\5\u00e8u\2\u0258\u0259\7\u01cd\2\2"+
		"\u0259\u025b\5\u00e8u\2\u025a\u0258\3\2\2\2\u025b\u025e\3\2\2\2\u025c"+
		"\u025a\3\2\2\2\u025c\u025d\3\2\2\2\u025d\u0260\3\2\2\2\u025e\u025c\3\2"+
		"\2\2\u025f\u0261\7\u01ce\2\2\u0260\u025f\3\2\2\2\u0260\u0261\3\2\2\2\u0261"+
		"?\3\2\2\2\u0262\u0263\7\u00b3\2\2\u0263\u0266\7\u01b3\2\2\u0264\u0265"+
		"\7\u01c6\2\2\u0265\u0267\5\u010a\u0086\2\u0266\u0264\3\2\2\2\u0266\u0267"+
		"\3\2\2\2\u0267\u0268\3\2\2\2\u0268\u0269\7\u01ba\2\2\u0269\u026b\5Z.\2"+
		"\u026a\u026c\7\u01ce\2\2\u026b\u026a\3\2\2\2\u026b\u026c\3\2\2\2\u026c"+
		"\u0276\3\2\2\2\u026d\u026e\7\u00b3\2\2\u026e\u026f\7\u01b3\2\2\u026f\u0270"+
		"\5\u0110\u0089\2\u0270\u0272\5Z.\2\u0271\u0273\7\u01ce\2\2\u0272\u0271"+
		"\3\2\2\2\u0272\u0273\3\2\2\2\u0273\u0276\3\2\2\2\u0274\u0276\5X-\2\u0275"+
		"\u0262\3\2\2\2\u0275\u026d\3\2\2\2\u0275\u0274\3\2\2\2\u0276A\3\2\2\2"+
		"\u0277\u0278\7\u00cd\2\2\u0278\u027a\5\u010a\u0086\2\u0279\u027b\7\u01ce"+
		"\2\2\u027a\u0279\3\2\2\2\u027a\u027b\3\2\2\2\u027bC\3\2\2\2\u027c\u027d"+
		"\7\17\2\2\u027d\u0280\7\u00bb\2\2\u027e\u027f\7m\2\2\u027f\u0281\5\u010a"+
		"\u0086\2\u0280\u027e\3\2\2\2\u0280\u0281\3\2\2\2\u0281\u0283\3\2\2\2\u0282"+
		"\u0284\7\u01ce\2\2\u0283\u0282\3\2\2\2\u0283\u0284\3\2\2\2\u0284E\3\2"+
		"\2\2\u0285\u0286\7\17\2\2\u0286\u0287\t\13\2\2\u0287G\3\2\2\2\u0288\u0289"+
		"\7H\2\2\u0289\u028b\5\u00e6t\2\u028a\u028c\7\u01ce\2\2\u028b\u028a\3\2"+
		"\2\2\u028b\u028c\3\2\2\2\u028cI\3\2\2\2\u028d\u028f\7\u0169\2\2\u028e"+
		"\u0290\5\u010a\u0086\2\u028f\u028e\3\2\2\2\u028f\u0290\3\2\2\2\u0290\u0292"+
		"\3\2\2\2\u0291\u0293\7\u01ce\2\2\u0292\u0291\3\2\2\2\u0292\u0293\3\2\2"+
		"\2\u0293K\3\2\2\2\u0294\u0295\7\17\2\2\u0295\u0297\7\u00f3\2\2\u0296\u0298"+
		"\7\u01ce\2\2\u0297\u0296\3\2\2\2\u0297\u0298\3\2\2\2\u0298M\3\2\2\2\u0299"+
		"\u029a\7\u00ba\2\2\u029a\u029b\7\u01cb\2\2\u029b\u02a2\5P)\2\u029c\u029e"+
		"\7\u01cd\2\2\u029d\u029c\3\2\2\2\u029d\u029e\3\2\2\2\u029e\u029f\3\2\2"+
		"\2\u029f\u02a1\5P)\2\u02a0\u029d\3\2\2\2\u02a1\u02a4\3\2\2\2\u02a2\u02a0"+
		"\3\2\2\2\u02a2\u02a3\3\2\2\2\u02a3\u02a5\3\2\2\2\u02a4\u02a2\3\2\2\2\u02a5"+
		"\u02a6\7\u01cc\2\2\u02a6O\3\2\2\2\u02a7\u02aa\5R*\2\u02a8\u02aa\5V,\2"+
		"\u02a9\u02a7\3\2\2\2\u02a9\u02a8\3\2\2\2\u02aaQ\3\2\2\2\u02ab\u02af\5"+
		"\u00f2z\2\u02ac\u02b0\5\u0100\u0081\2\u02ad\u02ae\7\32\2\2\u02ae\u02b0"+
		"\5Z.\2\u02af\u02ac\3\2\2\2\u02af\u02ad\3\2\2\2\u02b0\u02b2\3\2\2\2\u02b1"+
		"\u02b3\5\u00fa~\2\u02b2\u02b1\3\2\2\2\u02b2\u02b3\3\2\2\2\u02b3S\3\2\2"+
		"\2\u02b4\u02b5\7\62\2\2\u02b5\u02b7\5\u010a\u0086\2\u02b6\u02b4\3\2\2"+
		"\2\u02b6\u02b7\3\2\2\2\u02b7\u02b9\3\2\2\2\u02b8\u02ba\5\u00fa~\2\u02b9"+
		"\u02b8\3\2\2\2\u02b9\u02ba\3\2\2\2\u02baU\3\2\2\2\u02bb\u02bc\7\62\2\2"+
		"\u02bc\u02be\5\u010a\u0086\2\u02bd\u02bb\3\2\2\2\u02bd\u02be\3\2\2\2\u02be"+
		"\u02bf\3\2\2\2\u02bf\u02c0\7\u01cb\2\2\u02c0\u02c1\5\u00f0y\2\u02c1\u02c2"+
		"\7\u01cc\2\2\u02c2W\3\2\2\2\u02c3\u02c4\7\u00b3\2\2\u02c4\u02c9\5\u010a"+
		"\u0086\2\u02c5\u02ca\5\u010a\u0086\2\u02c6\u02ca\5\u0104\u0083\2\u02c7"+
		"\u02ca\7\u01b3\2\2\u02c8\u02ca\5\u00f6|\2\u02c9\u02c5\3\2\2\2\u02c9\u02c6"+
		"\3\2\2\2\u02c9\u02c7\3\2\2\2\u02c9\u02c8\3\2\2\2\u02ca\u02cc\3\2\2\2\u02cb"+
		"\u02cd\7\u01ce\2\2\u02cc\u02cb\3\2\2\2\u02cc\u02cd\3\2\2\2\u02cd\u02e7"+
		"\3\2\2\2\u02ce\u02cf\7\u00b3\2\2\u02cf\u02d0\7\u00c2\2\2\u02d0\u02d1\7"+
		"\u0128\2\2\u02d1\u02da\7\u012e\2\2\u02d2\u02d3\7\u009a\2\2\u02d3\u02db"+
		"\7\u019c\2\2\u02d4\u02d5\7\u009a\2\2\u02d5\u02db\7\u00f0\2\2\u02d6\u02d7"+
		"\7\u016d\2\2\u02d7\u02db\7\u009a\2\2\u02d8\u02db\7\u0184\2\2\u02d9\u02db"+
		"\7\u017c\2\2\u02da\u02d2\3\2\2\2\u02da\u02d4\3\2\2\2\u02da\u02d6\3\2\2"+
		"\2\u02da\u02d8\3\2\2\2\u02da\u02d9\3\2\2\2\u02db\u02dd\3\2\2\2\u02dc\u02de"+
		"\7\u01ce\2\2\u02dd\u02dc\3\2\2\2\u02dd\u02de\3\2\2\2\u02de\u02e7\3\2\2"+
		"\2\u02df\u02e0\7\u00b3\2\2\u02e0\u02e1\7k\2\2\u02e1\u02e2\5\u00e6t\2\u02e2"+
		"\u02e4\5\u00f6|\2\u02e3\u02e5\7\u01ce\2\2\u02e4\u02e3\3\2\2\2\u02e4\u02e5"+
		"\3\2\2\2\u02e5\u02e7\3\2\2\2\u02e6\u02c3\3\2\2\2\u02e6\u02ce\3\2\2\2\u02e6"+
		"\u02df\3\2\2\2\u02e7Y\3\2\2\2\u02e8\u02e9\b.\1\2\u02e9\u02fe\7\u0081\2"+
		"\2\u02ea\u02fe\7\u01b3\2\2\u02eb\u02fe\5\u0104\u0083\2\u02ec\u02fe\5\u00fc"+
		"\177\2\u02ed\u02fe\5\u00a4S\2\u02ee\u02fe\5\u00bc_\2\u02ef\u02fe\5\u00ee"+
		"x\2\u02f0\u02f1\7\u01cb\2\2\u02f1\u02f2\5Z.\2\u02f2\u02f3\7\u01cc\2\2"+
		"\u02f3\u02fe\3\2\2\2\u02f4\u02f5\7\u01cb\2\2\u02f5\u02f6\5`\61\2\u02f6"+
		"\u02f7\7\u01cc\2\2\u02f7\u02fe\3\2\2\2\u02f8\u02f9\7\u01d5\2\2\u02f9\u02fe"+
		"\5Z.\b\u02fa\u02fb\t\f\2\2\u02fb\u02fe\5Z.\6\u02fc\u02fe\5\\/\2\u02fd"+
		"\u02e8\3\2\2\2\u02fd\u02ea\3\2\2\2\u02fd\u02eb\3\2\2\2\u02fd\u02ec\3\2"+
		"\2\2\u02fd\u02ed\3\2\2\2\u02fd\u02ee\3\2\2\2\u02fd\u02ef\3\2\2\2\u02fd"+
		"\u02f0\3\2\2\2\u02fd\u02f4\3\2\2\2\u02fd\u02f8\3\2\2\2\u02fd\u02fa\3\2"+
		"\2\2\u02fd\u02fc\3\2\2\2\u02fe\u030e\3\2\2\2\u02ff\u0300\f\7\2\2\u0300"+
		"\u0301\t\r\2\2\u0301\u030d\5Z.\b\u0302\u0303\f\5\2\2\u0303\u0304\t\16"+
		"\2\2\u0304\u030d\5Z.\6\u0305\u0306\f\4\2\2\u0306\u0307\5\u010e\u0088\2"+
		"\u0307\u0308\5Z.\5\u0308\u030d\3\2\2\2\u0309\u030a\f\r\2\2\u030a\u030b"+
		"\7.\2\2\u030b\u030d\5\u010a\u0086\2\u030c\u02ff\3\2\2\2\u030c\u0302\3"+
		"\2\2\2\u030c\u0305\3\2\2\2\u030c\u0309\3\2\2\2\u030d\u0310\3\2\2\2\u030e"+
		"\u030c\3\2\2\2\u030e\u030f\3\2\2\2\u030f[\3\2\2\2\u0310\u030e\3\2\2\2"+
		"\u0311\u0312\7\u0126\2\2\u0312\u0313\5^\60\2\u0313\u0314\t\17\2\2\u0314"+
		"]\3\2\2\2\u0315\u031e\7\u0081\2\2\u0316\u031e\5\u0104\u0083\2\u0317\u031e"+
		"\5\u00a4S\2\u0318\u031e\7\u01b3\2\2\u0319\u031a\7\u01cb\2\2\u031a\u031b"+
		"\5^\60\2\u031b\u031c\7\u01cc\2\2\u031c\u031e\3\2\2\2\u031d\u0315\3\2\2"+
		"\2\u031d\u0316\3\2\2\2\u031d\u0317\3\2\2\2\u031d\u0318\3\2\2\2\u031d\u0319"+
		"\3\2\2\2\u031e_\3\2\2\2\u031f\u0320\5*\26\2\u0320a\3\2\2\2\u0321\u0322"+
		"\5x=\2\u0322c\3\2\2\2\u0323\u0326\7\u00d6\2\2\u0324\u0325\7\u01a9\2\2"+
		"\u0325\u0327\7\u01cd\2\2\u0326\u0324\3\2\2\2\u0326\u0327\3\2\2\2\u0327"+
		"\u0328\3\2\2\2\u0328\u032d\5f\64\2\u0329\u032a\7\u01cd\2\2\u032a\u032c"+
		"\5f\64\2\u032b\u0329\3\2\2\2\u032c\u032f\3\2\2\2\u032d\u032b\3\2\2\2\u032d"+
		"\u032e\3\2\2\2\u032ee\3\2\2\2\u032f\u032d\3\2\2\2\u0330\u0335\5\u010a"+
		"\u0086\2\u0331\u0332\7\u01cb\2\2\u0332\u0333\5\u00f0y\2\u0333\u0334\7"+
		"\u01cc\2\2\u0334\u0336\3\2\2\2\u0335\u0331\3\2\2\2\u0335\u0336\3\2\2\2"+
		"\u0336\u0337\3\2\2\2\u0337\u0338\7\32\2\2\u0338\u0339\7\u01cb\2\2\u0339"+
		"\u033a\5*\26\2\u033a\u033b\7\u01cc\2\2\u033bg\3\2\2\2\u033c\u033f\5\u00ee"+
		"x\2\u033d\u033f\7\u01b3\2\2\u033e\u033c\3\2\2\2\u033e\u033d\3\2\2\2\u033f"+
		"\u0342\3\2\2\2\u0340\u0343\7\u01ba\2\2\u0341\u0343\5\u0110\u0089\2\u0342"+
		"\u0340\3\2\2\2\u0342\u0341\3\2\2\2\u0343\u0344\3\2\2\2\u0344\u034d\5Z"+
		".\2\u0345\u0346\5\u010a\u0086\2\u0346\u0347\7\u01c6\2\2\u0347\u0348\5"+
		"\u010a\u0086\2\u0348\u0349\7\u01cb\2\2\u0349\u034a\5\u00ba^\2\u034a\u034b"+
		"\7\u01cc\2\2\u034b\u034d\3\2\2\2\u034c\u033e\3\2\2\2\u034c\u0345\3\2\2"+
		"\2\u034di\3\2\2\2\u034e\u0353\5l\67\2\u034f\u0350\7\u01cd\2\2\u0350\u0352"+
		"\5l\67\2\u0351\u034f\3\2\2\2\u0352\u0355\3\2\2\2\u0353\u0351\3\2\2\2\u0353"+
		"\u0354\3\2\2\2\u0354k\3\2\2\2\u0355\u0353\3\2\2\2\u0356\u035b\5n8\2\u0357"+
		"\u0358\7\30\2\2\u0358\u035a\5n8\2\u0359\u0357\3\2\2\2\u035a\u035d\3\2"+
		"\2\2\u035b\u0359\3\2\2\2\u035b\u035c\3\2\2\2\u035cm\3\2\2\2\u035d\u035b"+
		"\3\2\2\2\u035e\u0363\5p9\2\u035f\u0360\7\u008d\2\2\u0360\u0362\5p9\2\u0361"+
		"\u035f\3\2\2\2\u0362\u0365\3\2\2\2\u0363\u0361\3\2\2\2\u0363\u0364\3\2"+
		"\2\2\u0364o\3\2\2\2\u0365\u0363\3\2\2\2\u0366\u0368\7\u0080\2\2\u0367"+
		"\u0366\3\2\2\2\u0367\u0368\3\2\2\2\u0368\u0369\3\2\2\2\u0369\u036a\5r"+
		":\2\u036aq\3\2\2\2\u036b\u036c\7V\2\2\u036c\u036d\7\u01cb\2\2\u036d\u036e"+
		"\5`\61\2\u036e\u036f\7\u01cc\2\2\u036f\u03a3\3\2\2\2\u0370\u0371\5Z.\2"+
		"\u0371\u0372\5\u010e\u0088\2\u0372\u0373\5Z.\2\u0373\u03a3\3\2\2\2\u0374"+
		"\u0375\5Z.\2\u0375\u0376\5\u010e\u0088\2\u0376\u0377\t\20\2\2\u0377\u0378"+
		"\7\u01cb\2\2\u0378\u0379\5`\61\2\u0379\u037a\7\u01cc\2\2\u037a\u03a3\3"+
		"\2\2\2\u037b\u037d\5Z.\2\u037c\u037e\7\u0080\2\2\u037d\u037c\3\2\2\2\u037d"+
		"\u037e\3\2\2\2\u037e\u037f\3\2\2\2\u037f\u0380\7 \2\2\u0380\u0381\5Z."+
		"\2\u0381\u0382\7\30\2\2\u0382\u0383\5Z.\2\u0383\u03a3\3\2\2\2\u0384\u0386"+
		"\5Z.\2\u0385\u0387\7\u0080\2\2\u0386\u0385\3\2\2\2\u0386\u0387\3\2\2\2"+
		"\u0387\u0388\3\2\2\2\u0388\u0389\7m\2\2\u0389\u038c\7\u01cb\2\2\u038a"+
		"\u038d\5`\61\2\u038b\u038d\5\u00ba^\2\u038c\u038a\3\2\2\2\u038c\u038b"+
		"\3\2\2\2\u038d\u038e\3\2\2\2\u038e\u038f\7\u01cc\2\2\u038f\u03a3\3\2\2"+
		"\2\u0390\u0392\5Z.\2\u0391\u0393\7\u0080\2\2\u0392\u0391\3\2\2\2\u0392"+
		"\u0393\3\2\2\2\u0393\u0394\3\2\2\2\u0394\u0395\7x\2\2\u0395\u0398\5Z."+
		"\2\u0396\u0397\7R\2\2\u0397\u0399\5Z.\2\u0398\u0396\3\2\2\2\u0398\u0399"+
		"\3\2\2\2\u0399\u03a3\3\2\2\2\u039a\u039b\5Z.\2\u039b\u039c\7s\2\2\u039c"+
		"\u039d\5\u00fa~\2\u039d\u03a3\3\2\2\2\u039e\u039f\7\u01cb\2\2\u039f\u03a0"+
		"\5l\67\2\u03a0\u03a1\7\u01cc\2\2\u03a1\u03a3\3\2\2\2\u03a2\u036b\3\2\2"+
		"\2\u03a2\u0370\3\2\2\2\u03a2\u0374\3\2\2\2\u03a2\u037b\3\2\2\2\u03a2\u0384"+
		"\3\2\2\2\u03a2\u0390\3\2\2\2\u03a2\u039a\3\2\2\2\u03a2\u039e\3\2\2\2\u03a3"+
		"s\3\2\2\2\u03a4\u03aa\5x=\2\u03a5\u03a6\7\u01cb\2\2\u03a6\u03a7\5t;\2"+
		"\u03a7\u03a8\7\u01cc\2\2\u03a8\u03aa\3\2\2\2\u03a9\u03a4\3\2\2\2\u03a9"+
		"\u03a5\3\2\2\2\u03aa\u03ae\3\2\2\2\u03ab\u03ad\5v<\2\u03ac\u03ab\3\2\2"+
		"\2\u03ad\u03b0\3\2\2\2\u03ae\u03ac\3\2\2\2\u03ae\u03af\3\2\2\2\u03afu"+
		"\3\2\2\2\u03b0\u03ae\3\2\2\2\u03b1\u03b3\7\u00c8\2\2\u03b2\u03b4\7\26"+
		"\2\2\u03b3\u03b2\3\2\2\2\u03b3\u03b4\3\2\2\2\u03b4\u03b8\3\2\2\2\u03b5"+
		"\u03b8\7S\2\2\u03b6\u03b8\7q\2\2\u03b7\u03b1\3\2\2\2\u03b7\u03b5\3\2\2"+
		"\2\u03b7\u03b6\3\2\2\2\u03b8\u03c2\3\2\2\2\u03b9\u03c3\5x=\2\u03ba\u03bb"+
		"\7\u01cb\2\2\u03bb\u03bc\5t;\2\u03bc\u03bd\7\u01cc\2\2\u03bd\u03bf\3\2"+
		"\2\2\u03be\u03ba\3\2\2\2\u03bf\u03c0\3\2\2\2\u03c0\u03be\3\2\2\2\u03c0"+
		"\u03c1\3\2\2\2\u03c1\u03c3\3\2\2\2\u03c2\u03b9\3\2\2\2\u03c2\u03be\3\2"+
		"\2\2\u03c3w\3\2\2\2\u03c4\u03c6\7\u00ad\2\2\u03c5\u03c7\t\21\2\2\u03c6"+
		"\u03c5\3\2\2\2\u03c6\u03c7\3\2\2\2\u03c7\u03d1\3\2\2\2\u03c8\u03c9\7\u00c0"+
		"\2\2\u03c9\u03cb\5Z.\2\u03ca\u03cc\7\u0091\2\2\u03cb\u03ca\3\2\2\2\u03cb"+
		"\u03cc\3\2\2\2\u03cc\u03cf\3\2\2\2\u03cd\u03ce\7\u00d6\2\2\u03ce\u03d0"+
		"\7\u0194\2\2\u03cf\u03cd\3\2\2\2\u03cf\u03d0\3\2\2\2\u03d0\u03d2\3\2\2"+
		"\2\u03d1\u03c8\3\2\2\2\u03d1\u03d2\3\2\2\2\u03d2\u03d3\3\2\2\2\u03d3\u03d6"+
		"\5\u008cG\2\u03d4\u03d5\7r\2\2\u03d5\u03d7\5\u00e6t\2\u03d6\u03d4\3\2"+
		"\2\2\u03d6\u03d7\3\2\2\2\u03d7\u03e1\3\2\2\2\u03d8\u03d9\7b\2\2\u03d9"+
		"\u03de\5\u0092J\2\u03da\u03db\7\u01cd\2\2\u03db\u03dd\5\u0092J\2\u03dc"+
		"\u03da\3\2\2\2\u03dd\u03e0\3\2\2\2\u03de\u03dc\3\2\2\2\u03de\u03df\3\2"+
		"\2\2\u03df\u03e2\3\2\2\2\u03e0\u03de\3\2\2\2\u03e1\u03d8\3\2\2\2\u03e1"+
		"\u03e2\3\2\2\2\u03e2\u03e5\3\2\2\2\u03e3\u03e4\7\u00d4\2\2\u03e4\u03e6"+
		"\5l\67\2\u03e5\u03e3\3\2\2\2\u03e5\u03e6\3\2\2\2\u03e6\u0403\3\2\2\2\u03e7"+
		"\u03e8\7g\2\2\u03e8\u03e9\7$\2\2\u03e9\u03ee\5\u0084C\2\u03ea\u03eb\7"+
		"\u01cd\2\2\u03eb\u03ed\5\u0084C\2\u03ec\u03ea\3\2\2\2\u03ed\u03f0\3\2"+
		"\2\2\u03ee\u03ec\3\2\2\2\u03ee\u03ef\3\2\2\2\u03ef\u03f3\3\2\2\2\u03f0"+
		"\u03ee\3\2\2\2\u03f1\u03f2\7\u00d6\2\2\u03f2\u03f4\7\u016f\2\2\u03f3\u03f1"+
		"\3\2\2\2\u03f3\u03f4\3\2\2\2\u03f4\u0404\3\2\2\2\u03f5\u03f6\7g\2\2\u03f6"+
		"\u03f7\7$\2\2\u03f7\u03f8\7\u016f\2\2\u03f8\u03f9\7\u01cb\2\2\u03f9\u03fe"+
		"\5\u0084C\2\u03fa\u03fb\7\u01cd\2\2\u03fb\u03fd\5\u0084C\2\u03fc\u03fa"+
		"\3\2\2\2\u03fd\u0400\3\2\2\2\u03fe\u03fc\3\2\2\2\u03fe\u03ff\3\2\2\2\u03ff"+
		"\u0401\3\2\2\2\u0400\u03fe\3\2\2\2\u0401\u0402\7\u01cc\2\2\u0402\u0404"+
		"\3\2\2\2\u0403\u03e7\3\2\2\2\u0403\u03f5\3\2\2\2\u0403\u0404\3\2\2\2\u0404"+
		"\u0407\3\2\2\2\u0405\u0406\7h\2\2\u0406\u0408\5l\67\2\u0407\u0405\3\2"+
		"\2\2\u0407\u0408\3\2\2\2\u0408y\3\2\2\2\u0409\u040a\7y\2\2\u040a\u040b"+
		"\5\u0106\u0084\2\u040b{\3\2\2\2\u040c\u040d\7\u008e\2\2\u040d\u040e\7"+
		"$\2\2\u040e\u0413\5\u0082B\2\u040f\u0410\7\u01cd\2\2\u0410\u0412\5\u0082"+
		"B\2\u0411\u040f\3\2\2\2\u0412\u0415\3\2\2\2\u0413\u0411\3\2\2\2\u0413"+
		"\u0414\3\2\2\2\u0414\u0421\3\2\2\2\u0415\u0413\3\2\2\2\u0416\u0417\7\u014e"+
		"\2\2\u0417\u0418\5Z.\2\u0418\u041f\t\22\2\2\u0419\u041a\7Z\2\2\u041a\u041b"+
		"\t\23\2\2\u041b\u041c\5Z.\2\u041c\u041d\t\22\2\2\u041d\u041e\7\u014f\2"+
		"\2\u041e\u0420\3\2\2\2\u041f\u0419\3\2\2\2\u041f\u0420\3\2\2\2\u0420\u0422"+
		"\3\2\2\2\u0421\u0416\3\2\2\2\u0421\u0422\3\2\2\2\u0422}\3\2\2\2\u0423"+
		"\u0424\7]\2\2\u0424\u0437\7\"\2\2\u0425\u0426\7]\2\2\u0426\u0427\7\u01a8"+
		"\2\2\u0427\u0429\7\u00e1\2\2\u0428\u042a\5\u0080A\2\u0429\u0428\3\2\2"+
		"\2\u0429\u042a\3\2\2\2\u042a\u0437\3\2\2\2\u042b\u042c\7]\2\2\u042c\u042d"+
		"\7\u01a8\2\2\u042d\u0431\7\u0156\2\2\u042e\u042f\7\u01cb\2\2\u042f\u0430"+
		"\7\u01b6\2\2\u0430\u0432\7\u01cc\2\2\u0431\u042e\3\2\2\2\u0431\u0432\3"+
		"\2\2\2\u0432\u0434\3\2\2\2\u0433\u0435\5\u0080A\2\u0434\u0433\3\2\2\2"+
		"\u0434\u0435\3\2\2\2\u0435\u0437\3\2\2\2\u0436\u0423\3\2\2\2\u0436\u0425"+
		"\3\2\2\2\u0436\u042b\3\2\2\2\u0437\177\3\2\2\2\u0438\u043d\7\u01cd\2\2"+
		"\u0439\u043a\7\u01b7\2\2\u043a\u043e\7\u00e3\2\2\u043b\u043e\7\u0199\2"+
		"\2\u043c\u043e\7\u0170\2\2\u043d\u0439\3\2\2\2\u043d\u043b\3\2\2\2\u043d"+
		"\u043c\3\2\2\2\u043e\u0081\3\2\2\2\u043f\u0441\5Z.\2\u0440\u0442\t\24"+
		"\2\2\u0441\u0440\3\2\2\2\u0441\u0442\3\2\2\2\u0442\u0083\3\2\2\2\u0443"+
		"\u0444\5Z.\2\u0444\u0085\3\2\2\2\u0445\u0446\7\u008c\2\2\u0446\u0447\7"+
		"\u01cb\2\2\u0447\u044c\5\u0088E\2\u0448\u0449\7\u01cd\2\2\u0449\u044b"+
		"\5\u0088E\2\u044a\u0448\3\2\2\2\u044b\u044e\3\2\2\2\u044c\u044a\3\2\2"+
		"\2\u044c\u044d\3\2\2\2\u044d\u044f\3\2\2\2\u044e\u044c\3\2\2\2\u044f\u0450"+
		"\7\u01cc\2\2\u0450\u0087\3\2\2\2\u0451\u0452\7\u010f\2\2\u0452\u046a\7"+
		"\u01b4\2\2\u0453\u0454\t\25\2\2\u0454\u046a\7g\2\2\u0455\u0456\t\26\2"+
		"\2\u0456\u046a\7\u00c8\2\2\u0457\u0458\7\u0129\2\2\u0458\u046a\7\u0093"+
		"\2\2\u0459\u045a\7\u0151\2\2\u045a\u045b\7]\2\2\u045b\u045c\7\u01cb\2"+
		"\2\u045c\u0461\5\u008aF\2\u045d\u045e\7\u01cd\2\2\u045e\u0460\5\u008a"+
		"F\2\u045f\u045d\3\2\2\2\u0460\u0463\3\2\2\2\u0461\u045f\3\2\2\2\u0461"+
		"\u0462\3\2\2\2\u0462\u0464\3\2\2\2\u0463\u0461\3\2\2\2\u0464\u0465\7\u01cc"+
		"\2\2\u0465\u046a\3\2\2\2\u0466\u0467\7\u0151\2\2\u0467\u0468\7]\2\2\u0468"+
		"\u046a\7\u01a0\2\2\u0469\u0451\3\2\2\2\u0469\u0453\3\2\2\2\u0469\u0455"+
		"\3\2\2\2\u0469\u0457\3\2\2\2\u0469\u0459\3\2\2\2\u0469\u0466\3\2\2\2\u046a"+
		"\u0089\3\2\2\2\u046b\u046f\7\u01b3\2\2\u046c\u0470\7\u01a0\2\2\u046d\u046e"+
		"\7\u01ba\2\2\u046e\u0470\5\u0104\u0083\2\u046f\u046c\3\2\2\2\u046f\u046d"+
		"\3\2\2\2\u0470\u008b\3\2\2\2\u0471\u0476\5\u008eH\2\u0472\u0473\7\u01cd"+
		"\2\2\u0473\u0475\5\u008eH\2\u0474\u0472\3\2\2\2\u0475\u0478\3\2\2\2\u0476"+
		"\u0474\3\2\2\2\u0476\u0477\3\2\2\2\u0477\u008d\3\2\2\2\u0478\u0476\3\2"+
		"\2\2\u0479\u047a\5\u00e6t\2\u047a\u047b\7\u01c6\2\2\u047b\u047d\3\2\2"+
		"\2\u047c\u0479\3\2\2\2\u047c\u047d\3\2\2\2\u047d\u0481\3\2\2\2\u047e\u0482"+
		"\7\u01d0\2\2\u047f\u0480\7\u01ca\2\2\u0480\u0482\t\27\2\2\u0481\u047e"+
		"\3\2\2\2\u0481\u047f\3\2\2\2\u0482\u048f\3\2\2\2\u0483\u0484\5\u00b6\\"+
		"\2\u0484\u0485\7\u01ba\2\2\u0485\u0486\5Z.\2\u0486\u048f\3\2\2\2\u0487"+
		"\u048c\5Z.\2\u0488\u048a\7\32\2\2\u0489\u0488\3\2\2\2\u0489\u048a\3\2"+
		"\2\2\u048a\u048b\3\2\2\2\u048b\u048d\5\u00b6\\\2\u048c\u0489\3\2\2\2\u048c"+
		"\u048d\3\2\2\2\u048d\u048f\3\2\2\2\u048e\u047c\3\2\2\2\u048e\u0483\3\2"+
		"\2\2\u048e\u0487\3\2\2\2\u048f\u008f\3\2\2\2\u0490\u0491\7\u0155\2\2\u0491"+
		"\u0492\7$\2\2\u0492\u0493\5\u00ba^\2\u0493\u0091\3\2\2\2\u0494\u049a\5"+
		"\u0094K\2\u0495\u0496\7\u01cb\2\2\u0496\u0497\5\u0094K\2\u0497\u0498\7"+
		"\u01cc\2\2\u0498\u049a\3\2\2\2\u0499\u0494\3\2\2\2\u0499\u0495\3\2\2\2"+
		"\u049a\u0093\3\2\2\2\u049b\u049f\5\u0096L\2\u049c\u049e\5\u009aN\2\u049d"+
		"\u049c\3\2\2\2\u049e\u04a1\3\2\2\2\u049f\u049d\3\2\2\2\u049f\u04a0\3\2"+
		"\2\2\u04a0\u0095\3\2\2\2\u04a1\u049f\3\2\2\2\u04a2\u04a4\5\"\22\2\u04a3"+
		"\u04a5\5\u00a8U\2\u04a4\u04a3\3\2\2\2\u04a4\u04a5\3\2\2\2\u04a5\u04b2"+
		"\3\2\2\2\u04a6\u04a8\5\u009cO\2\u04a7\u04a9\5\u00a8U\2\u04a8\u04a7\3\2"+
		"\2\2\u04a8\u04a9\3\2\2\2\u04a9\u04b2\3\2\2\2\u04aa\u04af\5\u00a2R\2\u04ab"+
		"\u04ad\5\u00a8U\2\u04ac\u04ae\5\u00b4[\2\u04ad\u04ac\3\2\2\2\u04ad\u04ae"+
		"\3\2\2\2\u04ae\u04b0\3\2\2\2\u04af\u04ab\3\2\2\2\u04af\u04b0\3\2\2\2\u04b0"+
		"\u04b2\3\2\2\2\u04b1\u04a2\3\2\2\2\u04b1\u04a6\3\2\2\2\u04b1\u04aa\3\2"+
		"\2\2\u04b2\u0097\3\2\2\2\u04b3\u04b4\7\'\2\2\u04b4\u04b5\7\u01cb\2\2\u04b5"+
		"\u04b6\7(\2\2\u04b6\u04b7\5\u00e6t\2\u04b7\u04b8\7\u01cd\2\2\u04b8\u04b9"+
		"\t\30\2\2\u04b9\u04ba\7\u01cc\2\2\u04ba\u0099\3\2\2\2\u04bb\u04bd\7o\2"+
		"\2\u04bc\u04bb\3\2\2\2\u04bc\u04bd\3\2\2\2\u04bd\u04c3\3\2\2\2\u04be\u04c0"+
		"\t\31\2\2\u04bf\u04c1\7\u008f\2\2\u04c0\u04bf\3\2\2\2\u04c0\u04c1\3\2"+
		"\2\2\u04c1\u04c3\3\2\2\2\u04c2\u04bc\3\2\2\2\u04c2\u04be\3\2\2\2\u04c3"+
		"\u04c5\3\2\2\2\u04c4\u04c6\t\32\2\2\u04c5\u04c4\3\2\2\2\u04c5\u04c6\3"+
		"\2\2\2\u04c6\u04c7\3\2\2\2\u04c7\u04c8\7t\2\2\u04c8\u04c9\5\u0092J\2\u04c9"+
		"\u04ca\7\u0086\2\2\u04ca\u04cb\5l\67\2\u04cb\u04e2\3\2\2\2\u04cc\u04cd"+
		"\79\2\2\u04cd\u04ce\7t\2\2\u04ce\u04e2\5\u0092J\2\u04cf\u04d0\79\2\2\u04d0"+
		"\u04d1\7\u00de\2\2\u04d1\u04e2\5\u0092J\2\u04d2\u04d3\7\u008f\2\2\u04d3"+
		"\u04d4\7\u00de\2\2\u04d4\u04e2\5\u0092J\2\u04d5\u04d6\7\u012c\2\2\u04d6"+
		"\u04d7\7\u00d1\2\2\u04d7\u04d9\5\u00d0i\2\u04d8\u04da\5\u00aaV\2\u04d9"+
		"\u04d8\3\2\2\2\u04d9\u04da\3\2\2\2\u04da\u04df\3\2\2\2\u04db\u04dd\7\32"+
		"\2\2\u04dc\u04db\3\2\2\2\u04dc\u04dd\3\2\2\2\u04dd\u04de\3\2\2\2\u04de"+
		"\u04e0\5\u00b6\\\2\u04df\u04dc\3\2\2\2\u04df\u04e0\3\2\2\2\u04e0\u04e2"+
		"\3\2\2\2\u04e1\u04c2\3\2\2\2\u04e1\u04cc\3\2\2\2\u04e1\u04cf\3\2\2\2\u04e1"+
		"\u04d2\3\2\2\2\u04e1\u04d5\3\2\2\2\u04e2\u009b\3\2\2\2\u04e3\u04e5\5\u00e6"+
		"t\2\u04e4\u04e6\5\u00acW\2\u04e5\u04e4\3\2\2\2\u04e5\u04e6\3\2\2\2\u04e6"+
		"\u009d\3\2\2\2\u04e7\u04e8\7\u008a\2\2\u04e8\u04e9\7\u01cb\2\2\u04e9\u04ea"+
		"\7#\2\2\u04ea\u04eb\7\u01b6\2\2\u04eb\u04f5\7\u01cd\2\2\u04ec\u04f1\5"+
		"\u00a0Q\2\u04ed\u04ee\7\u01cd\2\2\u04ee\u04f0\5\u00a0Q\2\u04ef\u04ed\3"+
		"\2\2\2\u04f0\u04f3\3\2\2\2\u04f1\u04ef\3\2\2\2\u04f1\u04f2\3\2\2\2\u04f2"+
		"\u04f6\3\2\2\2\u04f3\u04f1\3\2\2\2\u04f4\u04f6\5\u010a\u0086\2\u04f5\u04ec"+
		"\3\2\2\2\u04f5\u04f4\3\2\2\2\u04f6\u04f7\3\2\2\2\u04f7\u04f8\7\u01cc\2"+
		"\2\u04f8\u009f\3\2\2\2\u04f9\u04fa\5\u010a\u0086\2\u04fa\u04fb\7\u01ba"+
		"\2\2\u04fb\u04fc\t\33\2\2\u04fc\u00a1\3\2\2\2\u04fd\u04fe\7\u01cb\2\2"+
		"\u04fe\u04ff\5`\61\2\u04ff\u0500\7\u01cc\2\2\u0500\u0503\3\2\2\2\u0501"+
		"\u0503\5\u00b8]\2\u0502\u04fd\3\2\2\2\u0502\u0501\3\2\2\2\u0503\u00a3"+
		"\3\2\2\2\u0504\u0508\5\u00be`\2\u0505\u0508\5\u00c0a\2\u0506\u0508\5\u00d2"+
		"j\2\u0507\u0504\3\2\2\2\u0507\u0505\3\2\2\2\u0507\u0506\3\2\2\2\u0508"+
		"\u00a5\3\2\2\2\u0509\u050a\7\u01b5\2\2\u050a\u00a7\3\2\2\2\u050b\u050d"+
		"\7\32\2\2\u050c\u050b\3\2\2\2\u050c\u050d\3\2\2\2\u050d\u050e\3\2\2\2"+
		"\u050e\u050f\5\u00aaV\2\u050f\u00a9\3\2\2\2\u0510\u0512\5\u010a\u0086"+
		"\2\u0511\u0513\5\u00acW\2\u0512\u0511\3\2\2\2\u0512\u0513\3\2\2\2\u0513"+
		"\u00ab\3\2\2\2\u0514\u0516\7\u00d6\2\2\u0515\u0514\3\2\2\2\u0515\u0516"+
		"\3\2\2\2\u0516\u0517\3\2\2\2\u0517\u0518\7\u01cb\2\2\u0518\u051d\5\u00ae"+
		"X\2\u0519\u051a\7\u01cd\2\2\u051a\u051c\5\u00aeX\2\u051b\u0519\3\2\2\2"+
		"\u051c\u051f\3\2\2\2\u051d\u051b\3\2\2\2\u051d\u051e\3\2\2\2\u051e\u0520"+
		"\3\2\2\2\u051f\u051d\3\2\2\2\u0520\u0521\7\u01cc\2\2\u0521\u00ad\3\2\2"+
		"\2\u0522\u0524\7\u0149\2\2\u0523\u0522\3\2\2\2\u0523\u0524\3\2\2\2\u0524"+
		"\u054b\3\2\2\2\u0525\u0526\7n\2\2\u0526\u0527\7\u01cb\2\2\u0527\u052c"+
		"\5\u00b2Z\2\u0528\u0529\7\u01cd\2\2\u0529\u052b\5\u00b2Z\2\u052a\u0528"+
		"\3\2\2\2\u052b\u052e\3\2\2\2\u052c\u052a\3\2\2\2\u052c\u052d\3\2\2\2\u052d"+
		"\u052f\3\2\2\2\u052e\u052c\3\2\2\2\u052f\u0530\7\u01cc\2\2\u0530\u054c"+
		"\3\2\2\2\u0531\u0532\7n\2\2\u0532\u0533\7\u01ba\2\2\u0533\u054c\5\u00b2"+
		"Z\2\u0534\u0543\7^\2\2\u0535\u0536\7\u01cb\2\2\u0536\u0537\5\u00b2Z\2"+
		"\u0537\u0538\7\u01cb\2\2\u0538\u053d\5\u00b0Y\2\u0539\u053a\7\u01cd\2"+
		"\2\u053a\u053c\5\u00b0Y\2\u053b\u0539\3\2\2\2\u053c\u053f\3\2\2\2\u053d"+
		"\u053b\3\2\2\2\u053d\u053e\3\2\2\2\u053e\u0540\3\2\2\2\u053f\u053d\3\2"+
		"\2\2\u0540\u0541\7\u01cc\2\2\u0541\u0542\7\u01cc\2\2\u0542\u0544\3\2\2"+
		"\2\u0543\u0535\3\2\2\2\u0543\u0544\3\2\2\2\u0544\u054c\3\2\2\2\u0545\u054c"+
		"\7\u017c\2\2\u0546\u054c\7\u0184\2\2\u0547\u0548\7\u0186\2\2\u0548\u0549"+
		"\7\u01ba\2\2\u0549\u054c\7\u01b4\2\2\u054a\u054c\7\u01b5\2\2\u054b\u0525"+
		"\3\2\2\2\u054b\u0531\3\2\2\2\u054b\u0534\3\2\2\2\u054b\u0545\3\2\2\2\u054b"+
		"\u0546\3\2\2\2\u054b\u0547\3\2\2\2\u054b\u054a\3\2\2\2\u054b\u054c\3\2"+
		"\2\2\u054c\u00af\3\2\2\2\u054d\u054e\7\u01b5\2\2\u054e\u00b1\3\2\2\2\u054f"+
		"\u0550\t\34\2\2\u0550\u00b3\3\2\2\2\u0551\u0552\7\u01cb\2\2\u0552\u0557"+
		"\5\u00b6\\\2\u0553\u0554\7\u01cd\2\2\u0554\u0556\5\u00b6\\\2\u0555\u0553"+
		"\3\2\2\2\u0556\u0559\3\2\2\2\u0557\u0555\3\2\2\2\u0557\u0558\3\2\2\2\u0558"+
		"\u055a\3\2\2\2\u0559\u0557\3\2\2\2\u055a\u055b\7\u01cc\2\2\u055b\u00b5"+
		"\3\2\2\2\u055c\u055f\5\u010a\u0086\2\u055d\u055f\7\u01b6\2\2\u055e\u055c"+
		"\3\2\2\2\u055e\u055d\3\2\2\2\u055f\u00b7\3\2\2\2\u0560\u0561\7\u00cf\2"+
		"\2\u0561\u0562\7\u01cb\2\2\u0562\u0563\5\u00ba^\2\u0563\u056b\7\u01cc"+
		"\2\2\u0564\u0565\7\u01cd\2\2\u0565\u0566\7\u01cb\2\2\u0566\u0567\5\u00ba"+
		"^\2\u0567\u0568\7\u01cc\2\2\u0568\u056a\3\2\2\2\u0569\u0564\3\2\2\2\u056a"+
		"\u056d\3\2\2\2\u056b\u0569\3\2\2\2\u056b\u056c\3\2\2\2\u056c\u00b9\3\2"+
		"\2\2\u056d\u056b\3\2\2\2\u056e\u0573\5Z.\2\u056f\u0570\7\u01cd\2\2\u0570"+
		"\u0572\5Z.\2\u0571\u056f\3\2\2\2\u0572\u0575\3\2\2\2\u0573\u0571\3\2\2"+
		"\2\u0573\u0574\3\2\2\2\u0574\u00bb\3\2\2\2\u0575\u0573\3\2\2\2\u0576\u0577"+
		"\7&\2\2\u0577\u057d\5Z.\2\u0578\u0579\7\u00d3\2\2\u0579\u057a\5Z.\2\u057a"+
		"\u057b\7\u00be\2\2\u057b\u057c\5Z.\2\u057c\u057e\3\2\2\2\u057d\u0578\3"+
		"\2\2\2\u057e\u057f\3\2\2\2\u057f\u057d\3\2\2\2\u057f\u0580\3\2\2\2\u0580"+
		"\u0583\3\2\2\2\u0581\u0582\7O\2\2\u0582\u0584\5Z.\2\u0583\u0581\3\2\2"+
		"\2\u0583\u0584\3\2\2\2\u0584\u0585\3\2\2\2\u0585\u0586\7P\2\2\u0586\u0598"+
		"\3\2\2\2\u0587\u058d\7&\2\2\u0588\u0589\7\u00d3\2\2\u0589\u058a\5l\67"+
		"\2\u058a\u058b\7\u00be\2\2\u058b\u058c\5Z.\2\u058c\u058e\3\2\2\2\u058d"+
		"\u0588\3\2\2\2\u058e\u058f\3\2\2\2\u058f\u058d\3\2\2\2\u058f\u0590\3\2"+
		"\2\2\u0590\u0593\3\2\2\2\u0591\u0592\7O\2\2\u0592\u0594\5Z.\2\u0593\u0591"+
		"\3\2\2\2\u0593\u0594\3\2\2\2\u0594\u0595\3\2\2\2\u0595\u0596\7P\2\2\u0596"+
		"\u0598\3\2\2\2\u0597\u0576\3\2\2\2\u0597\u0587\3\2\2\2\u0598\u00bd\3\2"+
		"\2\2\u0599\u059a\7\u0164\2\2\u059a\u059b\7\u01cb\2\2\u059b\u059c\7\u01cc"+
		"\2\2\u059c\u05ac\5\u00d8m\2\u059d\u059e\7\u0103\2\2\u059e\u059f\7\u01cb"+
		"\2\2\u059f\u05a0\7\u01cc\2\2\u05a0\u05ac\5\u00d8m\2\u05a1\u05a2\7\u014b"+
		"\2\2\u05a2\u05a3\7\u01cb\2\2\u05a3\u05a4\5Z.\2\u05a4\u05a5\7\u01cc\2\2"+
		"\u05a5\u05a6\5\u00d8m\2\u05a6\u05ac\3\2\2\2\u05a7\u05a8\7\u0175\2\2\u05a8"+
		"\u05a9\7\u01cb\2\2\u05a9\u05aa\7\u01cc\2\2\u05aa\u05ac\5\u00d8m\2\u05ab"+
		"\u0599\3\2\2\2\u05ab\u059d\3\2\2\2\u05ab\u05a1\3\2\2\2\u05ab\u05a7\3\2"+
		"\2\2\u05ac\u00bf\3\2\2\2\u05ad\u05b4\5\u00ccg\2\u05ae\u05b4\5\u00ceh\2"+
		"\u05af\u05b4\5\u00c6d\2\u05b0\u05b4\5\u00c4c\2\u05b1\u05b4\5\u00c2b\2"+
		"\u05b2\u05b4\5\u00c8e\2\u05b3\u05ad\3\2\2\2\u05b3\u05ae\3\2\2\2\u05b3"+
		"\u05af\3\2\2\2\u05b3\u05b0\3\2\2\2\u05b3\u05b1\3\2\2\2\u05b3\u05b2\3\2"+
		"\2\2\u05b4\u00c1\3\2\2\2\u05b5\u05b6\t\35\2\2\u05b6\u05b7\7\u01cb\2\2"+
		"\u05b7\u05bc\5Z.\2\u05b8\u05b9\7\u01cd\2\2\u05b9\u05bb\5Z.\2\u05ba\u05b8"+
		"\3\2\2\2\u05bb\u05be\3\2\2\2\u05bc\u05ba\3\2\2\2\u05bc\u05bd\3\2\2\2\u05bd"+
		"\u05bf\3\2\2\2\u05be\u05bc\3\2\2\2\u05bf\u05c0\7\u01cc\2\2\u05c0\u00c3"+
		"\3\2\2\2\u05c1\u05c2\t\36\2\2\u05c2\u05c3\7\u01cb\2\2\u05c3\u05c4\5Z."+
		"\2\u05c4\u05c5\7\u01cd\2\2\u05c5\u05c6\5Z.\2\u05c6\u05c7\7\u01cd\2\2\u05c7"+
		"\u05c8\5Z.\2\u05c8\u05c9\7\u01cc\2\2\u05c9\u00c5\3\2\2\2\u05ca\u05cb\t"+
		"\37\2\2\u05cb\u05cc\7\u01cb\2\2\u05cc\u05cd\5Z.\2\u05cd\u05ce\7\u01cd"+
		"\2\2\u05ce\u05cf\5Z.\2\u05cf\u05d0\7\u01cc\2\2\u05d0\u00c7\3\2\2\2\u05d1"+
		"\u05d2\7\u010d\2\2\u05d2\u05d3\7\u01cb\2\2\u05d3\u05d4\5\u00caf\2\u05d4"+
		"\u05d5\7b\2\2\u05d5\u05d6\5Z.\2\u05d6\u05d7\7\u01cc\2\2\u05d7\u00c9\3"+
		"\2\2\2\u05d8\u05db\7\u01aa\2\2\u05d9\u05db\5Z.\2\u05da\u05d8\3\2\2\2\u05da"+
		"\u05d9\3\2\2\2\u05db\u00cb\3\2\2\2\u05dc\u05dd\t \2\2\u05dd\u05de\7\u01cb"+
		"\2\2\u05de\u05df\5Z.\2\u05df\u05e0\7\u01cc\2\2\u05e0\u05e7\3\2\2\2\u05e1"+
		"\u05e2\7\u00e8\2\2\u05e2\u05e3\7\u01cb\2\2\u05e3\u05e4\5\u00d6l\2\u05e4"+
		"\u05e5\7\u01cc\2\2\u05e5\u05e7\3\2\2\2\u05e6\u05dc\3\2\2\2\u05e6\u05e1"+
		"\3\2\2\2\u05e7\u00cd\3\2\2\2\u05e8\u05e9\t!\2\2\u05e9\u05ea\7\u01cb\2"+
		"\2\u05ea\u05eb\7\u01cc\2\2\u05eb\u00cf\3\2\2\2\u05ec\u05ed\7\u010c\2\2"+
		"\u05ed\u05ee\7\u01cb\2\2\u05ee\u05ef\5Z.\2\u05ef\u05f0\7\u01cc\2\2\u05f0"+
		"\u00d1\3\2\2\2\u05f1\u05f2\7\u00e2\2\2\u05f2\u05f3\7\u01cb\2\2\u05f3\u05f4"+
		"\5\u00d4k\2\u05f4\u05f6\7\u01cc\2\2\u05f5\u05f7\5\u00d8m\2\u05f6\u05f5"+
		"\3\2\2\2\u05f6\u05f7\3\2\2\2\u05f7\u065b\3\2\2\2\u05f8\u05f9\7\u00ee\2"+
		"\2\u05f9\u05fa\7\u01cb\2\2\u05fa\u05fb\5\u00d4k\2\u05fb\u05fc\7\u01cc"+
		"\2\2\u05fc\u065b\3\2\2\2\u05fd\u05fe\7\u011e\2\2\u05fe\u05ff\7\u01cb\2"+
		"\2\u05ff\u0600\5Z.\2\u0600\u0601\7\u01cc\2\2\u0601\u065b\3\2\2\2\u0602"+
		"\u0603\7\u011f\2\2\u0603\u0604\7\u01cb\2\2\u0604\u0605\5\u00ba^\2\u0605"+
		"\u0606\7\u01cc\2\2\u0606\u065b\3\2\2\2\u0607\u0608\7\u013b\2\2\u0608\u0609"+
		"\7\u01cb\2\2\u0609\u060a\5\u00d4k\2\u060a\u060c\7\u01cc\2\2\u060b\u060d"+
		"\5\u00d8m\2\u060c\u060b\3\2\2\2\u060c\u060d\3\2\2\2\u060d\u065b\3\2\2"+
		"\2\u060e\u060f\7\u013d\2\2\u060f\u0610\7\u01cb\2\2\u0610\u0611\5\u00d4"+
		"k\2\u0611\u0613\7\u01cc\2\2\u0612\u0614\5\u00d8m\2\u0613\u0612\3\2\2\2"+
		"\u0613\u0614\3\2\2\2\u0614\u065b\3\2\2\2\u0615\u0616\7\u018f\2\2\u0616"+
		"\u0617\7\u01cb\2\2\u0617\u0618\5\u00d4k\2\u0618\u061a\7\u01cc\2\2\u0619"+
		"\u061b\5\u00d8m\2\u061a\u0619\3\2\2\2\u061a\u061b\3\2\2\2\u061b\u065b"+
		"\3\2\2\2\u061c\u061d\7\u018a\2\2\u061d\u061e\7\u01cb\2\2\u061e\u061f\5"+
		"\u00d4k\2\u061f\u0621\7\u01cc\2\2\u0620\u0622\5\u00d8m\2\u0621\u0620\3"+
		"\2\2\2\u0621\u0622\3\2\2\2\u0622\u065b\3\2\2\2\u0623\u0624\7\u018c\2\2"+
		"\u0624\u0625\7\u01cb\2\2\u0625\u0626\5\u00d4k\2\u0626\u0628\7\u01cc\2"+
		"\2\u0627\u0629\5\u00d8m\2\u0628\u0627\3\2\2\2\u0628\u0629\3\2\2\2\u0629"+
		"\u065b\3\2\2\2\u062a\u062b\7\u018d\2\2\u062b\u062c\7\u01cb\2\2\u062c\u062d"+
		"\5\u00d4k\2\u062d\u062f\7\u01cc\2\2\u062e\u0630\5\u00d8m\2\u062f\u062e"+
		"\3\2\2\2\u062f\u0630\3\2\2\2\u0630\u065b\3\2\2\2\u0631\u0632\7\u01a3\2"+
		"\2\u0632\u0633\7\u01cb\2\2\u0633\u0634\5\u00d4k\2\u0634\u0636\7\u01cc"+
		"\2\2\u0635\u0637\5\u00d8m\2\u0636\u0635\3\2\2\2\u0636\u0637\3\2\2\2\u0637"+
		"\u065b\3\2\2\2\u0638\u0639\7\u01a4\2\2\u0639\u063a\7\u01cb\2\2\u063a\u063b"+
		"\5\u00d4k\2\u063b\u063d\7\u01cc\2\2\u063c\u063e\5\u00d8m\2\u063d\u063c"+
		"\3\2\2\2\u063d\u063e\3\2\2\2\u063e\u065b\3\2\2\2\u063f\u0640\7\u00f6\2"+
		"\2\u0640\u0643\7\u01cb\2\2\u0641\u0644\7\u01d0\2\2\u0642\u0644\5\u00d4"+
		"k\2\u0643\u0641\3\2\2\2\u0643\u0642\3\2\2\2\u0644\u0645\3\2\2\2\u0645"+
		"\u0647\7\u01cc\2\2\u0646\u0648\5\u00d8m\2\u0647\u0646\3\2\2\2\u0647\u0648"+
		"\3\2\2\2\u0648\u065b\3\2\2\2\u0649\u064a\7\u0147\2\2\u064a\u064b\7\u01cb"+
		"\2\2\u064b\u064c\5\u00d4k\2\u064c\u064e\7\u01cc\2\2\u064d\u064f\5\u00d8"+
		"m\2\u064e\u064d\3\2\2\2\u064e\u064f\3\2\2\2\u064f\u065b\3\2\2\2\u0650"+
		"\u0651\7\u00f7\2\2\u0651\u0654\7\u01cb\2\2\u0652\u0655\7\u01d0\2\2\u0653"+
		"\u0655\5\u00d4k\2\u0654\u0652\3\2\2\2\u0654\u0653\3\2\2\2\u0655\u0656"+
		"\3\2\2\2\u0656\u0658\7\u01cc\2\2\u0657\u0659\5\u00d8m\2\u0658\u0657\3"+
		"\2\2\2\u0658\u0659\3\2\2\2\u0659\u065b\3\2\2\2\u065a\u05f1\3\2\2\2\u065a"+
		"\u05f8\3\2\2\2\u065a\u05fd\3\2\2\2\u065a\u0602\3\2\2\2\u065a\u0607\3\2"+
		"\2\2\u065a\u060e\3\2\2\2\u065a\u0615\3\2\2\2\u065a\u061c\3\2\2\2\u065a"+
		"\u0623\3\2\2\2\u065a\u062a\3\2\2\2\u065a\u0631\3\2\2\2\u065a\u0638\3\2"+
		"\2\2\u065a\u063f\3\2\2\2\u065a\u0649\3\2\2\2\u065a\u0650\3\2\2\2\u065b"+
		"\u00d3\3\2\2\2\u065c\u065e\t\21\2\2\u065d\u065c\3\2\2\2\u065d\u065e\3"+
		"\2\2\2\u065e\u065f\3\2\2\2\u065f\u0660\5Z.\2\u0660\u00d5\3\2\2\2\u0661"+
		"\u0662\5Z.\2\u0662\u0663\7\32\2\2\u0663\u0664\5\u0100\u0081\2\u0664\u00d7"+
		"\3\2\2\2\u0665\u0666\7\u0090\2\2\u0666\u0668\7\u01cb\2\2\u0667\u0669\5"+
		"\u0090I\2\u0668\u0667\3\2\2\2\u0668\u0669\3\2\2\2\u0669\u066b\3\2\2\2"+
		"\u066a\u066c\5|?\2\u066b\u066a\3\2\2\2\u066b\u066c\3\2\2\2\u066c\u066e"+
		"\3\2\2\2\u066d\u066f\5\u00dan\2\u066e\u066d\3\2\2\2\u066e\u066f\3\2\2"+
		"\2\u066f\u0670\3\2\2\2\u0670\u0671\7\u01cc\2\2\u0671\u00d9\3\2\2\2\u0672"+
		"\u0673\t\"\2\2\u0673\u0674\5\u00dco\2\u0674\u00db\3\2\2\2\u0675\u067c"+
		"\5\u00e0q\2\u0676\u0677\7 \2\2\u0677\u0678\5\u00dep\2\u0678\u0679\7\30"+
		"\2\2\u0679\u067a\5\u00dep\2\u067a\u067c\3\2\2\2\u067b\u0675\3\2\2\2\u067b"+
		"\u0676\3\2\2\2\u067c\u00dd\3\2\2\2\u067d\u0680\5\u00e0q\2\u067e\u0680"+
		"\5\u00e2r\2\u067f\u067d\3\2\2\2\u067f\u067e\3\2\2\2\u0680\u00df\3\2\2"+
		"\2\u0681\u0682\7\u019b\2\2\u0682\u0688\7\u015c\2\2\u0683\u0684\7\u01b4"+
		"\2\2\u0684\u0688\7\u015c\2\2\u0685\u0686\7:\2\2\u0686\u0688\7\u0172\2"+
		"\2\u0687\u0681\3\2\2\2\u0687\u0683\3\2\2\2\u0687\u0685\3\2\2\2\u0688\u00e1"+
		"\3\2\2\2\u0689\u068a\7\u019b\2\2\u068a\u068e\7\u0115\2\2\u068b\u068c\7"+
		"\u01b4\2\2\u068c\u068e\7\u0115\2\2\u068d\u0689\3\2\2\2\u068d\u068b\3\2"+
		"\2\2\u068e\u00e3\3\2\2\2\u068f\u0690\5\u010a\u0086\2\u0690\u0691\7\u01c6"+
		"\2\2\u0691\u0692\5\u010a\u0086\2\u0692\u0693\7\u01c6\2\2\u0693\u0694\5"+
		"\u010a\u0086\2\u0694\u0695\7\u01c6\2\2\u0695\u06a1\3\2\2\2\u0696\u0697"+
		"\5\u010a\u0086\2\u0697\u0699\7\u01c6\2\2\u0698\u069a\5\u010a\u0086\2\u0699"+
		"\u0698\3\2\2\2\u0699\u069a\3\2\2\2\u069a\u069b\3\2\2\2\u069b\u069c\7\u01c6"+
		"\2\2\u069c\u06a1\3\2\2\2\u069d\u069e\5\u010a\u0086\2\u069e\u069f\7\u01c6"+
		"\2\2\u069f\u06a1\3\2\2\2\u06a0\u068f\3\2\2\2\u06a0\u0696\3\2\2\2\u06a0"+
		"\u069d\3\2\2\2\u06a0\u06a1\3\2\2\2\u06a1\u06a2\3\2\2\2\u06a2\u06a3\5\u010a"+
		"\u0086\2\u06a3\u00e5\3\2\2\2\u06a4\u06a5\5\u010a\u0086\2\u06a5\u06a7\7"+
		"\u01c6\2\2\u06a6\u06a8\5\u010a\u0086\2\u06a7\u06a6\3\2\2\2\u06a7\u06a8"+
		"\3\2\2\2\u06a8\u06a9\3\2\2\2\u06a9\u06aa\7\u01c6\2\2\u06aa\u06af\3\2\2"+
		"\2\u06ab\u06ac\5\u010a\u0086\2\u06ac\u06ad\7\u01c6\2\2\u06ad\u06af\3\2"+
		"\2\2\u06ae\u06a4\3\2\2\2\u06ae\u06ab\3\2\2\2\u06ae\u06af\3\2\2\2\u06af"+
		"\u06b0\3\2\2\2\u06b0\u06b1\5\u010a\u0086\2\u06b1\u00e7\3\2\2\2\u06b2\u06b3"+
		"\5\u010a\u0086\2\u06b3\u06b4\7\u01c6\2\2\u06b4\u06b6\3\2\2\2\u06b5\u06b2"+
		"\3\2\2\2\u06b5\u06b6\3\2\2\2\u06b6\u06b7\3\2\2\2\u06b7\u06b8\5\u010a\u0086"+
		"\2\u06b8\u00e9\3\2\2\2\u06b9\u06ba\5\u010a\u0086\2\u06ba\u06bc\7\u01c6"+
		"\2\2\u06bb\u06bd\5\u010a\u0086\2\u06bc\u06bb\3\2\2\2\u06bc\u06bd\3\2\2"+
		"\2\u06bd\u06be\3\2\2\2\u06be\u06bf\7\u01c6\2\2\u06bf\u06c4\3\2\2\2\u06c0"+
		"\u06c1\5\u010a\u0086\2\u06c1\u06c2\7\u01c6\2\2\u06c2\u06c4\3\2\2\2\u06c3"+
		"\u06b9\3\2\2\2\u06c3\u06c0\3\2\2\2\u06c3\u06c4\3\2\2\2\u06c4\u06c5\3\2"+
		"\2\2\u06c5\u06c6\5\u010a\u0086\2\u06c6\u00eb\3\2\2\2\u06c7\u06ca\5\u00e4"+
		"s\2\u06c8\u06ca\7\u01b3\2\2\u06c9\u06c7\3\2\2\2\u06c9\u06c8\3\2\2\2\u06ca"+
		"\u00ed\3\2\2\2\u06cb\u06cc\5\u00e6t\2\u06cc\u06cd\7\u01c6\2\2\u06cd\u06cf"+
		"\3\2\2\2\u06ce\u06cb\3\2\2\2\u06ce\u06cf\3\2\2\2\u06cf\u06d0\3\2\2\2\u06d0"+
		"\u06d1\5\u00f2z\2\u06d1\u00ef\3\2\2\2\u06d2\u06d7\5\u00f2z\2\u06d3\u06d4"+
		"\7\u01cd\2\2\u06d4\u06d6\5\u00f2z\2\u06d5\u06d3\3\2\2\2\u06d6\u06d9\3"+
		"\2\2\2\u06d7\u06d5\3\2\2\2\u06d7\u06d8\3\2\2\2\u06d8\u00f1\3\2\2\2\u06d9"+
		"\u06d7\3\2\2\2\u06da\u06db\5\u010a\u0086\2\u06db\u00f3\3\2\2\2\u06dc\u06df"+
		"\5\u010a\u0086\2\u06dd\u06df\7\u01b3\2\2\u06de\u06dc\3\2\2\2\u06de\u06dd"+
		"\3\2\2\2\u06df\u00f5\3\2\2\2\u06e0\u06e1\t#\2\2\u06e1\u00f7\3\2\2\2\u06e2"+
		"\u06e3\t$\2\2\u06e3\u00f9\3\2\2\2\u06e4\u06e6\7\u0080\2\2\u06e5\u06e4"+
		"\3\2\2\2\u06e5\u06e6\3\2\2\2\u06e6\u06e7\3\2\2\2\u06e7\u06e8\7\u0081\2"+
		"\2\u06e8\u00fb\3\2\2\2\u06e9\u06ea\t%\2\2\u06ea\u00fd\3\2\2\2\u06eb\u06f1"+
		"\5\u00eav\2\u06ec\u06f1\7\u00a4\2\2\u06ed\u06f1\7w\2\2\u06ee\u06f1\7\u00e5"+
		"\2\2\u06ef\u06f1\7\u00ed\2\2\u06f0\u06eb\3\2\2\2\u06f0\u06ec\3\2\2\2\u06f0"+
		"\u06ed\3\2\2\2\u06f0\u06ee\3\2\2\2\u06f0\u06ef\3\2\2\2\u06f1\u00ff\3\2"+
		"\2\2\u06f2\u06f4\5\u010a\u0086\2\u06f3\u06f5\7i\2\2\u06f4\u06f3\3\2\2"+
		"\2\u06f4\u06f5\3\2\2\2\u06f5\u06fd\3\2\2\2\u06f6\u06f7\7\u01cb\2\2\u06f7"+
		"\u06fa\t&\2\2\u06f8\u06f9\7\u01cd\2\2\u06f9\u06fb\7\u01b4\2\2\u06fa\u06f8"+
		"\3\2\2\2\u06fa\u06fb\3\2\2\2\u06fb\u06fc\3\2\2\2\u06fc\u06fe\7\u01cc\2"+
		"\2\u06fd\u06f6\3\2\2\2\u06fd\u06fe\3\2\2\2\u06fe\u0101\3\2\2\2\u06ff\u0702"+
		"\7\u0081\2\2\u0700\u0702\5\u0104\u0083\2\u0701\u06ff\3\2\2\2\u0701\u0700"+
		"\3\2\2\2\u0702\u0103\3\2\2\2\u0703\u0710\7\u01b6\2\2\u0704\u0710\7\u01b7"+
		"\2\2\u0705\u0710\5\u0106\u0084\2\u0706\u0708\5\u0108\u0085\2\u0707\u0706"+
		"\3\2\2\2\u0707\u0708\3\2\2\2\u0708\u0709\3\2\2\2\u0709\u0710\t\'\2\2\u070a"+
		"\u070c\5\u0108\u0085\2\u070b\u070a\3\2\2\2\u070b\u070c\3\2\2\2\u070c\u070d"+
		"\3\2\2\2\u070d\u070e\7\u01ca\2\2\u070e\u0710\t\2\2\2\u070f\u0703\3\2\2"+
		"\2\u070f\u0704\3\2\2\2\u070f\u0705\3\2\2\2\u070f\u0707\3\2\2\2\u070f\u070b"+
		"\3\2\2\2\u0710\u0105\3\2\2\2\u0711\u0713\5\u0108\u0085\2\u0712\u0711\3"+
		"\2\2\2\u0712\u0713\3\2\2\2\u0713\u0714\3\2\2\2\u0714\u0715\7\u01b4\2\2"+
		"\u0715\u0107\3\2\2\2\u0716\u0717\t\f\2\2\u0717\u0109\3\2\2\2\u0718\u071d"+
		"\5\u010c\u0087\2\u0719\u071d\7\u01b0\2\2\u071a\u071d\7\u01b2\2\2\u071b"+
		"\u071d\7\u01b1\2\2\u071c\u0718\3\2\2\2\u071c\u0719\3\2\2\2\u071c\u071a"+
		"\3\2\2\2\u071c\u071b\3\2\2\2\u071d\u010b\3\2\2\2\u071e\u071f\t(\2\2\u071f"+
		"\u010d\3\2\2\2\u0720\u0736\7\u01ba\2\2\u0721\u0736\7\u01bb\2\2\u0722\u0736"+
		"\7\u01bc\2\2\u0723\u0736\7\4\2\2\u0724\u0736\7\5\2\2\u0725\u0736\7\6\2"+
		"\2\u0726\u0736\7\7\2\2\u0727\u0736\7\b\2\2\u0728\u0736\7\t\2\2\u0729\u072a"+
		"\7\u01bc\2\2\u072a\u0736\7\u01ba\2\2\u072b\u072c\7\u01bb\2\2\u072c\u0736"+
		"\7\u01ba\2\2\u072d\u072e\7\u01bc\2\2\u072e\u0736\7\u01bb\2\2\u072f\u0730"+
		"\7\u01bd\2\2\u0730\u0736\7\u01ba\2\2\u0731\u0732\7\u01bd\2\2\u0732\u0736"+
		"\7\u01bb\2\2\u0733\u0734\7\u01bd\2\2\u0734\u0736\7\u01bc\2\2\u0735\u0720"+
		"\3\2\2\2\u0735\u0721\3\2\2\2\u0735\u0722\3\2\2\2\u0735\u0723\3\2\2\2\u0735"+
		"\u0724\3\2\2\2\u0735\u0725\3\2\2\2\u0735\u0726\3\2\2\2\u0735\u0727\3\2"+
		"\2\2\u0735\u0728\3\2\2\2\u0735\u0729\3\2\2\2\u0735\u072b\3\2\2\2\u0735"+
		"\u072d\3\2\2\2\u0735\u072f\3\2\2\2\u0735\u0731\3\2\2\2\u0735\u0733\3\2"+
		"\2\2\u0736\u010f\3\2\2\2\u0737\u0738\t)\2\2\u0738\u0111\3\2\2\2\u00e7"+
		"\u011d\u0122\u0125\u012b\u0135\u013b\u013e\u0144\u0148\u014d\u0151\u015b"+
		"\u0165\u0176\u0180\u0183\u0188\u0191\u0196\u019d\u01a5\u01a8\u01ab\u01af"+
		"\u01b2\u01b5\u01b8\u01c0\u01c6\u01cc\u01ce\u01d2\u01d5\u01d8\u01dd\u01e2"+
		"\u01e5\u01ed\u01f2\u01f6\u01ff\u0203\u0208\u0213\u0218\u021f\u0222\u022e"+
		"\u0236\u0238\u023e\u0245\u024b\u024f\u0255\u025c\u0260\u0266\u026b\u0272"+
		"\u0275\u027a\u0280\u0283\u028b\u028f\u0292\u0297\u029d\u02a2\u02a9\u02af"+
		"\u02b2\u02b6\u02b9\u02bd\u02c9\u02cc\u02da\u02dd\u02e4\u02e6\u02fd\u030c"+
		"\u030e\u031d\u0326\u032d\u0335\u033e\u0342\u034c\u0353\u035b\u0363\u0367"+
		"\u037d\u0386\u038c\u0392\u0398\u03a2\u03a9\u03ae\u03b3\u03b7\u03c0\u03c2"+
		"\u03c6\u03cb\u03cf\u03d1\u03d6\u03de\u03e1\u03e5\u03ee\u03f3\u03fe\u0403"+
		"\u0407\u0413\u041f\u0421\u0429\u0431\u0434\u0436\u043d\u0441\u044c\u0461"+
		"\u0469\u046f\u0476\u047c\u0481\u0489\u048c\u048e\u0499\u049f\u04a4\u04a8"+
		"\u04ad\u04af\u04b1\u04bc\u04c0\u04c2\u04c5\u04d9\u04dc\u04df\u04e1\u04e5"+
		"\u04f1\u04f5\u0502\u0507\u050c\u0512\u0515\u051d\u0523\u052c\u053d\u0543"+
		"\u054b\u0557\u055e\u056b\u0573\u057f\u0583\u058f\u0593\u0597\u05ab\u05b3"+
		"\u05bc\u05da\u05e6\u05f6\u060c\u0613\u061a\u0621\u0628\u062f\u0636\u063d"+
		"\u0643\u0647\u064e\u0654\u0658\u065a\u065d\u0668\u066b\u066e\u067b\u067f"+
		"\u0687\u068d\u0699\u06a0\u06a7\u06ae\u06b5\u06bc\u06c3\u06c9\u06ce\u06d7"+
		"\u06de\u06e5\u06f0\u06f4\u06fa\u06fd\u0701\u0707\u070b\u070f\u0712\u071c"+
		"\u0735";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}
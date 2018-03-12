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

/*
T-SQL (Transact-SQL, MSSQL) grammar.
The MIT License (MIT).
Copyright (c) 2015-2016, Ivan Kochurkin (kvanttt@gmail.com), Positive Technologies.
Copyright (c) 2016, Scott Ure (scott@redstormsoftware.com).
Copyright (c) 2016, Rui Zhang (ruizhang.ccs@gmail.com).
Copyright (c) 2016, Marcus Henriksson (kuseman80@gmail.com).

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

grammar VerdictSQL;

// VERDICT

verdict_statement
    : select_statement
    | create_sample_statement
    | delete_sample_statement
    | show_samples_statement
    | config_statement
    | other_statement
    | create_table
    | create_table_as_select
    | create_view
    | drop_table
    | drop_view
    ;

//WITH SIZE size=(FLOAT | DECIMAL) '%' (STORE poission_cols=DECIMAL POISSON COLUMNS)? (STRATIFIED BY column_name (',' column_name)*)?
create_sample_statement
    : CREATE (size=(FLOAT | DECIMAL) '%')? (sample_type)? SAMPLE (FROM | OF) original_table=table_name (on_columns)?
    ;
    
sample_type
    : UNIFORM
    | UNIVERSE
    | STRATIFIED
    | RECOMMENDED
    ;
    
on_columns
    : ON column_name (',' column_name)*
    ;

delete_sample_statement
    : (DELETE | DROP) (size=(FLOAT | DECIMAL) '%')? (sample_type)? SAMPLES OF original_table=table_name (on_columns)?
    ;

show_samples_statement
    : SHOW type=(STRATIFIED | UNIFORM | ALL)? SAMPLES ((FOR | OF) database=id)?
    ;

config_statement
    : config_set_statement
    | config_get_statement
    ;

other_statement
    : use_statement
    | show_tables_statement
    | show_databases_statement
    | describe_table_statement
    | refresh_statement
    | show_config_statement
    | create_database
    | drop_database
    ;
    
create_database
    : CREATE ( DATABASE | SCHEMA ) (database=id)
    ;
    
drop_database
    : DROP ( DATABASE | SCHEMA ) ( IF EXISTS )? (database=id)
    ;

//config_set_statement: SET key=config_key '=' value=config_value percent='%'?;
config_set_statement: SET key=config_key '=' value=config_value;

config_get_statement: GET key=config_key;

config_key: ID('.' (ID | TRIALS))*;

//config_value
//    : ALL
//    | UNIFORM
//    | STRATIFIED
//    | ON
//    | OFF
//    | ID (',' ID)*
//    | STRING
//    | DOUBLE_QUOTE_STRING
//    | DECIMAL
//    | FLOAT
//    ;

config_value
    : DOUBLE_QUOTE_ID
    | STRING
    | ID (',' ID)*
    ;

SIZE: S I Z E;
//STORE: S T O R E;
POISSON: P O I S S O N;
COLUMNS: C O L U M N S;
STRATIFIED: S T R A T I F I E D;
SHOW: S H O W;
UNIFORM: U N I F O R M;
SAMPLES: S A M P L E S;
GET: G E T;
CONFIDENCE: C O N F I D E N C E;
TRIALS: T R I A L S;


//DOUBLE_QUOTE_STRING: '"' (~'"' | '\\"')* '"';

confidence_clause: CONFIDENCE confidence=(FLOAT | DECIMAL) percent='%'?;

trials_clause: TRIALS trials=number;

table_name_with_sample: table_name SAMPLE size=(FLOAT | DECIMAL) percent='%'? ;

// VERDICT

tsql_file
    : sql_clause* EOF
    ;

sql_clause
    : ddl_clause
    | other_statement
    ;

// Data Definition Language: https://msdn.microsoft.com/en-us/library/ff848799.aspx)
ddl_clause
    : create_table
    | create_view
    | alter_table
    | alter_database
    | drop_table
    | drop_view
    ;

// DML

// https://msdn.microsoft.com/en-us/library/ms189499.aspx
select_statement
    : with_expression? EXACT? query_expression order_by_clause? limit_clause? confidence_clause? ';'?
    ;

// https://msdn.microsoft.com/en-us/library/ms177564.aspx
output_clause
    : OUTPUT output_dml_list_elem (',' output_dml_list_elem)*
            (INTO (LOCAL_ID | table_name) ('(' column_name_list ')')? )?
    ;

output_dml_list_elem
    : (output_column_name | expression) (AS? column_alias)?  // TODO: scalar_expression
    ;

output_column_name
    : (DELETED | INSERTED | table_name) '.' ('*' | column_name)
    | DOLLAR_ACTION
    ;

// DDL

// https://msdn.microsoft.com/en-us/library/ms174979.aspx
create_table
    : CREATE TABLE table_name '(' column_def_table_constraint (','? column_def_table_constraint)* ','? ')'
    ;

create_table_as_select
    : CREATE TABLE (IF NOT EXISTS)? table_name STORED_AS_PARQUET? AS select_statement ';'?
    ;

// https://msdn.microsoft.com/en-us/library/ms187956.aspx
create_view
    : CREATE VIEW view_name ('(' column_name (',' column_name)* ')')?
      AS select_statement (WITH CHECK OPTION)? ';'?
    ;

// https://msdn.microsoft.com/en-us/library/ms190273.aspx
alter_table
    : ALTER TABLE table_name SET '(' LOCK_ESCALATION '=' (AUTO | TABLE | DISABLE) ')' ';'?
    | ALTER TABLE table_name ADD column_def_table_constraint ';'?
    ;

// https://msdn.microsoft.com/en-us/library/ms174269.aspx
alter_database
    : ALTER DATABASE (database=id | CURRENT)
      MODIFY NAME '=' new_name=id ';'?
    ;

// https://msdn.microsoft.com/en-us/library/ms173790.aspx
drop_table
    : DROP TABLE (IF EXISTS)? table_name ';'?
    ;

// https://msdn.microsoft.com/en-us/library/ms173492.aspx
drop_view
    : DROP VIEW (IF EXISTS)? view_name (',' view_name)* ';'?
    ;

// Other statements.

// https://msdn.microsoft.com/en-us/library/ms189484.aspx
set_statment
    : SET LOCAL_ID ('.' member_name=id)? '=' expression ';'?
    | SET LOCAL_ID assignment_operator expression ';'?
    // https://msdn.microsoft.com/en-us/library/ms189837.aspx
    | set_special
    ;

// https://msdn.microsoft.com/en-us/library/ms188366.aspx
use_statement
    : USE database=id ';'?
    ;

show_tables_statement
    : SHOW TABLES (IN schema=id)? ';'?
    ;

show_databases_statement
    : SHOW ( DATABASES | SCHEMAS )
    ;

describe_table_statement
    : DESCRIBE table=table_name ';'?
    ;
    
refresh_statement
    : REFRESH (schema=id)? ';'?
    ;
    
show_config_statement
    : SHOW CONFIG ';'?
    ;

table_type_definition
    : TABLE '(' column_def_table_constraint (','? column_def_table_constraint)* ')'
    ;

column_def_table_constraint
    : column_definition
    | table_constraint
    ;

// https://msdn.microsoft.com/en-us/library/ms187742.aspx
column_definition
    : column_name (data_type | AS expression) null_notnull?
    ;

// https://msdn.microsoft.com/en-us/library/ms186712.aspx
column_constraint
    :(CONSTRAINT id)? null_notnull?
    ;

// https://msdn.microsoft.com/en-us/library/ms188066.aspx
table_constraint
    : (CONSTRAINT id)? '(' column_name_list ')'
    ;

// https://msdn.microsoft.com/en-us/library/ms190356.aspx
// Runtime check.
set_special
    : SET id (id | constant | LOCAL_ID | on_off) ';'?
    // https://msdn.microsoft.com/en-us/library/ms173763.aspx
    | SET TRANSACTION ISOLATION LEVEL
      (READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SNAPSHOT | SERIALIZABLE) ';'?
    // https://msdn.microsoft.com/en-us/library/ms188059.aspx
    | SET IDENTITY_INSERT table_name on_off ';'?
    ;

// Expression.

// https://msdn.microsoft.com/en-us/library/ms190286.aspx
// Operator precendence: https://msdn.microsoft.com/en-us/library/ms190276.aspx
expression
    : NULL                                                     #primitive_expression
    | LOCAL_ID                                                 #primitive_expression
    | constant                                                 #primitive_expression
    | true_orfalse                                             #primitive_expression
    | function_call                                            #function_call_expression
    | expression COLLATE id                                    #function_call_expression
    | case_expr                                                #case_expression
    | full_column_name                                         #column_ref_expression
    | '(' expression ')'                                       #bracket_expression
    | '(' subquery ')'                                         #subquery_expression
    | '~' expression                                           #unary_operator_expression
    | expression op=('*' | '/' | '%') expression               #binary_operator_expression
    | op=('+' | '-') expression                                #unary_operator_expression
    | expression op=('+' | '-' | '&' | '^' | '|') expression   #binary_operator_expression
    | expression comparison_operator expression                #binary_operator_expression
    ;

constant_expression
    : NULL
    | constant
    // system functions: https://msdn.microsoft.com/en-us/library/ms187786.aspx
    | function_call
    | LOCAL_ID         // TODO: remove.
    | '(' constant_expression ')'
    ;

subquery
    : select_statement
    ;

dml_table_source
    : query_specification
    ;

// https://msdn.microsoft.com/en-us/library/ms175972.aspx
with_expression
    : WITH (XMLNAMESPACES ',')? common_table_expression (',' common_table_expression)*
    ;

common_table_expression
    : expression_name=id ('(' column_name_list ')')? AS '(' select_statement ')'
    ;

update_elem
    : (full_column_name | LOCAL_ID) ('=' | assignment_operator) expression
    | udt_column_name=id '.' method_name=id '(' expression_list ')'
    //| full_column_name '.' WRITE (expression, )
    ;

// https://msdn.microsoft.com/en-us/library/ms173545.aspx
search_condition_list
    : search_condition (',' search_condition)*
    ;

search_condition
    : search_condition_or (AND search_condition_or)*
    ;

search_condition_or
    : search_condition_not (OR search_condition_not)*
    ;

search_condition_not
    : NOT? predicate
    ;

predicate
    : EXISTS '(' subquery ')'                                               # exists_predicate
    | expression comparison_operator expression                             # comp_expr_predicate
    | expression comparison_operator (ALL | SOME | ANY) '(' subquery ')'    # setcomp_expr_predicate
    | expression NOT? BETWEEN expression AND expression                     # comp_between_expr
    | expression NOT? IN '(' (subquery | expression_list) ')'               # in_predicate
    | expression NOT? LIKE expression (ESCAPE expression)?                  # like_predicate
    | expression IS null_notnull                                            # is_predicate
    | '(' search_condition ')'                                              # bracket_predicate
    ;

query_expression
    : (query_specification | '(' query_expression ')') union*
    ;

union
    : (UNION ALL? | EXCEPT | INTERSECT) (query_specification | ('(' query_expression ')')+)
    ;

// https://msdn.microsoft.com/en-us/library/ms176104.aspx
query_specification
    : SELECT (ALL | DISTINCT)? (TOP expression PERCENT? (WITH TIES)?)?
      select_list
      // https://msdn.microsoft.com/en-us/library/ms188029.aspx
      (INTO into_table=table_name)?
      (FROM table_source (',' table_source)*)?
      (WHERE where=search_condition)?
      // https://msdn.microsoft.com/en-us/library/ms177673.aspx
      (GROUP BY group_by_item (',' group_by_item)*)?
      (HAVING having=search_condition)?
    ;

limit_clause
    : LIMIT number
    ;


// https://msdn.microsoft.com/en-us/library/ms188385.aspx
order_by_clause
    : ORDER BY order_by_expression (',' order_by_expression)*
      (OFFSET expression (ROW | ROWS) (FETCH (FIRST | NEXT) expression (ROW | ROWS) ONLY)?)?
    ;

// https://msdn.microsoft.com/en-us/library/ms173812.aspx
for_clause
    : FOR BROWSE
    | FOR XML AUTO xml_common_directives?
    | FOR XML PATH ('(' STRING ')')? xml_common_directives?
    ;

xml_common_directives
    : ',' (BINARY BASE64 | TYPE | ROOT)
    ;

order_by_expression
    : expression (ASC | DESC)?
    ;

group_by_item
    : expression
    /*| rollup_spec
    | cube_spec
    | grouping_sets_spec
    | grand_total*/
    ;

option_clause
    // https://msdn.microsoft.com/en-us/library/ms181714.aspx
    : OPTION '(' option (',' option)* ')'
    ;

option
    : FAST number_rows=DECIMAL
    | (HASH | ORDER) GROUP
    | (MERGE | HASH | CONCAT) UNION
    | KEEPFIXED PLAN
    | OPTIMIZE FOR '(' optimize_for_arg (',' optimize_for_arg)* ')'
    | OPTIMIZE FOR UNKNOWN
    ;

optimize_for_arg
    : LOCAL_ID (UNKNOWN | '=' constant)
    ;

// https://msdn.microsoft.com/en-us/library/ms176104.aspx
select_list
    : select_list_elem (',' select_list_elem)*
    ;

select_list_elem
    : (table_name '.')? (STAR | '$' (IDENTITY | ROWGUID))
    | column_alias '=' expression
    | expression (AS? column_alias)?
    ;

partition_by_clause
    : PARTITION BY expression_list
    ;

// https://msdn.microsoft.com/en-us/library/ms177634.aspx
table_source
    : table_source_item_joined
    | '(' table_source_item_joined ')'
    ;

table_source_item_joined
    : table_source_item join_part*
    ;

table_source_item
    : table_name_with_sample      as_table_alias?                           # sample_table_name_item
    | table_name_with_hint        as_table_alias?                           # hinted_table_name_item
    | derived_table              (as_table_alias column_alias_list?)?       # derived_table_source_item
    ;

change_table
	: CHANGETABLE '(' CHANGES table_name ',' (NULL | DECIMAL | LOCAL_ID) ')'
	;

// https://msdn.microsoft.com/en-us/library/ms191472.aspx
join_part
    // https://msdn.microsoft.com/en-us/library/ms173815(v=sql.120).aspx
    : (INNER? |
       join_type=(LEFT | RIGHT | FULL) OUTER?) (join_hint=(LOOP | HASH | MERGE | REMOTE))?
       JOIN table_source ON search_condition
    | CROSS JOIN table_source
    | CROSS APPLY table_source
    | OUTER APPLY table_source
    | LATERAL VIEW lateral_view_function table_alias? (AS? column_alias)?
    ;

table_name_with_hint
    : table_name with_table_hints?
    ;

// https://msdn.microsoft.com/en-us/library/ms190312.aspx
rowset_function
    : OPENROWSET '(' BULK data_file=STRING ',' (bulk_option (',' bulk_option)* | id)')'
    ;

// runtime check.
bulk_option
    : id '=' (DECIMAL | STRING)
    ;

derived_table
    : '(' subquery ')'
    | table_value_constructor
    ;

function_call
    : ranking_windowed_function
    | value_manipulation_function
    | aggregate_windowed_function
    ;

datepart
    : ID
    ;

as_table_alias
    : AS? table_alias
    ;

table_alias
    : id with_table_hints?
    ;

// https://msdn.microsoft.com/en-us/library/ms187373.aspx
with_table_hints
    : WITH? '(' table_hint (',' table_hint)* ')'
    ;

// Id runtime check. Id can be (FORCESCAN, HOLDLOCK, NOLOCK, NOWAIT, PAGLOCK, READCOMMITTED,
// READCOMMITTEDLOCK, READPAST, READUNCOMMITTED, REPEATABLEREAD, ROWLOCK, TABLOCK, TABLOCKX
// UPDLOCK, XLOCK)

table_hint
    : NOEXPAND? ( INDEX '(' index_value (',' index_value)* ')'
                | INDEX '=' index_value
                | FORCESEEK ('(' index_value '(' index_column_name  (',' index_column_name)* ')' ')')?
                | SERIALIZABLE
                | SNAPSHOT
                | SPATIAL_WINDOW_MAX_CELLS '=' DECIMAL
                | ID)?
    ;

index_column_name
	: ID
	;

index_value
    : ID | DECIMAL
    ;

column_alias_list
    : '(' column_alias (',' column_alias)* ')'
    ;

column_alias
    : id
    | STRING
    ;

table_value_constructor
    : VALUES '(' expression_list ')' (',' '(' expression_list ')')*
    ;

expression_list
    : expression (',' expression)*
    ;

// https://msdn.microsoft.com/en-us/library/ms181765.aspx
case_expr
    : CASE expression (WHEN expression THEN expression)+ (ELSE expression)? END
    | CASE (WHEN search_condition THEN expression)+ (ELSE expression)? END
    ;

// https://msdn.microsoft.com/en-us/library/ms189798.aspx
ranking_windowed_function
    : RANK '(' ')' over_clause
    | DENSE_RANK '(' ')' over_clause
    | NTILE '(' expression ')' over_clause
    | ROW_NUMBER '(' ')' over_clause
    ;

value_manipulation_function
    : unary_manipulation_function
    | noparam_manipulation_function
    | binary_manipulation_function
    | ternary_manipulation_function
    | nary_manipulation_function
    | extract_time_function
    ;

nary_manipulation_function
	: function_name=(CONCAT | CONCAT_WS)
		'(' expression (',' expression)* ')'
    ;

ternary_manipulation_function
    : function_name=(CONV | SUBSTR)
      '(' expression ',' expression ',' expression ')'
    ;

binary_manipulation_function
    : function_name=(MOD | PMOD | STRTOL | POW | PERCENTILE | SPLIT | INSTR | ENCODE | DECODE | SHIFTLEFT | SHIFTRIGHT | SHIFTRIGHTUNSIGNED | NVL | FIND_IN_SET | FORMAT_NUMBER | GET_JSON_OBJECT | IN_FILE | LOCATE | REPEAT | AES_ENCRYPT | AES_DECRYPT)
      '(' expression ',' expression ')'
    ;
    
extract_time_function
    : function_name=EXTRACT
      '(' extract_unit FROM expression ')'
    ;
    
extract_unit
    : YEAR | expression
    ;

unary_manipulation_function
    : function_name=(ROUND | FLOOR | CEIL | EXP | LN | LOG10 | LOG2 | SIN | COS | TAN | SIGN | RAND | FNV_HASH
     | ABS | STDDEV | SQRT | MD5 | CRC32 | YEAR | QUARTER | MONTH | DAY | HOUR | MINUTE | SECOND | WEEKOFYEAR | LOWER | UPPER | ASCII | CHARACTER_LENGTH | FACTORIAL | CBRT | LENGTH | TRIM | ASIN | ACOS | ATAN | DEGREES | RADIANS | POSITIVE | NEGATIVE | BROUND | BIN | HEX | UNHEX | FROM_UNIXTIME | TO_DATE | CHR | LTRIM | REVERSE | SPACE_FUNCTION | SHA1 | SHA2 )
      '(' expression ')'
    | function_name=CAST '(' cast_as_expression ')'    
    ;
    
noparam_manipulation_function
    : function_name=(UNIX_TIMESTAMP | CURRENT_TIMESTAMP | RANDOM | NATURAL_CONSTANT | PI)
      '(' ')'
    ;
    
lateral_view_function
    : function_name=EXPLODE
      '(' expression ')'
    ;


// https://msdn.microsoft.com/en-us/library/ms173454.aspx
aggregate_windowed_function
    : AVG '(' all_distinct_expression ')' over_clause?
    | CHECKSUM_AGG '(' all_distinct_expression ')'
    | GROUPING '(' expression ')'
    | GROUPING_ID '(' expression_list ')'
    | MAX '(' all_distinct_expression ')' over_clause?
    | MIN '(' all_distinct_expression ')' over_clause?
    | SUM '(' all_distinct_expression ')' over_clause?
    | STDEV '(' all_distinct_expression ')' over_clause?
    | STDEVP '(' all_distinct_expression ')' over_clause?
    | STDDEV_SAMP '(' all_distinct_expression ')' over_clause?
    | VAR '(' all_distinct_expression ')' over_clause?
    | VARP '(' all_distinct_expression ')' over_clause?
    | COUNT '(' ('*' | all_distinct_expression) ')' over_clause?
    | NDV '(' all_distinct_expression ')' over_clause?
    | COUNT_BIG '(' ('*' | all_distinct_expression) ')' over_clause?
    ;

all_distinct_expression
    : (ALL | DISTINCT)? expression
    ;
    
cast_as_expression
    : expression AS data_type
    ;

// https://msdn.microsoft.com/en-us/library/ms189461.aspx
over_clause
    : OVER '(' partition_by_clause? order_by_clause? row_or_range_clause? ')'
    ;

row_or_range_clause
    : (ROWS | RANGE) window_frame_extent
    ;

window_frame_extent
    : window_frame_preceding
    | BETWEEN window_frame_bound AND window_frame_bound
    ;

window_frame_bound
    : window_frame_preceding
    | window_frame_following
    ;

window_frame_preceding
    : UNBOUNDED PRECEDING
    | DECIMAL PRECEDING
    | CURRENT ROW
    ;

window_frame_following
    : UNBOUNDED FOLLOWING
    | DECIMAL FOLLOWING
    ;

// Primitive.

full_table_name
    : (server=id '.' database=id '.'  schema=id   '.'
      |              database=id '.' (schema=id)? '.'
      |                               schema=id   '.')? table=id
    ;

table_name
    : (database=id '.' (schema=id)? '.' | schema=id '.')? table=id
    ;

view_name
    : (schema=id '.')? view=id
    ;

func_proc_name
    : (database=id '.' (schema=id)? '.' | (schema=id) '.')? procedure=id
    ;

ddl_object
    : full_table_name
    | LOCAL_ID
    ;

full_column_name
    : (table_name '.')? column_name
    ;

column_name_list
    : column_name (',' column_name)*
    ;

column_name
    : id
    ;

cursor_name
    : id
    | LOCAL_ID
    ;

on_off
    : ON
    | OFF
    ;

clustered
    : CLUSTERED
    | NONCLUSTERED
    ;

null_notnull
    : NOT? NULL
    ;
    
true_orfalse
    : TRUE | FALSE
    ;

scalar_function_name
    : func_proc_name
    | RIGHT
    | LEFT
    | BINARY_CHECKSUM
    | CHECKSUM
    ;

// https://msdn.microsoft.com/en-us/library/ms187752.aspx
// TODO: implement runtime check or add new tokens.
data_type
/*    : BIGINT
    | BINARY '(' DECIMAL ')'
    | BIT
    | CHAR '(' DECIMAL ')'
    | DATE
    | DATETIME
    | DATETIME2
    | DATETIMEOFFSET '(' DECIMAL ')'
    | DECIMAL '(' DECIMAL ',' DECIMAL ')'
    | DOUBLE
    | FLOAT
    | GEOGRAPHY
    | GEOMETRY
    | HIERARCHYID
    | IMAGE
    | INT
    | MONEY
    | NCHAR '(' DECIMAL ')'
    | NTEXT
    | NUMERIC '(' DECIMAL ',' DECIMAL ')'
    | NVARCHAR '(' DECIMAL | MAX ')'
    | REAL
    | SMALLDATETIME
    | SMALLINT
    | SMALLMONEY
    | SQL_VARIANT
    | TEXT
    | TIME '(' DECIMAL ')'
    | TIMESTAMP
    | TINYINT
    | UNIQUEIDENTIFIER
    | VARBINARY '(' DECIMAL | MAX ')'
    | VARCHAR '(' DECIMAL | MAX ')'
    | XML */
    : id IDENTITY? ('(' (DECIMAL | MAX) (',' DECIMAL)? ')')?
    ;

default_value
    : NULL
    | constant
    ;

// https://msdn.microsoft.com/en-us/library/ms179899.aspx
constant
    : STRING // string, datetime or uniqueidentifier
    | BINARY
    | number
    | sign? (REAL | FLOAT)  // float or decimal
    | sign? '$' (DECIMAL | FLOAT)       // money
    ;

number
    : sign? DECIMAL
    ;

sign
    : PLUS
    | MINUS
    ;

// https://msdn.microsoft.com/en-us/library/ms175874.aspx
id
    : simple_id
    | DOUBLE_QUOTE_ID
    | SQUARE_BRACKET_ID
    | BACKTICK_ID
    ;

simple_id
    : ID
    | ABSOLUTE
    | APPLY
    | AUTO
    | AVG
    | BASE64
    | CALLER
    | CAST
    | CATCH
    | CHECKSUM_AGG
    | COMMITTED
    | CONCAT
    | CONCAT_WS
    | COOKIE
    | COUNT
    | COUNT_BIG
    | DELAY
    | DELETED
    | DENSE_RANK
    | DISABLE
    | DYNAMIC
    | ENCRYPTION
    | EXTRACT
    | FAST
    | FAST_FORWARD
    | FIRST
    | FOLLOWING
    | FORCESEEK
    | FORWARD_ONLY
    | FULLSCAN
    | GLOBAL
    | GO
    | GROUPING
    | GROUPING_ID
    | HASH
    | INSENSITIVE
    | INSERTED
    | ISOLATION
    | KEYSET
    | KEEPFIXED
    | LAST
    | LEVEL
    | LOCAL
    | LOCK_ESCALATION
    | LOGIN
    | LOOP
    | MARK
    | MAX
    | MIN
    | MODIFY
    | NAME
    | NEXT
    | NOCOUNT
    | NOEXPAND
    | NORECOMPUTE
    | NTILE
    | NUMBER
    | OFFSET
    | ONLY
    | OPTIMISTIC
    | OPTIMIZE
    | OUT
    | OUTPUT
    | OWNER
    | PARTITION
    | PATH
    | PRECEDING
    | PRIOR
    | RANGE
    | RANK
    | READONLY
    | READ_ONLY
    | RECOMPILE
    | RELATIVE
    | REMOTE
    | REPEATABLE
    | ROOT
    | ROW
    | ROWGUID
    | ROWS
    | ROW_NUMBER
    | SAMPLE
    | SCHEMABINDING
    | SCROLL
    | SCROLL_LOCKS
    | SELF
    | SERIALIZABLE
    | SNAPSHOT
    | SPATIAL_WINDOW_MAX_CELLS
    | STATIC
    | STATS_STREAM
    | STDEV
    | STDEVP
    | STDDEV_SAMP
    | STRTOL
    | SUM
    | THROW
    | TIES
    | TIME
    | TRY
    | TYPE
    | TYPE_WARNING
    | UNBOUNDED
    | UNCOMMITTED
    | UNKNOWN
    | USING
    | VAR
    | VARP
    | VIEW_METADATA
    | WORK
    | XML
    | XMLNAMESPACES
    ;

// https://msdn.microsoft.com/en-us/library/ms188074.aspx
// Spaces are allowed for comparison operators.
comparison_operator
    : '=' | '>' | '<' | '<=' | '>=' | '<>' | '!=' | '!>' | '!<' | '<' '=' | '>' '=' | '<' '>' | '!' '=' | '!' '>' | '!' '<'
    ;

assignment_operator
    : '+=' | '-=' | '*=' | '/=' | '%=' | '&=' | '^=' | '|='
    ;

// Lexer

// Basic keywords (from https://msdn.microsoft.com/en-us/library/ms189822.aspx)
ADD:                             A D D;
ALL:                             A L L;
ALTER:                           A L T E R;
AND:                             A N D;
ANY:                             A N Y;
AS:                              A S;
ASC:                             A S C;
ASCII:                           A S C I I;
AUTHORIZATION:                   A U T H O R I Z A T I O N;
BACKUP:                          B A C K U P;
BEGIN:                           B E G I N;
BETWEEN:                         B E T W E E N;
BREAK:                           B R E A K;
BROWSE:                          B R O W S E;
BULK:                            B U L K;
BY:                              B Y;
CASCADE:                         C A S C A D E;
CASE:                            C A S E;
CHANGETABLE:                     C H A N G E T A B L E;
CHANGES:                         C H A N G E S; 
CHECK:                           C H E C K;
CHECKPOINT:                      C H E C K P O I N T;
CLOSE:                           C L O S E;
CLUSTERED:                       C L U S T E R E D;
COALESCE:                        C O A L E S C E;
COLLATE:                         C O L L A T E;
COLUMN:                          C O L U M N;
COMMIT:                          C O M M I T;
COMPUTE:                         C O M P U T E;
CONSTRAINT:                      C O N S T R A I N T;
CONTAINS:                        C O N T A I N S;
CONTAINSTABLE:                   C O N T A I N S T A B L E;
CONTINUE:                        C O N T I N U E;
CONV:                            C O N V;
CONVERT:                         C O N V E R T;
CREATE:                          C R E A T E;
CROSS:                           C R O S S;
CURRENT:                         C U R R E N T;
CURRENT_DATE:                    C U R R E N T '_' D A T E;
CURRENT_TIME:                    C U R R E N T '_' T I M E;
CURRENT_TIMESTAMP:               C U R R E N T '_' T I M E S T A M P;
CURRENT_USER:                    C U R R E N T '_' U S E R;
CURSOR:                          C U R S O R;
DATABASE:                        D A T A B A S E;
DATABASES:                       D A T A B A S E S;
DBCC:                            D B C C;
DEALLOCATE:                      D E A L L O C A T E;
DECLARE:                         D E C L A R E;
DELETE:                          D E L E T E;
DENY:                            D E N Y;
DESC:                            D E S C;
DESCRIBE:                        D E S C R I B E;
DISK:                            D I S K;
DISTINCT:                        D I S T I N C T;
DISTRIBUTED:                     D I S T R I B U T E D;
DOUBLE:                          D O U B L E;
DROP:                            D R O P;
DUMP:                            D U M P;
ELSE:                            E L S E;
END:                             E N D;
ERRLVL:                          E R R L V L;
ESCAPE:                          E S C A P E;
EXCEPT:                          E X C E P T;
EXEC:                            E X E C;
EXECUTE:                         E X E C U T E;
EXISTS:                          E X I S T S;
EXIT:                            E X I T;
EXTERNAL:                        E X T E R N A L;
FALSE:                           F A L S E;
FETCH:                           F E T C H;
FILE:                            F I L E;
FILLFACTOR:                      F I L L F A C T O R;
FOR:                             F O R;
FORCESEEK:                       F O R C E S E E K;
FOREIGN:                         F O R E I G N;
FREETEXT:                        F R E E T E X T;
FREETEXTTABLE:                   F R E E T E X T T A B L E;
FROM:                            F R O M;
FULL:                            F U L L;
FUNCTION:                        F U N C T I O N;
GOTO:                            G O T O;
GRANT:                           G R A N T;
GROUP:                           G R O U P;
HAVING:                          H A V I N G;
IDENTITY:                        I D E N T I T Y;
IDENTITYCOL:                     I D E N T I T Y C O L;
IDENTITY_INSERT:                 I D E N T I T Y '_' I N S E R T;
IF:                              I F;
IN:                              I N;
INDEX:                           I N D E X;
INNER:                           I N N E R;
INSERT:                          I N S E R T;
INTERSECT:                       I N T E R S E C T;
INTO:                            I N T O;
IS:                              I S;
JOIN:                            J O I N;
KEY:                             K E Y;
KILL:                            K I L L;
LEFT:                            L E F T;
LIKE:                            L I K E;
LIMIT:                           L I M I T;
LINENO:                          L I N E N O;
LOAD:                            L O A D;
MERGE:                           M E R G E;
NATIONAL:                        N A T I O N A L;
NOCHECK:                         N O C H E C K;
NONCLUSTERED:                    N O N C L U S T E R E D;
NOT:                             N O T;
NULL:                            N U L L;
NULLIF:                          N U L L I F;
OF:                              O F;
OFF:                             O F F;
OFFSETS:                         O F F S E T S;
ON:                              O N;
OPEN:                            O P E N;
OPENDATASOURCE:                  O P E N D A T A S O U R C E;
OPENQUERY:                       O P E N Q U E R Y;
OPENROWSET:                      O P E N R O W S E T;
OPENXML:                         O P E N X M L;
OPTION:                          O P T I O N;
OR:                              O R;
ORDER:                           O R D E R;
OUTER:                           O U T E R;
OVER:                            O V E R;
PERCENT:                         P E R C E N T;
PIVOT:                           P I V O T;
PLAN:                            P L A N;
PRECISION:                       P R E C I S I O N;
PRIMARY:                         P R I M A R Y;
PRINT:                           P R I N T;
PROC:                            P R O C;
PROCEDURE:                       P R O C E D U R E;
RAISERROR:                       R A I S E R R O R;
READ:                            R E A D;
READTEXT:                        R E A D T E X T;
RECONFIGURE:                     R E C O N F I G U R E;
REFERENCES:                      R E F E R E N C E S;
REPLICATION:                     R E P L I C A T I O N;
RESTORE:                         R E S T O R E;
RESTRICT:                        R E S T R I C T;
RETURN:                          R E T U R N;
REVERT:                          R E V E R T;
REVOKE:                          R E V O K E;
RIGHT:                           R I G H T;
ROLLBACK:                        R O L L B A C K;
ROWCOUNT:                        R O W C O U N T;
ROWGUIDCOL:                      R O W G U I D C O L;
RULE:                            R U L E;
SAVE:                            S A V E;
SCHEMA:                          S C H E M A;
SCHEMAS:                         S C H E M A S;
SECURITYAUDIT:                   S E C U R I T Y A U D I T;
SELECT:                          S E L E C T;
SEMANTICKEYPHRASETABLE:          S E M A N T I C K E Y P H R A S E T A B L E;
SEMANTICSIMILARITYDETAILSTABLE:  S E M A N T I C S I M I L A R I T Y D E T A I L S T A B L E;
SEMANTICSIMILARITYTABLE:         S E M A N T I C S I M I L A R I T Y T A B L E;
SESSION_USER:                    S E S S I O N '_' U S E R;
SET:                             S E T;
SETUSER:                         S E T U S E R;
SHUTDOWN:                        S H U T D O W N;
SOME:                            S O M E;
SUBSTR:                          S U B S T R;
STATISTICS:                      S T A T I S T I C S;
SYSTEM_USER:                     S Y S T E M '_' U S E R;
TABLE:                           T A B L E;
TABLES:                          T A B L E S;
TABLESAMPLE:                     T A B L E S A M P L E;
TEXTSIZE:                        T E X T S I Z E;
THEN:                            T H E N;
TO:                              T O;
TOP:                             T O P;
TRAN:                            T R A N;
TRANSACTION:                     T R A N S A C T I O N;
TRIGGER:                         T R I G G E R;
TRUE:                            T R U E;
TRUNCATE:                        T R U N C A T E;
TRY_CONVERT:                     T R Y '_' C O N V E R T;
TSEQUAL:                         T S E Q U A L;
UNION:                           U N I O N;
UNIQUE:                          U N I Q U E;
UNPIVOT:                         U N P I V O T;
UPDATE:                          U P D A T E;
UPDATETEXT:                      U P D A T E T E X T;
USE:                             U S E;
USER:                            U S E R;
VALUES:                          V A L U E S;
VARYING:                         V A R Y I N G;
VIEW:                            V I E W;
WAITFOR:                         W A I T F O R;
WHEN:                            W H E N;
WHERE:                           W H E R E;
WHILE:                           W H I L E;
WITH:                            W I T H;
WITHIN:                          W I T H I N;
WRITETEXT:                       W R I T E T E X T;

// Additional keywords (they can be id).
ABSOLUTE:                        A B S O L U T E;
ABS:                             A B S;
ACOS:                            A C O S;
AES_DECRYPT:                     A E S '_' D E C R Y P T;
AES_ENCRYPT:                     A E S '_' E N C R Y P T;
APPLY:                           A P P L Y;
ASIN:                            A S I N;
ATAN:                            A T A N;
AUTO:                            A U T O;
AVG:                             A V G;
BASE64:                          B A S E '64';
BIN:                             B I N;
BINARY_CHECKSUM:                 B I N A R Y '_' C H E C K S U M;
BROUND:                          B R O U N D;
CALLER:                          C A L L E R;
CAST:                            C A S T;
CATCH:                           C A T C H;
CBRT:                            C B R T;
CEIL:                            C E I L;
CHARACTER_LENGTH:                C H A R A C T E R '_' L E N G T H;
CHECKSUM:                        C H E C K S U M;
CHECKSUM_AGG:                    C H E C K S U M '_' A G G;
CHR:                             C H R;
COMMITTED:                       C O M M I T T E D;
CONCAT:                          C O N C A T;
CONCAT_WS:                       C O N C A T '_' W S;
CONFIG:                          C O N F I G;
COOKIE:                          C O O K I E;
COS:                             C O S;
COUNT:                           C O U N T;
COUNT_BIG:                       C O U N T '_' B I G;
CRC32:                           C R C '32';
DATEADD:                         D A T E A D D;
DATEDIFF:                        D A T E D I F F;
DATENAME:                        D A T E N A M E;
DATEPART:                        D A T E P A R T;
DAY:                             D A Y;
DECODE:                          D E C O D E;
DEGREES:                         D E G R E E S;
DELAY:                           D E L A Y;
DELETED:                         D E L E T E D;
DENSE_RANK:                      D E N S E '_' R A N K;
DISABLE:                         D I S A B L E;
DYNAMIC:                         D Y N A M I C;
NATURAL_CONSTANT:                E;
ENCODE:                          E N C O D E;
ENCRYPTION:                      E N C R Y P T I O N;
ESCAPED_BY:                      E S C A P E D ' ' B Y;
EXACT:                           E X A C T;
EXP:                             E X P;
EXPLODE:                         E X P L O D E;
EXTRACT:                         E X T R A C T;
FACTORIAL:                       F A C T O R I A L;
FAST:                            F A S T;
FAST_FORWARD:                    F A S T '_' F O R W A R D;
FIELDS_SEPARATED_BY:             F I E L D S ' ' S E P A R A T E D ' ' B Y;
FIND_IN_SET:                     F I N D '_' I N '_' S E T;
FIRST:                           F I R S T;
FLOOR:                           F L O O R;
FOLLOWING:                       F O L L O W I N G;
FORMAT_NUMBER:                   F O R M A T '_' N U M B E R;
FORWARD_ONLY:                    F O R W A R D '_' O N L Y;
FNV_HASH:                        F N V '_' H A S H;
FROM_UNIXTIME:                   F R O M '_' U N I X T I M E;
FULLSCAN:                        F U L L S C A N;
GET_JSON_OBJECT:                 G E T '_' J S O N '_' O B J E C T;
GLOBAL:                          G L O B A L;
GO:                              G O;
GROUPING:                        G R O U P I N G;
GROUPING_ID:                     G R O U P I N G '_' I D;
HASH:                            H A S H;
HEX:                             H E X;
HOUR:                            H O U R;
INSENSITIVE:                     I N S E N S I T I V E;
INSERTED:                        I N S E R T E D;
INSTR:                           I N S T R;
IN_FILE:                         I N '_' F I L E;
ISOLATION:                       I S O L A T I O N;
KEEPFIXED:                       K E E P F I X E D;
KEYSET:                          K E Y S E T;
LAST:                            L A S T;
LATERAL:                         L A T E R A L;
LENGTH:                          L E N G T H;
LEVEL:                           L E V E L;
LN:                              L N;
LOCAL:                           L O C A L;
LOCATE:                          L O C A T E;
LOCATION:                        L O C A T I O N;
LOCK_ESCALATION:                 L O C K '_' E S C A L A T I O N;
LOG2:                            L O G '2';
LOG10:                           L O G '10';
LOGIN:                           L O G I N;
LOOP:                            L O O P;
LOWER:                           L O W E R;
LTRIM:                           L T R I M;
MARK:                            M A R K;
MAX:                             M A X;
MD5:                             M D '5';
MIN:                             M I N;
MIN_ACTIVE_ROWVERSION:           M I N '_' A C T I V E '_' R O W V E R S I O N;
MINUTE:                          M I N U T E;
MOD:                             M O D;
MODIFY:                          M O D I F Y;
MONTH:                           M O N T H;
NEGATIVE:                        N E G A T I V E;
NEXT:                            N E X T;
NAME:                            N A M E;
NDV:                             N D V;
NOCOUNT:                         N O C O U N T;
NOEXPAND:                        N O E X P A N D;
NORECOMPUTE:                     N O R E C O M P U T E;
NTILE:                           N T I L E;
NUMBER:                          N U M B E R;
NVL:                             N V L;
OFFSET:                          O F F S E T;
ONLY:                            O N L Y;
OPTIMISTIC:                      O P T I M I S T I C;
OPTIMIZE:                        O P T I M I Z E;
OUT:                             O U T;
OUTPUT:                          O U T P U T;
OWNER:                           O W N E R;
PARTITION:                       P A R T I T I O N;
PATH:                            P A T H;
PERCENTILE:                      P E R C E N T I L E;
PI:                              P I;
PMOD:                            P M O D;
POSITIVE:                        P O S I T I V E;
POW:                             P O W;
PRECEDING:                       P R E C E D I N G;
PRIOR:                           P R I O R;
QUARTER:                         Q U A R T E R;
QUOTED_BY:                       Q U O T E D ' ' B Y;
RADIANS:                         R A D I A N S;
RAND:                            R A N D;
RANDOM:                          R A N D O M;
RANGE:                           R A N G E;
RANK:                            R A N K;
READONLY:                        R E A D O N L Y;
READ_ONLY:                       R E A D '_' O N L Y;
RECOMMENDED:                     R E C O M M E N D E D;
RECOMPILE:                       R E C O M P I L E;
REFRESH:                         R E F R E S H;
RELATIVE:                        R E L A T I V E;
REMOTE:                          R E M O T E;
REPEAT:                          R E P E A T;
REPEATABLE:                      R E P E A T A B L E;
REVERSE:                         R E V E R S E;
ROOT:                            R O O T;
ROUND:                           R O U N D;
ROW:                             R O W;
ROWGUID:                         R O W G U I D;
ROWS:                            R O W S;
ROW_NUMBER:                      R O W '_' N U M B E R;
SAMPLE:                          S A M P L E;
SCHEMABINDING:                   S C H E M A B I N D I N G;
SCROLL:                          S C R O L L;
SCROLL_LOCKS:                    S C R O L L '_' L O C K S;
SECOND:                          S E C O N D;
SELF:                            S E L F;
SERIALIZABLE:                    S E R I A L I Z A B L E;
SHA1:                            S H A '1';
SHA2:                            S H A '2';
SHIFTLEFT:                       S H I F T L E F T;
SHIFTRIGHT:                      S H I F T R I G H T;
SHIFTRIGHTUNSIGNED:              S H I F T R I G H T U N S I G N E D;
SIGN:                            S I G N;
SIN:                             S I N;
SNAPSHOT:                        S N A P S H O T;
SPACE_FUNCTION:                  S P A C E;
SPATIAL_WINDOW_MAX_CELLS:        S P A T I A L '_' W I N D O W '_' M A X '_' C E L L S;
SPLIT:                           S P L I T;
STATIC:                          S T A T I C;
STATS_STREAM:                    S T A T S '_' S T R E A M;
STDEV:                           S T D E V;
STDDEV:                          S T D D E V;
STDEVP:                          S T D E V P;
STDDEV_SAMP:                     S T D D E V '_' S A M P;
STORED_AS_PARQUET:               S T O R E D ' ' A S ' ' P A R Q U E T;
SUM:                             S U M;
SQRT:                            S Q R T;
STRTOL:                          S T R T O L;
TAN:                             T A N;
THROW:                           T H R O W;
TIES:                            T I E S;
TIME:                            T I M E;
TO_DATE:                         T O '_' D A T E;
TRIM:                            T R I M;
TRY:                             T R Y;
TYPE:                            T Y P E;
TYPE_WARNING:                    T Y P E '_' W A R N I N G;
UNBOUNDED:                       U N B O U N D E D;
UNCOMMITTED:                     U N C O M M I T T E D;
UNHEX:                           U N H E X;
UNIVERSE:                        U N I V E R S E;
UNIX_TIMESTAMP:                  U N I X '_' T I M E S T A M P;
UNKNOWN:                         U N K N O W N;
UPPER:                           U P P E R;
USING:                           U S I N G;
VAR:                             V A R;
VARP:                            V A R P;
VIEW_METADATA:                   V I E W '_' M E T A D A T A;
WEEKOFYEAR:                      W E E K O F Y E A R;
WORK:                            W O R K;
XML:                             X M L;
XMLNAMESPACES:                   X M L N A M E S P A C E S;
YEAR:                            Y E A R;

DOLLAR_ACTION:                   '$' A C T I O N;

SPACE:              [ \t\r\n]+    -> channel(1);
COMMENT:            '/*' .*? '*/' -> channel(HIDDEN);
LINE_COMMENT:       '--' ~[\r\n]* -> channel(HIDDEN);

// TODO: ID can be not only Latin.
DOUBLE_QUOTE_ID:    '"' ~'"'+ '"';
BACKTICK_ID:        '`' ~'`'+ '`';
SQUARE_BRACKET_ID:  '[' ~']'+ ']';
LOCAL_ID:           '@' [a-zA-Z_$@#0-9]+;
DECIMAL:             DEC_DIGIT+;
ID:                  [a-zA-Z_#][a-zA-Z_#$@0-9]*;
STRING:              N? '\'' (~'\'' | '\'\'')* '\'';
BINARY:              '0' X HEX_DIGIT*;
FLOAT:               DEC_DOT_DEC;
REAL:                DEC_DOT_DEC (E [+-]? DEC_DIGIT+)?;

EQUAL:               '=';

GREATER:             '>';
LESS:                '<';
EXCLAMATION:         '!';

PLUS_ASSIGN:         '+=';
MINUS_ASSIGN:        '-=';
MULT_ASSIGN:         '*=';
DIV_ASSIGN:          '/=';
MOD_ASSIGN:          '%=';
AND_ASSIGN:          '&=';
XOR_ASSIGN:          '^=';
OR_ASSIGN:           '|=';

DOT:                 '.';
UNDERLINE:           '_';
AT:                  '@';
SHARP:               '#';
DOLLAR:              '$';
LR_BRACKET:          '(';
RR_BRACKET:          ')';
COMMA:               ',';
SEMI:                ';';
COLON:               ':';
STAR:                '*';
DIVIDE:              '/';
MODULE:              '%';
PLUS:                '+';
MINUS:               '-';
BIT_NOT:             '~';
BIT_OR:              '|';
BIT_AND:             '&';
BIT_XOR:             '^';

fragment LETTER:       [a-zA-Z_];
fragment DEC_DOT_DEC:  (DEC_DIGIT+ '.' DEC_DIGIT+ |  DEC_DIGIT+ '.' | '.' DEC_DIGIT+);
fragment HEX_DIGIT:    [0-9A-Fa-f];
fragment DEC_DIGIT:    [0-9];

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];

.. _features:

****************************************************
Supported Queries
****************************************************

Verdict supports a subset of standard SQL statements, which we overview below. Our team is currently
extending the types of queries Verdict can support. Please email our development team if there are
important features Verdict misses.


Select Statement
====================================================

Verdict supports select statements including (but not limited to) arbitrary selection predicates in
the :code:`where` clause, a :code:`group by` clause, a :code:`having` clause, multiple tables in the
:code:`from` clause, and subqueries (i.e., nested select statements)
in the :code:`from` clause and the :code:`where` clause.

Verdict supports the following aggregate functions currently:

.. rst-class:: center

    +------------------------+------------------------------+
    | Aggregate function     | Remarks                      |
    +========================+==============================+
    | AVG                    |                              |
    +------------------------+------------------------------+
    | COUNT                  |                              |
    +------------------------+------------------------------+
    | SUM                    |                              |
    +------------------------+------------------------------+



The composition of the above aggregate functions is also allowed. For example, :code:`AVG(sales) *
AVG(discount)` can be specified in the select statement.  We are currently extending Verdict to support
other aggregate functions, such as :code:`COUNT(distinct column)`, :code:`VAR`, :code:`STDEV`, etc.

The select statement may include the following mathematical functions:

.. rst-class:: center

    +------------------------+------------------------------+
    | Mathematical function  | Remarks                      |
    +========================+==============================+
    | ROUND                  |                              |
    +------------------------+------------------------------+
    | FLOOR                  |                              |
    +------------------------+------------------------------+
    | CEIL                   |                              |
    +------------------------+------------------------------+
    | EXP                    |                              |
    +------------------------+------------------------------+
    | LN                     | a natural logarithm          |
    +------------------------+------------------------------+
    | LOG10                  | log with base 10             |
    +------------------------+------------------------------+
    | LOG2                   | log with base 2              |
    +------------------------+------------------------------+
    | SIN                    |                              |
    +------------------------+------------------------------+
    | COS                    |                              |
    +------------------------+------------------------------+
    | TAN                    |                              |
    +------------------------+------------------------------+
    | SIGN                   |                              |
    +------------------------+------------------------------+


The goal of Verdict is to support most of the built-in functions supported by `Hive
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF>`_.


Sample Statement
====================================================

Verdict's sample statements create, browse, and delete sample tables. The created sample tables are
stored in the same database catalog (or called schema in some databases) as the original tables.
Therefore, the authorizations for the original tables are applied in the same way to the sample
tables (unless the authorizations were specified for individual tables). As a result, a user who
does not have access to the original table cannot read the data from the sample table as well.


Sample Creation
****************************************************

The command for creating a sample is as follows. A user can specify a sample size as a ratio to the
original table size (i.e., :code:`table_name` below). If there is a sample table already created for
the :code:`table_name`, Verdicts deletes the existing sample table, and create a new one. If the
sample size is not specified, the sample size is set to 1% by default.

.. code-block:: sql

    CREATE [xx%] SAMPLE FROM table_name;

A sample creation statement also creates two meta data tables in the same catalog as the original
table. By default, the names of the meta tables are :code:`verdict_meta_name` and
:code:`verdict_meta_size`. The names of these meta tables can be changed in the configuration file
(see :ref:`configuration`).


Show Samples
****************************************************

A show sample statement displays currently available samples in the current database catalog. To
view the samples for another database catalog, the user must first change the current database
catalog using :code:`use catalog` statement.

.. code-block:: sql

    SHOW SAMPLES;

This show sample statement displays the original table, its corresponding sample table, and the
sizes of the original tables and the sample tables.


Sample Deletion
****************************************************

A delete sample statement deletes the sample created for :code:`table_name`. The meta data are
updated accordingly.

.. code-block:: sql

    (DELETE | DROP) SAMPLE table_name;



Other DML Statement
====================================================

Verdict also accepts other standard DML statements. One important distinction is :code:`CREATE
TABLE` and :code:`CREATE VIEW` statements that include select statements. If those select statements
involve tables for which sample tables have been created, Verdict creates a new table or a new view using
the sample tables. This feature is to support complex nested queries more conveniently. If users do not
want this option, the users can turn off the feature by :code:`set bypass='true'`.


Table Statement
****************************************************

Verdict supports the standard create table statement.

.. code-block:: sql

    CREATE TABLE [If NOT EXISTS] table_name
    (create_definition, ...);


.. code-block:: sql

    CREATE TABLE [If NOT EXISTS] table_name AS
    (select_statement, ...);


View Statement
****************************************************

Verdict supports the standard create view statement.

.. code-block:: sql

    CREATE VIEW view_name AS
    (select_statement);


Delete Statement
****************************************************

Verdict supports the standard delete statement.

.. code-block:: sql

    DELETE FROM table_name
    [WHERE where_condition];




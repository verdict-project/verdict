/**
 * Core querying and scramling logics of VerdictDB.
 * <p>
 * Currently active subpackages:
 * <ol>
 * <li>connection</li>
 * <li>execplan</li>
 * <li>querying</li>
 * <li>scrambling</li>
 * <li>sqlobject</li>
 * </ol>
 * <p>
 * Notes on partitioning:
 * <ol>
 * <li>Not always true:
 * When scrambling, we first create partitioned tables; then, insert values. This is the due to the
 * restrictions of existing DBMS (e.g., Hive, Postgres) which prohibits creating a partitioned table as a
 * result of a select query. For Hive, see
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTableCreate/Drop/TruncateTable</li>
 * <li>Postgres supports dynamic data insertions into appropriate partitions: https://www.postgresql.org/docs/10/static/sql-insert.html</li>
 * <li>Hive supports dynamic data insertions into appropriate partitions when options are turned on.</li>
 * </ol>
 */

package org.verdictdb.core;